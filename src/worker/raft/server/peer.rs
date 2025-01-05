use std::error::Error;
use std::time::Duration;

use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tracing::{info, warn};

use crate::worker::raft::server::RequestVoteAsk;
use crate::worker::raft::server::rpc::TryAdvanceCommitIndexMsg;

use super::{AppendEntriesAsk, Config, PeerId, RaftMsg, RaftRole, RaftShared};

pub(super) struct Peer;

#[derive(RactorMessage)]
pub(super) enum PeerMsg {
    RunLoop,
    NotifyRoleChange(RaftShared),
    NotifyStateChange(RaftShared),
}

pub(super) struct PeerState {
    /// Global config.
    config: Config,

    /// Per-server raft state
    raft: RaftShared,

    /// Parent Raft server's id
    parent_id: PeerId,

    /// Remote server's reference
    peer: ActorRef<RaftMsg>,

    /// Whether the peer has requested for vote
    request_vote_done: bool,

    /// Index of the next log entry to send to that peer.
    ///
    /// Initialized to leader last log index + 1.
    next_index: usize,

    /// Index of the highest log entry known to be replicated on the peer.
    ///
    /// Initialized to 0, increases monotonically.
    match_index: usize,
}

pub(super) struct PeerArgs {
    /// Global config.
    pub(super) config: Config,
    /// Per-server raft state
    pub(super) raft: RaftShared,
    /// Parent's id
    pub(super) parent_id: PeerId,
    /// Remote server's reference
    pub(super) peer: ActorRef<RaftMsg>,
    /// Index of the last entry in the leader's log.
    pub(super) last_log_index: usize,
}

#[ractor::async_trait]
impl Actor for Peer {
    type Msg = PeerMsg;
    type State = PeerState;
    type Arguments = PeerArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(PeerState {
            config: args.config,
            raft: args.raft,
            parent_id: args.parent_id,
            peer: args.peer,
            request_vote_done: false,
            next_index: args.last_log_index + 1,
            match_index: 0,
        })
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");
        state.run_loop(myself).await
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            PeerMsg::RunLoop => {
                state.run_loop(myself).await?;
            }
            PeerMsg::NotifyRoleChange(raft) => {
                state.raft = raft;
                match state.raft.role {
                    RaftRole::Follower => {}
                    RaftRole::Leader => {
                        state.next_index = 1; // TODO
                        state.match_index = 0;
                        state.run_loop(myself).await?;
                    }
                    RaftRole::Candidate => {
                        state.request_vote_done = false;
                        state.run_loop(myself).await?;
                    }
                }
            }
            PeerMsg::NotifyStateChange(raft) => {
                state.raft = raft;
            }
        }
        Ok(())
    }
}

impl PeerState {
    async fn run_loop(&mut self, myself: ActorRef<PeerMsg>) -> Result<(), ActorProcessingErr> {
        let parent = myself
            .try_get_supervisor()
            .expect("peer must have a supervisor");
        match self.raft.role {
            RaftRole::Follower => {}
            RaftRole::Candidate => {
                if !self.request_vote_done {
                    self.request_vote(parent.into()).await?;
                }
            }
            RaftRole::Leader => {
                self.append_entries(parent.into()).await?;
                let next_heartbeat = Duration::from_millis(self.config.raft.heartbeat_ms);
                myself.send_after(next_heartbeat, || PeerMsg::RunLoop);
            }
        }
        Ok(())
    }

    async fn request_vote(&mut self, parent: ActorRef<RaftMsg>) -> Result<(), ActorProcessingErr> {
        let request = RequestVoteAsk {
            term: self.raft.current_term,
            candidate_id: self.parent_id,
            last_log_index: 0,
            last_log_term: 0,
        };
        info!(
            target: "raft",
            from = tracing::field::display(self.parent_id),
            to = tracing::field::display(self.peer.get_id()),
            "request_vote"
        );

        let call_result = ractor::call!(self.peer, RaftMsg::RequestVote, request);
        if let Err(ref err) = call_result {
            warn!(target: "rpc", error = err as &dyn Error, "request_vote failed");
            return Ok(());
        }

        self.request_vote_done = true;

        let response = call_result.unwrap();
        if response.term > self.raft.current_term {
            info!(
                target: "raft",
                peer = tracing::field::display(self.peer.get_id()),
                peer_term = response.term,
                current_term = self.raft.current_term,
                "received request_vote response",
            );
            // TODO ask parent to step down
        } else {
            if response.vote_granted {
                info!(
                    target: "raft",
                    peer = tracing::field::display(self.peer.get_id()),
                    peer_term = response.term,
                    current_term = self.raft.current_term,
                    "got one vote",
                );
                ractor::cast!(parent, RaftMsg::ReceivedVote(self.peer.get_id().into()))?;
            } else {
                info!(
                    target: "raft",
                    peer = tracing::field::display(self.peer.get_id()),
                    peer_term = response.term,
                    current_term = self.raft.current_term,
                    "vote was denied",
                );
            }
        }

        Ok(())
    }

    async fn append_entries(
        &mut self,
        parent: ActorRef<RaftMsg>,
    ) -> Result<(), ActorProcessingErr> {
        assert!(matches!(self.raft.role, RaftRole::Leader));

        let prev_log_index = self.next_index - 1;
        let prev_log_term = 0;
        let num_entries = 0;
        let commit_index = self.raft.commit_index.min(prev_log_index + num_entries);
        let current_term = self.raft.current_term;

        let request = AppendEntriesAsk {
            term: current_term,
            leader_id: self.parent_id,
            prev_log_index,
            prev_log_term,
            entries: vec![],
            commit_index,
        };

        let call_result = ractor::call!(self.peer, RaftMsg::AppendEntries, request);
        if let Err(ref err) = call_result {
            warn!(target: "rpc", error = err as &dyn Error, "append_entries failed;");
            return Ok(());
        }

        let response = call_result.unwrap();
        if response.term > current_term {
            info!(
                target: "raft",
                "received AppendEntries response from server {} in term {} (this server's term was {})",
                self.peer.get_id(),
                response.term,
                current_term,
            );
            // TODO ask parent to step down
        } else {
            assert_eq!(response.term, current_term);
            if response.success {
                self.match_index = prev_log_index + num_entries;

                let msg = TryAdvanceCommitIndexMsg {
                    peer_id: Some(self.peer.get_id().into()),
                    match_index: self.match_index,
                };
                parent.cast(RaftMsg::TryAdvanceCommitIndex(msg))?;

                self.next_index = self.match_index + 1;
            } else {
                self.next_index = self.next_index.saturating_sub(1);
                // TODO optimize for skipping last_log_index
            }
        }

        Ok(())
    }
}
