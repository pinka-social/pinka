use std::time::Duration;
use std::{error::Error, ops::Deref};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tracing::{info, warn};

use crate::worker::raft::RequestVoteAsk;
use crate::worker::raft::rpc::TryAdvanceCommitIndexMsg;

use super::{AppendEntriesAsk, Config, RaftMsg, RaftRole, RaftShared};

pub(super) struct Peer;

#[derive(RactorMessage)]
pub(super) enum PeerMsg {
    RunLoop,
    NotifyRoleChange(RaftShared),
    NotifyStateChange(RaftShared),
}

pub(super) struct PeerState {
    /// Actor reference
    myself: ActorRef<PeerMsg>,

    /// Parent actor reference
    parent: ActorRef<RaftMsg>,

    /// Global config.
    config: Config,

    /// Per-server raft state
    raft: RaftShared,

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
    pub(super) parent: ActorRef<RaftMsg>,
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
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(PeerState {
            myself,
            parent: args.parent,
            config: args.config,
            raft: args.raft,
            peer: args.peer,
            request_vote_done: false,
            next_index: args.last_log_index + 1,
            match_index: 0,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");
        state.run_loop().await
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            PeerMsg::RunLoop => {
                state.run_loop().await?;
            }
            PeerMsg::NotifyRoleChange(raft) => {
                state.raft = raft;
                match state.raft.role {
                    RaftRole::Follower => {}
                    RaftRole::Leader => {
                        state.next_index = 1; // TODO
                        state.match_index = 0;
                        state.run_loop().await?;
                    }
                    RaftRole::Candidate => {
                        state.request_vote_done = false;
                        state.run_loop().await?;
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

impl Deref for PeerState {
    type Target = ActorRef<PeerMsg>;

    fn deref(&self) -> &Self::Target {
        &self.myself
    }
}

impl PeerState {
    async fn run_loop(&mut self) -> Result<(), ActorProcessingErr> {
        match self.raft.role {
            RaftRole::Follower => {}
            RaftRole::Candidate => {
                if !self.request_vote_done {
                    self.request_vote().await?;
                }
            }
            RaftRole::Leader => {
                self.append_entries().await?;
                let next_heartbeat = Duration::from_millis(self.config.raft.heartbeat_ms);
                self.send_after(next_heartbeat, || PeerMsg::RunLoop);
            }
        }
        Ok(())
    }

    async fn request_vote(&mut self) -> Result<(), ActorProcessingErr> {
        let request = RequestVoteAsk {
            term: self.raft.current_term,
            candidate_id: self.parent.get_id().into(),
            last_log_index: 0,
            last_log_term: 0,
        };
        info!(
            target: "raft",
            from = %self.parent.get_id(),
            to = %self.peer.get_id(),
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
                peer = %self.peer.get_id(),
                peer_term = response.term,
                current_term = self.raft.current_term,
                "received request_vote response",
            );
            // TODO ask parent to step down
        } else {
            if response.vote_granted {
                info!(
                    target: "raft",
                    myself = %self.parent.get_id(),
                    peer = %self.peer.get_id(),
                    peer_term = response.term,
                    current_term = self.raft.current_term,
                    "got one vote",
                );
                ractor::cast!(
                    self.parent,
                    RaftMsg::ReceivedVote(self.peer.get_id().into())
                )?;
            } else {
                info!(
                    target: "raft",
                    peer = %self.peer.get_id(),
                    peer_term = response.term,
                    current_term = self.raft.current_term,
                    "vote was denied",
                );
            }
        }

        Ok(())
    }

    async fn append_entries(&mut self) -> Result<(), ActorProcessingErr> {
        assert!(matches!(self.raft.role, RaftRole::Leader));

        let prev_log_index = self.next_index - 1;
        let prev_log_term = 0;
        let num_entries = 0;
        let commit_index = self.raft.commit_index.min(prev_log_index + num_entries);
        let current_term = self.raft.current_term;

        let request = AppendEntriesAsk {
            term: current_term,
            leader_id: self.parent.get_id().into(),
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
                ractor::cast!(self.parent, RaftMsg::TryAdvanceCommitIndex(msg))?;

                self.next_index = self.match_index + 1;
            } else {
                self.next_index = self.next_index.saturating_sub(1);
                // TODO optimize for skipping last_log_index
            }
        }

        Ok(())
    }
}
