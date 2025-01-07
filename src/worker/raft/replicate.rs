use std::time::Duration;
use std::{error::Error, ops::Deref};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tracing::{info, warn};

use crate::worker::raft::rpc::TryAdvanceCommitIndexMsg;

use super::{AppendEntriesAsk, Config, RaftMsg, RaftShared};

pub(super) struct ReplicateWorker;

#[derive(RactorMessage)]
pub(super) enum ReplicateMsg {
    RunLoop,
    NotifyStateChange(RaftShared),
}

pub(super) struct ReplicateState {
    /// Actor reference
    myself: ActorRef<ReplicateMsg>,

    /// Parent actor reference
    parent: ActorRef<RaftMsg>,

    /// Global config.
    config: Config,

    /// Per-server raft state
    raft: RaftShared,

    /// Remote server's reference
    peer: ActorRef<RaftMsg>,

    /// Index of the next log entry to send to that peer.
    ///
    /// Initialized to leader last log index + 1.
    next_index: usize,

    /// Index of the highest log entry known to be replicated on the peer.
    ///
    /// Initialized to 0, increases monotonically.
    match_index: usize,
}

pub(super) struct ReplicateArgs {
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

impl Actor for ReplicateWorker {
    type Msg = ReplicateMsg;
    type State = ReplicateState;
    type Arguments = ReplicateArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(ReplicateState {
            myself,
            parent: args.parent,
            config: args.config,
            raft: args.raft,
            peer: args.peer,
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
            ReplicateMsg::RunLoop => {
                state.run_loop().await?;
            }
            ReplicateMsg::NotifyStateChange(raft) => {
                state.raft = raft;
            }
        }
        Ok(())
    }
}

impl Deref for ReplicateState {
    type Target = ActorRef<ReplicateMsg>;

    fn deref(&self) -> &Self::Target {
        &self.myself
    }
}

impl ReplicateState {
    async fn run_loop(&mut self) -> Result<(), ActorProcessingErr> {
        self.append_entries().await?;
        let next_heartbeat = Duration::from_millis(self.config.raft.heartbeat_ms);
        self.send_after(next_heartbeat, || ReplicateMsg::RunLoop);
        Ok(())
    }

    async fn append_entries(&mut self) -> Result<(), ActorProcessingErr> {
        let prev_log_index = self.next_index - 1;
        let prev_log_term = 0;
        let num_entries = 0;
        let commit_index = self.raft.commit_index.min(prev_log_index + num_entries);
        let current_term = self.raft.current_term;

        let request = AppendEntriesAsk {
            term: current_term,
            // FIXME
            leader_id: self.parent.get_name().unwrap(),
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
                    peer_id: Some(self.peer.get_name().unwrap()),
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
