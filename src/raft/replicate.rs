use std::ops::Deref;
use std::time::{Duration, Instant};

use anyhow::Result;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tracing::{info, trace, warn};

use super::log_entry::LogEntry;
use super::{AdvanceCommitIndexMsg, AppendEntriesAsk, RaftLog, RaftMsg, RaftShared, RuntimeConfig};

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
    config: RuntimeConfig,

    /// Per-server raft state
    raft: RaftShared,

    /// Parent's name
    name: String,

    /// Remote server's reference
    peer: ActorRef<RaftMsg>,

    /// Raft log
    log: RaftLog,

    /// Index of the next log entry to send to that peer.
    ///
    /// Initialized to leader last log index + 1.
    next_index: u64,

    /// Index of the highest log entry known to be replicated on the peer.
    ///
    /// Initialized to 0, increases monotonically.
    match_index: u64,

    /// Whether this peer is only an observer.
    observer: bool,

    /// Timestamp of last append_entries
    anchor: Instant,

    /// Failed attempts
    failed_attempts: u64,
}

pub(super) struct ReplicateArgs {
    /// Global config.
    pub(super) config: RuntimeConfig,
    /// Per-server raft state
    pub(super) raft: RaftShared,
    /// Parent's name
    pub(super) name: String,
    /// Parent's reference
    pub(super) parent: ActorRef<RaftMsg>,
    /// Remote server's reference
    pub(super) peer: ActorRef<RaftMsg>,
    /// Raft log
    pub(super) log: RaftLog,
    /// Index of the last entry in the leader's log.
    pub(super) last_log_index: u64,
    /// Whether this peer is only an observer.
    pub(super) observer: bool,
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
            name: args.name,
            parent: args.parent,
            config: args.config,
            raft: args.raft,
            peer: args.peer,
            log: args.log,
            next_index: args.last_log_index + 1,
            match_index: 0,
            observer: args.observer,
            anchor: Instant::now(),
            failed_attempts: 0,
        })
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(
            "replication worker for {} started",
            state.peer.get_name().unwrap()
        );
        state.run_loop().await?;
        Ok(())
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
                if state.anchor.elapsed()
                    > Duration::from_millis(state.config.init.raft.heartbeat_ms)
                {
                    // Schedule append_entries to avoid notify_state_change flooding
                    // caused election timeout.
                    state.append_entries().await?;
                }
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
    async fn run_loop(&mut self) -> Result<()> {
        self.append_entries().await?;
        let next_heartbeat = Duration::from_millis(self.config.init.raft.heartbeat_ms);
        self.send_after(next_heartbeat, || ReplicateMsg::RunLoop);
        Ok(())
    }

    async fn append_entries(&mut self) -> Result<()> {
        // NB: Replicate worker only runs when the parent is a Leader
        self.anchor = Instant::now();

        let prev_log_index = self.next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index > 0 {
            self.log.get_log_entry(prev_log_index).await?.term
        } else {
            0
        };
        let entries = self.get_log_entries().await?;
        let num_entries = entries.len() as u64;
        let commit_index = self.raft.commit_index.min(prev_log_index + num_entries);
        let current_term = self.raft.current_term;

        let request = AppendEntriesAsk {
            term: current_term,
            leader_id: self.name.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            commit_index,
        };

        trace!(
            peer = %self.peer.get_name().unwrap(),
            ?request,
            "send append_entries"
        );
        let response = ractor::call!(self.peer, RaftMsg::AppendEntries, request)?;
        if response.term < current_term {
            warn!(
                term = response.term,
                "discard stale append_entries response"
            );
            return Ok(());
        }
        if response.term > current_term {
            info!(
                peer = self.peer.get_name().unwrap(),
                response_term = response.term,
                current_term,
                "received append_entries response from server {} in term {} (this server's term was {})",
                self.peer.get_name().unwrap(),
                response.term,
                current_term,
            );
            ractor::cast!(self.parent, RaftMsg::UpdateTerm(response.term))?;
            return Ok(());
        }

        assert_eq!(response.term, current_term);
        if response.success {
            self.failed_attempts = 0;
            self.match_index = prev_log_index + num_entries;

            if !self.observer {
                let msg = AdvanceCommitIndexMsg {
                    peer_id: Some(self.peer.get_name().unwrap()),
                    match_index: self.match_index,
                };
                ractor::cast!(self.parent, RaftMsg::AdvanceCommitIndex(msg))?;
            }

            self.next_index = self.match_index + 1;
        } else {
            let new_next_index = self
                .next_index
                .saturating_sub(num_entries.clamp(1, u64::MAX) * 1 << self.failed_attempts);
            trace!(
                "append_entries failed {} times, decrement next_index for {} from {} to {}",
                self.failed_attempts,
                self.peer.get_name().unwrap(),
                self.next_index,
                new_next_index
            );
            self.next_index = new_next_index;
            self.failed_attempts += 1;
        }

        Ok(())
    }

    async fn get_log_entries(&self) -> Result<Vec<LogEntry>> {
        let from = self.next_index;
        self.log.log_entry_range(from..from + 10).await
    }
}
