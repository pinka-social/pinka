use std::ops::Deref;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use fjall::PartitionHandle;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::RactorMessage;
use tokio::task::block_in_place;
use tracing::{info, trace, warn};

use super::log_entry::LogEntry;
use super::{AdvanceCommitIndexMsg, AppendEntriesAsk, RaftMsg, RaftShared, RuntimeConfig};

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
    log: PartitionHandle,

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
    pub(super) log: PartitionHandle,
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
            name: args.name,
            parent: args.parent,
            config: args.config,
            raft: args.raft,
            peer: args.peer,
            log: args.log,
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

        let prev_log_index = self.next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index > 0 {
            self.get_log_entry(prev_log_index)?.term
        } else {
            0
        };
        let entries = self.get_log_entries()?;
        let num_entries = entries.len();
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
            target: "raft",
            peer = %self.peer.get_name().unwrap(),
            ?request,
            "send append_entries"
        );
        let call_result = ractor::call!(self.peer, RaftMsg::AppendEntries, request);
        if let Err(error) = call_result {
            warn!(target: "rpc", %error, "append_entries failed;");
            return Ok(());
        }

        let response = call_result.unwrap();
        if response.term < current_term {
            warn!(target: "raft", term = response.term, "discard stale append_entries response");
            return Ok(());
        }
        if response.term > current_term {
            info!(
                target: "raft",
                peer = self.peer.get_name().unwrap(),
                response_term = response.term,
                current_term,
                "received append_entries response from server {} in term {} (this server's term was {})",
                self.peer.get_name().unwrap(),
                response.term,
                current_term,
            );
            ractor::cast!(
                self.parent,
                RaftMsg::UpdateTerm(response.term, self.peer.get_name().unwrap())
            )?;
            return Ok(());
        }

        assert_eq!(response.term, current_term);
        if response.success {
            self.match_index = prev_log_index + num_entries;

            let msg = AdvanceCommitIndexMsg {
                peer_id: Some(self.peer.get_name().unwrap()),
                match_index: self.match_index,
            };
            ractor::cast!(self.parent, RaftMsg::AdvanceCommitIndex(msg))?;

            self.next_index = self.match_index + 1;
        } else {
            self.next_index = self.next_index.saturating_sub(1);
            // TODO optimize for skipping last_log_index
        }

        Ok(())
    }

    fn get_log_entry(&self, index: usize) -> anyhow::Result<LogEntry> {
        block_in_place(|| {
            self.log
                .get(&index.to_be_bytes())
                .context("get log entry failed")?
                .ok_or_else(|| anyhow!("log entry index {index} does not exist"))
                .and_then(|slice| {
                    postcard::from_bytes::<LogEntry>(&slice)
                        .context("failed to deserialize log entry")
                })
        })
    }

    fn get_log_entries(&self) -> anyhow::Result<Vec<LogEntry>> {
        block_in_place(|| {
            let mut entries = vec![];
            let from = self.next_index.to_be_bytes();
            let to = (self.next_index + 1).to_be_bytes();
            for rkv in self.log.range(from..to) {
                match rkv {
                    Ok((_, v)) => entries.push(postcard::from_bytes(&v)?),
                    Err(_) => bail!("failed to read all entries"),
                }
            }
            Ok(entries)
        })
    }
}
