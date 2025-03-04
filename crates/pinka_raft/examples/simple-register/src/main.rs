use std::str::from_utf8;

use anyhow::Result;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use fjall::{Config, Keyspace};
use pinka_raft::{
    ClientResult, LogEntryValue, RaftAppliedMsg, RaftClientMsg, RaftConfig, RaftServer,
    StateMachineMsg, get_raft_applied, get_raft_local_client,
};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use ractor_cluster::NodeServer;
use ractor_cluster::node::NodeConnectionMode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::spawn_blocking;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let flags = xflags::parse_or_exit! {
        /// Server name
        required -n,--name NAME: String
        /// Comma separated list of server addresses
        required -s,--servers SERVERS: String
    };
    let servers: Vec<_> = flags.servers.split(',').map(|s| s.to_string()).collect();
    let keyspace = Config::new("keyspace").open()?;
    let raft_config = RaftConfig {
        server_name: flags.name.clone(),
        heartbeat_ms: 100,
        min_election_ms: 500,
        max_election_ms: 1000,
        readonly_replica: false,
        servers: servers.clone(),
        replicas: vec![],
    };

    // Start the node server
    let node = NodeServer::new(
        8001,
        "supersecretcookie".to_string(),
        flags.name.clone(),
        flags.name.clone(),
        None,
        Some(NodeConnectionMode::Isolated),
    );
    let (node_server, _) = Actor::spawn(None, node, ()).await?;

    // Connect to peer servers
    for server in &servers {
        if server == &flags.name {
            continue;
        }
        loop {
            if ractor_cluster::client_connect(&node_server, format!("{server}:8001"))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    // Start the raft server
    Actor::spawn(
        Some("raft_worker".to_string()),
        RaftServer,
        (raft_config, keyspace.clone()),
    )
    .await?;

    // Start the state machine
    Actor::spawn(Some("state_machine".to_string()), KvStore, keyspace).await?;

    // Start the http server
    let app = Router::new()
        .route("/{key}", get(get_key))
        .route("/", post(set_key));
    let listener = TcpListener::bind("[::]:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_key(Path(key): Path<String>) -> Json<Value> {
    let raft_client = get_raft_local_client().expect("no raft server");
    let command = KvCommand::Read(key);
    let log_value =
        LogEntryValue::Command(serde_json::to_vec(&command).expect("unable to serialize"));
    let result = ractor::call_t!(raft_client, RaftClientMsg::ClientRequest, 1000, log_value)
        .expect("failed to get value");
    let value = match result {
        ClientResult::Ok(bytes) => {
            let value: Option<u64> = serde_json::from_slice(&bytes).expect("unable to deserialize");
            Value::from(value)
        }
        ClientResult::Err(_) => Value::Null,
    };
    Json(json!({
        "value": value
    }))
}

async fn set_key(Json(command): Json<KvCommand>) -> StatusCode {
    let raft_client = get_raft_local_client().expect("no raft server");
    let log_value =
        LogEntryValue::Command(serde_json::to_vec(&command).expect("unable to serialize"));
    let result = ractor::call_t!(raft_client, RaftClientMsg::ClientRequest, 1000, log_value)
        .expect("failed to get value");
    match result {
        ClientResult::Ok(_) => StatusCode::OK,
        ClientResult::Err(_) => StatusCode::PRECONDITION_FAILED,
    }
}

struct KvStore;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum KvCommand {
    Read(String),
    Write(String, u64),
    Cas(String, u64, u64),
}

impl Actor for KvStore {
    type Msg = StateMachineMsg;
    type State = Keyspace;
    type Arguments = Keyspace;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let StateMachineMsg::Apply(log_entry) = message;

        let keyspace = state.clone();
        let index = log_entry.index;
        match log_entry.value {
            LogEntryValue::NewTermStarted => {}
            LogEntryValue::ClusterMessage(_) => {}
            LogEntryValue::Command(bytes) => {
                let cmd_str = from_utf8(&bytes)?;
                let cmd: KvCommand = serde_json::from_str(&cmd_str)?;

                info!("Applying command: {:?}", cmd);
                match cmd {
                    KvCommand::Read(key) => {
                        let v = spawn_blocking(move || {
                            let par = keyspace
                                .open_partition("kv", Default::default())
                                .expect("IO error");
                            par.get(&key).expect("IO error").map(|v| {
                                u64::from_le_bytes(
                                    v.as_ref().try_into().expect("interger to large"),
                                )
                            })
                        })
                        .await
                        .expect("Unable to finish task");
                        let value = serde_json::to_string(&v)?;
                        let reply = get_raft_applied()?;
                        ractor::cast!(
                            reply,
                            RaftAppliedMsg::Applied(
                                index,
                                ClientResult::Ok(value.as_bytes().to_vec())
                            )
                        )?;
                    }
                    KvCommand::Write(key, value) => {
                        spawn_blocking(move || {
                            let par = keyspace
                                .open_partition("kv", Default::default())
                                .expect("IO error");
                            par.insert(&key, value.to_le_bytes()).expect("IO error");
                        })
                        .await
                        .expect("Unable to finish task");
                        let reply = get_raft_applied()?;
                        ractor::cast!(reply, RaftAppliedMsg::Applied(index, ClientResult::ok()))?;
                    }
                    KvCommand::Cas(key, v1, v2) => {
                        let res = spawn_blocking(move || {
                            let par = keyspace
                                .open_partition("kv", Default::default())
                                .expect("IO error");
                            let Some(current_value) = par.get(&key).expect("IO error") else {
                                return Err("key not found");
                            };
                            if current_value != v1.to_le_bytes() {
                                return Err("v1 is different from current value");
                            }
                            par.insert(&key, v2.to_le_bytes()).expect("IO error");
                            Ok(())
                        })
                        .await
                        .expect("Unable to finish task");
                        let reply = get_raft_applied()?;
                        if res.is_ok() {
                            ractor::cast!(
                                reply,
                                RaftAppliedMsg::Applied(index, ClientResult::ok())
                            )?;
                        } else {
                            ractor::cast!(
                                reply,
                                RaftAppliedMsg::Applied(index, ClientResult::err())
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
