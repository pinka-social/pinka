use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, bail};
use ractor::concurrency::Duration;
use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::node::{NodeConnectionMode, NodeServerSessionInformation};
use ractor_cluster::{
    IncomingEncryptionMode, NodeEventSubscription, NodeServer, NodeServerMessage, RactorMessage,
};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
    ClientConfig as TlsClientConfig, RootCertStore, ServerConfig as TlsServerConfig,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

use crate::config::{RuntimeConfig, ServerConfig};

pub(super) struct ClusterMaint;

#[derive(RactorMessage, Debug)]
pub(super) enum ClusterMaintMsg {
    CheckConnection,
    ServerConnected(String),
    ServerDisconnected(String),
}

#[derive(Debug)]
pub(super) struct ClusterState {
    server: ServerConfig,
    config: RuntimeConfig,
    server_status: BTreeMap<String, ServerStatus>,
    myself: ActorRef<ClusterMaintMsg>,
}

#[derive(Debug)]
enum ServerStatus {
    Connected,
    Disconnected,
}

impl Actor for ClusterMaint {
    type Msg = ClusterMaintMsg;
    type State = ClusterState;
    type Arguments = (ServerConfig, RuntimeConfig);

    async fn pre_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (server, config) = args;

        let mut server_status = BTreeMap::new();
        for s in &config.init.cluster.servers {
            if s.name == server.name {
                continue;
            }
            server_status.insert(s.name.clone(), ServerStatus::Disconnected);
        }

        Ok(ClusterState {
            server,
            config,
            server_status,
            myself,
        })
    }

    async fn post_start(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        myself.send_interval(
            Duration::from_millis(state.config.init.cluster.reconnect_timeout_ms),
            || ClusterMaintMsg::CheckConnection,
        );
        state.spawn_node_server().await?;
        state.connect_peers().await?;
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let _ = state;
        match message {
            ClusterMaintMsg::CheckConnection => {
                state.connect_peers().await?;
            }
            ClusterMaintMsg::ServerConnected(name) => {
                info!(name, "server connected");
                state.server_status.insert(name, ServerStatus::Connected);
            }
            ClusterMaintMsg::ServerDisconnected(name) => {
                info!(name, "server disconnected");
                state.server_status.insert(name, ServerStatus::Disconnected);
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            SupervisionEvent::ActorStarted(_actor_cell) => {}
            SupervisionEvent::ActorTerminated(_actor_cell, _boxed_statee, _) => {}
            SupervisionEvent::ActorFailed(_actor_cell, _errorr) => {
                state.spawn_node_server().await?;
                state.connect_peers().await?;
            }
            SupervisionEvent::ProcessGroupChanged(_group_change_message) => {}
            SupervisionEvent::PidLifecycleEvent(_pid_lifecycle_event) => {}
        }
        Ok(())
    }
}

impl ClusterState {
    async fn spawn_node_server(&self) -> Result<ActorRef<NodeServerMessage>, ActorProcessingErr> {
        let encryption_mode = if self.config.init.cluster.use_mtls {
            IncomingEncryptionMode::Tls(self.get_tls_acceptor().await?)
        } else {
            IncomingEncryptionMode::Raw
        };
        let node = NodeServer::new(
            self.server.port,
            self.config.init.cluster.auth_cookie.clone(),
            self.server.name.clone(),
            self.server.hostname.clone(),
            Some(encryption_mode),
            Some(NodeConnectionMode::Isolated),
        );
        let (node_server, _) =
            Actor::spawn_linked(Some("node_server".into()), node, (), self.myself.get_cell())
                .await?;
        node_server.cast(NodeServerMessage::SubscribeToEvents {
            id: "cluster_maint".into(),
            subscription: Box::new(NodeEventListener),
        })?;
        Ok(node_server)
    }

    async fn connect_peers(&self) -> Result<(), ActorProcessingErr> {
        let node_server = match ActorRef::<NodeServerMessage>::where_is("node_server".into()) {
            Some(node_server) => node_server,
            None => self.spawn_node_server().await?,
        };
        for peer in self.config.init.cluster.servers.iter().cloned() {
            if peer.name == self.server.name {
                continue;
            }
            if let Some(status) = self.server_status.get(&peer.name) {
                if matches!(status, ServerStatus::Connected) {
                    continue;
                }
            }
            let node_server = node_server.clone();
            let tls_connector = if self.config.init.cluster.use_mtls {
                Some(self.get_tls_connector().await?)
            } else {
                None
            };
            ractor::concurrency::spawn(async move {
                info!(target: "raft", "connecting to {}@{}:{}", peer.name, peer.hostname, peer.port);
                let conn_result = if let Some(tls_connector) = tls_connector {
                    ractor_cluster::client_connect_enc(
                        &node_server,
                        (peer.hostname.as_str(), peer.port),
                        tls_connector,
                        peer.hostname
                            .clone()
                            .try_into()
                            .expect("hostname should be a valid DNS name"),
                    )
                    .await
                } else {
                    ractor_cluster::client_connect(
                        &node_server,
                        (peer.hostname.as_str(), peer.port),
                    )
                    .await
                };
                if let Err(_) = conn_result {
                    warn!(target: "raft", "unable to connect to {}@{}:{}", peer.name, peer.hostname, peer.port);
                }
            });
        }
        Ok(())
    }

    fn get_cert_dir(&self) -> anyhow::Result<PathBuf> {
        self.config
            .init
            .cluster
            .pem_dir
            .clone()
            .context("cluster.cert_dir must be defined when use_mtls is true")
    }

    async fn get_root_store(
        &self,
        extra_roots: &[PathBuf],
    ) -> Result<Arc<RootCertStore>, ActorProcessingErr> {
        let mut roots = RootCertStore::empty();

        let cert_dir = self.get_cert_dir()?;
        info!(target: "session", "loading CA certificates");
        for cert_name in self
            .config
            .init
            .cluster
            .ca_certs
            .iter()
            .chain(extra_roots.iter())
        {
            if cert_name.is_absolute() {
                warn!(
                    "\"{}\" is not a relative path to cert_dir, skipped",
                    cert_name.display()
                );
                continue;
            }
            match CertificateDer::from_pem_slice(&tokio::fs::read(cert_dir.join(cert_name)).await?)
            {
                Ok(cert) => roots.add(cert)?,
                Err(_) => {
                    warn!("\"{}\" is not a certificate, skipped", cert_name.display());
                }
            }
        }
        Ok(Arc::new(roots))
    }

    async fn get_cert_chain(
        &self,
        cert_names: &[PathBuf],
    ) -> Result<Vec<CertificateDer<'static>>, ActorProcessingErr> {
        let cert_dir = self.get_cert_dir()?;
        let mut cert_chain = vec![];
        for cert_name in cert_names {
            if cert_name.is_absolute() {
                warn!(
                    "\"{}\" is not a relative path to cert_dir, skipped",
                    cert_name.display()
                );
                continue;
            }
            match CertificateDer::from_pem_slice(&tokio::fs::read(cert_dir.join(cert_name)).await?)
            {
                Ok(cert) => cert_chain.push(cert.into_owned()),
                Err(_) => {
                    warn!("\"{}\" is not a certificate, skipped", cert_name.display());
                }
            }
        }
        Ok(cert_chain)
    }

    async fn get_priv_key(&self, path: Option<&PathBuf>) -> anyhow::Result<PrivateKeyDer<'static>> {
        if path.is_none() {
            bail!("must have client_key to use mtls");
        }
        let path = path.unwrap();
        let cert_dir = self.get_cert_dir()?;
        if path.is_absolute() {
            bail!("\"{}\" is not a relative path to cert_dir", path.display());
        }
        PrivateKeyDer::from_pem_slice(&tokio::fs::read(cert_dir.join(path)).await?)
            .context("Failed to read private key")
    }

    async fn get_tls_acceptor(&self) -> Result<TlsAcceptor, ActorProcessingErr> {
        let roots = self.get_root_store(&self.server.server_ca_certs).await?;

        info!(target: "session", "loading server certificates");
        let cert_chain = self.get_cert_chain(&self.server.server_cert_chain).await?;

        info!(target: "session", "loading server private key");
        let priv_key = self.get_priv_key(self.server.server_key.as_ref()).await?;

        let client_verifier = WebPkiClientVerifier::builder(roots).build()?;
        Ok(TlsAcceptor::from(Arc::new(
            TlsServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(cert_chain, priv_key)?,
        )))
    }

    async fn get_tls_connector(&self) -> Result<TlsConnector, ActorProcessingErr> {
        let roots = self.get_root_store(&self.server.client_ca_certs).await?;

        info!(target: "session", "loading client certificates");
        let cert_chain = self.get_cert_chain(&self.server.client_cert_chain).await?;

        info!(target: "session", "loading client private key");
        let priv_key = self.get_priv_key(self.server.client_key.as_ref()).await?;

        Ok(TlsConnector::from(Arc::new(
            TlsClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(cert_chain, priv_key)?,
        )))
    }
}

struct NodeEventListener;

impl NodeEventSubscription for NodeEventListener {
    fn node_session_opened(&self, ses: NodeServerSessionInformation) {
        let _ = ses;
    }

    fn node_session_disconnected(&self, ses: NodeServerSessionInformation) {
        if let Some(peer_name) = ses.peer_name {
            if let Some(cluster_maint) =
                ActorRef::<ClusterMaintMsg>::where_is("cluster_maint".into())
            {
                if let Some((server_name, _hostname)) = peer_name.name.split_once('@') {
                    cluster_maint
                        .cast(ClusterMaintMsg::ServerDisconnected(server_name.to_string()))
                        .expect("unable to send message to cluster_maint");
                }
            }
        }
    }

    fn node_session_authenicated(&self, ses: NodeServerSessionInformation) {
        if let Some(peer_name) = ses.peer_name {
            if let Some(cluster_maint) =
                ActorRef::<ClusterMaintMsg>::where_is("cluster_maint".into())
            {
                if let Some((server_name, _hostname)) = peer_name.name.split_once('@') {
                    cluster_maint
                        .cast(ClusterMaintMsg::ServerConnected(server_name.to_string()))
                        .expect("unable to send message to cluster_maint");
                }
            }
        }
    }
}
