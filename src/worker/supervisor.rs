use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor_cluster::node::NodeConnectionMode;
use ractor_cluster::{IncomingEncryptionMode, NodeServer, NodeServerMessage, RactorMessage};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{
    ClientConfig as TlsClientConfig, RootCertStore, ServerConfig as TlsServerConfig,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

use crate::config::{Config, ServerConfig};
use crate::flags::Flags;

use super::raft::{RaftMsg, RaftWorker};

pub(crate) struct Supervisor;

#[derive(RactorMessage)]
pub(crate) enum SupervisorMsg {}

pub(crate) struct SupervisorState {
    server: ServerConfig,
    config: Config,
    myself: ActorRef<SupervisorMsg>,
}

impl Actor for Supervisor {
    type Msg = SupervisorMsg;
    type State = SupervisorState;
    type Arguments = (Flags, Config);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let flags = &args.0;
        let config = args.1.clone();
        let server = config.cluster.servers[flags.server.unwrap_or_default()].clone();

        Actor::spawn_linked(
            Some(server.name.clone()),
            RaftWorker,
            (args.0.bootstrap, config.clone()),
            myself.get_cell(),
        )
        .await?;

        let state = SupervisorState {
            server,
            config,
            myself,
        };

        state.spawn_node_server().await?;

        if flags.connect {
            state.connect_peers().await?;
        }

        Ok(state)
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        info!(target: "lifecycle", "started");
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        message: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        use SupervisionEvent::*;

        match message {
            ActorStarted(_) => {}
            ActorTerminated(_, _, _) => {}
            ActorFailed(actor_cell, _error) => {
                if matches!(
                    actor_cell.is_message_type_of::<NodeServerMessage>(),
                    Some(true)
                ) {
                    info!(target: "supervision", "node_server crashed, restarting...");
                    state.spawn_node_server().await?;
                    state.connect_peers().await?;
                }
                if matches!(actor_cell.is_message_type_of::<RaftMsg>(), Some(true)) {
                    info!(target: "supervision", "raft_worker crashed, restarting...");
                    Actor::spawn_linked(
                        Some(state.server.name.clone()),
                        RaftWorker,
                        (false, state.config.clone()),
                        myself.into(),
                    )
                    .await?;
                }
            }
            ProcessGroupChanged(_) => {}
            PidLifecycleEvent(_) => {}
        }

        Ok(())
    }
}

impl SupervisorState {
    async fn spawn_node_server(&self) -> Result<ActorRef<NodeServerMessage>, ActorProcessingErr> {
        let encryption_mode = if self.config.cluster.use_mtls {
            IncomingEncryptionMode::Tls(self.get_tls_acceptor().await?)
        } else {
            IncomingEncryptionMode::Raw
        };
        let node = NodeServer::new(
            self.server.port,
            self.config.cluster.auth_cookie.clone(),
            self.server.name.clone(),
            self.server.hostname.clone(),
            Some(encryption_mode),
            Some(NodeConnectionMode::Isolated),
        );
        let (node_server, _) =
            Actor::spawn_linked(Some("node_server".into()), node, (), self.myself.get_cell())
                .await?;
        Ok(node_server)
    }

    async fn connect_peers(&self) -> Result<(), ActorProcessingErr> {
        let node_server = match ActorRef::<NodeServerMessage>::where_is("node_server".into()) {
            Some(node_server) => node_server,
            None => self.spawn_node_server().await?,
        };
        for peer in self.config.cluster.servers.iter().cloned() {
            if peer.name == self.server.name {
                continue;
            }
            let node_server = node_server.clone();
            let tls_connector = if self.config.cluster.use_mtls {
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
        Ok(self
            .config
            .cluster
            .pem_dir
            .clone()
            .ok_or_else(|| anyhow!("cluster.cert_dir must be defined when use_mtls is true"))?)
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
