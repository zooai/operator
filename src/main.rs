//! Zoo Network Kubernetes Operator
//!
//! Manages Zoo Network deployments on Kubernetes:
//! - ZooNetwork custom resources (validator node clusters)
//! - ZooChain custom resources (EVM chain tracking)
//! - ZooExplorer custom resources (Blockscout instances)
//! - ZooGateway custom resources (RPC gateway/ingress)

mod controller;
mod crd;
mod error;
mod leader;
mod metrics;

use axum::{routing::get, Router};
use clap::Parser;
use kube::Client;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "zoo-operator")]
#[command(about = "Kubernetes operator for Zoo Network", long_about = None)]
struct Args {
    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Namespace to watch (empty for all namespaces)
    #[arg(long, default_value = "")]
    namespace: String,

    /// Metrics port
    #[arg(long, default_value = "8080")]
    metrics_port: u16,

    /// Health check port
    #[arg(long, default_value = "8081")]
    health_port: u16,

    /// Enable leader election
    #[arg(long, default_value = "true")]
    leader_election: bool,
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(
    axum::extract::State(leader_flag): axum::extract::State<Arc<AtomicBool>>,
) -> (axum::http::StatusCode, &'static str) {
    if leader_flag.load(Ordering::Relaxed) {
        (axum::http::StatusCode::OK, "ok")
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not leader")
    }
}

async fn metrics_handler() -> String {
    metrics::encode()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    metrics::init();

    info!("Starting Zoo Network Operator v{}", env!("CARGO_PKG_VERSION"));
    info!("Log level: {}", args.log_level);
    info!(
        "Namespace: {}",
        if args.namespace.is_empty() {
            "all"
        } else {
            &args.namespace
        }
    );
    info!("Leader election: {}", args.leader_election);

    let client = Client::try_default().await?;
    info!("Connected to Kubernetes cluster");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let operator_namespace = std::env::var("OPERATOR_NAMESPACE")
        .unwrap_or_else(|_| "zoo-system".to_string());
    let leader_election = leader::LeaderElection::new(client.clone(), operator_namespace);
    let leader_flag = leader_election.leader_flag();

    if !args.leader_election {
        leader_flag.store(true, Ordering::Relaxed);
        info!("Leader election disabled, running as leader");
    }

    // Health server
    let health_addr = SocketAddr::from(([0, 0, 0, 0], args.health_port));
    let health_flag = leader_flag.clone();
    let health_app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(health_flag);
    info!("Health server listening on {}", health_addr);

    // Metrics server
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], args.metrics_port));
    let metrics_app = Router::new().route("/metrics", get(metrics_handler));
    info!("Metrics server listening on {}", metrics_addr);

    // Prepare controller args
    let network_client = client.clone();
    let chain_client = client.clone();
    let explorer_client = client.clone();
    let gateway_client = client.clone();
    let network_ns = args.namespace.clone();
    let chain_ns = args.namespace.clone();
    let explorer_ns = args.namespace.clone();
    let gateway_ns = args.namespace.clone();

    let controllers_flag = leader_flag.clone();

    tokio::select! {
        // Leader election loop
        _ = async {
            if args.leader_election {
                leader_election.run(shutdown_rx.clone()).await;
            } else {
                std::future::pending::<()>().await;
            }
        } => {
            info!("Leader election exited");
        }

        // Controllers — wait for leadership then run
        _ = async {
            loop {
                if controllers_flag.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            info!("This instance is the leader, starting controllers");

            tokio::select! {
                res = controller::run_network_controller(network_client, network_ns) => {
                    if let Err(e) = res {
                        tracing::error!("Network controller exited with error: {:?}", e);
                    }
                }
                res = controller::run_chain_controller(chain_client, chain_ns) => {
                    if let Err(e) = res {
                        tracing::error!("Chain controller exited with error: {:?}", e);
                    }
                }
                res = controller::run_explorer_controller(explorer_client, explorer_ns) => {
                    if let Err(e) = res {
                        tracing::error!("Explorer controller exited with error: {:?}", e);
                    }
                }
                res = controller::run_gateway_controller(gateway_client, gateway_ns) => {
                    if let Err(e) = res {
                        tracing::error!("Gateway controller exited with error: {:?}", e);
                    }
                }
                // Stop controllers if leadership lost
                _ = async {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        if !controllers_flag.load(Ordering::Relaxed) {
                            warn!("Lost leadership, stopping controllers");
                            break;
                        }
                    }
                } => {
                    warn!("Controllers stopped due to leadership loss");
                }
            }
        } => {}

        // Health server
        res = axum::serve(
            tokio::net::TcpListener::bind(health_addr).await.unwrap(),
            health_app.into_make_service(),
        ) => {
            if let Err(e) = res {
                tracing::error!("Health server exited with error: {:?}", e);
            }
        }

        // Metrics server
        res = axum::serve(
            tokio::net::TcpListener::bind(metrics_addr).await.unwrap(),
            metrics_app.into_make_service(),
        ) => {
            if let Err(e) = res {
                tracing::error!("Metrics server exited with error: {:?}", e);
            }
        }

        // Graceful shutdown
        _ = async {
            tokio::signal::ctrl_c().await.expect("failed to listen for ctrl_c");
        } => {
            info!("Received shutdown signal, shutting down gracefully");
        }
    }

    let _ = shutdown_tx.send(true);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    info!("Zoo operator stopped");
    Ok(())
}
