//! Lease-based leader election for the Zoo operator.
//!
//! Uses Kubernetes Lease objects in the coordination.k8s.io API group.
//! Only the holder of the lease runs the controllers; all other replicas
//! wait and retry every 15 seconds.

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::Client;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

const LEASE_NAME: &str = "zoo-operator-leader";
const LEASE_DURATION_SECONDS: i32 = 30;
const RENEW_INTERVAL_SECS: u64 = 10;
const RETRY_INTERVAL_SECS: u64 = 15;

pub struct LeaderElection {
    is_leader: Arc<AtomicBool>,
    identity: String,
    namespace: String,
    client: Client,
}

impl LeaderElection {
    pub fn new(client: Client, namespace: String) -> Self {
        let identity = std::env::var("HOSTNAME").unwrap_or_else(|_| {
            format!("zoo-operator-{}", std::process::id())
        });

        LeaderElection {
            is_leader: Arc::new(AtomicBool::new(false)),
            identity,
            namespace,
            client,
        }
    }

    pub fn leader_flag(&self) -> Arc<AtomicBool> {
        self.is_leader.clone()
    }

    /// Run the leader election loop. Never returns under normal operation.
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        let leases: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let mut shutdown = shutdown;

        loop {
            match self.try_acquire_or_renew(&leases).await {
                Ok(true) => {
                    if !self.is_leader.load(Ordering::Relaxed) {
                        info!(identity = %self.identity, "Acquired leader lease");
                        self.is_leader.store(true, Ordering::Relaxed);
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(RENEW_INTERVAL_SECS)) => {}
                        _ = shutdown.changed() => {
                            self.release(&leases).await;
                            return;
                        }
                    }
                }
                Ok(false) => {
                    if self.is_leader.load(Ordering::Relaxed) {
                        warn!(identity = %self.identity, "Lost leader lease");
                        self.is_leader.store(false, Ordering::Relaxed);
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL_SECS)) => {}
                        _ = shutdown.changed() => {
                            return;
                        }
                    }
                }
                Err(e) => {
                    warn!(identity = %self.identity, error = %e, "Leader election error, retrying");
                    if self.is_leader.load(Ordering::Relaxed) {
                        self.is_leader.store(false, Ordering::Relaxed);
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(RETRY_INTERVAL_SECS)) => {}
                        _ = shutdown.changed() => {
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn try_acquire_or_renew(&self, leases: &Api<Lease>) -> anyhow::Result<bool> {
        let now = chrono::Utc::now();

        match leases.get(LEASE_NAME).await {
            Ok(existing) => {
                let spec = existing.spec.as_ref();
                let holder = spec.and_then(|s| s.holder_identity.as_deref());
                let renew_time = spec.and_then(|s| s.renew_time.as_ref()).map(|t| t.0);
                let duration = spec
                    .and_then(|s| s.lease_duration_seconds)
                    .unwrap_or(LEASE_DURATION_SECONDS);
                let transitions = spec.and_then(|s| s.lease_transitions).unwrap_or(0);

                let is_expired = match renew_time {
                    Some(t) => now.signed_duration_since(t).num_seconds() > duration as i64,
                    None => true,
                };

                if holder == Some(self.identity.as_str()) {
                    let patch = serde_json::json!({
                        "spec": {
                            "renewTime": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                        }
                    });
                    leases
                        .patch(LEASE_NAME, &PatchParams::default(), &Patch::Merge(patch))
                        .await?;
                    Ok(true)
                } else if is_expired {
                    let patch = serde_json::json!({
                        "spec": {
                            "holderIdentity": self.identity,
                            "leaseDurationSeconds": LEASE_DURATION_SECONDS,
                            "acquireTime": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                            "renewTime": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                            "leaseTransitions": transitions + 1,
                        }
                    });
                    leases
                        .patch(LEASE_NAME, &PatchParams::default(), &Patch::Merge(patch))
                        .await?;
                    info!(
                        identity = %self.identity,
                        previous = holder.unwrap_or("<none>"),
                        "Took over expired leader lease"
                    );
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(kube::Error::Api(err)) if err.code == 404 => {
                let lease = Lease {
                    metadata: kube::core::ObjectMeta {
                        name: Some(LEASE_NAME.to_string()),
                        namespace: Some(self.namespace.clone()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(self.identity.clone()),
                        lease_duration_seconds: Some(LEASE_DURATION_SECONDS),
                        acquire_time: Some(MicroTime(now)),
                        renew_time: Some(MicroTime(now)),
                        lease_transitions: Some(0),
                    }),
                };
                leases.create(&PostParams::default(), &lease).await?;
                info!(identity = %self.identity, "Created leader lease");
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn release(&self, leases: &Api<Lease>) {
        if !self.is_leader.load(Ordering::Relaxed) {
            return;
        }
        info!(identity = %self.identity, "Releasing leader lease");
        let patch = serde_json::json!({
            "spec": {
                "holderIdentity": null,
            }
        });
        let _ = leases
            .patch(LEASE_NAME, &PatchParams::default(), &Patch::Merge(patch))
            .await;
        self.is_leader.store(false, Ordering::Relaxed);
    }
}
