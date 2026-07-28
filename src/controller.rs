//! Kubernetes controllers for Zoo Network custom resources.

use crate::crd::{
    ZooChain, ZooChainStatus, ZooExplorer, ZooExplorerStatus, ZooGateway, ZooGatewayStatus,
    ZooNetwork, ZooNetworkStatus,
};
use crate::error::{OperatorError, Result};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe, ResourceRequirements, Service,
    ServicePort, ServiceSpec as K8sServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher::Config as WatcherConfig,
    },
    Client, ResourceExt,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Controller context shared across all reconcile functions.
pub struct Context {
    pub client: Client,
}

// ───────────────────────── ZooNetwork Controller ─────────────────────────

pub async fn run_network_controller(client: Client, namespace: String) -> Result<()> {
    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    let networks: Api<ZooNetwork> = if namespace.is_empty() {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), &namespace)
    };

    info!("Starting ZooNetwork controller");

    Controller::new(networks, WatcherConfig::default())
        .run(reconcile_network, network_error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled network: {:?}", o),
                Err(e) => error!("Network reconcile error: {:?}", e),
            }
        })
        .await;

    Ok(())
}

fn network_error_policy(
    network: Arc<ZooNetwork>,
    error: &OperatorError,
    _ctx: Arc<Context>,
) -> Action {
    crate::metrics::record_reconcile(&network.name_any(), "error", Instant::now());
    error!(
        "Error reconciling network {}: {:?}",
        network.name_any(),
        error
    );
    Action::requeue(Duration::from_secs(30))
}

async fn reconcile_network(network: Arc<ZooNetwork>, ctx: Arc<Context>) -> Result<Action> {
    let start = Instant::now();
    let name = network.name_any();
    let namespace = network.namespace().unwrap_or_else(|| "default".to_string());

    info!("Reconciling ZooNetwork {}/{}", namespace, name);

    let networks: Api<ZooNetwork> = Api::namespaced(ctx.client.clone(), &namespace);
    let current_status = network.status.clone().unwrap_or_default();
    let phase = current_status.phase.as_str();

    let new_status = match phase {
        "" | "Pending" => {
            info!("Network {} is pending, starting creation", name);
            create_network(&network, &ctx).await?
        }
        "Creating" => {
            info!("Network {} is creating, checking progress", name);
            check_creation_progress(&network, &ctx).await?
        }
        "Running" => {
            debug!("Network {} is running, checking health", name);

            // Sync StatefulSet replicas if changed
            let statefulsets: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);
            if let Ok(sts) = statefulsets.get(&name).await {
                let current = sts
                    .spec
                    .as_ref()
                    .and_then(|s| s.replicas)
                    .unwrap_or(0) as u32;
                if current != network.spec.validators {
                    info!(
                        "Network {} scaling: {} -> {} validators",
                        name, current, network.spec.validators
                    );
                    let patch = Patch::Merge(serde_json::json!({
                        "spec": { "replicas": network.spec.validators }
                    }));
                    statefulsets
                        .patch(&name, &PatchParams::default(), &patch)
                        .await
                        .map_err(OperatorError::KubeApi)?;
                }
            }

            check_health(&network, &ctx).await?
        }
        "Degraded" => {
            warn!("Network {} is degraded, re-checking", name);
            check_health(&network, &ctx).await?
        }
        _ => {
            warn!("Network {} unknown phase: {}", name, phase);
            current_status
        }
    };

    // Update status
    let patch = Patch::Merge(serde_json::json!({ "status": new_status }));
    networks
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(OperatorError::KubeApi)?;

    crate::metrics::record_reconcile(&name, "success", start);
    crate::metrics::set_network_phase(&name, &new_status.phase);
    crate::metrics::set_validators(
        &name,
        new_status.ready_validators,
        new_status.total_validators,
    );

    let requeue = match new_status.phase.as_str() {
        "Running" => Duration::from_secs(60),
        "Degraded" => Duration::from_secs(15),
        _ => Duration::from_secs(10),
    };

    Ok(Action::requeue(requeue))
}

/// Create the StatefulSet, headless Service, and RPC Service for a new ZooNetwork.
async fn create_network(network: &ZooNetwork, ctx: &Context) -> Result<ZooNetworkStatus> {
    let name = network.name_any();
    let namespace = network.namespace().unwrap_or_else(|| "default".to_string());

    let owner_ref = OwnerReference {
        api_version: "zoo.network/v1alpha1".to_string(),
        kind: "ZooNetwork".to_string(),
        name: name.clone(),
        uid: network.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    };

    let labels: BTreeMap<String, String> = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "zoo-node".to_string()),
        (
            "app.kubernetes.io/instance".to_string(),
            name.clone(),
        ),
        (
            "app.kubernetes.io/managed-by".to_string(),
            "zoo-operator".to_string(),
        ),
        (
            "zoo.network/network-id".to_string(),
            network.spec.network_id.to_string(),
        ),
    ]);

    // Headless Service for peer discovery
    let headless_svc = Service {
        metadata: kube::core::ObjectMeta {
            name: Some(format!("{}-headless", name)),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(labels.clone()),
            ports: Some(vec![
                ServicePort {
                    name: Some("staking".to_string()),
                    port: network.spec.ports.staking as i32,
                    target_port: Some(IntOrString::Int(network.spec.ports.staking as i32)),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("http".to_string()),
                    port: network.spec.ports.http as i32,
                    target_port: Some(IntOrString::Int(network.spec.ports.http as i32)),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };

    // RPC Service
    let rpc_svc = Service {
        metadata: kube::core::ObjectMeta {
            name: Some(format!("{}-rpc", name)),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            type_: Some(network.spec.service.service_type.clone()),
            selector: Some(labels.clone()),
            ports: Some(vec![ServicePort {
                name: Some("http".to_string()),
                port: network.spec.ports.http as i32,
                target_port: Some(IntOrString::Int(network.spec.ports.http as i32)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    // StatefulSet
    let image = format!("{}:{}", network.spec.image.repository, network.spec.image.tag);

    let mut env_vars = vec![
        EnvVar {
            name: "ZOO_NETWORK_ID".to_string(),
            value: Some(network.spec.network_id.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "ZOO_HTTP_PORT".to_string(),
            value: Some(network.spec.ports.http.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "ZOO_STAKING_PORT".to_string(),
            value: Some(network.spec.ports.staking.to_string()),
            ..Default::default()
        },
    ];

    // spec.config entries become both LUXD_*-prefixed env vars (viper
    // EnvPrefix in luxfi/node/config) AND --kebab-case CLI args. luxd
    // resolves the same setting via either path; emitting both means a
    // user can override at runtime via env without rebuilding the
    // CR, and the CLI args show up in `ps`/k8s describe for forensics.
    //
    // Key mapping: spec.config key `BOOTSTRAP_IPS` →
    //   env: LUXD_BOOTSTRAP_IPS=<value>
    //   arg: --bootstrap-ips=<value>
    let mut config_args: Vec<String> = Vec::new();
    for (key, value) in &network.spec.config {
        let val_str = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let env_name = format!("LUXD_{}", key.to_uppercase().replace('-', "_"));
        env_vars.push(EnvVar {
            name: env_name,
            value: Some(val_str.clone()),
            ..Default::default()
        });
        let flag = key.to_lowercase().replace('_', "-");
        config_args.push(format!("--{}={}", flag, val_str));
    }
    // Always-on baseline args (network-id + ports). spec.config can
    // override by re-declaring the same key; later-wins is fine since
    // luxd's flag parser takes the last value.
    let mut node_args: Vec<String> = vec![
        format!("--network-id={}", network.spec.network_id),
        format!("--http-host=0.0.0.0"),
        format!("--http-port={}", network.spec.ports.http),
        format!("--staking-port={}", network.spec.ports.staking),
        format!("--data-dir=/data"),
        format!("--plugin-dir=/data/plugins"),
    ];
    node_args.extend(config_args);

    let mut resources = BTreeMap::new();
    resources.insert(
        "cpu".to_string(),
        Quantity(network.spec.resources.cpu_request.clone()),
    );
    resources.insert(
        "memory".to_string(),
        Quantity(network.spec.resources.memory_request.clone()),
    );

    let mut limits = BTreeMap::new();
    limits.insert(
        "cpu".to_string(),
        Quantity(network.spec.resources.cpu_limit.clone()),
    );
    limits.insert(
        "memory".to_string(),
        Quantity(network.spec.resources.memory_limit.clone()),
    );

    let sts = StatefulSet {
        metadata: kube::core::ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(network.spec.validators as i32),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            service_name: format!("{}-headless", name),
            template: PodTemplateSpec {
                metadata: Some(kube::core::ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "zoo-node".to_string(),
                        image: Some(image),
                        image_pull_policy: Some(network.spec.image.pull_policy.clone()),
                        ports: Some(vec![
                            ContainerPort {
                                name: Some("staking".to_string()),
                                container_port: network.spec.ports.staking as i32,
                                protocol: Some("TCP".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                name: Some("http".to_string()),
                                container_port: network.spec.ports.http as i32,
                                protocol: Some("TCP".to_string()),
                                ..Default::default()
                            },
                        ]),
                        args: Some(node_args),
                        env: Some(env_vars),
                        resources: Some(ResourceRequirements {
                            requests: Some(resources),
                            limits: Some(limits),
                            ..Default::default()
                        }),
                        volume_mounts: Some(vec![VolumeMount {
                            name: "data".to_string(),
                            mount_path: "/data".to_string(),
                            ..Default::default()
                        }]),
                        liveness_probe: Some(Probe {
                            http_get: Some(
                                k8s_openapi::api::core::v1::HTTPGetAction {
                                    path: Some("/ext/health".to_string()),
                                    port: IntOrString::Int(network.spec.ports.http as i32),
                                    ..Default::default()
                                },
                            ),
                            initial_delay_seconds: Some(30),
                            period_seconds: Some(30),
                            ..Default::default()
                        }),
                        readiness_probe: Some(Probe {
                            http_get: Some(
                                k8s_openapi::api::core::v1::HTTPGetAction {
                                    path: Some("/ext/health".to_string()),
                                    port: IntOrString::Int(network.spec.ports.http as i32),
                                    ..Default::default()
                                },
                            ),
                            initial_delay_seconds: Some(10),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: kube::core::ObjectMeta {
                    name: Some("data".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from([(
                            "storage".to_string(),
                            Quantity(network.spec.storage.size.clone()),
                        )])),
                        ..Default::default()
                    }),
                    storage_class_name: network.spec.storage.storage_class.clone(),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Apply resources via server-side apply
    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let statefulsets: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);
    let pp = PatchParams::apply("zoo-operator").force();

    services
        .patch(
            &format!("{}-headless", name),
            &pp,
            &Patch::Apply(headless_svc),
        )
        .await
        .map_err(OperatorError::KubeApi)?;

    services
        .patch(&format!("{}-rpc", name), &pp, &Patch::Apply(rpc_svc))
        .await
        .map_err(OperatorError::KubeApi)?;

    statefulsets
        .patch(&name, &pp, &Patch::Apply(sts))
        .await
        .map_err(OperatorError::KubeApi)?;

    info!("Created resources for network {}", name);

    Ok(ZooNetworkStatus {
        phase: "Creating".to_string(),
        ready_validators: 0,
        total_validators: network.spec.validators,
        message: "Resources created, waiting for pods".to_string(),
    })
}

/// Check if all pods in the StatefulSet are ready.
async fn check_creation_progress(
    network: &ZooNetwork,
    ctx: &Context,
) -> Result<ZooNetworkStatus> {
    let name = network.name_any();
    let namespace = network.namespace().unwrap_or_else(|| "default".to_string());

    let statefulsets: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);

    match statefulsets.get(&name).await {
        Ok(sts) => {
            let ready = sts
                .status
                .as_ref()
                .and_then(|s| s.ready_replicas)
                .unwrap_or(0) as u32;
            let desired = network.spec.validators;

            if ready >= desired {
                info!("Network {} all {} validators ready", name, desired);
                Ok(ZooNetworkStatus {
                    phase: "Running".to_string(),
                    ready_validators: ready,
                    total_validators: desired,
                    message: "All validators ready".to_string(),
                })
            } else {
                info!(
                    "Network {} waiting for validators: {}/{}",
                    name, ready, desired
                );
                Ok(ZooNetworkStatus {
                    phase: "Creating".to_string(),
                    ready_validators: ready,
                    total_validators: desired,
                    message: format!("Waiting for validators: {}/{}", ready, desired),
                })
            }
        }
        Err(e) => {
            warn!("Network {} StatefulSet not found: {}", name, e);
            Ok(ZooNetworkStatus {
                phase: "Pending".to_string(),
                ready_validators: 0,
                total_validators: network.spec.validators,
                message: "StatefulSet not found, will recreate".to_string(),
            })
        }
    }
}

/// Check health of a running network.
async fn check_health(network: &ZooNetwork, ctx: &Context) -> Result<ZooNetworkStatus> {
    let name = network.name_any();
    let namespace = network.namespace().unwrap_or_else(|| "default".to_string());

    let statefulsets: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), &namespace);

    match statefulsets.get(&name).await {
        Ok(sts) => {
            let ready = sts
                .status
                .as_ref()
                .and_then(|s| s.ready_replicas)
                .unwrap_or(0) as u32;
            let desired = network.spec.validators;

            let phase = if ready >= desired {
                "Running"
            } else if ready > 0 {
                "Degraded"
            } else {
                "Creating"
            };

            Ok(ZooNetworkStatus {
                phase: phase.to_string(),
                ready_validators: ready,
                total_validators: desired,
                message: format!("{}/{} validators ready", ready, desired),
            })
        }
        Err(e) => {
            error!("Network {} health check failed: {}", name, e);
            Ok(ZooNetworkStatus {
                phase: "Degraded".to_string(),
                ready_validators: 0,
                total_validators: network.spec.validators,
                message: format!("StatefulSet check failed: {}", e),
            })
        }
    }
}

// ───────────────────────── ZooChain Controller ─────────────────────────

pub async fn run_chain_controller(client: Client, namespace: String) -> Result<()> {
    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    let chains: Api<ZooChain> = if namespace.is_empty() {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), &namespace)
    };

    info!("Starting ZooChain controller");

    Controller::new(chains, WatcherConfig::default())
        .run(reconcile_chain, chain_error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled chain: {:?}", o),
                Err(e) => error!("Chain reconcile error: {:?}", e),
            }
        })
        .await;

    Ok(())
}

fn chain_error_policy(
    chain: Arc<ZooChain>,
    error: &OperatorError,
    _ctx: Arc<Context>,
) -> Action {
    crate::metrics::record_reconcile(&chain.name_any(), "error", Instant::now());
    error!(
        "Error reconciling chain {}: {:?}",
        chain.name_any(),
        error
    );
    Action::requeue(Duration::from_secs(30))
}

async fn reconcile_chain(chain: Arc<ZooChain>, ctx: Arc<Context>) -> Result<Action> {
    let start = Instant::now();
    let name = chain.name_any();
    let namespace = chain.namespace().unwrap_or_else(|| "default".to_string());

    info!("Reconciling ZooChain {}/{}", namespace, name);

    let chains: Api<ZooChain> = Api::namespaced(ctx.client.clone(), &namespace);

    let status = ZooChainStatus {
        phase: "Active".to_string(),
        chain_id: chain.spec.chain_id.to_string(),
        blockchain_id: chain.spec.blockchain_id.clone().unwrap_or_default(),
        message: "Chain tracked".to_string(),
    };

    let patch = Patch::Merge(serde_json::json!({ "status": status }));
    chains
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(OperatorError::KubeApi)?;

    crate::metrics::record_reconcile(&name, "success", start);
    Ok(Action::requeue(Duration::from_secs(300)))
}

// ───────────────────────── ZooExplorer Controller ─────────────────────────

pub async fn run_explorer_controller(client: Client, namespace: String) -> Result<()> {
    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    let explorers: Api<ZooExplorer> = if namespace.is_empty() {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), &namespace)
    };

    info!("Starting ZooExplorer controller");

    Controller::new(explorers, WatcherConfig::default())
        .run(reconcile_explorer, explorer_error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled explorer: {:?}", o),
                Err(e) => error!("Explorer reconcile error: {:?}", e),
            }
        })
        .await;

    Ok(())
}

fn explorer_error_policy(
    explorer: Arc<ZooExplorer>,
    error: &OperatorError,
    _ctx: Arc<Context>,
) -> Action {
    crate::metrics::record_reconcile(&explorer.name_any(), "error", Instant::now());
    error!(
        "Error reconciling explorer {}: {:?}",
        explorer.name_any(),
        error
    );
    Action::requeue(Duration::from_secs(30))
}

async fn reconcile_explorer(
    explorer: Arc<ZooExplorer>,
    ctx: Arc<Context>,
) -> Result<Action> {
    let start = Instant::now();
    let name = explorer.name_any();
    let namespace = explorer.namespace().unwrap_or_else(|| "default".to_string());
    let spec = &explorer.spec;

    info!("Reconciling ZooExplorer {}/{}", namespace, name);

    let deploy_name = format!("{}-explorer", name);
    let port = spec.service.port as i32;

    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), "zoo-explorer".to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), name.clone());
    labels.insert("app.kubernetes.io/component".to_string(), "explorer".to_string());
    labels.insert("app.kubernetes.io/managed-by".to_string(), "zoo-operator".to_string());

    let owner_ref = OwnerReference {
        api_version: "zoo.network/v1alpha1".to_string(),
        kind: "ZooExplorer".to_string(),
        name: name.clone(),
        uid: explorer.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    };

    // luxfi/explorer is configured by a chains.yaml file, not by ad-hoc flags —
    // it only defines -config/-data/-http/-mdns, so the old --rpc/--chain/--port
    // made it exit 2 on every start. Render the CR into that file instead. JSON
    // is valid YAML, so serde_json both builds and escapes the document.
    let coin = spec.coin_symbol.clone().unwrap_or_default();
    let chains_yaml = serde_json::json!({
        "data_dir": "/data",
        "http_addr": format!(":{port}"),
        "brand_default": { "name": &name, "coin": &coin },
        "chains": [{
            "slug": &spec.chain_name,
            "name": &spec.chain_name,
            "type": "evm",
            "rpc": &spec.rpc_endpoint,
            "coin": &coin,
            "enabled": true,
            "default": true,
        }],
    })
    .to_string();

    let config_map = ConfigMap {
        metadata: kube::core::ObjectMeta {
            name: Some(deploy_name.clone()),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([("chains.yaml".to_string(), chains_yaml)])),
        ..Default::default()
    };

    let args = vec!["--config=/etc/explorer/chains.yaml".to_string()];

    let deployment = Deployment {
        metadata: kube::core::ObjectMeta {
            name: Some(deploy_name.clone()),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref.clone()]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(kube::core::ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "indexer".to_string(),
                        image: Some(spec.image.clone()),
                        args: Some(args),
                        ports: Some(vec![ContainerPort {
                            container_port: port,
                            name: Some("http".to_string()),
                            ..Default::default()
                        }]),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                name: "config".to_string(),
                                mount_path: "/etc/explorer".to_string(),
                                read_only: Some(true),
                                ..Default::default()
                            },
                            VolumeMount {
                                name: "data".to_string(),
                                mount_path: "/data".to_string(),
                                ..Default::default()
                            },
                        ]),
                        liveness_probe: Some(Probe {
                            http_get: Some(
                                k8s_openapi::api::core::v1::HTTPGetAction {
                                    path: Some("/health".to_string()),
                                    port: IntOrString::Int(port),
                                    ..Default::default()
                                },
                            ),
                            initial_delay_seconds: Some(10),
                            period_seconds: Some(30),
                            ..Default::default()
                        }),
                        readiness_probe: Some(Probe {
                            http_get: Some(
                                k8s_openapi::api::core::v1::HTTPGetAction {
                                    path: Some("/health".to_string()),
                                    port: IntOrString::Int(port),
                                    ..Default::default()
                                },
                            ),
                            initial_delay_seconds: Some(5),
                            period_seconds: Some(10),
                            ..Default::default()
                        }),
                        security_context: Some(k8s_openapi::api::core::v1::SecurityContext {
                            run_as_non_root: Some(true),
                            run_as_user: Some(65532),
                            read_only_root_filesystem: Some(false),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    volumes: Some(vec![
                        Volume {
                            name: "config".to_string(),
                            config_map: Some(ConfigMapVolumeSource {
                                name: Some(deploy_name.clone()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        Volume {
                            name: "data".to_string(),
                            empty_dir: Some(Default::default()),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let svc = Service {
        metadata: kube::core::ObjectMeta {
            name: Some(deploy_name.clone()),
            namespace: Some(namespace.clone()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_ref]),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            selector: Some(labels),
            ports: Some(vec![ServicePort {
                name: Some("http".to_string()),
                port: port,
                target_port: Some(IntOrString::Int(port)),
                ..Default::default()
            }]),
            type_: Some(spec.service.service_type.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let pp = PatchParams::apply("zoo-operator").force();
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);
    cm_api
        .patch(&deploy_name, &pp, &Patch::Apply(config_map))
        .await
        .map_err(OperatorError::KubeApi)?;

    let deploy_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    deploy_api
        .patch(&deploy_name, &pp, &Patch::Apply(deployment))
        .await
        .map_err(OperatorError::KubeApi)?;

    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    svc_api
        .patch(&deploy_name, &pp, &Patch::Apply(svc))
        .await
        .map_err(OperatorError::KubeApi)?;

    // Check readiness
    let ready = match deploy_api.get(&deploy_name).await {
        Ok(d) => d.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0) > 0,
        Err(_) => false,
    };

    let explorers: Api<ZooExplorer> = Api::namespaced(ctx.client.clone(), &namespace);
    let status = ZooExplorerStatus {
        phase: if ready { "Ready" } else { "Creating" }.to_string(),
        url: String::new(),
        message: String::new(),
    };

    let patch = Patch::Merge(serde_json::json!({ "status": status }));
    explorers
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(OperatorError::KubeApi)?;

    crate::metrics::record_reconcile(&name, "success", start);
    Ok(Action::requeue(Duration::from_secs(60)))
}

// ───────────────────────── ZooGateway Controller ─────────────────────────

pub async fn run_gateway_controller(client: Client, namespace: String) -> Result<()> {
    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    let gateways: Api<ZooGateway> = if namespace.is_empty() {
        Api::all(client.clone())
    } else {
        Api::namespaced(client.clone(), &namespace)
    };

    info!("Starting ZooGateway controller");

    Controller::new(gateways, WatcherConfig::default())
        .run(reconcile_gateway, gateway_error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled gateway: {:?}", o),
                Err(e) => error!("Gateway reconcile error: {:?}", e),
            }
        })
        .await;

    Ok(())
}

fn gateway_error_policy(
    gateway: Arc<ZooGateway>,
    error: &OperatorError,
    _ctx: Arc<Context>,
) -> Action {
    crate::metrics::record_reconcile(&gateway.name_any(), "error", Instant::now());
    error!(
        "Error reconciling gateway {}: {:?}",
        gateway.name_any(),
        error
    );
    Action::requeue(Duration::from_secs(30))
}

async fn reconcile_gateway(
    gateway: Arc<ZooGateway>,
    ctx: Arc<Context>,
) -> Result<Action> {
    let start = Instant::now();
    let name = gateway.name_any();
    let namespace = gateway.namespace().unwrap_or_else(|| "default".to_string());

    info!("Reconciling ZooGateway {}/{}", namespace, name);

    let gateways: Api<ZooGateway> = Api::namespaced(ctx.client.clone(), &namespace);

    let status = ZooGatewayStatus {
        phase: "Pending".to_string(),
        route_count: gateway.spec.hosts.len() as u32,
        message: "Gateway tracked, ingress managed externally".to_string(),
    };

    let patch = Patch::Merge(serde_json::json!({ "status": status }));
    gateways
        .patch_status(&name, &PatchParams::default(), &patch)
        .await
        .map_err(OperatorError::KubeApi)?;

    crate::metrics::record_reconcile(&name, "success", start);
    Ok(Action::requeue(Duration::from_secs(300)))
}
