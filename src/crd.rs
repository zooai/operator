//! Custom Resource Definitions for Zoo Network

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ───────────────────────── ZooNetwork ─────────────────────────

/// ZooNetwork is the primary CRD for deploying Zoo validator node clusters.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "zoo.network",
    version = "v1alpha1",
    kind = "ZooNetwork",
    namespaced,
    status = "ZooNetworkStatus",
    shortname = "zoonet",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.readyValidators"}"#,
    printcolumn = r#"{"name":"Total","type":"string","jsonPath":".status.totalValidators"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ZooNetworkSpec {
    /// Network ID (1=mainnet, 2=testnet, 3=devnet, custom)
    pub network_id: u32,

    /// Number of validator nodes
    pub validators: u32,

    /// Image configuration
    #[serde(default)]
    pub image: ImageSpec,

    /// Storage configuration
    #[serde(default)]
    pub storage: StorageSpec,

    /// Resource requirements
    #[serde(default)]
    pub resources: ResourceSpec,

    /// Node configuration overrides
    #[serde(default)]
    pub config: BTreeMap<String, serde_json::Value>,

    /// Genesis configuration (for custom networks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genesis: Option<serde_json::Value>,

    /// Genesis ConfigMap name (for pre-existing genesis)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genesis_config_map: Option<String>,

    /// Metrics configuration
    #[serde(default)]
    pub metrics: MetricsSpec,

    /// Service configuration
    #[serde(default)]
    pub service: ServiceSpec,

    /// Port configuration
    #[serde(default)]
    pub ports: PortSpec,

    /// Staking key configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staking: Option<StakingSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ZooNetworkStatus {
    /// Current phase: Pending, Creating, Bootstrapping, Running, Degraded
    #[serde(default)]
    pub phase: String,

    /// Number of ready validator pods
    #[serde(default)]
    pub ready_validators: u32,

    /// Total number of desired validator pods
    #[serde(default)]
    pub total_validators: u32,

    /// Human-readable message
    #[serde(default)]
    pub message: String,
}

// ───────────────────────── ZooChain ─────────────────────────

/// ZooChain tracks Zoo EVM chain deployments.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "zoo.network",
    version = "v1alpha1",
    kind = "ZooChain",
    namespaced,
    status = "ZooChainStatus",
    shortname = "zoochain",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"ChainID","type":"string","jsonPath":".status.chainId"}"#,
    printcolumn = r#"{"name":"BlockchainID","type":"string","jsonPath":".status.blockchainId"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ZooChainSpec {
    /// EVM chain ID
    pub chain_id: u64,

    /// Platform blockchain ID (base58)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blockchain_id: Option<String>,

    /// VM ID (base58)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_id: Option<String>,

    /// Genesis ConfigMap name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genesis_config_map: Option<String>,

    /// Reference to the ZooNetwork managing this chain
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_ref: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ZooChainStatus {
    #[serde(default)]
    pub phase: String,

    #[serde(default)]
    pub chain_id: String,

    #[serde(default)]
    pub blockchain_id: String,

    #[serde(default)]
    pub message: String,
}

// ───────────────────────── ZooExplorer ─────────────────────────

/// ZooExplorer manages Lux explorer (Go binary) instances for Zoo chains.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "zoo.network",
    version = "v1alpha1",
    kind = "ZooExplorer",
    namespaced,
    status = "ZooExplorerStatus",
    shortname = "zooexp",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"URL","type":"string","jsonPath":".status.url"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ZooExplorerSpec {
    /// Reference to the ZooChain this explorer indexes
    pub chain_ref: String,

    /// Explorer image
    #[serde(default = "default_explorer_image")]
    pub image: String,

    /// RPC endpoint URL for the chain to index
    pub rpc_endpoint: String,

    /// Chain identifier passed to --chain (e.g. "cchain", "zoo")
    pub chain_name: String,

    /// Coin symbol (e.g. "ZOO")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin_symbol: Option<String>,

    /// Service configuration
    #[serde(default)]
    pub service: ExplorerServiceSpec,
}

fn default_explorer_image() -> String {
    "ghcr.io/luxfi/explorer:1.2.18".to_string()
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerServiceSpec {
    /// Service type (ClusterIP, LoadBalancer, NodePort)
    #[serde(default = "default_service_type")]
    pub service_type: String,

    /// Service port
    #[serde(default = "default_explorer_port")]
    pub port: u16,
}

fn default_explorer_port() -> u16 {
    8090
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ZooExplorerStatus {
    #[serde(default)]
    pub phase: String,

    #[serde(default)]
    pub url: String,

    #[serde(default)]
    pub message: String,
}

// ───────────────────────── ZooGateway ─────────────────────────

/// ZooGateway manages API gateway/ingress for Zoo RPC endpoints.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "zoo.network",
    version = "v1alpha1",
    kind = "ZooGateway",
    namespaced,
    status = "ZooGatewayStatus",
    shortname = "zoogw",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Host","type":"string","jsonPath":".spec.hosts[0]"}"#,
    printcolumn = r#"{"name":"Routes","type":"integer","jsonPath":".status.routeCount"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ZooGatewaySpec {
    /// Reference to the ZooNetwork this gateway routes to
    pub network_ref: String,

    /// Hostnames for the gateway
    pub hosts: Vec<String>,

    /// TLS configuration
    #[serde(default)]
    pub tls: TlsSpec,

    /// Service configuration
    #[serde(default)]
    pub service: GatewayServiceSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsSpec {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,

    /// Secret name containing TLS cert/key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GatewayServiceSpec {
    /// Service type (ClusterIP, LoadBalancer)
    #[serde(default = "default_service_type")]
    pub service_type: String,

    /// RPC port
    #[serde(default = "default_rpc_port")]
    pub rpc_port: u16,

    /// WebSocket port
    #[serde(default = "default_ws_port")]
    pub ws_port: u16,
}

fn default_rpc_port() -> u16 {
    9650
}

fn default_ws_port() -> u16 {
    9651
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ZooGatewayStatus {
    #[serde(default)]
    pub phase: String,

    #[serde(default)]
    pub route_count: u32,

    #[serde(default)]
    pub message: String,
}

// ───────────────────────── Shared Types ─────────────────────────

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImageSpec {
    /// Container image repository
    #[serde(default = "default_image")]
    pub repository: String,

    /// Image tag
    #[serde(default = "default_tag")]
    pub tag: String,

    /// Image pull policy
    #[serde(default = "default_pull_policy")]
    pub pull_policy: String,
}

impl Default for ImageSpec {
    fn default() -> Self {
        Self {
            repository: default_image(),
            tag: default_tag(),
            pull_policy: default_pull_policy(),
        }
    }
}

fn default_image() -> String {
    "ghcr.io/zoo-labs/node".to_string()
}

fn default_tag() -> String {
    "latest".to_string()
}

fn default_pull_policy() -> String {
    "IfNotPresent".to_string()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// Storage class name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,

    /// Storage size
    #[serde(default = "default_storage_size")]
    pub size: String,
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            storage_class: None,
            size: default_storage_size(),
        }
    }
}

fn default_storage_size() -> String {
    "100Gi".to_string()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpec {
    /// CPU request
    #[serde(default = "default_cpu_request")]
    pub cpu_request: String,

    /// CPU limit
    #[serde(default = "default_cpu_limit")]
    pub cpu_limit: String,

    /// Memory request
    #[serde(default = "default_memory_request")]
    pub memory_request: String,

    /// Memory limit
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            cpu_request: default_cpu_request(),
            cpu_limit: default_cpu_limit(),
            memory_request: default_memory_request(),
            memory_limit: default_memory_limit(),
        }
    }
}

fn default_cpu_request() -> String {
    "2".to_string()
}

fn default_cpu_limit() -> String {
    "4".to_string()
}

fn default_memory_request() -> String {
    "4Gi".to_string()
}

fn default_memory_limit() -> String {
    "8Gi".to_string()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSpec {
    /// Enable metrics
    #[serde(default)]
    pub enabled: bool,

    /// Metrics port
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_metrics_port(),
        }
    }
}

fn default_metrics_port() -> u16 {
    9090
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    /// Service type (ClusterIP, LoadBalancer, NodePort)
    #[serde(default = "default_service_type")]
    pub service_type: String,

    /// Enable per-pod services (one Service per validator pod)
    #[serde(default)]
    pub per_pod_services: bool,
}

impl Default for ServiceSpec {
    fn default() -> Self {
        Self {
            service_type: default_service_type(),
            per_pod_services: false,
        }
    }
}

fn default_service_type() -> String {
    "ClusterIP".to_string()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PortSpec {
    /// P2P staking port
    #[serde(default = "default_staking_port")]
    pub staking: u16,

    /// HTTP API port
    #[serde(default = "default_http_port")]
    pub http: u16,
}

impl Default for PortSpec {
    fn default() -> Self {
        Self {
            staking: default_staking_port(),
            http: default_http_port(),
        }
    }
}

fn default_staking_port() -> u16 {
    9651
}

fn default_http_port() -> u16 {
    9650
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StakingSpec {
    /// Secret name containing staking TLS cert/key per validator
    pub secret_prefix: String,
}
