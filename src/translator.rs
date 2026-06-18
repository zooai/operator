//! Pure-functional translation between Zoo brand CRDs and the canonical
//! `bootno.de` web3-orchestration semantics owned by `bootnode/operator`.
//!
//! The Zoo operator is a thin whitelabel shim: it owns the four
//! `zoo.network/Zoo*` Kinds so Zoo operators keep brand-native CRs, but the
//! actual reconcile work is delegated to the canonical bootnode orchestrator.
//! Every byte of brand-to-canonical mapping lives here, in one place, as pure
//! functions with no I/O — so it is fully unit-testable without a cluster.
//!
//! Direction of travel:
//!   spec  : `Zoo*Spec`        -> `bootnode::*Spec`   (request, brand -> canonical)
//!   status: `bootnode::*Status` -> `Zoo*Status`      (result,  canonical -> brand)
//!
//! These canonical types intentionally mirror — and are kept byte-compatible
//! with — `bootno.de/v1alpha1` as produced by `bootnode/operator` (Wave 1B).
//! They live here as plain serde structs so the translator compiles and is
//! testable before the bootnode crate is published; once bootnode ships as a
//! library these become a re-export (see `delegate` backend wiring).

use crate::crd::{
    ZooChainSpec, ZooChainStatus, ZooExplorerSpec, ZooExplorerStatus, ZooGatewaySpec,
    ZooGatewayStatus, ZooNetworkSpec, ZooNetworkStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Brand tag carried on every canonical resource so the bootnode orchestrator
/// can apply whitelabel-specific defaults (image registry, branding labels)
/// without the shim leaking brand strings into reconcile logic.
pub const BRAND: &str = "zoo";

/// Canonical group the bootnode orchestrator owns.
pub const CANONICAL_GROUP: &str = "bootno.de";

/// Canonical API version the bootnode orchestrator serves.
pub const CANONICAL_VERSION: &str = "v1alpha1";

// ───────────────────────── Canonical bootno.de shapes ─────────────────────────

/// Canonical `bootno.de/Network` spec — brand-neutral web3 validator cluster.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSpec {
    /// Owning brand (whitelabel tenant): "zoo", "lux", "hanzo", ...
    pub brand: String,
    pub network_id: u32,
    pub validators: u32,
    pub image: ImageSpec,
    pub storage: StorageSpec,
    pub resources: ResourceSpec,
    pub config: BTreeMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genesis: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genesis_config_map: Option<String>,
    pub metrics: MetricsSpec,
    pub service: ServiceSpec,
    pub ports: PortSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staking: Option<StakingSpec>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NetworkStatus {
    pub phase: String,
    pub ready_validators: u32,
    pub total_validators: u32,
    pub message: String,
}

/// Canonical `bootno.de/Chain` spec — EVM chain tracked against a Network.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChainSpec {
    pub brand: String,
    pub chain_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blockchain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genesis_config_map: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_ref: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChainStatus {
    pub phase: String,
    pub chain_id: String,
    pub blockchain_id: String,
    pub message: String,
}

/// Canonical `bootno.de/Explorer` spec — block explorer/indexer for a Chain.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerSpec {
    pub brand: String,
    pub chain_ref: String,
    pub image: String,
    pub rpc_endpoint: String,
    pub chain_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin_symbol: Option<String>,
    pub service: ExplorerServiceSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerStatus {
    pub phase: String,
    pub url: String,
    pub message: String,
}

/// Canonical `bootno.de/Gateway` spec — RPC gateway/ingress for a Network.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewaySpec {
    pub brand: String,
    pub network_ref: String,
    pub hosts: Vec<String>,
    pub tls: TlsSpec,
    pub service: GatewayServiceSpec,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayStatus {
    pub phase: String,
    pub route_count: u32,
    pub message: String,
}

// Shared canonical sub-shapes. These mirror the brand CRD sub-specs 1:1, so
// the translator is a field-for-field copy rather than a lossy projection —
// keeping the mapping total and reversible.

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ImageSpec {
    pub repository: String,
    pub tag: String,
    pub pull_policy: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    pub size: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpec {
    pub cpu_request: String,
    pub cpu_limit: String,
    pub memory_request: String,
    pub memory_limit: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSpec {
    pub enabled: bool,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    pub service_type: String,
    pub per_pod_services: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PortSpec {
    pub staking: u16,
    pub http: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StakingSpec {
    pub secret_prefix: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerServiceSpec {
    pub service_type: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsSpec {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GatewayServiceSpec {
    pub service_type: String,
    pub rpc_port: u16,
    pub ws_port: u16,
}

// ───────────────────────── Network translation ─────────────────────────

/// Translate a brand `ZooNetworkSpec` into the canonical bootnode `NetworkSpec`.
/// Defaults on the brand spec are already resolved by serde at deserialize
/// time, so this is a total field-for-field map plus the brand tag.
pub fn network_to_canonical(spec: &ZooNetworkSpec) -> NetworkSpec {
    NetworkSpec {
        brand: BRAND.to_string(),
        network_id: spec.network_id,
        validators: spec.validators,
        image: ImageSpec {
            repository: spec.image.repository.clone(),
            tag: spec.image.tag.clone(),
            pull_policy: spec.image.pull_policy.clone(),
        },
        storage: StorageSpec {
            storage_class: spec.storage.storage_class.clone(),
            size: spec.storage.size.clone(),
        },
        resources: ResourceSpec {
            cpu_request: spec.resources.cpu_request.clone(),
            cpu_limit: spec.resources.cpu_limit.clone(),
            memory_request: spec.resources.memory_request.clone(),
            memory_limit: spec.resources.memory_limit.clone(),
        },
        config: spec.config.clone(),
        genesis: spec.genesis.clone(),
        genesis_config_map: spec.genesis_config_map.clone(),
        metrics: MetricsSpec {
            enabled: spec.metrics.enabled,
            port: spec.metrics.port,
        },
        service: ServiceSpec {
            service_type: spec.service.service_type.clone(),
            per_pod_services: spec.service.per_pod_services,
        },
        ports: PortSpec {
            staking: spec.ports.staking,
            http: spec.ports.http,
        },
        staking: spec.staking.as_ref().map(|s| StakingSpec {
            secret_prefix: s.secret_prefix.clone(),
        }),
    }
}

/// Translate a canonical bootnode `NetworkStatus` back into the brand
/// `ZooNetworkStatus` written to the `zoo.network/ZooNetwork` `.status`.
pub fn network_from_canonical(status: &NetworkStatus) -> ZooNetworkStatus {
    ZooNetworkStatus {
        phase: status.phase.clone(),
        ready_validators: status.ready_validators,
        total_validators: status.total_validators,
        message: status.message.clone(),
    }
}

// ───────────────────────── Chain translation ─────────────────────────

pub fn chain_to_canonical(spec: &ZooChainSpec) -> ChainSpec {
    ChainSpec {
        brand: BRAND.to_string(),
        chain_id: spec.chain_id,
        blockchain_id: spec.blockchain_id.clone(),
        vm_id: spec.vm_id.clone(),
        genesis_config_map: spec.genesis_config_map.clone(),
        network_ref: spec.network_ref.clone(),
    }
}

pub fn chain_from_canonical(status: &ChainStatus) -> ZooChainStatus {
    ZooChainStatus {
        phase: status.phase.clone(),
        chain_id: status.chain_id.clone(),
        blockchain_id: status.blockchain_id.clone(),
        message: status.message.clone(),
    }
}

// ───────────────────────── Explorer translation ─────────────────────────

pub fn explorer_to_canonical(spec: &ZooExplorerSpec) -> ExplorerSpec {
    ExplorerSpec {
        brand: BRAND.to_string(),
        chain_ref: spec.chain_ref.clone(),
        image: spec.image.clone(),
        rpc_endpoint: spec.rpc_endpoint.clone(),
        chain_name: spec.chain_name.clone(),
        coin_symbol: spec.coin_symbol.clone(),
        service: ExplorerServiceSpec {
            service_type: spec.service.service_type.clone(),
            port: spec.service.port,
        },
    }
}

pub fn explorer_from_canonical(status: &ExplorerStatus) -> ZooExplorerStatus {
    ZooExplorerStatus {
        phase: status.phase.clone(),
        url: status.url.clone(),
        message: status.message.clone(),
    }
}

// ───────────────────────── Gateway translation ─────────────────────────

pub fn gateway_to_canonical(spec: &ZooGatewaySpec) -> GatewaySpec {
    GatewaySpec {
        brand: BRAND.to_string(),
        network_ref: spec.network_ref.clone(),
        hosts: spec.hosts.clone(),
        tls: TlsSpec {
            enabled: spec.tls.enabled,
            secret_name: spec.tls.secret_name.clone(),
        },
        service: GatewayServiceSpec {
            service_type: spec.service.service_type.clone(),
            rpc_port: spec.service.rpc_port,
            ws_port: spec.service.ws_port,
        },
    }
}

pub fn gateway_from_canonical(status: &GatewayStatus) -> ZooGatewayStatus {
    ZooGatewayStatus {
        phase: status.phase.clone(),
        route_count: status.route_count,
        message: status.message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        GatewayServiceSpec as ZooGatewayServiceSpec, ImageSpec as ZooImageSpec,
        MetricsSpec as ZooMetricsSpec, PortSpec as ZooPortSpec, ResourceSpec as ZooResourceSpec,
        ServiceSpec as ZooServiceSpec, StakingSpec as ZooStakingSpec,
        StorageSpec as ZooStorageSpec, TlsSpec as ZooTlsSpec,
    };

    fn sample_network() -> ZooNetworkSpec {
        let mut config = BTreeMap::new();
        config.insert(
            "BOOTSTRAP_IPS".to_string(),
            serde_json::Value::String("1.2.3.4:9651".to_string()),
        );
        ZooNetworkSpec {
            network_id: 200200,
            validators: 5,
            image: ZooImageSpec {
                repository: "ghcr.io/zoo-labs/node".to_string(),
                tag: "v1.0.0".to_string(),
                pull_policy: "IfNotPresent".to_string(),
            },
            storage: ZooStorageSpec {
                storage_class: Some("do-block-storage".to_string()),
                size: "200Gi".to_string(),
            },
            resources: ZooResourceSpec {
                cpu_request: "2".to_string(),
                cpu_limit: "4".to_string(),
                memory_request: "4Gi".to_string(),
                memory_limit: "8Gi".to_string(),
            },
            config,
            genesis: Some(serde_json::json!({ "alloc": {} })),
            genesis_config_map: Some("zoo-genesis".to_string()),
            metrics: ZooMetricsSpec {
                enabled: true,
                port: 9090,
            },
            service: ZooServiceSpec {
                service_type: "LoadBalancer".to_string(),
                per_pod_services: true,
            },
            ports: ZooPortSpec {
                staking: 9651,
                http: 9650,
            },
            staking: Some(ZooStakingSpec {
                secret_prefix: "zoo-staker".to_string(),
            }),
        }
    }

    #[test]
    fn network_round_trips_every_field() {
        let zoo = sample_network();
        let canonical = network_to_canonical(&zoo);

        assert_eq!(canonical.brand, BRAND);
        assert_eq!(canonical.network_id, zoo.network_id);
        assert_eq!(canonical.validators, zoo.validators);
        assert_eq!(canonical.image.repository, zoo.image.repository);
        assert_eq!(canonical.image.tag, zoo.image.tag);
        assert_eq!(canonical.image.pull_policy, zoo.image.pull_policy);
        assert_eq!(canonical.storage.storage_class, zoo.storage.storage_class);
        assert_eq!(canonical.storage.size, zoo.storage.size);
        assert_eq!(canonical.resources.cpu_request, zoo.resources.cpu_request);
        assert_eq!(canonical.resources.cpu_limit, zoo.resources.cpu_limit);
        assert_eq!(
            canonical.resources.memory_request,
            zoo.resources.memory_request
        );
        assert_eq!(canonical.resources.memory_limit, zoo.resources.memory_limit);
        assert_eq!(canonical.config, zoo.config);
        assert_eq!(canonical.genesis, zoo.genesis);
        assert_eq!(canonical.genesis_config_map, zoo.genesis_config_map);
        assert_eq!(canonical.metrics.enabled, zoo.metrics.enabled);
        assert_eq!(canonical.metrics.port, zoo.metrics.port);
        assert_eq!(canonical.service.service_type, zoo.service.service_type);
        assert_eq!(
            canonical.service.per_pod_services,
            zoo.service.per_pod_services
        );
        assert_eq!(canonical.ports.staking, zoo.ports.staking);
        assert_eq!(canonical.ports.http, zoo.ports.http);
        assert_eq!(
            canonical.staking.as_ref().map(|s| s.secret_prefix.as_str()),
            zoo.staking.as_ref().map(|s| s.secret_prefix.as_str())
        );
    }

    #[test]
    fn network_status_maps_back() {
        let canonical = NetworkStatus {
            phase: "Running".to_string(),
            ready_validators: 5,
            total_validators: 5,
            message: "All validators ready".to_string(),
        };
        let zoo = network_from_canonical(&canonical);
        assert_eq!(zoo.phase, canonical.phase);
        assert_eq!(zoo.ready_validators, canonical.ready_validators);
        assert_eq!(zoo.total_validators, canonical.total_validators);
        assert_eq!(zoo.message, canonical.message);
    }

    #[test]
    fn chain_translates_both_ways() {
        let zoo = ZooChainSpec {
            chain_id: 200200,
            blockchain_id: Some("2abc".to_string()),
            vm_id: Some("xyzzy".to_string()),
            genesis_config_map: Some("cm".to_string()),
            network_ref: Some("zoo-mainnet".to_string()),
        };
        let canonical = chain_to_canonical(&zoo);
        assert_eq!(canonical.brand, BRAND);
        assert_eq!(canonical.chain_id, zoo.chain_id);
        assert_eq!(canonical.blockchain_id, zoo.blockchain_id);
        assert_eq!(canonical.vm_id, zoo.vm_id);
        assert_eq!(canonical.network_ref, zoo.network_ref);

        let status = ChainStatus {
            phase: "Active".to_string(),
            chain_id: "200200".to_string(),
            blockchain_id: "2abc".to_string(),
            message: "tracked".to_string(),
        };
        let back = chain_from_canonical(&status);
        assert_eq!(back.phase, status.phase);
        assert_eq!(back.chain_id, status.chain_id);
        assert_eq!(back.blockchain_id, status.blockchain_id);
        assert_eq!(back.message, status.message);
    }

    #[test]
    fn explorer_translates_both_ways() {
        let zoo = ZooExplorerSpec {
            chain_ref: "zoo-chain".to_string(),
            image: "ghcr.io/luxfi/explorer:1.2.18".to_string(),
            rpc_endpoint: "http://zoo-rpc:9650/ext/bc/C/rpc".to_string(),
            chain_name: "zoo".to_string(),
            coin_symbol: Some("ZOO".to_string()),
            service: crate::crd::ExplorerServiceSpec {
                service_type: "ClusterIP".to_string(),
                port: 8090,
            },
        };
        let canonical = explorer_to_canonical(&zoo);
        assert_eq!(canonical.brand, BRAND);
        assert_eq!(canonical.chain_ref, zoo.chain_ref);
        assert_eq!(canonical.image, zoo.image);
        assert_eq!(canonical.rpc_endpoint, zoo.rpc_endpoint);
        assert_eq!(canonical.chain_name, zoo.chain_name);
        assert_eq!(canonical.coin_symbol, zoo.coin_symbol);
        assert_eq!(canonical.service.service_type, zoo.service.service_type);
        assert_eq!(canonical.service.port, zoo.service.port);

        let status = ExplorerStatus {
            phase: "Ready".to_string(),
            url: "https://explore-zoo.zoo.network".to_string(),
            message: String::new(),
        };
        let back = explorer_from_canonical(&status);
        assert_eq!(back.phase, status.phase);
        assert_eq!(back.url, status.url);
        assert_eq!(back.message, status.message);
    }

    #[test]
    fn gateway_translates_both_ways() {
        let zoo = ZooGatewaySpec {
            network_ref: "zoo-mainnet".to_string(),
            hosts: vec!["api.zoo.network".to_string()],
            tls: ZooTlsSpec {
                enabled: true,
                secret_name: Some("zoo-tls".to_string()),
            },
            service: ZooGatewayServiceSpec {
                service_type: "ClusterIP".to_string(),
                rpc_port: 9650,
                ws_port: 9651,
            },
        };
        let canonical = gateway_to_canonical(&zoo);
        assert_eq!(canonical.brand, BRAND);
        assert_eq!(canonical.network_ref, zoo.network_ref);
        assert_eq!(canonical.hosts, zoo.hosts);
        assert_eq!(canonical.tls.enabled, zoo.tls.enabled);
        assert_eq!(canonical.tls.secret_name, zoo.tls.secret_name);
        assert_eq!(canonical.service.service_type, zoo.service.service_type);
        assert_eq!(canonical.service.rpc_port, zoo.service.rpc_port);
        assert_eq!(canonical.service.ws_port, zoo.service.ws_port);

        let status = GatewayStatus {
            phase: "Pending".to_string(),
            route_count: 1,
            message: "tracked".to_string(),
        };
        let back = gateway_from_canonical(&status);
        assert_eq!(back.phase, status.phase);
        assert_eq!(back.route_count, status.route_count);
        assert_eq!(back.message, status.message);
    }

    #[test]
    fn canonical_network_serializes_camel_case_with_brand() {
        let canonical = network_to_canonical(&sample_network());
        let v = serde_json::to_value(&canonical).unwrap();
        assert_eq!(v["brand"], "zoo");
        assert_eq!(v["networkId"], 200200);
        // round-trip through JSON is lossless
        let back: NetworkSpec = serde_json::from_value(v).unwrap();
        assert_eq!(back, canonical);
    }
}
