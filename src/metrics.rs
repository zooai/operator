//! Prometheus metrics for the Zoo operator.

use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::sync::OnceLock;
use std::time::Instant;

static REGISTRY: OnceLock<Metrics> = OnceLock::new();

pub struct Metrics {
    pub registry: Registry,

    /// zoo_operator_reconcile_total{resource,result}
    pub reconcile_total: IntCounterVec,

    /// zoo_operator_reconcile_duration_seconds{resource}
    pub reconcile_duration: HistogramVec,

    /// zoo_operator_network_phase{network,phase}
    pub network_phase: GaugeVec,

    /// zoo_operator_validators_ready{network}
    pub validators_ready: GaugeVec,

    /// zoo_operator_validators_total{network}
    pub validators_total: GaugeVec,
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new();

        let reconcile_total = IntCounterVec::new(
            Opts::new(
                "zoo_operator_reconcile_total",
                "Total number of reconciliations",
            ),
            &["resource", "result"],
        )
        .expect("metric can be created");

        let reconcile_duration = HistogramVec::new(
            HistogramOpts::new(
                "zoo_operator_reconcile_duration_seconds",
                "Duration of reconciliation in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["resource"],
        )
        .expect("metric can be created");

        let network_phase = GaugeVec::new(
            Opts::new(
                "zoo_operator_network_phase",
                "Current phase of each network (1 = active)",
            ),
            &["network", "phase"],
        )
        .expect("metric can be created");

        let validators_ready = GaugeVec::new(
            Opts::new(
                "zoo_operator_validators_ready",
                "Number of ready validators per network",
            ),
            &["network"],
        )
        .expect("metric can be created");

        let validators_total = GaugeVec::new(
            Opts::new(
                "zoo_operator_validators_total",
                "Total number of validators per network",
            ),
            &["network"],
        )
        .expect("metric can be created");

        registry
            .register(Box::new(reconcile_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(reconcile_duration.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(network_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(validators_ready.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(validators_total.clone()))
            .expect("metric can be registered");

        Metrics {
            registry,
            reconcile_total,
            reconcile_duration,
            network_phase,
            validators_ready,
            validators_total,
        }
    }
}

/// Initialize the global metrics registry. Call once at startup.
pub fn init() {
    REGISTRY.get_or_init(Metrics::new);
}

/// Get a reference to the global metrics.
pub fn get() -> &'static Metrics {
    REGISTRY
        .get()
        .expect("metrics must be initialized before use")
}

/// Encode all metrics as Prometheus text format.
pub fn encode() -> String {
    let encoder = TextEncoder::new();
    let metric_families = get().registry.gather();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("encoding metrics should not fail");
    String::from_utf8(buffer).expect("metrics are valid utf-8")
}

/// Record a reconcile result and duration.
pub fn record_reconcile(resource: &str, result: &str, start: Instant) {
    let m = get();
    m.reconcile_total
        .with_label_values(&[resource, result])
        .inc();
    m.reconcile_duration
        .with_label_values(&[resource])
        .observe(start.elapsed().as_secs_f64());
}

/// Set the current phase for a network.
pub fn set_network_phase(network: &str, phase: &str) {
    let m = get();
    for p in &["Pending", "Creating", "Bootstrapping", "Running", "Degraded"] {
        m.network_phase
            .with_label_values(&[network, p])
            .set(if *p == phase { 1.0 } else { 0.0 });
    }
}

/// Set validator counts for a network.
pub fn set_validators(network: &str, ready: u32, total: u32) {
    let m = get();
    m.validators_ready
        .with_label_values(&[network])
        .set(ready as f64);
    m.validators_total
        .with_label_values(&[network])
        .set(total as f64);
}
