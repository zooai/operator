//! Delegation backend: how the Zoo shim hands canonical `bootno.de` resources
//! to the bootnode orchestrator.
//!
//! The shim owns the brand CRDs; it does not re-implement web3 chain
//! orchestration. After [`crate::translator`] maps a `Zoo*` spec to its
//! canonical `bootno.de` form, [`Delegate::reconcile`] routes that canonical
//! resource to whichever backend is configured:
//!
//! - [`Backend::Local`]  — reconcile in this process using the operator's own
//!   built-in controllers. This is the shipping default: behavior is identical
//!   to the pre-shim operator, so adopting the shim is a no-op until a remote
//!   backend is selected.
//! - [`Backend::InProcess`] — link the `bootnode-operator` library crate and
//!   call its reconciler directly in this process. Gated behind the
//!   `delegate-in-process` cargo feature because the crate is published by
//!   Wave 1B; selecting it without the feature is a configuration error
//!   surfaced at startup, never a silent fallback.
//! - [`Backend::OutOfProcess`] — materialize `bootno.de` CRs into the cluster
//!   and let an in-cluster `bootnode/operator` deployment reconcile them. Used
//!   when bootnode ships binary-only (no library crate to link).
//!
//! `reconcile` is generic over the canonical resource so all four Kinds share
//! one routing surface — DRY, one way to delegate, regardless of Kind.

use crate::error::{OperatorError, Result};
use std::fmt;

/// Which delegation backend the shim routes canonical resources through.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Backend {
    /// Reconcile locally with this operator's built-in controllers (default).
    #[default]
    Local,
    /// Call the linked `bootnode-operator` library in-process.
    InProcess,
    /// Materialize `bootno.de` CRs for an in-cluster bootnode operator.
    OutOfProcess,
}

impl Backend {
    /// Parse a backend selector from config (env / CLI). Unknown values are an
    /// explicit error — the operator must never guess which backend to use.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Backend::Local),
            "in-process" | "inprocess" => Ok(Backend::InProcess),
            "out-of-process" | "outofprocess" => Ok(Backend::OutOfProcess),
            other => Err(OperatorError::Config(format!(
                "unknown delegate backend {other:?} (expected local|in-process|out-of-process)"
            ))),
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::Local => f.write_str("local"),
            Backend::InProcess => f.write_str("in-process"),
            Backend::OutOfProcess => f.write_str("out-of-process"),
        }
    }
}

/// Outcome of routing a canonical resource to a backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routed {
    /// The shim's local controllers own this reconcile; the caller proceeds
    /// with its built-in reconcile body.
    Local,
    /// Delegation accepted the canonical resource; the caller skips its local
    /// reconcile body and translates status back from the backend. Constructed
    /// only by a remote backend that has taken ownership of the reconcile —
    /// today that is the in-process backend behind `delegate-in-process`.
    #[cfg_attr(not(feature = "delegate-in-process"), allow(dead_code))]
    Delegated,
}

/// Routes canonical `bootno.de` resources to the configured backend.
///
/// `reconcile` is the single delegation entry point shared by every Kind.
/// It takes the canonical resource (serializable to `bootno.de` JSON) and the
/// resource's Kind name so the backend can address the right CR family.
#[derive(Clone, Debug, Default)]
pub struct Delegate {
    backend: Backend,
}

impl Delegate {
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> Backend {
        self.backend
    }

    /// Route one canonical resource. Returns [`Routed::Local`] when the caller
    /// should run its own reconcile body, or [`Routed::Delegated`] when a
    /// remote backend has taken ownership.
    ///
    /// `kind` is the canonical Kind (e.g. "Network"); `canonical` is the
    /// `bootno.de` resource produced by [`crate::translator`], already
    /// serialized to JSON so this routing surface is Kind-agnostic.
    pub async fn reconcile(&self, kind: &str, canonical: serde_json::Value) -> Result<Routed> {
        match self.backend {
            Backend::Local => Ok(Routed::Local),
            Backend::InProcess => self.reconcile_in_process(kind, canonical).await,
            Backend::OutOfProcess => self.reconcile_out_of_process(kind, canonical).await,
        }
    }

    #[cfg(feature = "delegate-in-process")]
    async fn reconcile_in_process(
        &self,
        kind: &str,
        canonical: serde_json::Value,
    ) -> Result<Routed> {
        // The translator already produces a byte-compatible `bootno.de` value,
        // so handing off to the linked bootnode-operator library is a direct
        // call. Wave 1B uncomments the `bootnode-operator` dependency in
        // Cargo.toml and replaces the line below with:
        //
        //     bootnode_operator::reconcile(kind, &canonical)
        //         .await
        //         .map_err(|e| OperatorError::Reconcile(e.to_string()))?;
        //
        // The hand-off succeeding means bootnode owns the reconcile, so we
        // report Delegated and the caller translates status back.
        bootnode_handoff(kind, &canonical)?;
        Ok(Routed::Delegated)
    }

    #[cfg(not(feature = "delegate-in-process"))]
    async fn reconcile_in_process(
        &self,
        _kind: &str,
        _canonical: serde_json::Value,
    ) -> Result<Routed> {
        Err(OperatorError::Config(
            "in-process backend selected but operator built without the delegate-in-process feature".to_string(),
        ))
    }

    async fn reconcile_out_of_process(
        &self,
        kind: &str,
        _canonical: serde_json::Value,
    ) -> Result<Routed> {
        // Out-of-process delegation materializes a `bootno.de/<kind>` CR from
        // the canonical value via server-side apply, then lets the in-cluster
        // bootnode operator own it. The cluster wiring (Api<DynamicObject>
        // against the bootno.de group) lands with the Wave 1B CRDs, which
        // define the target group/version this shim applies into.
        let target = format!(
            "{}/{}/{}",
            crate::translator::CANONICAL_GROUP,
            crate::translator::CANONICAL_VERSION,
            kind
        );
        Err(OperatorError::Config(format!(
            "out-of-process backend selected for target {target:?} but bootno.de CRDs are not yet installed (pending bootnode/operator)"
        )))
    }
}

/// In-process hand-off to the linked bootnode-operator library.
///
/// Until Wave 1B publishes bootnode/operator as a library crate (and the
/// `bootnode-operator` dependency in Cargo.toml is uncommented), this returns
/// an explicit configuration error rather than calling an unlinked symbol —
/// so a `--features delegate-in-process` build is honest about the missing
/// backend instead of silently no-opping. The single call site in
/// [`Delegate::reconcile_in_process`] swaps to `bootnode_operator::reconcile`
/// here with no other change.
#[cfg(feature = "delegate-in-process")]
fn bootnode_handoff(kind: &str, _canonical: &serde_json::Value) -> Result<()> {
    Err(OperatorError::Config(format!(
        "delegate-in-process feature enabled for kind {kind:?} but the bootnode-operator library crate is not yet linked (pending bootnode/operator Wave 1B)"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_canonical_spellings() {
        assert_eq!(Backend::parse("local").unwrap(), Backend::Local);
        assert_eq!(Backend::parse("Local").unwrap(), Backend::Local);
        assert_eq!(Backend::parse("in-process").unwrap(), Backend::InProcess);
        assert_eq!(Backend::parse("inprocess").unwrap(), Backend::InProcess);
        assert_eq!(
            Backend::parse(" out-of-process ").unwrap(),
            Backend::OutOfProcess
        );
    }

    #[test]
    fn parse_rejects_unknown() {
        assert!(Backend::parse("bogus").is_err());
    }

    #[test]
    fn default_backend_is_local() {
        assert_eq!(Backend::default(), Backend::Local);
        assert_eq!(Delegate::default().backend(), Backend::Local);
    }

    #[test]
    fn display_round_trips_through_parse() {
        for b in [Backend::Local, Backend::InProcess, Backend::OutOfProcess] {
            assert_eq!(Backend::parse(&b.to_string()).unwrap(), b);
        }
    }

    #[tokio::test]
    async fn local_backend_keeps_reconcile_local() {
        let d = Delegate::new(Backend::Local);
        let routed = d
            .reconcile("Network", serde_json::json!({ "brand": "zoo" }))
            .await
            .unwrap();
        assert_eq!(routed, Routed::Local);
    }

    #[tokio::test]
    async fn out_of_process_errors_until_crds_exist() {
        let d = Delegate::new(Backend::OutOfProcess);
        let err = d
            .reconcile("Network", serde_json::json!({ "brand": "zoo" }))
            .await
            .unwrap_err();
        assert!(matches!(err, OperatorError::Config(_)));
    }
}
