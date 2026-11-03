//! R971-T6 — object-store fencing primitives, extracted from `turso-backup`.
//!
//! * [`BackupTarget`] + [`join_key`]: a store and a key prefix under it.
//! * [`claim`]: the store-side ownership claim (mint / read / assert an epoch).
//! * [`probe_conditional_puts`]: preflight proof that the sink really honours
//!   `If-None-Match` / `If-Match`, which the claim and every watermark CAS rest on.
//!
//! Depends on `object_store` and `anyhow` only (W363 §1/§3).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::path::Path as ObjPath;
use object_store::ObjectStore;

pub mod claim;
mod preflight;

pub use preflight::{probe_conditional_puts, PreconditionSupport, PreflightStage};

/// Configuration for an object-store backup target (S3 / R2 / MinIO, path-style).
///
/// Wraps a generic `object_store::ObjectStore` so the caller picks the backend
/// (e.g., `AmazonS3Builder::new().with_endpoint("http://minio:9000").build()`),
/// then hands this crate a `BackupTarget` to use for snapshot operations.
pub struct BackupTarget {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: String,
}

/// Join an object-store prefix and a leaf into a normalized [`ObjPath`],
/// tolerating empty/slash-padded prefixes.
pub fn join_key(prefix: &str, leaf: &str) -> ObjPath {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        ObjPath::from(leaf)
    } else {
        ObjPath::from(format!("{prefix}/{leaf}"))
    }
}

pub(crate) fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
