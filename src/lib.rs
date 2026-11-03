//! turso-backup — back up a local Turso (rewrite) database to S3-compatible
//! object storage and restore it, with the at-rest artifact readable by
//! vanilla SQLite.
//!
//! Plan + findings: `.yah/docs/working/turso-s3-backup.md`
//!
//! Three tiers (see modules), all three implemented:
//! - [`snapshot`] — Tier 1a: full `VACUUM INTO` snapshot → object store, behind
//!   a two-gate skip. [`snapshot::upload_base_snapshot`] publishes an image the
//!   caller already holds, for a caller that may not open the file itself.
//! - [`dedup`]    — Tier 1b: incremental page-dedup snapshot.
//! - [`stream`]   — Tier 2: WAL-frame streaming anchored to a 1a base, with
//!   restore by frame replay, an RPO watermark, two-level fencing, bounded
//!   spill backpressure ([`backpressure`]), one-puller-per-box fan-out
//!   ([`puller`]), and a [`stream::probe_conditional_puts`] preflight.
//!
//! Both streaming tiers accumulate garbage a writer deliberately does not
//! delete, and each has an explicitly-invoked sweep that does:
//! [`dedup::gc_dedup`] for 1b's `pages/`, [`stream::gc_stream`] for 2's
//! superseded base snapshots and orphaned frame prefixes.
//!
//! This header called tier 2 "deferred, engine-coupled" until 2026-08-28. It
//! was neither by then: `stream.rs` had shipped in R005-F2/F3 and grown fencing
//! (R732-F2), the RPO watermark (R574-T4) and frame batching (R761-F2). The
//! claim was disproved by R760-F6 while wiring roadcase onto it, and corrected
//! here rather than filed — a stale doc comment costs every reader after it the
//! same wrong first impression.
//!
//! @yah:relay(Q002, "Turso → S3 backup crate")
//! @yah:at(2026-05-26T22:28:29Z)
//! @yah:kind(quest)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//!
//! @yah:ticket(R003-T1, "Spike: decide crate shape (sibling vs wrapper) + name; pin deps (turso/object_store/anyhow/tokio); flesh skeleton")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:03Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R003)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:handoff("Spike complete. Sibling crate (standalone, depends on turso via crates.io, no re-exports) >> wrapper — less coupling, simpler maintenance, matches how writer/ already depends on turso. Name confirmed: turso-backup.")
//! @yah:handoff("Prior-art eval: tier 1a (VACUUM INTO + object_store) is greenfield-worthy — no crate wraps turso-rewrite snapshot to S3. smugglr/verneuil (page-dedup, tier 1b) and walrust/wal-backup (WAL streaming, tier 2) remain candidates per working doc for their respective phases; license-check deferrable to those spikes.")
//! @yah:handoff("Deps pinned in Cargo.toml: turso 0.6.1, object_store 0.13, anyhow 1, tokio 1 (rt+macros). Skeleton modules fleshed: snapshot.rs has real BackupTarget struct + async signatures; dedup.rs/stream.rs use anyhow::Result. Crate compiles cleanly.")
//! @yah:next("R003-F2: implement snapshot_and_upload (VACUUM INTO temp + object_store put + change_counter skip)")
//! @yah:next("R003-F3: implement restore_latest (object_store get + write file)")
//! @yah:next("R003-T4: green e2e in harness (writer -> snapshot sink -> MinIO -> restore -> verifier)")

/// R977-B19: per-request timeout for every S3 store the bins build.
pub const S3_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// R977-B19: total time object_store may spend retrying one request
/// (object_store's own default is 180 s; a list + a get at that ceiling is the
/// shape of the 364 s headscale restore R977-B16 measured).
pub const S3_RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Apply [`S3_REQUEST_TIMEOUT`] / [`S3_RETRY_TIMEOUT`] to an S3 builder.
pub fn bounded_s3(b: object_store::aws::AmazonS3Builder) -> object_store::aws::AmazonS3Builder {
    b.with_client_options(
        object_store::ClientOptions::new()
            .with_timeout(S3_REQUEST_TIMEOUT)
            .with_connect_timeout(std::time::Duration::from_secs(5)),
    )
    .with_retry(object_store::RetryConfig {
        retry_timeout: S3_RETRY_TIMEOUT,
        ..Default::default()
    })
}

pub mod snapshot;
pub mod dedup;
pub mod stream;
pub mod backpressure;
pub mod puller;
/// R850-F1: hydrate-on-place — fill an empty volume's declared databases from
/// the store before the workload starts, under a [`store_claim::claim`].
pub mod hydrate;
/// R977-T18: recover a spliced durability chain — fresh tier-1a snapshot from
/// a chosen volume, spliced manifests moved aside (never deleted), then verify.
pub mod reseed;
/// R850-F1: the other half of [`hydrate`] — keep a *running* appliance's
/// declared databases shipped to the store, beside an application that holds
/// them open, under the same [`claim`].
pub mod tail;
