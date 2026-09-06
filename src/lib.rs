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

pub mod snapshot;
pub mod dedup;
pub mod stream;
pub mod backpressure;
pub mod puller;
/// R850-F1: mints the fencing epoch `stream::StreamConfig::epoch` enforces, for
/// callers with no raft state machine to ask.
pub mod claim;
/// R850-F1: hydrate-on-place — fill an empty volume's declared databases from
/// the store before the workload starts, under a [`claim`].
pub mod hydrate;
