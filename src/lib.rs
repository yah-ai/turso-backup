//! turso-backup — back up a local Turso (rewrite) database to S3-compatible
//! object storage and restore it, with the at-rest artifact readable by
//! vanilla SQLite.
//!
//! Plan + findings: `.yah/docs/working/turso-s3-backup.md`
//!
//! Three tiers (see modules):
//! - [`snapshot`] — Tier 1a: full `VACUUM INTO` snapshot → object store (today).
//! - [`dedup`]    — Tier 1b: incremental page-dedup snapshot.
//! - [`stream`]   — Tier 2: WAL-frame streaming (deferred, engine-coupled).
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
