//! Tier 2 — WAL-frame streaming sink (R005-F2).
//!
//! Tee WAL frames from a live turso connection to an object store, anchored
//! to a tier-1a base snapshot. Restore (R005-F3) replays the frames onto the
//! snapshot. Near-zero RPO; engine-coupled but the public seam is small —
//! see the spike findings in `.yah/docs/working/turso-s3-backup.md`.
//!
//! ## Object layout under `BackupTarget::prefix`
//!
//! ```text
//! frames/{epoch:020}/{checkpoint_seq:010}/{first:020}-{last:020}  batch of consecutive WAL frames
//! frames/{checkpoint_seq:010}/{first:020}-{last:020}              ditto, epoch 0 (unfenced)
//! frames/{epoch:020}/{checkpoint_seq:010}/{frame_no:020}          one raw frame, pre-R761-F2 layout
//! frames/{checkpoint_seq:010}/{frame_no:020}                      ditto, epoch 0 (pre-fencing)
//! generations/gen-{unix_nanos:020}.manifest                one per `tail_frames` call that uploaded
//! latest.stream-watermark    text sidecar: "<checkpoint_seq> <last_frame> <written_at_nanos> <epoch> <pointer_generation> <salt1> <salt2>"
//! ```
//!
//! R858-B19: the trailing salt pair is the WAL generation the `last_frame`
//! position belongs to. `checkpoint_seq` alone does not identify a generation —
//! a writer-process restart recreates the WAL back at sequence 0 — so the salt
//! is what `tail_frames` compares and what a generation manifest stamps. See
//! [`WalGeneration`].
//!
//! A frame object holds `n` consecutive frames, each `24 + page_size` bytes,
//! concatenated in ascending frame order — so a batch is exactly the bytes the
//! old per-frame objects held, glued together, and offset `i * frame_size`
//! within it is frame `first + i`.
//!
//! The generation manifest names the base snapshot key, the page size, the
//! frame range covered, which batch objects cover it (since R761-F2), and
//! (since R732-F2) the fencing epoch and owner label. Object keys are
//! zero-padded so lexical order matches chronological order (same convention as
//! tier 1a snapshots / tier 1b manifests; clock-skew-immune).
//!
//! ## Frame batching (R761-F2, W248/W313 §9)
//!
//! One object per WAL frame made per-write Class A ops scale with *frames*,
//! and a frame is one page — at 4 KB pages, every 4 KB of changed data was a
//! billed op, which is a pathologically small object. A tail call's frames now
//! go up as one ranged object per drain batch (bounded by
//! [`BackpressureConfig::spill_buffer_frames`], the same bound that already
//! caps how much this sink holds in memory), so an uploading call costs
//! `ceil(frames / spill_buffer_frames) + 2` PUTs instead of `frames + 2` —
//! typically **3, flat**, whatever the write volume in the interval.
//!
//! Reading stays compatible in the direction that matters: a manifest without
//! a `frame_batch` list is a pre-R761-F2 generation and its frames are fetched
//! one object each, so backups written before this change (and chains that
//! straddle it) still restore. The reverse is a loud refusal by construction —
//! the manifest header moved to `v3`, so an older binary reading a batched
//! generation says "unexpected manifest header" rather than mis-reading it.
//!
//! ## Fencing (R732-F2 / R732-T3, W245; R736-T2, W250)
//!
//! [`StreamConfig::epoch`] is a per-tenant fencing token minted by yubaba's
//! raft state machine. It is checked against the sidecar before any frame is
//! uploaded, and the sidecar advance is a compare-and-swap on the version that
//! check read — so a stale owner is *rejected* (with
//! [`StreamOutcome::Fenced`]) both when it arrives late and when it races. An
//! epoch of `0` means unfenced, which preserves single-writer behaviour but is
//! still fenced *by* a claimed sink.
//!
//! That epoch is a **local** raft counter — it fences ownership moves within
//! one cell, but two cells are independent raft groups that share no epoch
//! counter, so it is blind to a tenant moving to a *different* cell.
//! [`StreamConfig::pointer_generation`] is the second, cross-cell fence: the
//! writer's belief about the global tenant→cell pointer's generation
//! (`yah_tenant_pointer::PointerRecord`). It is checked alongside `epoch` at
//! the same two points — up front against the sidecar, and again on a lost
//! watermark CAS — so a stale generation bounces exactly like a stale epoch.
//! A node is the real owner of a tenant only when **both** fences pass.
//!
//! ## Seam isolation
//!
//! Every call into `turso_core::Connection`'s `feature = "conn_raw_api"`
//! surface goes through the [`WalSeam`] trait. If a future turso release
//! renames or reshapes those calls, the delta is a single impl block — not a
//! sed across the whole sink. The spike measured one breaking rename in three
//! months (`wal_auto_checkpoint_disable` → `wal_auto_actions_disable`); the
//! trait makes that a one-file fix.
//!
//! @yah:relay(R005, "Tier 2 — WAL-frame streaming (deferred)")
//! @yah:at(2026-05-26T22:28:31Z)
//! @yah:status(open)
//! @yah:phase(P3)
//! @yah:parent(Q002)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//!
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//!
//! @yah:ticket(R005-F2, "Frame-streaming sink: base snapshot + incremental frames + generation tracking")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:09Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R005)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:handoff("Implemented in src/stream.rs (~590 LOC). Public API: WalSeam trait (3 methods: wal_state, wal_get_frame, wal_auto_actions_disable) + real CoreWalSeam impl over turso_core::Connection + StreamConfig{base_snapshot_key, page_size} + tail_frames(seam, target, cfg) -> StreamOutcome (Empty | Streamed | Restarted) + GenerationManifest format/parse + Watermark sidecar.")
//! @yah:handoff("Object layout under prefix: frames/{checkpoint_seq:010}/{frame_no:020} for raw frames (24-byte header + page), generations/gen-{nanos:020}.manifest for per-call manifests, latest.stream-watermark for the (checkpoint_seq, last_frame) sidecar. Same zero-padded-key convention as tier 1a snapshots and tier 1b manifests — clock-skew-immune.")
//! @yah:handoff("Tier-2 invariants from the R005-T1 spike are baked in: (checkpoint_seq, frame_no) compound key (not raw frame_no), so a WAL restart -> Restarted outcome under a new seq; page_size is a required cfg parameter (no 4096 hardcode like sync_server.rs); WalSeam isolates all turso_core::Connection calls (one-file delta if upstream renames again); CoreWalSeam::open() takes WAL ownership via wal_auto_actions_disable() at construction.")
//! @yah:handoff("Cargo.toml: added turso_core = '0.6.1' with features = ['conn_raw_api'] as sibling dep to turso='0.6.1'. The friendly turso wrapper does NOT re-export the raw WAL API; pinned in lockstep — if either bumps, bump both.")
//! @yah:handoff("Verified: 6 new stream unit tests (empty-on-empty-wal, initial-tail-records-watermark, second-tail-no-new-frames-is-Empty, second-tail-uploads-only-new, wal-restart-emits-Restarted-under-new-seq, manifest-roundtrip+rejection) using a mockable WalSeam. Full crate: 18/18 tests green. cargo clippy --all-targets -- --deny=warnings clean.")
//! @yah:handoff("What's NOT verified at F2 level: live-DB ping-pong of CoreWalSeam (seed rows -> wal_state -> wal_get_frame -> assert is_commit_frame on the last frame). F3's restore path will be the natural end-to-end exercise. Optional sanity test could be added under F2 if you'd rather catch a CoreWalSeam regression here vs in F3.")
//! @yah:handoff("Generation manifest text format: 'TURSO-BACKUP STREAM v1' header, then `base_snapshot <key>`, `page_size <n>`, `checkpoint_seq <n>`, `first_frame <n>`, `last_frame <n>`. Dependency-free, same convention as dedup::Manifest. parse_generation_manifest fails loudly on any other shape.")
//! @yah:next("User: review/approve F2. If you want a live-DB CoreWalSeam sanity test before signoff, say so and I'll add it under F2; otherwise F3 picks it up naturally.")
//! @yah:next("On approval: archive F2, claim R005-F3 (Restore via frame replay onto snapshot). F3 fetches latest gen-manifest, downloads referenced base_snapshot + frames in (checkpoint_seq, frame_no) order, replays into a writable DB via wal_insert_begin/wal_insert_frame/wal_insert_end (the same WalSeam trait, extended with the insert side).")
//! @yah:next("Optional independent of F3: file a tiny upstream PR to re-export conn_raw_api from the `turso` wrapper crate so the sibling turso_core dep collapses to one.")
//!
//! @yah:ticket(R005-F3, "Restore via frame replay onto snapshot + restart/crash-consistency handling")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:10Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R005)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R005-F2)
//! @yah:handoff("Built restore_latest_stream(&BackupTarget, dest_path) + the WalInsertSeam trait extension on the existing WalSeam pattern. Public surface: WalInsertSeam{wal_insert_begin, wal_insert_frame, wal_insert_end} + impl for CoreWalSeam (wraps turso_core::Connection::wal_insert_*); RestoreOutcome{base_snapshot_key, checkpoint_seq, generation_count, frames_replayed, last_frame}; restore_latest_stream(target, dest_path)->RestoreOutcome.")
//! @yah:handoff("Flow: list+sort all generations/*.manifest keys lexicographically → parse each → validate_generation_chain checks all share one base, one page_size, one checkpoint_seq, frames start at 1 and are contiguous → download base_snapshot to dest_path → CoreWalSeam::open(dest) → wal_insert_begin → replay_frames_into walks (seq, frame_no) in order, downloads frames/{seq:010}/{frame_no:020}, asserts byte length = 24+page_size, calls wal_insert_frame → wal_insert_end(force_commit=false). Crash-consistency story = the engine's own truncate-to-last-commit-frame on insert_end(false).")
//! @yah:handoff("Restart handling = REFUSE: a chain spanning two checkpoint_seqs means the source engine folded WAL into main between generations, so the post-restart frames don't replay onto our pre-restart base. validate_generation_chain bails with 'WAL restart between generations, restore needs a fresh tier-1a snapshot'. Same refusal for cross-base chains and frame gaps. This is the right semantics — heroic restart-spanning replay would silently corrupt.")
//! @yah:handoff("Verified with 13 new stream tests on top of F2's 6: validate_chain accepts single/contiguous-multi, rejects empty/gap/non-one-start/restart/base-mismatch/page_size-mismatch (7 tests); replay_walks_manifests_in_frame_order with MockInsertSeam; restore_errors_when_no_generations; replay_rejects_wrong_size_frame; live_db_seed_snapshot_tail_restore_round_trips (real turso + CoreWalSeam: seed 50 → snapshot → checkpoint → seed 25 more → tail → restore → assert dest has 75 rows); manifest_with_uncommitted_tail_rolls_back_via_insert_end (live: stage a phantom non-commit frame in the sink, extend the manifest, assert restore drops it and dest=3 rows = 2 base + 1 committed, NOT 4).")
//! @yah:handoff("cargo test -p turso-backup = 31/31 green (up from 18); cargo clippy --all-targets -- --deny=warnings = clean. One clippy lint fixed in the new test code: manual_is_multiple_of (Rust 1.95 lint). The live tests use turso::Builder for writes/reads + CoreWalSeam for tailing — exclusive WAL lock means we drop the high-level conn before opening the low-level seam (matched by the snapshot tests' pattern).")
//! @yah:handoff("Two known design decisions worth flagging to the reviewer: (1) replay starts at frame 1 in the dest's fresh WAL but the source's frames could overlap content already in the base snapshot (since VACUUM INTO point-in-time includes WAL state). turso's wal_insert_frame compares-and-returns-OK on identical content, so redundant frames are no-ops — clean orchestration via checkpoint-then-snapshot-then-stream avoids them entirely (which is what the live test does). (2) on replay error we attempt a best-effort wal_insert_end(false) before bubbling the error up, so we don't leave the dest's WAL with an uncommitted suffix half-open.")
//! @yah:next("User: review/approve F3. Once green, archive F3 and the F2/F3-dependent state of R005 collapses to F4 only (concurrent-writer-safe raw copy).")
//! @yah:verify("cargo test -p turso-backup")
//! @yah:verify("cargo clippy --all-targets -- --deny=warnings")
//!
//! @yah:ticket(R005-F4, "Concurrent-writer-safe raw copy: read-only main + WAL-frame replay (no TRUNCATE-checkpoint dependency), for backing up under a live writer")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-27T03:05:00Z)
//! @yah:status(review)
//! @yah:phase(P3)
//! @yah:parent(R005)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:gotcha("R004's dedup::raw_consistent_copy assumes no live writer: it folds WAL->main via PRAGMA wal_checkpoint(TRUNCATE), which a concurrent writer can make return 'busy'. The live-writer alternative copies the main file under a read txn and replays WAL frames itself (wal_get_frame seam) — tier-2 territory, gated on R005-T1's seam assessment. Filed as an R004-T4 followup.")
//! @yah:handoff("Implemented raw_consistent_copy_live(db_path, page_size) + pub(crate) replay_wal_onto_main inner. Algorithm: open CoreWalSeam (auto-actions disabled on our conn) -> wal_state for (cp_seq, max_frame) -> std::fs::read main -> walk frames 1..=max_frame collecting (page_no, db_size, page_bytes), locate the last is_commit_frame -> grow image to fit the largest page slot in the commit prefix -> overwrite each page at (page_no-1)*page_size -> truncate to db_size*page_size. Frames past the last commit are dropped wholesale (crash-consistency, matches restore's wal_insert_end(false)).")
//! @yah:next("User: review/approve F4. Once green, archive F4 and R005 collapses to closed (R005-F2/F3/F4 all in review).")
//! @yah:verify("cargo test -p turso-backup: 38/38 green (up from 31). New: 5 unit tests on replay_wal_onto_main with MockWal (empty-WAL/single-commit/uncommitted-tail-dropped/multi-commit-prefix-grows-image/only-uncommitted-frames-returns-main) + 2 live tests on raw_consistent_copy_live (live_db_consistent_copy_without_truncate_round_trips: seed -> checkpoint_truncate -> seed more uncheckpointed -> copy-live -> reopen = 35 rows; live_db_consistent_copy_reflects_new_writes: repeatable copy after subsequent writes).")
//! @yah:verify("cargo clippy --all-targets -- --deny=warnings: clean.")
//!
//! ## R574-F2 — explicit R2 backpressure
//!
//! `tail_frames`'s upload loop drains frames through
//! [`crate::backpressure::put_with_backoff`] against a bounded spill buffer
//! (see [`drain_frames_with_backpressure`]) instead of putting each frame
//! directly and bubbling any error raw. Full design in the `backpressure`
//! module doc; ticket tracked in the W248 relay, not in-source.
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)
//!
//! ## R574-T4 — explicit RPO knob
//!
//! Cadence was entirely implicit in the caller's `tail_frames` invocation
//! interval (doc §10). `StreamConfig::rpo_target` states that interval as a
//! number; every `tail_frames` call reports [`RpoStatus`] (age of the last
//! durably-persisted watermark, and whether that age has drifted past the
//! target) on its [`StreamOutcome`] so an orchestrator can alert without
//! reimplementing the bookkeeping. This crate still does not schedule
//! anything itself — cadence/retention policy stays caller-driven per the
//! doc's "Not in scope" — `rpo_target` documents the contract the caller's
//! own scheduler is expected to uphold, and `RpoStatus` is the receipt.
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)
//!
//! ## R761-T1 — a measured default tail cadence
//!
//! [`DEFAULT_TAIL_INTERVAL`] (60 s) and [`DEFAULT_RPO_TARGET`] (120 s) are
//! the cadence this crate recommends, derived from
//! `examples/tail_sweep_harness.rs`'s 2026-08-13 sweep rather than chosen:
//! every uploading `tail_frames` call writes two fixed objects (generation
//! manifest + watermark CAS) on top of the frames, so per-write cost was
//! `frames/write + 2/writes_per_tail` — 4.03 PUTs/write at one write per
//! tail, 2.05 at a hundred. The constants' docs carry the full table and
//! the reasoning for landing at 60 s instead of chasing the last 3 %. Still
//! no scheduler in this crate; these are numbers for the caller's.
//!
//! R761-F2 then removed the `frames/write` term those numbers were floored
//! by (see *Frame batching* above), so the cost is now `3/writes_per_tail`
//! for a call whose frames fit one batch — the cadence lever and the layout
//! lever compose, and the second is worth more the longer the interval.
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)
//!
//! ## R574-F3 — one-puller-per-box fan-out + warm applier
//!
//! [`crate::puller::WalPuller`] is the read side's counterpart to
//! `tail_frames`'s write side: it pulls each newly-uploaded frame from R2
//! exactly once per box and fans it out in-process to every attached warm
//! applier (a [`WalInsertSeam`] with its page cache trimmed hard via
//! [`CoreWalSeam::trim_page_cache_kb`]), so R2 read ops are O(boxes), not
//! O(replicas). See the `puller` module doc for the full design and its v1
//! scope cut (attach is cold-start-only; no mid-stream backlog replay).
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backpressure::{put_with_backoff, BackpressureConfig, BackpressurePolicy, BackpressureReport};
use crate::snapshot::BackupTarget;

/// WAL frame layout: 24-byte frame header (page_no big-endian at 0..4,
/// db_size big-endian at 4..8, salts/checksums in the remainder) followed by
/// `page_size` bytes of page data. Constant per the SQLite WAL format; the
/// page size is read from the base snapshot's header (page 1, byte 16, `u16`
/// with the value `1` meaning 65536). `sync_server.rs` hardcodes 4096 — we
/// don't, see [`StreamConfig::page_size`].
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

/// Position in the WAL: a `(checkpoint_seq, last_frame)` pair. `max_frame`
/// resets to 0 every time the WAL header restarts (`WalAutoActions::Restart`
/// fires), and `checkpoint_seq_no` increments alongside it — so the pair is
/// the right primary key for sink objects, not raw frame_no.
///
/// **R858-B19 — `checkpoint_seq` alone does NOT identify a WAL generation, and
/// this doc used to claim it did ("increments monotonically across restarts").
/// That claim is false and it cost a silent wrong restore.** It holds only for
/// an *in-process* restart, where the same WAL file is reused. When the last
/// connection to a SQLite database closes, the engine checkpoints and DELETES
/// the `-wal` file; the next writer creates a fresh WAL back at
/// checkpoint-sequence `0` with a brand-new random salt. Across that fold
/// `checkpoint_seq` goes `0 -> 0` over two completely unrelated WALs (measured
/// 2026-09-06 against system sqlite3 3.51.0 by
/// `examples/foreign_checkpoint_probe.rs`, probes B and F).
///
/// Generation identity therefore lives in [`WalGeneration`], which pairs the
/// sequence with the WAL header's salt. Never compare two `Watermark`s'
/// `checkpoint_seq` to decide "same WAL" — that is exactly the inference this
/// ticket exists to delete. `last_frame` is only meaningful *relative to a
/// known generation*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermark {
    pub checkpoint_seq: u32,
    pub last_frame: u64,
}

/// R858-B19 — the WAL header's `(salt1, salt2)`, the two 32-bit values SQLite
/// re-rolls on every WAL reset. Stored big-endian at bytes 16..24 of the
/// 32-byte WAL header, and copied verbatim into bytes 8..16 of **every frame
/// header** written under that header — which is where this crate reads it
/// from, since `turso_core::WalState` exposes only `checkpoint_seq_no` and
/// `max_frame`.
///
/// Why the salt and not the sequence: the salt moves in *both* fold regimes,
/// and the sequence moves in only one.
///
/// - WAL recreated by a writer restart: fresh randomness (measured
///   `b83c03f5 -> 9ca89e39`, unrelated), while `checkpoint_seq` resets `0 -> 0`.
/// - In-process autocheckpoint: `salt1` increments in lockstep with the
///   sequence (measured `d492ea8a -> ... -> d492ea92` alongside seq `0 -> 8`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalSalt {
    pub salt1: u32,
    pub salt2: u32,
}

impl WalSalt {
    /// Read the salt out of a WAL **frame** header (bytes 8..16 of the 24-byte
    /// header, big-endian). Panics on a short slice — every caller here sizes
    /// its buffer at `WAL_FRAME_HEADER_SIZE + page_size`.
    pub(crate) fn from_frame_header(frame: &[u8]) -> Self {
        let be = |o: usize| u32::from_be_bytes(frame[o..o + 4].try_into().unwrap());
        Self { salt1: be(8), salt2: be(12) }
    }

    /// R858-B18 — read the salt out of the **32-byte `-wal` file header**
    /// (bytes 16..24, big-endian), the copy SQLite writes once per WAL
    /// generation and duplicates into every frame header.
    ///
    /// The two constructors differ only in offset, and they live together
    /// deliberately: one type, one place, so the two ways this crate can reach
    /// the same value can never disagree. [`from_frame_header`](Self::from_frame_header)
    /// is the seam-only path ([`read_wal_salt`] — works through a
    /// [`CoreWalSeam::from_conn`] that has no path); this one is the
    /// path-only path ([`SourceFingerprint`] — works with no engine open at
    /// all, which is what makes it usable as an independent check *on* a copy
    /// the engine took).
    ///
    /// Panics on a slice shorter than 24 bytes; [`WalFileHeader::parse`] is the
    /// length-checked entry point every caller here actually uses.
    pub(crate) fn from_wal_file_header(hdr: &[u8]) -> Self {
        let be = |o: usize| u32::from_be_bytes(hdr[o..o + 4].try_into().unwrap());
        Self { salt1: be(16), salt2: be(20) }
    }
}

impl std::fmt::Display for WalSalt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}/{:08x}", self.salt1, self.salt2)
    }
}

/// R858-B19 — the identity of one WAL generation: the checkpoint sequence
/// **and** the header salt that actually distinguishes it. This is what
/// [`tail_frames`] compares across calls, what the watermark sidecar persists,
/// and what a generation manifest stamps.
///
/// `salt: None` means **unknown generation**, and unknown is a value here, not
/// a missing one: it arises from a sidecar or manifest written before this
/// field existed, or from a WAL with no frames to read a salt out of. An
/// unknown generation is never provably equal to anything — see
/// [`WalGeneration::is_provably_same_as`] — so it forces a restart on the write
/// side and a refusal on the restore side rather than a guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalGeneration {
    pub checkpoint_seq: u32,
    pub salt: Option<WalSalt>,
}

impl WalGeneration {
    /// True only when both sides carry a **known** salt and every component
    /// agrees — i.e. only when these are *provably* the same WAL.
    ///
    /// The asymmetry is the whole point. "Not provably the same" is treated as
    /// "different", which costs a redundant re-upload (or a loud restore
    /// refusal) in the worst case. The opposite default — "no evidence of a
    /// change, so assume it is the same WAL" — is what spliced two WAL
    /// generations into one chain and restored a plausible wrong image.
    pub fn is_provably_same_as(&self, other: &WalGeneration) -> bool {
        match (self.salt, other.salt) {
            (Some(a), Some(b)) => a == b && self.checkpoint_seq == other.checkpoint_seq,
            _ => false,
        }
    }

    /// Render for an error message: `seq 4 salt 1109ca5e/7acf42a3`, or
    /// `seq 4 salt <unknown>` when the salt was never recorded.
    pub(crate) fn describe(&self) -> String {
        match self.salt {
            Some(s) => format!("seq {} salt {s}", self.checkpoint_seq),
            None => format!("seq {} salt <unknown>", self.checkpoint_seq),
        }
    }
}

/// R858-B18 — the 32-byte `-wal` file header, read straight off disk with no
/// engine open. Ground truth about which WAL generation is on disk and how far
/// it has been written, independent of anything turso caches.
///
/// Only the three fields that move are kept. The rest of the header (magic,
/// format version, the two header checksums) is either constant for a given
/// build or a function of these; a change to any of it that did *not* move one
/// of these three would not be a change this crate can act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalFileHeader {
    /// Page size the WAL's frames carry (bytes 8..12).
    pub page_size: u32,
    /// Checkpoint sequence (bytes 12..16) — the field `tail_frames` used to key
    /// restart detection on before [`WalGeneration`] paired it with the salt.
    pub checkpoint_seq: u32,
    /// The generation's salt (bytes 16..24).
    pub salt: WalSalt,
}

impl WalFileHeader {
    /// SQLite's WAL header is exactly this many bytes, ahead of frame 1.
    pub const SIZE: usize = 32;

    /// Parse a WAL header out of the first [`Self::SIZE`] bytes of a `-wal`
    /// file. `None` for anything shorter — a truncated or freshly-created WAL
    /// has no generation to name yet, which is an honest unknown rather than an
    /// error (see [`WalGeneration`]'s `salt: None`).
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SIZE {
            return None;
        }
        let be = |o: usize| u32::from_be_bytes(bytes[o..o + 4].try_into().unwrap());
        Some(Self {
            page_size: be(8),
            checkpoint_seq: be(12),
            salt: WalSalt::from_wal_file_header(bytes),
        })
    }

    /// Read `{db_path}-wal`'s header. `Ok(None)` when the sidecar is absent
    /// (WAL folded and deleted, or journal_mode != WAL) or too short to parse.
    pub fn read(db_path: &str) -> Result<Option<Self>> {
        match std::fs::File::open(format!("{db_path}-wal")) {
            Ok(mut f) => {
                let mut buf = [0u8; Self::SIZE];
                let mut filled = 0usize;
                loop {
                    match std::io::Read::read(&mut f, &mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            return Err(e).with_context(|| format!("reading {db_path}-wal header"))
                        }
                    }
                    if filled == Self::SIZE {
                        break;
                    }
                }
                Ok(Self::parse(&buf[..filled]))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("opening {db_path}-wal")),
        }
    }
}

/// R858-B18 — everything about a source database's **files** that must hold
/// still for a copy of them to be a point-in-time image, sampled with no engine
/// open and no lock taken.
///
/// This is the other half of reading a foreign-written database. A ReadOnly
/// open ([`CoreWalSeam::open_reader`]) gets us in the door without stealing the
/// whole-file lock, but it buys **no shared locking protocol** with upstream C
/// SQLite: turso locks whole files with `fcntl`, C SQLite uses byte-range locks
/// plus the `-shm` WAL index, and neither engine observes the other's. So a
/// foreign checkpoint landing in the middle of our copy can fold WAL frames
/// into the main file we have already half-read, and the result is a torn image
/// that still passes `PRAGMA integrity_check`.
///
/// Rather than reimplement SQLite's reader protocol (registering a read-mark in
/// the `-shm` WAL index — a research project and a permanent compatibility
/// liability against an engine we do not control), [`raw_consistent_copy_live`]
/// uses textbook **optimistic validation**: sample this before the copy, sample
/// it again after, and accept the copy only if nothing moved. That converts
/// "may silently read torn state" into "detects torn state and refuses", needs
/// no cooperation from the foreign engine, and costs two stats and a 132-byte
/// read per attempt.
///
/// ## Why these fields
///
/// - `wal` (salt + checkpoint_seq) moves on **every** WAL reset, in both fold
///   regimes — fresh randomness on a writer restart, `salt1` incrementing on an
///   in-process autocheckpoint (both measured; see [`WalSalt`]).
/// - `main_len` moves when a checkpoint grows the main database.
/// - `change_counter` (main header bytes 24..28) moves on every write to the
///   main file — i.e. on every checkpoint — even one that leaves its length
///   alone. It is meaningful here **only because the foreign writer is C
///   SQLite**: turso does not maintain this field (it stays `1` in every
///   journal mode, verified — see `snapshot.rs`'s two-gate rationale), which is
///   exactly why the WAL salt carries the weight and this one is corroboration.
/// - `wal_len` is recorded for the report but deliberately **not** part of the
///   accept/reject test — see [`Self::stable_across`], which is the comparison
///   to use. There is no `PartialEq` on this type on purpose: a bare `==` would
///   silently include `wal_len` and refuse every copy taken while the
///   application was merely writing.
#[derive(Debug, Clone, Copy)]
pub struct SourceFingerprint {
    /// Length of the main database file.
    pub main_len: u64,
    /// The main header's change counter, or `None` when the file is too short
    /// to carry a SQLite header at all (a database whose page 1 still lives
    /// only in the WAL). Unknown-and-unknown compares equal, which is safe
    /// because `main_len` participates in the same comparison.
    pub change_counter: Option<u32>,
    /// The `-wal` header, or `None` when there is no WAL sidecar.
    pub wal: Option<WalFileHeader>,
    /// Length of the `-wal` file (`0` when absent).
    pub wal_len: u64,
}

impl SourceFingerprint {
    /// Offset of the change counter in SQLite's 100-byte database header.
    const CHANGE_COUNTER_OFFSET: usize = 24;

    /// Sample the fingerprint of the database at `db_path`. Touches nothing:
    /// two metadata calls plus a 28-byte and a 32-byte read.
    pub fn read(db_path: &str) -> Result<Self> {
        let mut hdr = [0u8; Self::CHANGE_COUNTER_OFFSET + 4];
        let main_len = match std::fs::File::open(db_path) {
            Ok(mut f) => {
                let len = f
                    .metadata()
                    .with_context(|| format!("stat {db_path}"))?
                    .len();
                if len as usize >= hdr.len() {
                    std::io::Read::read_exact(&mut f, &mut hdr)
                        .with_context(|| format!("reading {db_path} header"))?;
                }
                len
            }
            Err(e) => return Err(e).with_context(|| format!("opening source db {db_path}")),
        };
        let change_counter = (main_len as usize >= hdr.len()).then(|| {
            u32::from_be_bytes(hdr[Self::CHANGE_COUNTER_OFFSET..].try_into().unwrap())
        });
        Ok(Self {
            main_len,
            change_counter,
            wal: WalFileHeader::read(db_path)?,
            wal_len: std::fs::metadata(format!("{db_path}-wal"))
                .map(|m| m.len())
                .unwrap_or(0),
        })
    }

    /// True when nothing that can **tear** a copy moved between `self` (sampled
    /// before) and `after` (sampled after). This is the accept test in
    /// [`raw_consistent_copy_live`], and it is narrower than field equality on
    /// purpose.
    ///
    /// ## What can tear the copy, and what cannot
    ///
    /// The copy reads the main file, then replays WAL frames `1..=max_frame`
    /// captured when the seam opened. Against that algorithm:
    ///
    /// - **A checkpoint tears it.** It rewrites pages of the main file *and*
    ///   resets the WAL, so our already-read main bytes and our frame reads can
    ///   straddle the fold — replaying pre-fold frames over post-fold pages
    ///   rolls pages backwards. Caught: a checkpoint bumps `change_counter`
    ///   and/or `main_len`, and a WAL restart re-rolls the salt and the sequence
    ///   (measured in both regimes, `examples/foreign_checkpoint_probe.rs`
    ///   probes A and E).
    /// - **A plain append does NOT tear it.** SQLite only ever appends frames
    ///   within a generation, and only a reset (which re-rolls `salt1`) lets it
    ///   overwrite an existing frame. So frames `1..=max_frame` are immutable
    ///   for as long as the salt holds, and a writer that commits during our
    ///   copy just means our image is a slightly earlier point in time — which
    ///   is what a point-in-time copy *is*.
    ///
    /// Which is why `wal_len` and the frame count are excluded. Including them
    /// buys no additional safety and costs a refusal on every copy taken while
    /// the application is writing at all — turning a working backup into one
    /// that only succeeds against an idle database. R858-B18 measured that
    /// difference rather than assuming it; see probe H.
    pub fn stable_across(&self, after: &Self) -> bool {
        self.main_len == after.main_len
            && self.change_counter == after.change_counter
            && self.wal == after.wal
    }

    /// One-line rendering for the refusal message, so a failure names what
    /// actually moved instead of asserting "something did".
    pub(crate) fn describe(&self) -> String {
        let wal = match self.wal {
            Some(h) => format!("seq {} salt {}", h.checkpoint_seq, h.salt),
            None => "<no WAL>".to_string(),
        };
        format!(
            "main {}B change_counter {} | wal {}B {wal}",
            self.main_len,
            self.change_counter.map_or("<unknown>".to_string(), |c| c.to_string()),
            self.wal_len,
        )
    }
}

/// Subset of `turso_core::Connection`'s `feature = "conn_raw_api"` surface
/// we use. Every WAL call into turso goes through this trait. A future
/// signature shift becomes a one-impl delta.
pub trait WalSeam {
    /// Snapshot the WAL position (checkpoint seq + max_frame).
    fn wal_state(&self) -> Result<Watermark>;

    /// Fetch frame `frame_no` (1-based) into `buf` (must be
    /// `WAL_FRAME_HEADER_SIZE + page_size` bytes). Returns the page number
    /// the frame applies to and the post-frame DB size (in pages) — non-zero
    /// `db_size` marks a commit frame.
    fn wal_get_frame(&self, frame_no: u64, buf: &mut [u8]) -> Result<FrameInfo>;

    /// Take WAL ownership: turn off both the auto-checkpoint and the
    /// auto-WAL-restart so our watermark stays meaningful across calls. The
    /// in-tree consumer (`cli/sync_server.rs`) makes the same move at startup.
    fn wal_auto_actions_disable(&self);
}

/// Restore-side counterpart to [`WalSeam`]: the three `wal_insert_*` calls
/// `cli/sync_server.rs` clients use to replay frames into a fresh DB. Same
/// rationale — one impl block to update if upstream renames.
///
/// Ordering contract: `begin` → N × `insert_frame(monotonic frame_no)` → `end`.
/// `end(false)` is the crash-safe default: the engine rolls back any suffix of
/// frames written after the last commit frame (`db_size > 0`) in the session.
pub trait WalInsertSeam {
    /// Open a write transaction with auto-checkpoint/restart suppressed so our
    /// monotonically-numbered inserts aren't reshuffled mid-session.
    fn wal_insert_begin(&self) -> Result<()>;

    /// Insert `frame` (a 24-byte header + `page_size` bytes of page data) at
    /// position `frame_no` (1-based, must be exactly `prev + 1` — gaps error).
    /// Identical content at an already-written position is a no-op (the engine
    /// compares and returns OK).
    fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<()>;

    /// Close the session. With `force_commit = false` (the restore default) the
    /// engine drops any frames after the last commit frame — automatic
    /// crash-consistency for a tail captured mid-transaction. `force_commit = true`
    /// commits even an uncommitted suffix; not used by restore.
    fn wal_insert_end(&self, force_commit: bool) -> Result<()>;
}

/// Frame-header projection (what we need for streaming + commit detection).
/// Mirrors `turso_core::types::WalFrameInfo` without re-exporting the type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameInfo {
    pub page_no: u32,
    /// Number of pages in the DB after this frame's commit, or `0` if the
    /// frame is mid-transaction.
    pub db_size: u32,
}

impl FrameInfo {
    pub fn is_commit_frame(&self) -> bool {
        self.db_size > 0
    }
}

/// Real WAL seam backed by a live `turso_core::Connection`.
pub struct CoreWalSeam {
    conn: Arc<turso_core::Connection>,
}

impl CoreWalSeam {
    /// Open a **writable** connection at `path` and disable auto-checkpoint /
    /// auto-restart so the caller owns WAL maintenance. Requires `turso_core`
    /// with `features = ["conn_raw_api"]` (set in this crate's Cargo.toml).
    ///
    /// This takes turso's **whole-file exclusive `fcntl` lock**, so it is
    /// mutually exclusive with any other process holding the database open —
    /// including upstream C SQLite (measured: `examples/foreign_checkpoint_probe.rs`
    /// probes D and G). That is correct for the restore/apply direction, which
    /// owns the destination file outright, and wrong for reading a live source:
    /// use [`Self::open_reader`] there.
    pub fn open(path: &str) -> Result<Self> {
        let io: Arc<dyn turso_core::IO> =
            Arc::new(turso_core::PlatformIO::new().context("creating turso_core PlatformIO")?);
        let db = turso_core::Database::open_file(io, path)
            .with_context(|| format!("opening turso_core db {path}"))?;
        let conn = db.connect().context("connecting to turso_core db")?;
        // Take WAL ownership — same first move sync_server.rs makes.
        conn.wal_auto_actions_disable();
        Ok(Self { conn })
    }

    /// R858-B18 — open a **read-only** connection at `path`, taking **no**
    /// whole-file lock. This is the constructor the backup direction wants: a
    /// backup is a reader, and it must not lock out the application whose
    /// database it is reading.
    ///
    /// `OpenFlags::ReadOnly` is what buys that, per handle and with no
    /// process-wide effect. `turso_core-0.7.2/io/unix.rs:67-72` takes the
    /// exclusive lock only when
    /// `env::var(ENV_DISABLE_FILE_LOCK).is_err() && !flags.intersects(ReadOnly | NoLock)`;
    /// `io_uring.rs:463` and `windows.rs:305` carry the identical condition, so
    /// this is not a unix-only accident. The `LIMBO_DISABLE_FILE_LOCK=1` escape
    /// hatch reaches the same no-lock state but does it for **every** open in
    /// the process, including the writable ones — never use it here.
    ///
    /// ## Two things this does NOT give you
    ///
    /// 1. **No shared locking protocol.** Getting in without a lock is not
    ///    coordination: a foreign checkpoint can still land mid-read. That is
    ///    what [`SourceFingerprint`] validation is for, and why
    ///    [`raw_consistent_copy_live`] pairs the two rather than shipping this
    ///    flag alone — the flag alone converts "refuses to open" into "may
    ///    return a torn image", which is strictly worse.
    /// 2. **No escape from turso's process-global registry.**
    ///    `Database::open_file_with_flags` consults `DATABASE_MANAGER`, keyed by
    ///    file id, *before* it looks at the flags (`lib.rs:940`), and hands back
    ///    an already-open `Database` with its own flags discarded. So in a
    ///    process that already holds a writable handle on this exact file, this
    ///    call returns that writable handle — same as `snapshot.rs`'s
    ///    `upload_base_snapshot` doc records. Cross-process (the headscale case)
    ///    is unaffected: the registry is per-process.
    pub fn open_reader(path: &str) -> Result<Self> {
        let io: Arc<dyn turso_core::IO> =
            Arc::new(turso_core::PlatformIO::new().context("creating turso_core PlatformIO")?);
        let db = turso_core::Database::open_file_with_flags(
            io,
            path,
            turso_core::OpenFlags::ReadOnly,
            turso_core::DatabaseOpts::new(),
            None,
        )
        .with_context(|| format!("opening turso_core db {path} read-only"))?;
        let conn = db.connect().context("connecting to turso_core db")?;
        // Belt and braces: a read-only connection cannot checkpoint anyway, but
        // the seam contract is that nothing we hold folds the WAL under us.
        conn.wal_auto_actions_disable();
        Ok(Self { conn })
    }

    /// Wrap an already-open connection. The caller is responsible for having
    /// called `wal_auto_actions_disable()` on it.
    pub fn from_conn(conn: Arc<turso_core::Connection>) -> Self {
        Self { conn }
    }
}

impl WalSeam for CoreWalSeam {
    fn wal_state(&self) -> Result<Watermark> {
        let s = self.conn.wal_state().context("turso_core wal_state")?;
        Ok(Watermark {
            checkpoint_seq: s.checkpoint_seq_no,
            last_frame: s.max_frame,
        })
    }

    fn wal_get_frame(&self, frame_no: u64, buf: &mut [u8]) -> Result<FrameInfo> {
        let info = self
            .conn
            .wal_get_frame(frame_no, buf)
            .with_context(|| format!("turso_core wal_get_frame({frame_no})"))?;
        Ok(FrameInfo {
            page_no: info.page_no,
            db_size: info.db_size,
        })
    }

    fn wal_auto_actions_disable(&self) {
        self.conn.wal_auto_actions_disable();
    }
}

impl WalInsertSeam for CoreWalSeam {
    fn wal_insert_begin(&self) -> Result<()> {
        self.conn
            .wal_insert_begin()
            .context("turso_core wal_insert_begin")
    }

    fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<()> {
        self.conn
            .wal_insert_frame(frame_no, frame)
            .with_context(|| format!("turso_core wal_insert_frame({frame_no})"))?;
        Ok(())
    }

    fn wal_insert_end(&self, force_commit: bool) -> Result<()> {
        self.conn
            .wal_insert_end(force_commit)
            .context("turso_core wal_insert_end")
    }
}

impl CoreWalSeam {
    /// Resize this connection's page cache toward `target_kb` kilobytes via
    /// the standard `PRAGMA cache_size` surface (negative value = KB, per
    /// SQLite's own convention — see `turso_core::translate::pragma`'s
    /// `update_cache_size`) rather than reaching into `turso_core`'s
    /// `Pager::change_page_cache_size` / `CacheResizeResult` directly: the
    /// latter are technically reachable (`Connection::get_pager()` and
    /// `Pager`/`Page`/`PageRef` are all `pub use`d at the crate root) but
    /// `CacheResizeResult` itself is not re-exported, so calling it from
    /// here would mean handling a value of an unnameable type. The pragma
    /// path exercises the exact same resize logic through turso_core's own
    /// public, documented SQL surface instead.
    ///
    /// R574-F3: a warm applier trims its cache hard immediately on attach —
    /// it never serves reads, so cached pages are pure standing RSS cost
    /// (see the R574-T1 measurement this sizes against).
    pub(crate) fn trim_page_cache_kb(&self, target_kb: i64) -> Result<()> {
        anyhow::ensure!(
            target_kb > 0,
            "trim_page_cache_kb: target_kb must be positive, got {target_kb}"
        );
        self.conn
            .execute(format!("PRAGMA cache_size = -{target_kb}"))
            .with_context(|| format!("PRAGMA cache_size = -{target_kb}"))
    }
}

/// R761-T1: the tail cadence this crate recommends when a caller has no
/// reason of its own to pick a different one — **60 s**, and the number is
/// a measurement result, not a guess.
///
/// ## The measurement
///
/// `examples/tail_sweep_harness.rs`, run 2026-08-13 against a casual-app
/// workload (single-row transactions into a table with a secondary index),
/// sweeping `writes_per_tail` — how many application writes accumulate
/// between two `tail_frames` calls:
///
/// | writes_per_tail | PUTs/write | frames/write |
/// |----------------:|-----------:|-------------:|
/// |               1 |       4.03 |         2.03 |
/// |               5 |       2.43 |         2.03 |
/// |              25 |       2.11 |         2.03 |
/// |             100 |       2.05 |         2.03 |
///
/// Those four points are not four independent facts. Every `tail_frames`
/// call that uploads anything writes exactly **two fixed objects** beyond
/// the frames themselves — one generation manifest and one watermark CAS —
/// so
///
/// ```text
/// PUTs/write = frames/write + 2 / writes_per_tail
/// ```
///
/// which reproduces all four measured rows to the last digit: `2.03 + 2/1`,
/// `2.03 + 2/5`, `2.03 + 2/25`, `2.03 + 2/100`. frames/write is FLAT across
/// the sweep — it is a property of the schema and transaction shape, and
/// cadence cannot touch it. The entire lever this default pulls is the
/// `2 / writes_per_tail` term.
///
/// ## Why 60 s and not longer
///
/// That term has sharply diminishing returns. Of the total reduction
/// available (4.03 → 2.05), moving from `writes_per_tail` 1 → 5 captures
/// 81 %, 1 → 25 captures 97 %, and everything from 25 → 100 is the last
/// 3 %. So the target is the 25 region, not 100.
///
/// Converting that writes axis into a time interval needs a write RATE,
/// and the honest one is the rate DURING an active session — W313 §3.1's
/// "~10 writes/day" arrives clustered in bursts, not spread evenly, and an
/// idle tenant costs nothing at any cadence (a tail call with no new frames
/// uploads no objects at all, so a long interval only ever helps a tenant
/// that is actively writing). Across the burst rates a casual app produces,
/// ~0.1–1 write/s:
///
/// - 15 s (what a 30 s RPO bound derives today) spans 1.5–15 writes/tail
///   → 3.36–2.16 PUTs/write. The slow-burst end is the worst case the
///   ticket is named after, and it is nearly the full 4.03.
/// - **60 s spans 6–60 writes/tail → 2.36–2.06 PUTs/write.**
/// - 300 s spans 30–300 → 2.10–2.04: at most 0.26 PUTs/write better than
///   60 s, bought with a 5× wider data-loss window on exactly the tenants
///   that are actively writing. Not a trade worth making for 3 % of a
///   bill.
///
/// 60 s is where the curve has flattened but the exposure window is still
/// something an operator can say out loud.
///
/// ## What it did not fix, and what did
///
/// The 2.03 frames/write floor was untouchable from here — it was one object
/// per WAL frame, and cadence cannot amortize a per-frame cost. **R761-F2
/// removed it** by uploading a tail call's frames as one ranged object, so
/// the curve above is now
///
/// ```text
/// PUTs/write = 3 / writes_per_tail
/// ```
///
/// (one batch object + manifest + watermark), i.e. 3.00 / 0.60 / 0.12 / 0.03
/// at the same four points — a 26 % cut at `writes_per_tail = 1` and 98.5 %
/// at 100. **That does not move this default**, and the reasoning above is
/// why rather than an accident: the 60 s choice was made against the shape
/// of the curve, and what batching changes is its scale. Over the same
/// 0.1–1 write/s burst band, going 60 s → 300 s now buys 0.50 → 0.10
/// PUTs/write at the slow end and 0.05 → 0.01 at the fast one — against a
/// pre-batching worst case of 3.36, an absolute difference small enough
/// that RPO exposure is the only term still worth optimizing here. The
/// measured points above are kept as the pre-batching baseline the
/// reduction is stated against.
///
/// Re-run the harness and revisit both numbers whenever schema shape, page
/// size, or turso's WAL behaviour changes:
/// `cargo run -p turso-backup --example tail_sweep_harness`.
///
/// This crate still schedules nothing itself (R574-T4's "document the
/// contract" choice stands) — the constant is the number a caller's
/// scheduler should adopt absent a reason not to, and
/// [`DEFAULT_RPO_TARGET`] is the bound that goes with it.
pub const DEFAULT_TAIL_INTERVAL: Duration = Duration::from_secs(60);

/// R761-T1: the RPO bound that goes with [`DEFAULT_TAIL_INTERVAL`] — twice
/// it, because tailing *at* the bound makes ordinary scheduling jitter read
/// as a breach, and tailing at half of it means one missed tick still lands
/// inside the promise. See [`DEFAULT_TAIL_INTERVAL`] for why the cadence is
/// 60 s; this is that number expressed as the promise rather than the
/// mechanism, and it is what belongs in [`StreamConfig::rpo_target`].
pub const DEFAULT_RPO_TARGET: Duration = Duration::from_secs(120);

/// Configuration for a streaming session.
pub struct StreamConfig<'a> {
    /// Object-store key of the base tier-1a snapshot the frames replay onto.
    /// Recorded in every generation manifest; restore re-fetches it.
    pub base_snapshot_key: &'a str,
    /// Page size of the base snapshot — read it from the snapshot header
    /// (offset 16, `u16` big-endian; the on-disk value `1` means 65 536).
    /// Required because the seam does not return it and `sync_server.rs`'s
    /// 4 KB hardcode is the wrong default to inherit.
    pub page_size: usize,
    /// R574-F2: bounded spill buffer + overflow policy + 429/503 backoff
    /// for the R2 upload side of the drain loop. `Default` is
    /// behavior-preserving (`BackpressurePolicy::Fail` — errors bubble
    /// immediately, same as before this field existed).
    pub backpressure: BackpressureConfig,
    /// R574-T4: the stated RPO bound — the caller's own scheduler is
    /// expected to invoke `tail_frames` often enough that the watermark
    /// never goes stale past this. `None` (the default) means no target is
    /// asserted; [`RpoStatus::breached`] is always `false` in that case,
    /// but [`RpoStatus::watermark_age`] is still reported so a caller can
    /// observe the real gap before picking a number.
    ///
    /// R761-T1: [`DEFAULT_RPO_TARGET`] (120 s, tailed at
    /// [`DEFAULT_TAIL_INTERVAL`] = 60 s) is the number to put here absent a
    /// reason to pick another — its doc carries the measured write-op table
    /// that justifies it. `None` stays the default so that a caller who has
    /// not thought about RPO never gets a breach flag it did not ask for.
    pub rpo_target: Option<Duration>,
    /// R732-F2 (W245): this writer's **fencing token** — the per-tenant epoch
    /// handed out by yubaba's raft state machine
    /// (`YubabaState::tenant_fencing_token`). It is stamped into every frame
    /// key, every generation manifest, and the watermark sidecar, and it is
    /// checked before a single frame is uploaded: a writer whose epoch is
    /// *lower* than the one already recorded at the sink is a stale owner and
    /// bounces with [`StreamOutcome::Fenced`] rather than interleaving its
    /// frames with the real owner's.
    ///
    /// **`0` means unfenced**, and is the behaviour-preserving default for a
    /// single-writer deployment that has no ownership authority to ask. Note
    /// that unfenced is not exempt: a `0` writer is still fenced by any sink
    /// already stamped with a real epoch, which is exactly what should happen
    /// when a tenant has been claimed and a legacy streamer is still running.
    pub epoch: u64,
    /// R732-F2: opaque label for *who* holds `epoch` — a yubaba node id, a
    /// hostname, whatever the caller finds useful. Recorded in the manifest
    /// and never interpreted here. Purely diagnostic: when you are staring at
    /// a fenced stream at 3am, "which owner wrote generation 7" is the first
    /// question, and the epoch alone does not answer it.
    pub owner: Option<&'a str>,
    /// R736-T2 (W250): the **cross-cell** fence — this writer's belief about
    /// the global tenant→cell pointer's generation
    /// (`yah_tenant_pointer::PointerRecord::generation`). `epoch` alone
    /// fences *within* one raft group; it says nothing when ownership moves
    /// to a different cell, because the two cells run independent raft
    /// groups that share no epoch counter. Checked alongside `epoch` before
    /// any frame is uploaded: a writer whose generation is *lower* than the
    /// one already recorded at the sink is a stale cell and bounces with
    /// [`StreamOutcome::Fenced`], exactly like a stale epoch.
    ///
    /// **`0` means unfenced** — the same behaviour-preserving default as
    /// `epoch`, for a single-cell deployment that has no global pointer to
    /// ask. This mirrors `yah_tenant_pointer`'s `FIRST_GENERATION = 1`: `0`
    /// is what an unfenced writer defaults to and must never be mistakable
    /// for a live cross-cell owner. The caller (yubaba's control plane)
    /// resolves the pointer and hands the generation in as a plain `u64` —
    /// this crate does not link `yah_tenant_pointer` to read it itself.
    pub pointer_generation: u64,
}

/// What a [`tail_frames`] call did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOutcome {
    /// No new frames since the last tail — sink is already current.
    Empty {
        watermark: Watermark,
        /// R574-T4: staleness of the last durably-persisted watermark —
        /// still meaningful here, since "nothing new to stream" and "the
        /// caller's scheduler stopped invoking us" look identical from the
        /// engine's side and only this field tells them apart.
        rpo: RpoStatus,
    },
    /// Uploaded a contiguous range of frames and wrote a generation manifest.
    Streamed {
        generation_key: String,
        first_frame: u64,
        last_frame: u64,
        checkpoint_seq: u32,
        frame_count: u64,
        /// R574-F2: backpressure activity during this call (policy,
        /// high-water spill-buffer occupancy, shed count, throttle retries).
        backpressure: BackpressureReport,
        /// R574-T4: see [`StreamOutcome::Empty::rpo`].
        rpo: RpoStatus,
    },
    /// The live WAL is not provably the one the sidecar watermark was taken
    /// from, so this call re-uploaded frames `1..N` from the top rather than
    /// resuming.
    ///
    /// R858-B19 widened this from "`checkpoint_seq` advanced" to "the
    /// [`WalGeneration`] did not prove itself unchanged", which is why both
    /// fields are now generations rather than bare sequence numbers: the
    /// motivating case is a writer restart where the sequence reads `0 -> 0`
    /// and only the salt moved. `previous_generation.salt == None` names the
    /// third case — a sidecar written before the salt existed, restarted
    /// because it cannot be checked, not because it was seen to change.
    Restarted {
        generation_key: String,
        previous_generation: WalGeneration,
        new_generation: WalGeneration,
        first_frame: u64,
        last_frame: u64,
        frame_count: u64,
        /// R574-F2: see [`StreamOutcome::Streamed::backpressure`].
        backpressure: BackpressureReport,
        /// R574-T4: see [`StreamOutcome::Empty::rpo`].
        rpo: RpoStatus,
    },
    /// R574-F2: `BackpressurePolicy::Shed` dropped every buffered frame in
    /// this call before any of them persisted (R2 was throttling harder
    /// than the spill buffer + backoff could absorb). No manifest/watermark
    /// was written — the next `tail_frames` call re-attempts the same
    /// range from the unchanged prior watermark. Distinct from `Empty`,
    /// which means the engine itself had nothing new.
    Shed {
        checkpoint_seq: u32,
        first_frame: u64,
        last_frame: u64,
        backpressure: BackpressureReport,
        /// R574-T4: see [`StreamOutcome::Empty::rpo`].
        rpo: RpoStatus,
    },
    /// R732-F2 (W245) / R736-T2 (W250): **this writer is a stale owner and
    /// wrote nothing.** Either the sink's watermark is stamped with an epoch
    /// higher than [`StreamConfig::epoch`] (ownership moved within the cell),
    /// or with a pointer generation higher than
    /// [`StreamConfig::pointer_generation`] (ownership moved to a different
    /// cell) — the two-level fence bounces on either. Detected before the
    /// first frame upload, so a fenced call is a pure read — no frames, no
    /// manifest, no watermark write.
    ///
    /// This is the outcome the whole fencing design exists to produce. Without
    /// it a partitioned old master and a freshly-promoted new master both
    /// stream into the same prefix and silently corrupt each other; with it
    /// the loser finds out on its very next tail and can stop.
    ///
    /// Deliberately carries no [`RpoStatus`]: a fenced writer's view of
    /// watermark staleness is not its stream's RPO any more, and reporting one
    /// here would page the wrong operator about the wrong node.
    Fenced {
        /// The epoch recorded at the sink — the real owner's token.
        current_epoch: u64,
        /// The (lower) epoch this writer tried to stream under.
        our_epoch: u64,
        /// R736-T2: the pointer generation recorded at the sink.
        current_pointer_generation: u64,
        /// R736-T2: the (lower) generation this writer tried to stream under.
        our_pointer_generation: u64,
    },
}

impl StreamOutcome {
    /// This call's RPO snapshot, or `None` for [`StreamOutcome::Fenced`] —
    /// see that variant's doc for why it deliberately carries none.
    ///
    /// R782: the accessor a caller (`tenant-streamer`'s tail loop) uses to
    /// push `watermark_age` onward without re-deriving this match on every
    /// call site that needs it.
    pub fn rpo(&self) -> Option<&RpoStatus> {
        match self {
            StreamOutcome::Empty { rpo, .. }
            | StreamOutcome::Streamed { rpo, .. }
            | StreamOutcome::Restarted { rpo, .. }
            | StreamOutcome::Shed { rpo, .. } => Some(rpo),
            StreamOutcome::Fenced { .. } => None,
        }
    }

    /// This call's backpressure activity, or `None` for the two outcomes that
    /// never reached the drain loop ([`StreamOutcome::Empty`] had nothing to
    /// send, [`StreamOutcome::Fenced`] was refused before the first frame).
    ///
    /// R760-B8: the accessor a *multi-tenant* caller needs. `Streamed` is not
    /// the same thing as "the sink took everything" — under
    /// [`BackpressurePolicy::Shed`] a call that persisted a partial prefix and
    /// dropped the rest reports `Streamed` with a nonzero
    /// [`BackpressureReport::frames_shed`], and a caller that only matches the
    /// variant cannot tell that apart from a clean tail. roadcase's shard
    /// flusher reads this on every arm so a struggling cell is nameable from
    /// its own metrics rather than by bisecting tenants.
    pub fn backpressure(&self) -> Option<&BackpressureReport> {
        match self {
            StreamOutcome::Streamed { backpressure, .. }
            | StreamOutcome::Restarted { backpressure, .. }
            | StreamOutcome::Shed { backpressure, .. } => Some(backpressure),
            StreamOutcome::Empty { .. } | StreamOutcome::Fenced { .. } => None,
        }
    }
}

/// R574-T4: watermark-staleness snapshot for RPO drift alerting, computed
/// fresh on every `tail_frames` call against [`StreamConfig::rpo_target`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RpoStatus {
    /// The stated bound from `StreamConfig::rpo_target`, echoed back for
    /// convenience (so a caller reading only the outcome still knows what
    /// was being enforced).
    pub target: Option<Duration>,
    /// Elapsed time since the watermark last durably advanced, measured at
    /// the start of this call. `None` when the sink has never persisted a
    /// watermark — there is nothing yet to measure staleness against.
    pub watermark_age: Option<Duration>,
    /// `true` when both `target` and `watermark_age` are set and
    /// `watermark_age > target` — the RPO bound is breached and an
    /// orchestrator should alert. Always `false` when no target is
    /// configured.
    pub breached: bool,
}

impl BackupTarget {
    pub(crate) fn watermark_key(&self) -> ObjPath {
        join_key(&self.prefix, "latest.stream-watermark")
    }

    /// R732-F2: frames are namespaced by the writer's fencing epoch, so two
    /// owners at different epochs cannot land on the same key even if they
    /// somehow both get as far as uploading. The epoch check in
    /// [`tail_frames`] is the guard; this key shape is the backstop that makes
    /// a guard failure recoverable (both owners' frames survive and the
    /// manifests say who wrote what) instead of a silent overwrite.
    ///
    /// Epoch `0` keeps the original two-level layout. That is not cosmetic:
    /// backups written before fencing existed are still restorable because
    /// their manifests say `epoch 0` and land back on this branch.
    ///
    /// R761-F2: this is the *read-side legacy* key shape now — nothing writes
    /// one-object-per-frame any more. See [`Self::frame_batch_key`].
    pub(crate) fn frame_key(&self, epoch: u64, checkpoint_seq: u32, frame_no: u64) -> ObjPath {
        let suffix = if epoch == 0 {
            format!("frames/{checkpoint_seq:010}/{frame_no:020}")
        } else {
            format!("frames/{epoch:020}/{checkpoint_seq:010}/{frame_no:020}")
        };
        join_key(&self.prefix, &suffix)
    }

    /// R761-F2: key of a batch object holding frames `first..=last`
    /// concatenated. Same epoch namespacing and zero-padding as
    /// [`Self::frame_key`], so lexical order still matches frame order — one
    /// stream's batches never overlap, because each is a slice of a single
    /// monotonic drain.
    ///
    /// The range is in the key rather than only in the manifest so the sink
    /// stays self-describing: a `ls` of the prefix tells an operator exactly
    /// which frames are present, which is what made the per-frame layout easy
    /// to reason about and is worth keeping.
    ///
    /// Distinguishable from a legacy per-frame key by construction (that one
    /// has no `-`), so a stream that straddles the layout change can hold both
    /// shapes under the same directory without collision.
    pub(crate) fn frame_batch_key(
        &self,
        epoch: u64,
        checkpoint_seq: u32,
        first: u64,
        last: u64,
    ) -> ObjPath {
        let suffix = if epoch == 0 {
            format!("frames/{checkpoint_seq:010}/{first:020}-{last:020}")
        } else {
            format!("frames/{epoch:020}/{checkpoint_seq:010}/{first:020}-{last:020}")
        };
        join_key(&self.prefix, &suffix)
    }

    /// Every object holding generation `m`'s frames, as `(key, first, last)`
    /// in ascending frame order.
    ///
    /// The single place that knows how a generation's frames are laid out:
    /// R761-F2 batches when the manifest carries a `frame_batch` list, the
    /// pre-R761-F2 one-object-per-frame layout when it does not. Restore's
    /// replay and [`crate::puller::WalPuller`] both go through here, so the
    /// two can never drift apart about where a frame lives.
    pub(crate) fn frame_objects_of(
        &self,
        m: &OwnedGenerationManifest,
    ) -> Vec<(ObjPath, u64, u64)> {
        if m.frame_batches.is_empty() {
            (m.first_frame..=m.last_frame)
                .map(|n| (self.frame_key(m.epoch, m.checkpoint_seq, n), n, n))
                .collect()
        } else {
            m.frame_batches
                .iter()
                .map(|&(first, last)| {
                    (
                        self.frame_batch_key(m.epoch, m.checkpoint_seq, first, last),
                        first,
                        last,
                    )
                })
                .collect()
        }
    }

    pub(crate) fn generation_key(&self, unix_nanos: u128) -> ObjPath {
        join_key(
            &self.prefix,
            &format!("generations/gen-{unix_nanos:020}.manifest"),
        )
    }
}

/// R858-B19 — identify the WAL generation `seam` is currently reading, by
/// pulling the salt out of **frame 1's** header.
///
/// Frame 1 rather than the 32-byte WAL header because this goes through the
/// [`WalSeam`] trait, which is the crate's one boundary against `turso_core`:
/// `turso_core::WalState` reports only `checkpoint_seq_no` and `max_frame`, and
/// reading the `-wal` file directly would need a path that
/// [`CoreWalSeam::from_conn`] does not have. Every frame header carries a
/// verbatim copy of the WAL header's salt, so frame 1 answers the same question
/// through machinery every seam already implements — including the mocks, and
/// including roadcase's connection-backed seam.
///
/// `Ok(None)` means the WAL holds no frames, so there is no generation to name
/// yet. That is not a failure: it is the honest "unknown", and
/// [`WalGeneration::is_provably_same_as`] treats it as such.
///
/// Costs one page-sized read per [`tail_frames`] call — negligible next to the
/// frames that call is about to upload, and it is the only thing standing
/// between a WAL recreate and a silently spliced generation chain.
pub(crate) fn read_wal_salt<S: WalSeam>(
    seam: &S,
    page_size: usize,
    last_frame: u64,
) -> Result<Option<WalSalt>> {
    if last_frame == 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; WAL_FRAME_HEADER_SIZE + page_size];
    seam.wal_get_frame(1, &mut buf)
        .context("reading WAL frame 1 to identify the WAL generation (R858-B19)")?;
    Ok(Some(WalSalt::from_frame_header(&buf)))
}

/// Tail new WAL frames from `seam` into `target`, anchored to a base
/// snapshot. Idempotent and resumable: on the second call only frames after
/// the recorded watermark are uploaded.
///
/// Ordering within a single call:
/// 1. Read [`Watermark`] from the seam (snapshot the current `(checkpoint_seq,
///    max_frame)`) and the current [`WalGeneration`] (that sequence plus the
///    WAL salt, via [`read_wal_salt`]).
/// 2. Read the prior watermark sidecar (if any).
/// 3. Unless the sidecar's generation is *provably* the same WAL as the live
///    one, treat this as a restart: upload frames `1..=max_frame`. R858-B19 —
///    the test is [`WalGeneration::is_provably_same_as`], not a `checkpoint_seq`
///    comparison, because a writer-process restart recreates the WAL at
///    sequence `0` and an unchanged sequence therefore proves nothing.
/// 4. Otherwise upload frames `prior.last_frame+1..=max_frame`.
/// 5. Advance the watermark sidecar with a compare-and-swap on the version
///    read in step 2, then write a generation manifest pointing at the base
///    snapshot + frame range + the batch objects covering it. Frames precede
///    both, so a manifest never references a missing frame; the manifest
///    follows the CAS, so a writer that loses the sidecar race publishes
///    nothing (R732-T3).
///
/// Returns [`StreamOutcome::Empty`] if there is nothing to do (max_frame
/// hasn't advanced and checkpoint_seq is unchanged), or
/// [`StreamOutcome::Fenced`] if this writer's [`StreamConfig::epoch`] has been
/// superseded — either observed up front in step 2 or discovered by losing the
/// CAS in step 5.
pub async fn tail_frames<S: WalSeam>(
    seam: &S,
    target: &BackupTarget,
    cfg: &StreamConfig<'_>,
) -> Result<StreamOutcome> {
    let current = seam.wal_state()?;
    // R858-B19: the salt is what actually names the WAL generation. Read it
    // before anything else touches the sink, so the restart decision below is
    // made against the live WAL rather than inferred from a sequence number
    // that a writer restart silently resets.
    let current_generation = WalGeneration {
        checkpoint_seq: current.checkpoint_seq,
        salt: read_wal_salt(seam, cfg.page_size, current.last_frame)?,
    };
    let persisted = read_watermark(&target.store, &target.watermark_key()).await?;
    // R574-T4: staleness measured at call start, against the *prior*
    // sidecar write — i.e. how long the sink had gone without durable
    // progress before this call ran. On the steady cadence the caller's
    // scheduler promises, this hovers at the invocation interval; a
    // skipped/late run pushes it past `rpo_target` and flips `breached`.
    let rpo = rpo_status(cfg.rpo_target, persisted.as_ref());

    // R732-F2 (W245) / R736-T2 (W250): the two-level fence, checked before
    // anything is written. A sink stamped with a higher epoch than ours means
    // ownership moved within the cell while we were away; a sink stamped with
    // a higher pointer generation means ownership moved to a *different*
    // cell — the two raft groups don't share an epoch counter, so the epoch
    // check alone is blind to that move. Either means every byte we are about
    // to upload belongs to somebody else's stream. Bounce here and the call
    // is a pure read; bounce anywhere later and we have already interleaved
    // frames into the real owner's range.
    if let Some(p) = &persisted {
        if p.epoch > cfg.epoch || p.pointer_generation > cfg.pointer_generation {
            return Ok(StreamOutcome::Fenced {
                current_epoch: p.epoch,
                our_epoch: cfg.epoch,
                current_pointer_generation: p.pointer_generation,
                our_pointer_generation: cfg.pointer_generation,
            });
        }
    }

    // Borrowed, not consumed: the conditional watermark advance at the end of
    // this call needs the object version this read observed (R732-T3).
    let prior = persisted.as_ref().map(|p| p.watermark);

    // R858-B19: the generation the sidecar was written under. `None` for a
    // sidecar that predates the salt field — which reads as *unknown*, not as
    // "the same WAL", and so forces the restart branch exactly once. After that
    // one full re-upload the sink carries a salt and the stream self-heals.
    let prior_generation = persisted.as_ref().map(|p| p.generation);

    // Decide the range to upload. Resuming at `last_frame + 1` is only sound
    // when the live WAL is PROVABLY the one the watermark was taken from: a
    // writer-process restart deletes the `-wal` file and the next writer starts
    // a fresh WAL at checkpoint-sequence 0 with a new salt, so an unchanged
    // sequence is not evidence of anything. Anything short of proof restarts.
    let (start_frame, restarted) = match prior_generation {
        Some(g) if g.is_provably_same_as(&current_generation) => {
            (prior.map(|p| p.last_frame).unwrap_or(0) + 1, false)
        }
        Some(_) => (1, true),
        None => (1, false),
    };

    if current.last_frame < start_frame {
        return Ok(StreamOutcome::Empty { watermark: current, rpo });
    }

    // Drain frames in ascending order through the bounded spill buffer +
    // backpressure policy (R574-F2). A partial upload leaves a prefix (the
    // manifest is written last, so a prefix without a manifest is invisible
    // to restore — the next tail just overwrites the same keys).
    let drain = drain_frames_with_backpressure(
        seam,
        target,
        cfg,
        cfg.epoch,
        current.checkpoint_seq,
        start_frame,
        current.last_frame,
    )
    .await?;

    let Some(uploaded_through) = drain.uploaded_through else {
        // BackpressurePolicy::Shed dropped everything before any frame
        // persisted — no manifest/watermark write, so the next call
        // re-attempts this exact range from the unchanged prior watermark.
        return Ok(StreamOutcome::Shed {
            checkpoint_seq: current.checkpoint_seq,
            first_frame: start_frame,
            last_frame: current.last_frame,
            backpressure: drain.report,
            rpo,
        });
    };

    // R858-B19: everything above sampled the generation ONCE, before the
    // drain. A WAL recreate between calls is what this ticket is about, but
    // nothing stops one landing *during* a call — and then the frames just
    // uploaded are a mix of two WALs, which is the same corruption arriving by
    // a narrower door. Re-read the salt and refuse if it moved.
    //
    // Refusing here is cheap and complete: the watermark has not advanced and
    // no manifest has been written, so the frames that landed are orphaned
    // under keys nothing references — invisible to restore, exactly like the
    // `Shed` path — and the next tail re-derives everything from the sidecar.
    let after = seam.wal_state()?;
    let after_salt = read_wal_salt(seam, cfg.page_size, after.last_frame)?;
    if after_salt != current_generation.salt {
        anyhow::bail!(
            "the source WAL was recreated while this tail was uploading ({} -> {}) — the frames \
             this call read span two WAL generations, so nothing is published and the sink is left \
             exactly as it was; the next tail will restart cleanly against the new WAL",
            current_generation.describe(),
            WalGeneration { checkpoint_seq: after.checkpoint_seq, salt: after_salt }.describe(),
        );
    }

    // Claim the range with a conditional watermark advance, THEN publish the
    // generation manifest. Under a Shed policy that persisted a partial
    // prefix, both cover only `start_frame..=uploaded_through`, not the full
    // engine range.
    //
    // R732-T3 reordered these two. The manifest used to be written last, so
    // that a manifest never referenced a missing frame — that invariant is
    // untouched, because frames still precede both. What the old order could
    // not do is fence: a writer that lost the sidecar race had already
    // published its manifest, which would then sit in the chain at a regressed
    // epoch and make every future restore refuse. Publishing only after the
    // CAS means a fenced writer leaves no manifest at all.
    let cas = write_watermark(
        &target.store,
        &target.watermark_key(),
        Watermark { checkpoint_seq: current.checkpoint_seq, last_frame: uploaded_through },
        current_generation.salt,
        cfg.epoch,
        cfg.pointer_generation,
        persisted.as_ref().and_then(|p| p.version.as_ref()),
    )
    .await?;
    if cas == WatermarkCas::Contended {
        // Somebody replaced the sidecar under us. Re-read to learn who.
        let now = read_watermark(&target.store, &target.watermark_key()).await?;
        let current_epoch = now.as_ref().map_or(0, |p| p.epoch);
        let current_pointer_generation = now.as_ref().map_or(0, |p| p.pointer_generation);
        if current_epoch > cfg.epoch || current_pointer_generation > cfg.pointer_generation {
            // A newer owner won the race. Our frames are orphaned under our
            // own epoch prefix with no manifest naming them, so restore never
            // sees them — the sink is exactly what the winner left.
            return Ok(StreamOutcome::Fenced {
                current_epoch,
                our_epoch: cfg.epoch,
                current_pointer_generation,
                our_pointer_generation: cfg.pointer_generation,
            });
        }
        // Same or lower epoch AND generation: neither fence can tell these two
        // writers apart. That means two processes are streaming the same
        // tenant under the SAME tokens — a caller bug (a duplicate streamer,
        // or an ownership token handed out twice), and exactly the
        // condition that must not be papered over with a retry.
        anyhow::bail!(
            "watermark CAS for {} lost to a concurrent writer at epoch {current_epoch} (pointer generation {current_pointer_generation}) while we hold epoch {} (pointer generation {}) — two streamers share one fencing token",
            target.watermark_key(),
            cfg.epoch,
            cfg.pointer_generation,
        );
    }

    let nanos = unix_nanos();
    let gen_key = target.generation_key(nanos);
    let manifest = format_generation_manifest(GenerationManifest {
        base_snapshot_key: cfg.base_snapshot_key,
        page_size: cfg.page_size,
        checkpoint_seq: current.checkpoint_seq,
        // R858-B19: stamp the generation this range actually came from, so
        // restore can refuse a chain that spans two WALs instead of splicing
        // them. Always `Some` on this path — a `None` salt means an empty WAL,
        // and an empty WAL took the `Empty` return above.
        salt: current_generation.salt,
        first_frame: start_frame,
        last_frame: uploaded_through,
        epoch: cfg.epoch,
        owner: cfg.owner,
        // R761-F2: exactly the batch objects the drain persisted. Restore
        // derives its keys from this list, so it is not a summary of the range
        // — it IS the range's index.
        frame_batches: &drain.frame_batches,
    });
    target
        .store
        .put(&gen_key, manifest.into_bytes().into())
        .await
        .with_context(|| format!("writing generation manifest {gen_key}"))?;

    let frame_count = uploaded_through - start_frame + 1;
    let gen_key = gen_key.to_string();
    if restarted {
        Ok(StreamOutcome::Restarted {
            generation_key: gen_key,
            previous_generation: prior_generation.unwrap_or_default(),
            new_generation: current_generation,
            first_frame: start_frame,
            last_frame: uploaded_through,
            frame_count,
            backpressure: drain.report,
            rpo,
        })
    } else {
        Ok(StreamOutcome::Streamed {
            generation_key: gen_key,
            first_frame: start_frame,
            last_frame: uploaded_through,
            checkpoint_seq: current.checkpoint_seq,
            frame_count,
            backpressure: drain.report,
            rpo,
        })
    }
}

/// Outcome of [`drain_frames_with_backpressure`]: the last frame_no
/// successfully persisted this call (`None` if every buffered frame was
/// shed before any of them landed) plus the backpressure activity report.
struct DrainOutcome {
    uploaded_through: Option<u64>,
    /// R761-F2: the `(first, last)` range of every batch object that actually
    /// landed, ascending and gap-free from the call's `start_frame` through
    /// `uploaded_through` (a batch is popped only on a successful upload, so a
    /// partial drain truncates this list rather than holing it). Copied into
    /// the generation manifest, which is what tells restore where to look.
    frame_batches: Vec<(u64, u64)>,
    report: BackpressureReport,
}

/// Read WAL frames `start_frame..=last_frame` into a bounded spill buffer
/// (`cfg.backpressure.spill_buffer_frames`) and drain them to `target`'s
/// object store through [`put_with_backoff`] (R574-F2), one **batch object**
/// per buffer-full (R761-F2).
///
/// The buffer only refills up to its bound, so once full the loop must
/// resolve the buffered batch — either by a successful upload or by the
/// configured [`BackpressurePolicy`] deciding what "stuck" means:
/// `Block` retries the batch forever on a throttling error (no data loss,
/// but the call can run long); `Fail` bubbles the error once
/// `backoff.max_retries` is exhausted (nothing persists past what already
/// landed); `Shed` drops the whole buffered backlog (loudly, via the
/// returned report) and returns whatever prefix already persisted. Any
/// non-throttling error bubbles immediately regardless of policy — this
/// mechanism is specifically for R2 throttling, not general fault
/// tolerance.
///
/// R761-F2 made the upload unit the buffer's contents rather than its head
/// frame, which is why `spill_buffer_frames` also sizes the largest object
/// this sink will write: `spill_buffer_frames * (24 + page_size)` bytes, ~1 MB
/// at the 256-frame default and a 4 KB page. That coupling is deliberate — the
/// bound already promises a memory ceiling, and a second knob for batch size
/// would only ever be set to some fraction of it.
async fn drain_frames_with_backpressure<S: WalSeam>(
    seam: &S,
    target: &BackupTarget,
    cfg: &StreamConfig<'_>,
    epoch: u64,
    checkpoint_seq: u32,
    start_frame: u64,
    last_frame: u64,
) -> Result<DrainOutcome> {
    let bp = &cfg.backpressure;
    let frame_size = WAL_FRAME_HEADER_SIZE + cfg.page_size;
    let bound = bp.spill_buffer_frames.max(1);

    let mut report = BackpressureReport { policy: bp.policy, ..Default::default() };
    let mut pending: VecDeque<(u64, Vec<u8>)> = VecDeque::new();
    let mut uploaded_through: Option<u64> = None;
    let mut frame_batches: Vec<(u64, u64)> = Vec::new();
    let mut next_to_read = start_frame;
    let mut buf = vec![0u8; frame_size];

    loop {
        // Refill up to the bound while there's more WAL to read. Once full,
        // the buffered batch must be resolved before we accept more.
        while pending.len() < bound && next_to_read <= last_frame {
            seam.wal_get_frame(next_to_read, &mut buf)
                .with_context(|| format!("reading wal frame {next_to_read}"))?;
            pending.push_back((next_to_read, buf.clone()));
            next_to_read += 1;
            report.high_water_frames = report.high_water_frames.max(pending.len());
        }
        let Some(&(first, _)) = pending.front() else {
            break; // Fully drained: nothing buffered, nothing left to read.
        };
        // Non-empty (we just matched `front`), and the buffer is filled in
        // ascending order without gaps, so the back frame closes the range.
        let last = pending.back().map_or(first, |&(n, _)| n);
        let mut body = Vec::with_capacity(pending.len() * frame_size);
        for (_, bytes) in &pending {
            body.extend_from_slice(bytes);
        }
        let key = target.frame_batch_key(epoch, checkpoint_seq, first, last);
        match put_with_backoff(&target.store, &key, body.into(), bp, &mut report.throttle_retries)
            .await
        {
            Ok(_) => {
                pending.clear();
                uploaded_through = Some(last);
                frame_batches.push((first, last));
            }
            Err(e) => match bp.policy {
                BackpressurePolicy::Shed => {
                    report.frames_shed += pending.len() as u64;
                    return Ok(DrainOutcome { uploaded_through, frame_batches, report });
                }
                BackpressurePolicy::Block | BackpressurePolicy::Fail => {
                    return Err(e).with_context(|| {
                        format!("uploading wal frames {first}-{last} to {key}")
                    });
                }
            },
        }
    }
    Ok(DrainOutcome { uploaded_through, frame_batches, report })
}

/// R858-B18 — how many times [`raw_consistent_copy_live`] will re-take a copy
/// whose [`SourceFingerprint`] moved underneath it before giving up loudly.
///
/// Bounded on purpose. An unbounded retry against a database under sustained
/// write pressure is an infinite loop that looks like a hang; a caller that
/// wants to keep trying should be the one deciding how long to keep trying, on
/// its own schedule. Four attempts is enough to ride out an isolated
/// checkpoint (headscale's database measures 94 KB, so an attempt is
/// sub-millisecond) and few enough that a genuinely hot database is reported as
/// hot within a few milliseconds instead of being ground at.
pub const COPY_VALIDATION_ATTEMPTS: u32 = 4;

/// Take a raw, point-in-time-consistent byte image of the database at `db_path`
/// WITHOUT folding the WAL via a `TRUNCATE` checkpoint, and WITHOUT locking out
/// the process that owns it. The live-writer pair of
/// [`crate::dedup::raw_consistent_copy`], which a concurrent writer can make
/// return `busy`.
///
/// Algorithm — the read-only "copy main + replay WAL frames ourselves" path
/// flagged in the working doc and the R005-T1 spike, wrapped in R858-B18's
/// optimistic validation:
///
/// 1. Sample the source's [`SourceFingerprint`] — WAL salt + checkpoint
///    sequence, WAL length, main length, main change counter — straight off
///    disk, with no engine open.
/// 2. Open a fresh `turso_core::Connection` via [`CoreWalSeam::open_reader`],
///    which takes **no** whole-file lock (so the application keeps working) and
///    calls `wal_auto_actions_disable` so our seam can't auto-checkpoint or
///    restart the WAL header mid-read on our connection.
/// 3. Read the main DB file bytes from disk. With auto-actions disabled on our
///    connection the file cannot be folded by us; a concurrent writer in a
///    separate connection only ever extends the WAL (the main file is only
///    written by a checkpoint).
/// 4. Walk WAL frames `1..=max_frame`, replaying each page into the in-memory
///    image at offset `(page_no - 1) * page_size`. Track the last
///    `is_commit_frame` and the corresponding `db_size`. Any frames past the
///    last commit are uncommitted mid-transaction garbage — drop them
///    (crash-consistency, matching restore's `wal_insert_end(false)`).
/// 5. Truncate / grow the image to `db_size * page_size`.
/// 6. Re-sample the fingerprint. **Accept the image only if
///    [`SourceFingerprint::stable_across`] holds** — i.e. only if nothing that
///    can tear the copy moved. A foreign checkpoint inside our read window would
///    splice pre- and post-fold state; discard that image and retry from step 1,
///    up to [`COPY_VALIDATION_ATTEMPTS`] times, then fail. (A plain WAL append
///    is *not* movement for this purpose, and that distinction is what keeps
///    this usable against a database that is actually in use — see
///    `stable_across`.)
///
/// Step 6 is the entire correctness story against a foreign engine, and it is
/// why this function never returns an unvalidated image: a torn WAL replay
/// still produces a structurally valid SQLite file that passes
/// `PRAGMA integrity_check`, so "it parsed" proves nothing. See
/// [`SourceFingerprint`] for why validation rather than SQLite's real
/// `-shm` reader protocol.
///
/// Returned bytes are a self-contained vanilla-SQLite image (page-offset
/// stable, no `-wal` sidecar required), ready to feed
/// [`crate::dedup::snapshot_dedup`]'s content-addressed chunking under a
/// concurrent writer.
pub async fn raw_consistent_copy_live(db_path: &str, page_size: usize) -> Result<Vec<u8>> {
    let image = validated_against_source(db_path, "live copy", || async {
        let seam = CoreWalSeam::open_reader(db_path)
            .with_context(|| format!("opening read-only WAL seam on {db_path}"))?;
        let main_bytes = std::fs::read(db_path)
            .with_context(|| format!("reading main db file {db_path}"))?;
        replay_wal_onto_main(&seam, main_bytes, page_size)
        // Seam dropped at the end of this block, so nothing of ours holds the
        // file while the post-copy fingerprint is sampled.
    })
    .await?;
    anyhow::ensure!(
        image.starts_with(b"SQLite format 3\0"),
        "live consistent copy of {db_path} is not a SQLite database"
    );
    Ok(image)
}

/// R858-B18 — run `take` against the live database at `db_path` and return its
/// result **only** if the source provably held still for the duration.
///
/// This is the optimistic-validation protocol both source-side tiers share:
/// tier 2's WAL-replay copy ([`raw_consistent_copy_live`]) and tier 1a's
/// `VACUUM INTO` (`crate::snapshot`). Both read a database a foreign engine may
/// be checkpointing underneath them, neither can take a lock that engine
/// respects, and both are therefore only correct if a fold inside their read
/// window is *detected*. `what` names the operation in the refusal message.
///
/// Each attempt samples a [`SourceFingerprint`] before and after, then
/// classifies the outcome on both axes — did it succeed, and did the source
/// move:
///
/// | | source held still | source moved |
/// |---|---|---|
/// | **`Ok`** | accept | discard, retry (it may splice pre-/post-fold state) |
/// | **`Err`** | return the error — nothing raced us, so it is real | retry (a symptom of the race) |
///
/// That bottom-right cell is why the fingerprint is load-bearing beyond
/// accept/reject: R858-B18's probe H measured the *common* shape of a caught
/// race not as a clean torn image but as `short read on WAL frame` — a foreign
/// checkpoint truncating the WAL while turso walked it. Without the fingerprint
/// there is no way to tell that transient apart from a genuinely corrupt
/// database, and the two want opposite handling.
///
/// After [`COPY_VALIDATION_ATTEMPTS`] attempts all of which saw movement, this
/// fails loudly and returns nothing.
pub(crate) async fn validated_against_source<T, F, Fut>(
    db_path: &str,
    what: &str,
    mut take: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut moved: Option<(SourceFingerprint, SourceFingerprint, Option<anyhow::Error>)> = None;
    for attempt in 1..=COPY_VALIDATION_ATTEMPTS {
        if attempt > 1 {
            // Back off between attempts: retrying instantly against a writer
            // mid-checkpoint just spends the whole budget inside one fold.
            tokio::time::sleep(std::time::Duration::from_millis(20 * u64::from(attempt - 1)))
                .await;
        }
        let before = SourceFingerprint::read(db_path)?;
        let attempted = take().await;
        let after = SourceFingerprint::read(db_path)?;
        let stable = before.stable_across(&after);
        match attempted {
            Ok(value) if stable => return Ok(value),
            Ok(_) => moved = Some((before, after, None)),
            Err(e) if !stable => moved = Some((before, after, Some(e))),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "{what} of {db_path} failed against a source that did NOT move during \
                         the attempt ({}), so this is not a concurrent-writer race",
                        before.describe()
                    )
                })
            }
        }
    }
    let (before, after, last_err) = moved.expect("COPY_VALIDATION_ATTEMPTS is non-zero");
    let because = match last_err {
        Some(e) => format!("the last attempt also failed mid-read ({e:#})"),
        None => "each attempt produced a result that could not be validated".to_string(),
    };
    anyhow::bail!(
        "refusing a {what} of {db_path}: the source moved under every one of \
         {COPY_VALIDATION_ATTEMPTS} attempts, so nothing could be validated as \
         point-in-time — {because}. The last attempt saw [{}] before and [{}] after, so a \
         foreign writer or checkpointer is active. Retry on your own schedule; NO result \
         is returned, because a torn read of a SQLite database still parses as valid SQLite.",
        before.describe(),
        after.describe(),
    )
}

/// Pure replay of every committed WAL frame visible through `seam` onto
/// `main_bytes`. Split out from [`raw_consistent_copy_live`] so it can be
/// driven by a mock seam in unit tests; the live entry point layers disk I/O
/// and magic-byte validation on top.
///
/// Contract: the highest `is_commit_frame` in `1..=wal_state.last_frame`
/// defines both the post-replay page count and the cutoff for which frames
/// are applied. If no frame in that window is a commit, `main_bytes` is
/// returned unchanged.
pub(crate) fn replay_wal_onto_main<S: WalSeam>(
    seam: &S,
    mut main_bytes: Vec<u8>,
    page_size: usize,
) -> Result<Vec<u8>> {
    anyhow::ensure!(page_size > 0, "page_size must be non-zero");
    let watermark = seam.wal_state()?;

    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let mut buf = vec![0u8; frame_size];

    struct PendingFrame {
        page_no: u32,
        db_size: u32,
        page_bytes: Vec<u8>,
    }
    let mut frames: Vec<PendingFrame> = Vec::new();
    let mut last_commit_idx: Option<usize> = None;
    for frame_no in 1..=watermark.last_frame {
        let info = seam
            .wal_get_frame(frame_no, &mut buf)
            .with_context(|| format!("reading WAL frame {frame_no}"))?;
        anyhow::ensure!(
            info.page_no >= 1,
            "WAL frame {frame_no}: page_no must be >= 1"
        );
        frames.push(PendingFrame {
            page_no: info.page_no,
            db_size: info.db_size,
            page_bytes: buf[WAL_FRAME_HEADER_SIZE..].to_vec(),
        });
        if info.is_commit_frame() {
            last_commit_idx = Some(frames.len() - 1);
        }
    }

    let Some(last_commit) = last_commit_idx else {
        // No committed frames in our view — main file alone is the image.
        // Any uncommitted suffix in the WAL is dropped by construction.
        return Ok(main_bytes);
    };
    let final_db_size = frames[last_commit].db_size as usize;
    let target_size = final_db_size
        .checked_mul(page_size)
        .context("db_size * page_size overflow")?;

    // Grow image to hold the final committed image AND any page slot we'll
    // touch in the commit prefix (a frame may write a page above db_size
    // mid-grow; the final truncate cuts that back to db_size).
    let max_off_needed: usize = frames[..=last_commit]
        .iter()
        .map(|f| f.page_no as usize * page_size)
        .max()
        .unwrap_or(0);
    let need = target_size.max(max_off_needed);
    if main_bytes.len() < need {
        main_bytes.resize(need, 0);
    }
    for f in &frames[..=last_commit] {
        let off = (f.page_no as usize - 1) * page_size;
        main_bytes[off..off + page_size].copy_from_slice(&f.page_bytes);
    }
    main_bytes.truncate(target_size);

    Ok(main_bytes)
}

/// Summary of a [`restore_latest_stream`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreOutcome {
    /// The tier-1a snapshot key every generation manifest referenced (must agree).
    pub base_snapshot_key: String,
    /// Sole `checkpoint_seq_no` across the replayed manifests (v1 refuses to
    /// span a WAL restart — see [`validate_generation_chain`]).
    pub checkpoint_seq: u32,
    /// Number of generation manifests replayed (≥ 1).
    pub generation_count: usize,
    /// Total frames inserted across all generations (`last_frame - 0`, since
    /// the chain is required to start at frame 1 and be gap-free).
    pub frames_replayed: u64,
    /// The last frame position written into the destination WAL.
    pub last_frame: u64,
    /// R732-F2: the highest fencing epoch contributing to this restore (`0`
    /// for a chain written before fencing existed). Reported so an operator
    /// restoring after an ownership transfer can see which owner's data they
    /// actually got.
    pub epoch: u64,
}

/// A consistency-checked sequence of generation manifests, ready to drive a
/// replay. The fields are the single base / page size / checkpoint sequence
/// shared by every manifest in the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedChain {
    pub base_snapshot_key: String,
    pub page_size: usize,
    /// R858-B19: the one WAL generation every manifest in the chain agrees on.
    /// `salt` is `Some` for any chain of two or more manifests — a chain that
    /// could not prove a single generation never gets this far.
    pub generation: WalGeneration,
    pub total_frames: u64,
    /// R732-F2: the highest fencing epoch in the chain — i.e. the most recent
    /// owner that contributed frames. Unlike the other fields this is a
    /// *maximum*, not a shared constant: ownership legitimately moves
    /// mid-chain, so a chain may span epochs as long as they never go
    /// backwards.
    pub epoch: u64,
}

/// Validate that a sorted list of generation manifests forms a single,
/// replayable chain: same base snapshot, same page size, single checkpoint
/// sequence, frames starting at 1 and contiguous across manifests.
///
/// V1 refuses to span a WAL restart (multiple `checkpoint_seq` values). A
/// restart implies the source engine folded the WAL into main between
/// generations — replaying the post-restart frames onto our pre-restart base
/// would skip that fold and corrupt the result. The remediation is a fresh
/// tier-1a snapshot, not heroics in restore.
///
/// # R858-B19 — refuse what cannot be proven
///
/// The `checkpoint_seq` test above was the *only* generation check here, and it
/// is blind to the fold that matters: a writer-process restart deletes the
/// `-wal` file and the next writer starts a fresh WAL back at sequence `0`, so
/// two unrelated WALs both report `0` and a spliced chain sailed through. That
/// produced a restore that reported SUCCESS while writing a stale-but-plausible
/// image (probe B: 9 rows against a 12-row source, `integrity_check ok`) or one
/// upstream sqlite3 calls malformed (probe F). **A silent wrong image is the
/// specific failure this function now exists to make impossible.**
///
/// So the salt is checked too, and — the part that matters — a chain that
/// cannot be *shown* to come from one WAL is refused rather than replayed:
///
/// - Two or more manifests, any of them lacking a salt (written before `v4`):
///   REFUSE. The splice is exactly what a pre-R858-B19 writer produced, and
///   nothing in those manifests records which WAL each range came from.
/// - Two or more manifests with disagreeing salts: REFUSE, naming both.
/// - A single manifest: accepted with whatever salt it has, including none.
///   One generation is not a splice; there is nothing to prove.
pub(crate) fn validate_generation_chain(
    manifests: &[OwnedGenerationManifest],
) -> Result<ValidatedChain> {
    let first = manifests
        .first()
        .context("validate_generation_chain: empty manifest list")?;
    let mut expected_next_frame: u64 = 1;
    let mut chain_epoch: u64 = 0;
    let multi = manifests.len() > 1;
    for (i, m) in manifests.iter().enumerate() {
        // R732-F2 (W245): generations are ordered by write time, so a chain
        // whose epoch goes BACKWARDS says a stale owner wrote after the sink
        // had already moved on — precisely the split-brain the fence exists to
        // stop, caught here on the read side too. Non-decreasing is fine and
        // expected: ownership transfers mid-stream and the new owner keeps
        // appending frames to the same contiguous range.
        if m.epoch < chain_epoch {
            anyhow::bail!(
                "generation #{i} was written at epoch {} but an earlier generation in the chain is at epoch {} — a fenced (stale) owner wrote to this sink, restore refuses rather than replay interleaved frames",
                m.epoch,
                chain_epoch,
            );
        }
        chain_epoch = m.epoch;
        if m.base_snapshot_key != first.base_snapshot_key {
            anyhow::bail!(
                "generation #{i} references base {:?}, expected {:?} — chain spans bases, restore needs a fresh tier-1a snapshot",
                m.base_snapshot_key,
                first.base_snapshot_key,
            );
        }
        if m.page_size != first.page_size {
            anyhow::bail!(
                "generation #{i} page_size {} differs from chain page_size {} — corrupt manifest or mixed sinks",
                m.page_size,
                first.page_size,
            );
        }
        if m.checkpoint_seq != first.checkpoint_seq {
            anyhow::bail!(
                "generation #{i} checkpoint_seq {} differs from chain checkpoint_seq {} — WAL restart between generations, restore needs a fresh tier-1a snapshot",
                m.checkpoint_seq,
                first.checkpoint_seq,
            );
        }
        // R858-B19: the check `checkpoint_seq` cannot make. Only enforced on a
        // multi-manifest chain — a lone generation has nothing to be spliced
        // to, so refusing it would strand every pre-v4 single-generation backup
        // for no safety gain.
        if multi {
            let Some(s) = m.salt else {
                anyhow::bail!(
                    "generation #{i} carries no WAL salt (written by a pre-R858-B19 writer) and this chain spans {} generations — which WAL each range came from was never recorded, so a chain spliced across a WAL recreate is indistinguishable from a good one. Restore refuses rather than replay a plausible wrong image; take a fresh tier-1a snapshot",
                    manifests.len(),
                );
            };
            if let Some(fs) = first.salt {
                if s != fs {
                    anyhow::bail!(
                        "generation #{i} was written under WAL salt {s} but the chain starts at salt {fs} (both at checkpoint_seq {}) — the source WAL was RECREATED mid-stream, so these frames belong to two different WALs and replaying them as one would corrupt the image. Restore needs a fresh tier-1a snapshot",
                        first.checkpoint_seq,
                    );
                }
            }
        }
        if m.first_frame != expected_next_frame {
            anyhow::bail!(
                "generation #{i} starts at frame {} but the previous generation ended at frame {} — gap in stream",
                m.first_frame,
                expected_next_frame.saturating_sub(1),
            );
        }
        if m.last_frame < m.first_frame {
            anyhow::bail!(
                "generation #{i} has last_frame {} < first_frame {} — corrupt manifest",
                m.last_frame,
                m.first_frame,
            );
        }
        expected_next_frame = m.last_frame + 1;
    }
    Ok(ValidatedChain {
        base_snapshot_key: first.base_snapshot_key.clone(),
        page_size: first.page_size,
        generation: WalGeneration {
            checkpoint_seq: first.checkpoint_seq,
            salt: first.salt,
        },
        total_frames: expected_next_frame - 1,
        epoch: chain_epoch,
    })
}

/// Fetch one generation's frames and hand each to `on_frame` in ascending
/// frame order, skipping anything before `from_frame`. Returns how many frames
/// were delivered.
///
/// R761-F2: the one read path that resolves a manifest to frame bytes,
/// batched or not — [`BackupTarget::frame_objects_of`] decides which layout
/// the generation used, and a batch object is split here at
/// `24 + page_size` boundaries. Both restore's replay and
/// [`crate::puller::WalPuller`] call it, so a layout the writer can produce
/// can never be readable by one and not the other.
pub(crate) async fn for_each_frame_in_generation<F>(
    target: &BackupTarget,
    m: &OwnedGenerationManifest,
    from_frame: u64,
    mut on_frame: F,
) -> Result<u64>
where
    F: FnMut(u64, &[u8]) -> Result<()>,
{
    let frame_size = WAL_FRAME_HEADER_SIZE + m.page_size;
    let mut delivered: u64 = 0;
    for (key, first, last) in target.frame_objects_of(m) {
        if last < from_frame {
            continue; // wholly behind the caller's cursor — don't pay for the GET
        }
        let bytes = target
            .store
            .get(&key)
            .await
            .with_context(|| format!("fetching frame object {key}"))?
            .bytes()
            .await
            .with_context(|| format!("reading frame object body {key}"))?;
        let frames_in_object = (last - first + 1) as usize;
        let want = frames_in_object * frame_size;
        if bytes.len() != want {
            anyhow::bail!(
                "frame object {key} is {} bytes, expected {want} ({frames_in_object} frame(s) x [{WAL_FRAME_HEADER_SIZE} header + {} page])",
                bytes.len(),
                m.page_size,
            );
        }
        for (i, frame_no) in (first..=last).enumerate() {
            if frame_no < from_frame {
                continue;
            }
            let at = i * frame_size;
            on_frame(frame_no, &bytes[at..at + frame_size])?;
            delivered += 1;
        }
    }
    Ok(delivered)
}

/// Replay every frame named by `manifests` into `seam`, in (checkpoint_seq,
/// frame_no) order. Caller must have already called `wal_insert_begin` on the
/// seam; `wal_insert_end` is also the caller's responsibility (so a test or a
/// future fault-injection path can choose `force_commit`).
///
/// Each manifest's own `page_size` sizes its frames —
/// [`validate_generation_chain`] has already established they all agree, so
/// there is no separate chain-level page size to thread through.
async fn replay_frames_into<S: WalInsertSeam>(
    target: &BackupTarget,
    seam: &S,
    manifests: &[OwnedGenerationManifest],
) -> Result<u64> {
    let mut total: u64 = 0;
    for m in manifests {
        total += for_each_frame_in_generation(target, m, 0, |frame_no, bytes| {
            seam.wal_insert_frame(frame_no, bytes)
                .with_context(|| format!("inserting frame {frame_no}"))
        })
        .await?;
    }
    Ok(total)
}

/// Restore a database from a streamed backup: download the base tier-1a
/// snapshot referenced by the generation manifests, then replay every uploaded
/// WAL frame onto it via the [`WalInsertSeam`] of a fresh `turso_core`
/// connection. The session ends with `force_commit = false` so any frames
/// captured after the last commit frame are dropped (crash-consistency).
///
/// Errors loudly when:
/// - There are no generation manifests under the prefix (caller should restore
///   via the tier-1a path instead).
/// - The chain spans more than one `checkpoint_seq` or more than one base
///   snapshot (restart / mixed bases — needs a fresh tier-1a snapshot).
/// - Frame ranges are not gap-free starting at 1.
/// - A referenced frame object is missing or the wrong byte length.
///
/// `dest_path`'s `-wal` and `-shm` sidecars are removed before the base is
/// written; any pre-existing turso connection on `dest_path` must be closed
/// by the caller.
///
/// A caller that has already listed the chain for its own reasons should call
/// [`restore_stream_from_manifests`] instead and skip the re-listing this one
/// does — see its docs for what that costs (R760-T18).
pub async fn restore_latest_stream(
    target: &BackupTarget,
    dest_path: &str,
) -> Result<RestoreOutcome> {
    let manifests = list_and_parse_generation_manifests(target).await?;
    restore_stream_from_manifests(target, dest_path, &manifests).await
}

/// [`restore_latest_stream`] for a caller that has already listed and parsed
/// the chain — same replay, minus the listing.
///
/// R760-T18: a caller that must inspect the chain *before* deciding how to
/// restore otherwise pays for every manifest twice. roadcase's `SinkHydrator`
/// is the live example: it calls [`list_and_parse_generation_manifests`] to
/// ask whether the era has any frames at all (an era whose base is still the
/// whole story restores via `snapshot::restore_latest` instead), and then
/// `restore_latest_stream` re-listed and re-fetched the identical objects. A
/// cold start with `n` generations was measured at `3n+1` Class B operations
/// against a `2n+1` floor — one GET per manifest, one per generation's frame
/// batch, one for the base. Handing the parse straight in removes the `n`
/// duplicates.
///
/// `manifests` must be ascending by generation, which is the order
/// [`list_and_parse_generation_manifests`] returns them in; the chain is
/// validated here exactly as it is on the listing path, so a hand-assembled
/// list cannot smuggle past a check.
pub async fn restore_stream_from_manifests(
    target: &BackupTarget,
    dest_path: &str,
    manifests: &[OwnedGenerationManifest],
) -> Result<RestoreOutcome> {
    if manifests.is_empty() {
        anyhow::bail!(
            "no generation manifests under {} — restore tier-1a directly via snapshot::restore_latest",
            join_key(&target.prefix, "generations"),
        );
    }
    let chain = validate_generation_chain(manifests)?;

    // Download the base snapshot and lay it down at dest_path. Strip any stale
    // WAL/-shm sidecars first — the base alone is the entire pre-replay image
    // (VACUUM INTO output has no WAL).
    let base_bytes = target
        .store
        .get(&ObjPath::from(chain.base_snapshot_key.clone()))
        .await
        .with_context(|| format!("downloading base snapshot {}", chain.base_snapshot_key))?
        .bytes()
        .await
        .with_context(|| format!("reading base snapshot body {}", chain.base_snapshot_key))?;
    for sfx in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{dest_path}{sfx}"));
    }
    std::fs::write(dest_path, &base_bytes)
        .with_context(|| format!("writing restored base snapshot to {dest_path}"))?;

    // Open a fresh seam on the laid-down base and replay. `CoreWalSeam::open`
    // disables auto-actions; `wal_insert_begin` further locks the session to
    // empty auto-actions for the txn, so restart can't race the replay.
    let seam = CoreWalSeam::open(dest_path)?;
    seam.wal_insert_begin()
        .context("starting WAL insert session on restore destination")?;
    let frames = match replay_frames_into(target, &seam, manifests).await {
        Ok(n) => n,
        Err(e) => {
            // Best-effort: roll the partial replay back so we never leave the
            // dest's WAL with an uncommitted suffix.
            let _ = seam.wal_insert_end(false);
            return Err(e);
        }
    };
    // force_commit=false: the engine drops any tail past the last commit
    // frame, which is exactly the crash-consistency story we want — a
    // mid-transaction tail captured by tail_frames gets truncated cleanly.
    seam.wal_insert_end(false)
        .context("closing WAL insert session on restore destination")?;

    Ok(RestoreOutcome {
        base_snapshot_key: chain.base_snapshot_key,
        checkpoint_seq: chain.generation.checkpoint_seq,
        generation_count: manifests.len(),
        frames_replayed: frames,
        last_frame: chain.total_frames,
        epoch: chain.epoch,
    })
}

/// Every generation manifest at a sink, in key (i.e. chronological) order.
///
/// **The GETs are serialized**, one manifest at a time, and so is the frame
/// replay in [`for_each_frame_in_generation`] — so a restore's wall clock is
/// `(2n+1) x RTT` plus transfer for `n` generations, not `max(RTT)`. That is
/// fine at the tens of generations a dedicated streamer accumulates between
/// checkpoints and is not fine at thousands; R760-T18 bounds it on the
/// *writer* side (roadcase caps generations per era) rather than by making
/// this concurrent, because concurrency here would mean a real
/// `futures`/`tokio::spawn` dependency in a crate that deliberately keeps
/// `futures_util` to `[dev-dependencies]`.
///
/// Public since R732-T5: a split-brain reconciliation oracle needs to ask what
/// a sink would actually replay — which owner wrote which frame range, under
/// which epoch — without running a full restore. Pair it with
/// [`validate_generation_chain`]'s public counterpart if you need the chain
/// checked rather than merely listed.
pub async fn list_and_parse_generation_manifests(
    target: &BackupTarget,
) -> Result<Vec<OwnedGenerationManifest>> {
    let prefix = join_key(&target.prefix, "generations");
    let listing = target
        .store
        .list_with_delimiter(Some(&prefix))
        .await
        .with_context(|| format!("listing generation manifests under {prefix}"))?;
    let mut keys: Vec<ObjPath> = listing.objects.into_iter().map(|o| o.location).collect();
    keys.sort();
    let mut manifests = Vec::with_capacity(keys.len());
    for key in &keys {
        let bytes = target
            .store
            .get(key)
            .await
            .with_context(|| format!("fetching generation manifest {key}"))?
            .bytes()
            .await
            .with_context(|| format!("reading generation manifest body {key}"))?;
        let m = parse_generation_manifest(&String::from_utf8_lossy(&bytes))
            .with_context(|| format!("parsing generation manifest {key}"))?;
        manifests.push(m);
    }
    Ok(manifests)
}

/// What the sink's fence currently stands at — the two comparands
/// [`tail_frames`] checks a writer against, and nothing else.
///
/// R869: this is the off-fleet copy of a fencing token. It matters because
/// `epoch` is minted by a raft group, and a raft group can be *destroyed* —
/// wipe the raft dir, re-form, and the fresh cluster's first
/// `ClaimTenant` grants epoch 1 while this sidecar still says 5, so the
/// rebuilt cluster is fenced out of its own sink. Reading the fence back is how
/// a rebuild starts above the number its dead predecessor left here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FenceState {
    /// The fencing epoch of the writer that last advanced the sidecar. `0`
    /// means "unfenced" — either nothing has claimed this sink or it predates
    /// R732-F2 — and fences nobody.
    pub epoch: u64,
    /// The pointer generation of that same writer. `0` means unfenced, exactly
    /// as for `epoch`.
    pub pointer_generation: u64,
}

/// Read the fence [`tail_frames`] would check a writer against, without writing
/// anything or needing a WAL.
///
/// `Ok(None)` means the sidecar does not exist: nothing has ever streamed to
/// this prefix, so **there is no fence here at all** and any writer is
/// accepted. Do not collapse that into `FenceState::default()` at a call site
/// that is deciding whether to fence — "unfenced because nobody has written"
/// and "unfenced because an old writer stamped 0" are the same *value* but a
/// caller that cares about the difference (a rebuild deciding whether a tenant
/// was ever live) needs the `Option`.
///
/// **A floor derived from `epoch` is a lower bound, not an upper one.** It is
/// the highest epoch anyone has *written under*, which is not the highest a
/// dead raft group *granted* — a tenant claimed twice while idle leaves a node
/// holding an epoch strictly above anything this sidecar ever saw. So a rebuild
/// that seeds from this number closes availability (its own writes are
/// accepted) and does **not** on its own fence a resurrected node; that takes
/// `pointer_generation`, which is minted off-fleet where a dead cluster cannot
/// reach it. `oss/yubaba/crates/yubaba/tests/raft_rebuild_fencing.rs` drives
/// both halves against this code.
pub async fn read_fence_state(target: &BackupTarget) -> Result<Option<FenceState>> {
    Ok(read_watermark(&target.store, &target.watermark_key())
        .await?
        .map(|p| FenceState {
            epoch: p.epoch,
            pointer_generation: p.pointer_generation,
        }))
}

/// In-memory shape of a generation manifest. Format on disk:
///
/// ```text
/// TURSO-BACKUP STREAM v4
/// base_snapshot <key>
/// page_size <n>
/// checkpoint_seq <n>
/// wal_salt <salt1>-<salt2>
/// first_frame <n>
/// last_frame <n>
/// epoch <n>
/// owner <label>        (optional)
/// frame_batch <first>-<last>   (one per batch object, ascending, gap-free)
/// ```
///
/// Text, dependency-free (no serde), one field per line. Mirrors the
/// `dedup::Manifest` convention so the crate stays consistent.
///
/// R732-F2 added `epoch`/`owner` and moved the header to `v2`. R761-F2 added
/// the `frame_batch` list and moved it to `v3`. R858-B19 added `wal_salt` and
/// moved it to `v4`. Older manifests still parse — `v1` with `epoch = 0`,
/// `owner = None`; `v1`/`v2` with no batch list, which is what marks their
/// frames as living one-per-object; `v1`/`v2`/`v3` with `salt = None` — so
/// backups written before any of those changes stay *parseable*. No older
/// version is ever written any more.
///
/// R858-B19, and this is the one place a legacy manifest is not merely
/// second-class: a chain of two or more manifests where any of them lacks a
/// salt is **refused** by [`validate_generation_chain`], because a pre-R858-B19
/// writer could splice two WAL generations into a contiguous-looking chain and
/// nothing in the manifest records which WAL each range came from. A
/// single-manifest chain is still restored — one generation cannot be a splice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationManifest<'a> {
    pub base_snapshot_key: &'a str,
    pub page_size: usize,
    pub checkpoint_seq: u32,
    /// R858-B19: the WAL header salt these frames were read under — the field
    /// that actually names the generation, since `checkpoint_seq` resets to 0
    /// across a writer restart. `None` only when re-formatting a pre-R858-B19
    /// manifest; [`tail_frames`] always has one.
    pub salt: Option<WalSalt>,
    pub first_frame: u64,
    pub last_frame: u64,
    /// R732-F2: the fencing epoch this generation was written under. `0` means
    /// it predates fencing (a `v1` manifest), which is also the key shape its
    /// frames live under — see [`BackupTarget::frame_key`].
    pub epoch: u64,
    /// R732-F2: opaque owner label, diagnostic only. See [`StreamConfig::owner`].
    pub owner: Option<&'a str>,
    /// R761-F2: the `(first, last)` frame range of each batch object holding
    /// this generation's frames, ascending and exactly tiling
    /// `first_frame..=last_frame`. Empty means the pre-batching layout (one
    /// object per frame); a writer emits it empty only when re-formatting a
    /// legacy manifest.
    pub frame_batches: &'a [(u64, u64)],
}

/// Header of a manifest written by a fencing-aware writer (R732-F2). The
/// version is bumped rather than the `epoch` key just being added, because
/// [`parse_generation_manifest`] rejects unknown keys: a pre-R732 binary
/// reading a fenced manifest would otherwise fail with `unknown manifest key:
/// epoch`, which reads like corruption. Failing on the *header* says the real
/// thing — this backup was written by a newer writer.
const MANIFEST_HEADER_V2: &str = "TURSO-BACKUP STREAM v2";
/// Pre-fencing header. Still accepted on read (those backups must stay
/// restorable) and parses with `epoch = 0`, `owner = None`; never written.
const MANIFEST_HEADER_V1: &str = "TURSO-BACKUP STREAM v1";
/// R761-F2 — carries the `frame_batch` list. Bumped for the same reason
/// `v2` was: [`parse_generation_manifest`] rejects unknown keys, so a
/// pre-R761-F2 binary reading a batched manifest would fail with `unknown
/// manifest key: frame_batch`, which reads like corruption. Failing on the
/// header says the true thing — a newer writer wrote this sink. The refusal
/// matters more here than it did for fencing: an older reader that somehow
/// skipped the batch list would look for per-frame keys that do not exist.
const MANIFEST_HEADER_V3: &str = "TURSO-BACKUP STREAM v3";
/// R858-B19 — carries `wal_salt`, the field that makes a generation chain
/// checkable across a WAL recreate. Bumped for the same reason `v2` and `v3`
/// were: [`parse_generation_manifest`] rejects unknown keys, so an older binary
/// reading one of these would fail with `unknown manifest key: wal_salt`, which
/// reads like corruption. Failing on the header says the true thing.
const MANIFEST_HEADER_V4: &str = "TURSO-BACKUP STREAM v4";

pub(crate) fn format_generation_manifest(m: GenerationManifest<'_>) -> String {
    // Each header promises the fields that version introduced, so stamp the
    // newest version whose promises this manifest can actually keep. The only
    // way to reach anything but v4 is re-formatting a legacy manifest (a test,
    // or a repair tool); tail_frames always has both a salt and batches.
    // A v4 manifest promises BOTH the salt and the batch list, so a legacy
    // shape missing either falls back rather than stamping a version whose
    // promises it cannot keep. `salt` without batches has no representation and
    // cannot occur: only tail_frames produces a salt, and it always batches.
    let salt = if m.frame_batches.is_empty() { None } else { m.salt };
    let header = if salt.is_some() {
        MANIFEST_HEADER_V4
    } else if m.frame_batches.is_empty() {
        MANIFEST_HEADER_V2
    } else {
        MANIFEST_HEADER_V3
    };
    let mut out = format!(
        "{}\nbase_snapshot {}\npage_size {}\ncheckpoint_seq {}\n",
        header, m.base_snapshot_key, m.page_size, m.checkpoint_seq,
    );
    if let Some(s) = salt {
        out.push_str(&format!("wal_salt {}-{}\n", s.salt1, s.salt2));
    }
    out.push_str(&format!(
        "first_frame {}\nlast_frame {}\nepoch {}\n",
        m.first_frame, m.last_frame, m.epoch,
    ));
    // Omitted rather than written empty: the parser splits on the first space,
    // so `owner ` with no value would round-trip to `Some("")`.
    if let Some(owner) = m.owner {
        out.push_str(&format!("owner {owner}\n"));
    }
    for (first, last) in m.frame_batches {
        out.push_str(&format!("frame_batch {first}-{last}\n"));
    }
    out
}

/// Parse a generation manifest. Tolerant to trailing whitespace; rejects any
/// other shape (so a corrupt manifest fails loudly during restore, doesn't
/// silently degrade).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedGenerationManifest {
    pub base_snapshot_key: String,
    pub page_size: usize,
    pub checkpoint_seq: u32,
    /// R858-B19: `None` for a pre-`v4` manifest, which means the WAL
    /// generation behind these frames was never recorded and cannot be
    /// recovered. Unknown, not "the same as its neighbour" — see
    /// [`validate_generation_chain`].
    pub salt: Option<WalSalt>,
    pub first_frame: u64,
    pub last_frame: u64,
    /// R732-F2: `0` for a `v1` (pre-fencing) manifest.
    pub epoch: u64,
    /// R732-F2: diagnostic only; `None` when the writer did not label itself.
    pub owner: Option<String>,
    /// R761-F2: the batch objects covering `first_frame..=last_frame`,
    /// ascending and gap-free (enforced by [`parse_generation_manifest`]).
    /// **Empty means the pre-batching layout** — one object per frame — which
    /// is how a `v1`/`v2` manifest keeps restoring. Resolve it to keys with
    /// `BackupTarget::frame_objects_of` (crate-private) rather than branching
    /// at each call site.
    pub frame_batches: Vec<(u64, u64)>,
}

pub fn parse_generation_manifest(text: &str) -> Result<OwnedGenerationManifest> {
    let mut lines = text.lines();
    let header = lines.next().context("empty generation manifest")?.trim();
    let (v2, v3, v4) = match header {
        MANIFEST_HEADER_V4 => (true, true, true),
        MANIFEST_HEADER_V3 => (true, true, false),
        MANIFEST_HEADER_V2 => (true, false, false),
        MANIFEST_HEADER_V1 => (false, false, false),
        other => anyhow::bail!("unexpected manifest header: {other:?}"),
    };
    let mut base_snapshot_key: Option<String> = None;
    let mut page_size: Option<usize> = None;
    let mut checkpoint_seq: Option<u32> = None;
    let mut salt: Option<WalSalt> = None;
    let mut first_frame: Option<u64> = None;
    let mut last_frame: Option<u64> = None;
    let mut epoch: Option<u64> = None;
    let mut owner: Option<String> = None;
    let mut frame_batches: Vec<(u64, u64)> = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (k, v) = line
            .split_once(' ')
            .with_context(|| format!("malformed manifest line: {line:?}"))?;
        match k {
            "base_snapshot" => base_snapshot_key = Some(v.to_string()),
            "page_size" => page_size = Some(v.parse().context("page_size")?),
            "checkpoint_seq" => checkpoint_seq = Some(v.parse().context("checkpoint_seq")?),
            "wal_salt" => {
                let (s1, s2) = v
                    .split_once('-')
                    .with_context(|| format!("malformed wal_salt pair: {v:?}"))?;
                salt = Some(WalSalt {
                    salt1: s1.trim().parse().context("wal_salt salt1")?,
                    salt2: s2.trim().parse().context("wal_salt salt2")?,
                });
            }
            "first_frame" => first_frame = Some(v.parse().context("first_frame")?),
            "last_frame" => last_frame = Some(v.parse().context("last_frame")?),
            "epoch" => epoch = Some(v.parse().context("epoch")?),
            "owner" => owner = Some(v.to_string()),
            "frame_batch" => {
                let (first, last) = v
                    .split_once('-')
                    .with_context(|| format!("malformed frame_batch range: {v:?}"))?;
                frame_batches.push((
                    first.trim().parse().context("frame_batch first")?,
                    last.trim().parse().context("frame_batch last")?,
                ));
            }
            other => anyhow::bail!("unknown manifest key: {other}"),
        }
    }
    // A v2 manifest without an epoch is corrupt, not legacy — the writer that
    // stamped the v2 header always writes one. Defaulting it to 0 would
    // silently demote a fenced generation to unfenced, which is the one
    // direction this whole mechanism must never fail in.
    if v2 && epoch.is_none() {
        anyhow::bail!("{MANIFEST_HEADER_V2} manifest is missing `epoch`");
    }
    // R858-B19: same reasoning as the `epoch` check above. A v4 writer always
    // stamps the salt, so a v4 manifest without one is corrupt, not legacy —
    // and defaulting it to `None` would silently demote a checkable generation
    // to an unknown one, which is the direction this mechanism must never fail
    // in. The converse guard matters just as much: a pre-v4 header carrying a
    // `wal_salt` line is hand-edited or truncated, and trusting that salt would
    // let a forged line wave a spliced chain through.
    if v4 && salt.is_none() {
        anyhow::bail!("{MANIFEST_HEADER_V4} manifest is missing `wal_salt`");
    }
    if !v4 && salt.is_some() {
        anyhow::bail!(
            "manifest header {header:?} carries a `wal_salt` line — the WAL salt is a {MANIFEST_HEADER_V4} field, so this manifest is corrupt or hand-edited"
        );
    }
    let first_frame = first_frame.context("missing first_frame")?;
    let last_frame = last_frame.context("missing last_frame")?;
    // R761-F2: the batch list IS the frame index, so a v3 manifest that does
    // not tile its own range exactly would send restore looking for objects
    // that were never written — caught here, at parse, rather than as a 404
    // halfway through a replay.
    if v3 {
        anyhow::ensure!(
            !frame_batches.is_empty(),
            "{MANIFEST_HEADER_V3} manifest has no `frame_batch` lines — it cannot say where its frames are"
        );
        let mut expected = first_frame;
        for &(first, last) in &frame_batches {
            anyhow::ensure!(
                first == expected && last >= first,
                "frame_batch {first}-{last} does not continue the range at frame {expected}"
            );
            expected = last + 1;
        }
        anyhow::ensure!(
            expected == last_frame + 1,
            "frame_batch list covers frames {first_frame}..={} but the manifest claims {first_frame}..={last_frame}",
            expected - 1,
        );
    } else if !frame_batches.is_empty() {
        anyhow::bail!(
            "manifest header {header:?} carries `frame_batch` lines — batching is a {MANIFEST_HEADER_V3} feature, so this manifest is corrupt or hand-edited"
        );
    }
    Ok(OwnedGenerationManifest {
        base_snapshot_key: base_snapshot_key.context("missing base_snapshot")?,
        page_size: page_size.context("missing page_size")?,
        checkpoint_seq: checkpoint_seq.context("missing checkpoint_seq")?,
        salt,
        first_frame,
        last_frame,
        epoch: epoch.unwrap_or(0),
        owner,
        frame_batches,
    })
}

/// A watermark as read back from the sidecar: the engine position plus the
/// wall-clock instant the sidecar was last written (R574-T4 — this is what
/// [`RpoStatus::watermark_age`] measures against). `written_at_nanos` is
/// `None` for sidecars written before the timestamp field existed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PersistedWatermark {
    watermark: Watermark,
    /// R858-B19: the WAL generation `watermark.last_frame` is a position
    /// *within*. `generation.salt == None` marks a sidecar written before the
    /// salt fields existed — unknown, therefore never provably equal to the
    /// live WAL, therefore a forced restart on the next tail. That is the
    /// deliberate choice: one redundant re-upload beats resuming into a WAL
    /// nobody can show is the same one.
    generation: WalGeneration,
    written_at_nanos: Option<u128>,
    /// R732-F2: the fencing epoch of the writer that last advanced this
    /// sidecar. `0` for a sidecar written before the field existed, which
    /// reads as "unfenced" and therefore fences nobody — the same
    /// behaviour-preserving default as [`StreamConfig::epoch`].
    ///
    /// This is the value [`tail_frames`] compares against, so it is the single
    /// piece of state the whole fence rests on. R732-T3 makes advancing it a
    /// compare-and-swap (R732-T3), so the *concurrent* hole is closed too:
    /// two writers racing the read-modify-write cannot both win, because the
    /// loser's conditional put fails against the version the winner replaced.
    epoch: u64,
    /// R736-T2: the pointer generation of the writer that last advanced this
    /// sidecar. `0` for a sidecar written before the field existed, which
    /// reads as "unfenced" and therefore fences nobody — the same
    /// behaviour-preserving default as [`StreamConfig::pointer_generation`].
    /// Checked alongside `epoch` in [`tail_frames`]; either being stale
    /// bounces the writer.
    pointer_generation: u64,
    /// R732-T3: the object version this record was read at, carried so the
    /// next advance can be conditional on it. `None` only when the sidecar
    /// does not exist yet, which selects [`PutMode::Create`] instead of
    /// [`PutMode::Update`] — "I believe nobody owns this sink" is as much a
    /// precondition as "I believe it is still at version V".
    version: Option<UpdateVersion>,
}

/// Sidecar format:
/// `<checkpoint_seq> <last_frame> [<written_at_unix_nanos> [<epoch>
/// [<pointer_generation> [<salt1> <salt2>]]]]`.
///
/// The third field was added by R574-T4, the fourth by R732-F2, the fifth by
/// R736-T2, and the sixth and seventh by R858-B19; all are positional appends,
/// which is what keeps this readable in both directions. A shorter sidecar (an
/// older writer) still parses, with the missing tail defaulting to `None` /
/// `0`, and an older reader stops after its last known field and never sees the
/// newer ones. Fields are only ever appended for exactly that reason — do not
/// reorder them.
///
/// R858-B19: a missing salt pair parses to `WalGeneration { salt: None }`,
/// which is *unknown*, not *unchanged*. `tail_frames` restarts against it
/// rather than resuming — see [`PersistedWatermark::generation`]. The two
/// fields are read as a pair: a sidecar carrying only one of them is corrupt
/// (this writer emits both or neither) and is rejected rather than
/// half-trusted.
async fn read_watermark(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
) -> Result<Option<PersistedWatermark>> {
    match store.get(key).await {
        Ok(res) => {
            // Capture the version BEFORE consuming the body — `bytes()` takes
            // `res` by value. Both fields are kept because stores differ in
            // which one they honour for a conditional put (object_store's own
            // `UpdateVersion` docs say to preserve both).
            let version = UpdateVersion {
                e_tag: res.meta.e_tag.clone(),
                version: res.meta.version.clone(),
            };
            let bytes = res.bytes().await.context("reading watermark sidecar")?;
            let s = String::from_utf8_lossy(&bytes);
            let mut fields = s.split_whitespace();
            let seq = fields
                .next()
                .context("watermark sidecar must be '<checkpoint_seq> <last_frame> [<nanos>]'")?;
            let frame = fields
                .next()
                .context("watermark sidecar must be '<checkpoint_seq> <last_frame> [<nanos>]'")?;
            let written_at_nanos = fields
                .next()
                .map(|n| n.parse::<u128>().context("watermark written_at nanos"))
                .transpose()?;
            let epoch = fields
                .next()
                .map(|e| e.parse::<u64>().context("watermark epoch"))
                .transpose()?
                .unwrap_or(0);
            let pointer_generation = fields
                .next()
                .map(|g| g.parse::<u64>().context("watermark pointer_generation"))
                .transpose()?
                .unwrap_or(0);
            // R858-B19: both salt words or neither. A lone `salt1` is not a
            // half-known generation, it is a torn write — say so instead of
            // silently downgrading it to "unknown" and papering over it.
            let salt = match (fields.next(), fields.next()) {
                (Some(s1), Some(s2)) => Some(WalSalt {
                    salt1: s1.parse().context("watermark salt1")?,
                    salt2: s2.parse().context("watermark salt2")?,
                }),
                (None, _) => None,
                (Some(_), None) => anyhow::bail!(
                    "watermark sidecar carries salt1 but no salt2 — truncated or hand-edited; \
                     refusing rather than guessing which WAL generation it names"
                ),
            };
            let checkpoint_seq: u32 = seq.parse().context("watermark checkpoint_seq")?;
            Ok(Some(PersistedWatermark {
                watermark: Watermark {
                    checkpoint_seq,
                    last_frame: frame.parse().context("watermark last_frame")?,
                },
                generation: WalGeneration { checkpoint_seq, salt },
                written_at_nanos,
                epoch,
                pointer_generation,
                version: Some(version),
            }))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).context("fetching watermark sidecar"),
    }
}

/// R732-T3: result of a conditional watermark advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatermarkCas {
    /// We held the version we read, and the sidecar now names us.
    Advanced,
    /// Somebody else replaced the sidecar between our read and our write.
    /// Says nothing about *who* — the caller re-reads to find out whether it
    /// was a newer owner (we are fenced) or a same-epoch racer (a bug).
    Contended,
}

/// Advance the watermark sidecar **conditionally** on the version it was read
/// at (R732-T3 / W245).
///
/// This is the atomic half of the fence. The epoch check in [`tail_frames`]
/// stops a *sequential* stale owner — one that returns after a transfer and
/// reads a sidecar already stamped higher. It cannot stop a *concurrent* one:
/// two writers that both read the old sidecar in the same instant would both
/// pass that check and then both blindly overwrite, last write winning, which
/// is exactly the corruption W245 describes. Making the advance a
/// compare-and-swap removes the window — at most one of them holds the version
/// the other replaced.
///
/// No new CAS primitive was needed: `object_store` already models this as
/// [`PutMode::Update`] (→ [`object_store::Error::Precondition`]) and
/// [`PutMode::Create`] (→ `AlreadyExists`) for the first write, and R2 honours
/// the underlying `If-Match` / `If-None-Match`.
///
/// **Deployment caveat, not a code path:** `AmazonS3Builder` must be
/// configured for conditional puts against a store that supports them. If the
/// backend silently degrades to unconditional writes, this returns `Advanced`
/// unconditionally and the concurrent window reopens — the sequential fence in
/// `tail_frames` still holds, but the race does not.
async fn write_watermark(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
    w: Watermark,
    salt: Option<WalSalt>,
    epoch: u64,
    pointer_generation: u64,
    expected: Option<&UpdateVersion>,
) -> Result<WatermarkCas> {
    let mut body = format!(
        "{} {} {} {} {}",
        w.checkpoint_seq,
        w.last_frame,
        unix_nanos(),
        epoch,
        pointer_generation
    );
    // R858-B19: omitted entirely rather than written as a sentinel, so an
    // unknown generation is one shape (absent) on both the read and write
    // sides, and no magic value can ever be mistaken for a real salt.
    if let Some(s) = salt {
        body.push_str(&format!(" {} {}", s.salt1, s.salt2));
    }
    body.push('\n');
    let opts = PutOptions {
        mode: match expected {
            Some(v) => PutMode::Update(v.clone()),
            // No sidecar when we read: assert that is *still* true, so two
            // writers bootstrapping the same fresh sink cannot both proceed.
            None => PutMode::Create,
        },
        ..Default::default()
    };
    match store.put_opts(key, body.into_bytes().into(), opts).await {
        Ok(_) => Ok(WatermarkCas::Advanced),
        Err(object_store::Error::Precondition { .. })
        | Err(object_store::Error::AlreadyExists { .. }) => Ok(WatermarkCas::Contended),
        Err(e) => Err(e).with_context(|| format!("writing watermark sidecar {key}")),
    }
}

/// Which conditional-put mode a preflight probe found unenforced.
///
/// Both matter and they fail independently — a store can honour `If-None-Match`
/// (guarding the bootstrap put) while ignoring `If-Match` (guarding every
/// steady-state advance), or the reverse. Naming which one degraded is the
/// difference between an operator fixing a bucket setting in a minute and
/// bisecting a corruption in a week.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightStage {
    /// [`PutMode::Create`] / `If-None-Match` — the guard on two writers
    /// bootstrapping the same fresh sink.
    Create,
    /// [`PutMode::Update`] / `If-Match` — the guard on every subsequent
    /// watermark advance, and therefore the one the steady state rests on.
    Update,
}

impl PreflightStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            PreflightStage::Create => "PutMode::Create (If-None-Match)",
            PreflightStage::Update => "PutMode::Update (If-Match)",
        }
    }
}

/// What [`probe_conditional_puts`] observed at a real sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreconditionSupport {
    /// Both conditional modes were enforced — a put that should have been
    /// rejected was rejected. The watermark CAS is real at this sink.
    Honoured,
    /// A put that *must* have failed its precondition succeeded instead, so
    /// this backend has silently degraded to unconditional writes.
    Degraded { stage: PreflightStage },
}

impl PreconditionSupport {
    pub fn is_honoured(&self) -> bool {
        matches!(self, PreconditionSupport::Honoured)
    }
}

/// R732-T4: prove, against the *real* configured sink, that conditional puts
/// are actually enforced — and hand the caller a hard answer so it can refuse
/// to start when they are not.
///
/// [`write_watermark`] carries a deployment caveat that no test can close:
/// if `AmazonS3Builder` is pointed at a store that does not honour
/// `If-Match` / `If-None-Match`, every conditional put silently succeeds, the
/// CAS degrades to last-write-wins, and the concurrent split-brain window
/// W245 exists to shut reopens — while every unit test stays green, because
/// the in-memory store used in tests does honour them. Support is a property
/// of configuration, not of code, so a runtime probe is the only guard that
/// can exist.
///
/// The probe writes a uniquely-keyed canary under `<prefix>/preflight/`,
/// exercises both modes with puts that MUST be rejected, and deletes it. It
/// touches no watermark, no manifest and no frame, so it is safe to run at
/// startup against a live sink another node owns — and it is deliberately
/// keyed per-process-per-nanosecond so two nodes probing at once cannot fail
/// each other.
///
/// `Ok(Degraded)` is the interesting return and is NOT an error: the probe
/// worked perfectly, and reported that the store is unsafe. An `Err` means the
/// probe could not reach a verdict at all (sink unreachable, credentials
/// wrong), which is also a refuse-to-start condition but a different one for
/// an operator to read.
pub async fn probe_conditional_puts(target: &BackupTarget) -> Result<PreconditionSupport> {
    let key = join_key(
        &target.prefix,
        &format!("preflight/conditional-put-{:020}-{}.canary", unix_nanos(), std::process::id()),
    );
    let result = probe_at_key(&target.store, &key).await;
    // Best-effort cleanup: a leaked canary is inert (nothing reads
    // `preflight/`), so a delete failure must not mask the verdict — which is
    // the whole reason the caller ran this.
    if let Err(e) = target.store.delete(&key).await {
        tracing_delete_failure(&key, &e);
    }
    result
}

/// The probe body, split out so the canary is deleted on every path.
async fn probe_at_key(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
) -> Result<PreconditionSupport> {
    let create = PutOptions { mode: PutMode::Create, ..Default::default() };

    // 1. Claim the key. This one is *expected* to succeed; if it doesn't, the
    //    probe cannot reach a verdict (and `AlreadyExists` here means the
    //    per-process-per-nanosecond key collided, which is a bug, not a store
    //    property — so it stays an error rather than a `Degraded` verdict).
    let first = store
        .put_opts(key, b"preflight-1".as_slice().into(), create.clone())
        .await
        .with_context(|| format!("preflight canary could not be created at {key}"))?;
    let v1 = UpdateVersion { e_tag: first.e_tag.clone(), version: first.version.clone() };

    // 2. Create again over a key that now exists. A store honouring
    //    `If-None-Match` rejects this; one that succeeds has degraded.
    match store.put_opts(key, b"preflight-2".as_slice().into(), create).await {
        Err(object_store::Error::AlreadyExists { .. })
        | Err(object_store::Error::Precondition { .. }) => {}
        Ok(_) => return Ok(PreconditionSupport::Degraded { stage: PreflightStage::Create }),
        Err(e) => {
            return Err(e).with_context(|| format!("preflight Create probe failed at {key}"))
        }
    }

    // 3. Advance the canary conditionally on the version we hold, to obtain a
    //    *superseded* version. Expected to succeed — it is the ordinary
    //    steady-state write `write_watermark` makes.
    store
        .put_opts(
            key,
            b"preflight-3".as_slice().into(),
            PutOptions { mode: PutMode::Update(v1.clone()), ..Default::default() },
        )
        .await
        .with_context(|| format!("preflight Update probe could not advance {key}"))?;

    // 4. Update again on the now-stale version — exactly the shape of a fenced
    //    writer losing the watermark race. A store honouring `If-Match`
    //    rejects it.
    match store
        .put_opts(
            key,
            b"preflight-4".as_slice().into(),
            PutOptions { mode: PutMode::Update(v1), ..Default::default() },
        )
        .await
    {
        Err(object_store::Error::Precondition { .. })
        | Err(object_store::Error::AlreadyExists { .. }) => Ok(PreconditionSupport::Honoured),
        Ok(_) => Ok(PreconditionSupport::Degraded { stage: PreflightStage::Update }),
        Err(e) => Err(e).with_context(|| format!("preflight Update probe failed at {key}")),
    }
}

/// turso-backup takes no logging dependency (it is a library consumed by
/// binaries that pick their own), so a failed canary cleanup goes to stderr
/// rather than through `tracing`.
fn tracing_delete_failure(key: &ObjPath, e: &object_store::Error) {
    eprintln!("turso-backup: preflight canary {key} could not be deleted: {e}");
}

/// R574-T4: compute the watermark-staleness snapshot for this call. Pure so
/// the breach edge cases are unit-testable without staging a sidecar.
fn rpo_status(target: Option<Duration>, prior: Option<&PersistedWatermark>) -> RpoStatus {
    let watermark_age = prior
        .and_then(|p| p.written_at_nanos)
        .and_then(|written_at| unix_nanos().checked_sub(written_at))
        .map(|nanos| Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX)));
    let breached = matches!((target, watermark_age), (Some(t), Some(age)) if age > t);
    RpoStatus { target, watermark_age, breached }
}

fn join_key(prefix: &str, leaf: &str) -> ObjPath {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        ObjPath::from(leaf)
    } else {
        ObjPath::from(format!("{prefix}/{leaf}"))
    }
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::fault_injection::{Fault, FaultyStore};
    use crate::backpressure::BackoffConfig;
    use object_store::memory::InMemory;
    use std::cell::RefCell;
    use std::time::Duration;

    /// In-memory WAL seam: a fixed page_size, a vector of frames the test
    /// appends to, and an advancing checkpoint_seq the test can bump.
    struct MockWal {
        page_size: usize,
        state: RefCell<MockState>,
    }
    struct MockState {
        checkpoint_seq: u32,
        /// R858-B19: the WAL header salt this mock's frames are stamped with.
        /// Modelled explicitly because the two fold regimes move it
        /// differently, and the whole bug was reading only the sequence:
        /// [`MockWal::restart`] moves both, [`MockWal::recreate`] moves only
        /// this one.
        salt: WalSalt,
        frames: Vec<MockFrame>,
        auto_actions_disabled: bool,
        /// R858-B19: re-roll `salt` once this many frame reads have happened,
        /// so a test can land a WAL recreate *inside* a single `tail_frames`
        /// call rather than only between two. See
        /// [`MockWal::recreate_after_reads`].
        swap_salt_after_reads: Option<u32>,
        reads: u32,
    }
    #[derive(Clone)]
    struct MockFrame {
        info: FrameInfo,
        page_bytes: Vec<u8>,
    }

    impl MockWal {
        fn new(page_size: usize) -> Self {
            Self {
                page_size,
                state: RefCell::new(MockState {
                    checkpoint_seq: 0,
                    salt: WalSalt { salt1: 0xd492_ea8a, salt2: 0x7acf_42a3 },
                    frames: Vec::new(),
                    auto_actions_disabled: false,
                    swap_salt_after_reads: None,
                    reads: 0,
                }),
            }
        }
        fn append(&self, page_no: u32, db_size: u32, fill: u8) {
            let mut s = self.state.borrow_mut();
            s.frames.push(MockFrame {
                info: FrameInfo { page_no, db_size },
                page_bytes: vec![fill; self.page_size],
            });
        }
        /// Simulate an **in-process** WAL restart — a long-lived connection
        /// folding its own WAL at the autocheckpoint threshold. The WAL file is
        /// reused, so `checkpoint_seq` advances and `salt1` advances with it
        /// (measured `d492ea8a -> d492ea8b` alongside seq `0 -> 1`). This is
        /// the regime the pre-R858-B19 `checkpoint_seq` test already caught.
        fn restart(&self) {
            let mut s = self.state.borrow_mut();
            s.checkpoint_seq += 1;
            s.salt.salt1 = s.salt.salt1.wrapping_add(1);
            s.frames.clear();
        }
        /// R858-B19 — simulate a **writer-process restart**: the last
        /// connection closed, SQLite checkpointed and DELETED the `-wal` file,
        /// and the next writer created a fresh WAL. `checkpoint_seq` goes back
        /// to 0 (it never moves, from a watcher's point of view: `0 -> 0`) and
        /// the salt is fresh randomness, unrelated to the old one.
        ///
        /// This is the regime `checkpoint_seq` is blind to, and the reason the
        /// mock models a salt at all.
        fn recreate(&self, salt: WalSalt) {
            let mut s = self.state.borrow_mut();
            s.checkpoint_seq = 0;
            s.salt = salt;
            s.frames.clear();
        }
        /// R858-B19: arm a mid-call WAL recreate — the salt flips once `reads`
        /// frame reads have been served, which lands it inside the drain loop
        /// rather than between two `tail_frames` calls.
        fn recreate_after_reads(&self, reads: u32) {
            self.state.borrow_mut().swap_salt_after_reads = Some(reads);
        }
        fn frame_count(&self) -> u64 {
            self.state.borrow().frames.len() as u64
        }
    }

    impl WalSeam for MockWal {
        fn wal_state(&self) -> Result<Watermark> {
            let s = self.state.borrow();
            Ok(Watermark {
                checkpoint_seq: s.checkpoint_seq,
                last_frame: s.frames.len() as u64,
            })
        }
        fn wal_get_frame(&self, frame_no: u64, buf: &mut [u8]) -> Result<FrameInfo> {
            let mut s = self.state.borrow_mut();
            s.reads += 1;
            if s.swap_salt_after_reads == Some(s.reads) {
                s.salt.salt1 = !s.salt.salt1;
            }
            let idx = frame_no
                .checked_sub(1)
                .context("frame_no must be >= 1")? as usize;
            let f = s
                .frames
                .get(idx)
                .with_context(|| format!("frame {frame_no} out of range"))?;
            // Synthesize a 24-byte header: big-endian page_no, db_size, the
            // WAL's salt pair (R858-B19 reads the generation out of exactly
            // these bytes), then zeros for the checksums (never validated).
            buf[0..4].copy_from_slice(&f.info.page_no.to_be_bytes());
            buf[4..8].copy_from_slice(&f.info.db_size.to_be_bytes());
            buf[8..12].copy_from_slice(&s.salt.salt1.to_be_bytes());
            buf[12..16].copy_from_slice(&s.salt.salt2.to_be_bytes());
            buf[16..WAL_FRAME_HEADER_SIZE].fill(0);
            buf[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&f.page_bytes);
            Ok(f.info)
        }
        fn wal_auto_actions_disable(&self) {
            self.state.borrow_mut().auto_actions_disabled = true;
        }
    }

    /// A store that accepts every put unconditionally — the exact failure the
    /// preflight probe exists to catch. It is not a contrived shape: it is
    /// what an S3-compatible backend without conditional-write support looks
    /// like from `object_store`'s side, and what `AmazonS3Builder` degrades to
    /// when pointed at one. Everything but `put_opts` delegates.
    #[derive(Debug)]
    struct UnconditionalStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Display for UnconditionalStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "UnconditionalStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for UnconditionalStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            mut opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            opts.mode = PutMode::Overwrite;
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A store that honours `If-None-Match` but not `If-Match`. The nastiest
    /// real-world shape, because the bootstrap put looks fine and only the
    /// steady-state advance — the one every tail after the first depends on —
    /// is unguarded.
    #[derive(Debug)]
    struct CreateOnlyStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Display for CreateOnlyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CreateOnlyStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CreateOnlyStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            mut opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if matches!(opts.mode, PutMode::Update(_)) {
                opts.mode = PutMode::Overwrite;
            }
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The happy path: a store that honours both modes passes, and — the part
    /// that matters for running this at startup against a live sink — leaves
    /// nothing behind.
    #[tokio::test]
    async fn the_preflight_probe_passes_on_a_conditional_store_and_leaves_no_trace() {
        let target = fresh_target();
        assert_eq!(
            probe_conditional_puts(&target).await.unwrap(),
            PreconditionSupport::Honoured
        );

        let leftovers = objects_under(&target).await;
        assert!(
            leftovers.is_empty(),
            "the probe must clean up its canary — found {leftovers:?}"
        );
    }

    /// The whole point: a backend that silently ignores preconditions is
    /// *reported*, not tolerated. Without this the watermark CAS degrades to
    /// last-write-wins and every other test in this file still passes.
    #[tokio::test]
    async fn the_preflight_probe_catches_a_store_that_ignores_preconditions() {
        let target = BackupTarget {
            store: Arc::new(UnconditionalStore { inner: Arc::new(InMemory::new()) }),
            prefix: "backups".into(),
        };
        assert_eq!(
            probe_conditional_puts(&target).await.unwrap(),
            PreconditionSupport::Degraded { stage: PreflightStage::Create },
            "an unconditional store fails at the first guard it meets"
        );
    }

    /// Half-degraded stores are the ones that actually ship. `If-None-Match`
    /// works, so bootstrapping looks healthy; `If-Match` does not, so every
    /// steady-state advance is unguarded. The probe must name `Update`
    /// specifically — "conditional puts are broken" would send an operator to
    /// the wrong setting.
    #[tokio::test]
    async fn the_preflight_probe_names_update_when_only_if_match_is_ignored() {
        let target = BackupTarget {
            store: Arc::new(CreateOnlyStore { inner: Arc::new(InMemory::new()) }),
            prefix: "backups".into(),
        };
        assert_eq!(
            probe_conditional_puts(&target).await.unwrap(),
            PreconditionSupport::Degraded { stage: PreflightStage::Update }
        );
    }

    /// The canary is keyed per process per nanosecond, so two nodes probing
    /// the same sink at once each get a real verdict instead of failing each
    /// other. A startup probe that flaked under concurrency would be turned
    /// off within a week.
    #[tokio::test]
    async fn concurrent_preflight_probes_do_not_collide() {
        let target = fresh_target();
        let (a, b) = tokio::join!(
            probe_conditional_puts(&target),
            probe_conditional_puts(&target)
        );
        assert_eq!(a.unwrap(), PreconditionSupport::Honoured);
        assert_eq!(b.unwrap(), PreconditionSupport::Honoured);
        assert!(objects_under(&target).await.is_empty());
    }

    async fn objects_under(target: &BackupTarget) -> Vec<String> {
        use futures_util::StreamExt;
        target
            .store
            .list(None)
            .map(|m| m.unwrap().location.to_string())
            .collect()
            .await
    }

    fn fresh_target() -> BackupTarget {
        BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: "backups".into(),
        }
    }

    fn cfg() -> StreamConfig<'static> {
        StreamConfig {
            base_snapshot_key: "backups/snapshots/snapshot-00000000000000000001.db",
            page_size: 4096,
            backpressure: BackpressureConfig::default(),
            rpo_target: None,
            epoch: 0,
            owner: None,
            pointer_generation: 0,
        }
    }

    /// Fast-ticking backoff for backpressure tests — a few ms, never the
    /// production defaults, so retry-heavy tests stay fast.
    fn fast_backoff() -> BackoffConfig {
        BackoffConfig {
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(4),
            multiplier: 2.0,
            max_retries: 3,
        }
    }

    fn bp_cfg(policy: BackpressurePolicy, spill_buffer_frames: usize) -> StreamConfig<'static> {
        StreamConfig {
            base_snapshot_key: "backups/snapshots/snapshot-00000000000000000001.db",
            page_size: 4096,
            backpressure: BackpressureConfig {
                spill_buffer_frames,
                policy,
                backoff: fast_backoff(),
            },
            rpo_target: None,
            epoch: 0,
            owner: None,
            pointer_generation: 0,
        }
    }

    fn faulty_target(faults: impl IntoIterator<Item = Fault>) -> BackupTarget {
        BackupTarget {
            store: Arc::new(FaultyStore::new(Arc::new(InMemory::new()), faults)),
            prefix: "backups".into(),
        }
    }

    /// Five WAL frames (1..=5), the last a commit — a small, deterministic
    /// range for the backpressure tests below.
    fn five_frame_seam() -> MockWal {
        let seam = MockWal::new(4096);
        for i in 1..=4u32 {
            seam.append(i, 0, i as u8);
        }
        seam.append(5, 5, 5); // commit
        seam
    }

    // --- R574-F2: explicit R2 backpressure ---------------------------------

    /// A single 429 triggers exactly one retry, then the upload succeeds —
    /// the whole range still lands and the report shows the retry.
    #[tokio::test]
    async fn throttled_429_retries_then_succeeds() {
        let seam = five_frame_seam();
        let target = faulty_target([Fault::TooManyRequests]);
        let cfg = bp_cfg(BackpressurePolicy::Fail, 8);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { frame_count, backpressure, .. } => {
                assert_eq!(frame_count, 5);
                assert_eq!(backpressure.throttle_retries, 1);
                assert_eq!(backpressure.frames_shed, 0);
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// A 503 is classified the same as a 429 and also triggers backoff then
    /// retry — two 503s in a row cost exactly two retries.
    #[tokio::test]
    async fn throttled_503_retries_then_succeeds() {
        let seam = five_frame_seam();
        let target = faulty_target([Fault::ServiceUnavailable, Fault::ServiceUnavailable]);
        let cfg = bp_cfg(BackpressurePolicy::Fail, 8);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { frame_count, backpressure, .. } => {
                assert_eq!(frame_count, 5);
                assert_eq!(backpressure.throttle_retries, 2);
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// Buffer at bound + `Fail`: sustained throttling exhausts backoff and
    /// the whole call errors — nothing is persisted.
    #[tokio::test]
    async fn buffer_at_bound_fail_policy_errors_without_persisting() {
        let seam = five_frame_seam();
        let faults = std::iter::repeat_n(Fault::TooManyRequests, 50);
        let target = faulty_target(faults);
        let cfg = bp_cfg(BackpressurePolicy::Fail, 2);
        let err = tail_frames(&seam, &target, &cfg).await.unwrap_err();
        assert!(format!("{err}").contains("uploading wal frame"), "err was {err}");
        assert!(
            read_watermark(&target.store, &target.watermark_key())
                .await
                .unwrap()
                .is_none(),
            "Fail must not persist a watermark when it gives up"
        );
    }

    /// Buffer at bound + `Shed`: the backlog is dropped with a loud report
    /// instead of erroring; nothing persists, so the next call would
    /// re-attempt the same range.
    #[tokio::test]
    async fn buffer_at_bound_shed_policy_drops_backlog_without_error() {
        let seam = five_frame_seam();
        let faults = std::iter::repeat_n(Fault::TooManyRequests, 50);
        let target = faulty_target(faults);
        let cfg = bp_cfg(BackpressurePolicy::Shed, 2);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Shed { first_frame, last_frame, backpressure, .. } => {
                assert_eq!(first_frame, 1);
                assert_eq!(last_frame, 5);
                assert_eq!(backpressure.high_water_frames, 2, "capped at the bound");
                assert!(backpressure.frames_shed >= 1, "must report the dropped backlog");
            }
            other => panic!("expected Shed, got {other:?}"),
        }
        assert!(
            read_watermark(&target.store, &target.watermark_key())
                .await
                .unwrap()
                .is_none(),
            "Shed must not persist a watermark for a fully-dropped batch"
        );
    }

    /// Buffer at bound + `Block`: retries never give up on a throttling
    /// error; once the store recovers, the full range still lands.
    #[tokio::test]
    async fn buffer_at_bound_block_policy_eventually_drains() {
        let seam = five_frame_seam();
        // More failures than fast_backoff's max_retries would tolerate under
        // Fail/Shed — Block must push through them anyway.
        let faults = std::iter::repeat_n(Fault::TooManyRequests, 4);
        let target = faulty_target(faults);
        let cfg = bp_cfg(BackpressurePolicy::Block, 2);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { frame_count, backpressure, .. } => {
                assert_eq!(frame_count, 5);
                assert_eq!(backpressure.high_water_frames, 2);
                assert_eq!(backpressure.throttle_retries, 4);
                assert_eq!(backpressure.frames_shed, 0);
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// R761-F2: a call with more frames than the spill buffer holds splits
    /// into one batch object per buffer-full, the manifest indexes every one
    /// of them, and replay puts the frames back in order. The bound is the
    /// only thing sizing an object, so this is also the assertion that the
    /// largest object this sink writes stays bounded.
    #[tokio::test]
    async fn a_call_larger_than_the_spill_buffer_splits_into_several_batch_objects() {
        let seam = MockWal::new(4096);
        for i in 1..=5u32 {
            seam.append(i, i, i as u8);
        }
        let target = fresh_target();
        let cfg = bp_cfg(BackpressurePolicy::Fail, 2);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        assert!(matches!(out, StreamOutcome::Streamed { frame_count: 5, .. }));

        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        assert_eq!(
            manifests[0].frame_batches,
            vec![(1, 2), (3, 4), (5, 5)],
            "bound 2 over 5 frames is two full batches and a remainder"
        );
        let frame_size = WAL_FRAME_HEADER_SIZE + 4096;
        for (first, last) in [(1u64, 2u64), (3, 4), (5, 5)] {
            let bytes = target
                .store
                .get(&target.frame_batch_key(0, 0, first, last))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(bytes.len() as u64, (last - first + 1) * frame_size as u64);
        }

        // And it all comes back, in order, through the normal read path.
        let insert = MockInsertSeam::new();
        assert_eq!(replay_frames_into(&target, &insert, &manifests).await.unwrap(), 5);
        let frames: Vec<u64> = insert
            .events()
            .into_iter()
            .filter_map(|e| match e {
                MockInsertEvent::Frame { frame_no, .. } => Some(frame_no),
                _ => None,
            })
            .collect();
        assert_eq!(frames, vec![1, 2, 3, 4, 5]);
    }

    /// R761-F2 + R574-F2: a drain that lands one batch and then gets stuck
    /// under `Shed` publishes ONLY the batch that landed. The manifest's index
    /// is what restore follows, so a batch list claiming a shed object would
    /// be a 404 mid-replay — worse than the frames simply not being there.
    #[tokio::test]
    async fn a_partially_shed_drain_indexes_only_the_batches_that_landed() {
        let seam = five_frame_seam();
        // First batch through; the next one throttled until Shed gives up.
        // Exactly enough faults to exhaust one put's retry budget and no more,
        // so the watermark + manifest writes that follow the shed still land
        // (they go through the same store).
        let faults = std::iter::once(Fault::Pass).chain(std::iter::repeat_n(
            Fault::TooManyRequests,
            fast_backoff().max_retries as usize + 1,
        ));
        let target = faulty_target(faults);
        let cfg = bp_cfg(BackpressurePolicy::Shed, 2);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { first_frame, last_frame, frame_count, backpressure, .. } => {
                assert_eq!((first_frame, last_frame, frame_count), (1, 2, 2));
                assert_eq!(backpressure.frames_shed, 2, "frames 3-4 were buffered and dropped");
            }
            other => panic!("expected a Streamed prefix, got {other:?}"),
        }
        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].last_frame, 2);
        assert_eq!(manifests[0].frame_batches, vec![(1, 2)]);
        // Every object the manifest names actually exists — the property that
        // makes the published prefix restorable.
        let insert = MockInsertSeam::new();
        assert_eq!(replay_frames_into(&target, &insert, &manifests).await.unwrap(), 2);
    }

    /// The high-water mark reports the peak spill-buffer occupancy for the
    /// call, capped at the configured bound even when more frames remain.
    #[tokio::test]
    async fn high_water_reports_peak_buffered_frames() {
        let seam = five_frame_seam();
        let target = faulty_target([]); // no faults — pure high-water measurement
        let cfg = bp_cfg(BackpressurePolicy::Fail, 3);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { backpressure, .. } => {
                assert_eq!(backpressure.high_water_frames, 3, "capped at the configured bound");
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    // --- R574-T4: explicit RPO knob --------------------------------------

    /// First tail ever: no sidecar, so age is unknown and a configured
    /// target cannot be breached (there is nothing to measure against).
    #[tokio::test]
    async fn rpo_first_tail_has_unknown_age_and_no_breach() {
        let seam = five_frame_seam();
        let target = fresh_target();
        let mut cfg = cfg();
        cfg.rpo_target = Some(Duration::from_secs(1));
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { rpo, .. } => {
                assert_eq!(rpo.target, Some(Duration::from_secs(1)));
                assert_eq!(rpo.watermark_age, None);
                assert!(!rpo.breached);
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// Second tail past the target: the sidecar's stamped write instant is
    /// older than `rpo_target`, so the outcome flags a breach — the
    /// staleness emission the orchestrator alerts on.
    #[tokio::test]
    async fn rpo_stale_watermark_past_target_reports_breach() {
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        let target = fresh_target();
        let mut cfg = cfg();
        // Zero target: any measurable gap between the two tails is a breach.
        cfg.rpo_target = Some(Duration::ZERO);
        let _ = tail_frames(&seam, &target, &cfg).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        seam.append(2, 2, 0xBB);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Streamed { rpo, .. } => {
                let age = rpo.watermark_age.expect("age known after first sidecar write");
                assert!(age >= Duration::from_millis(5), "age was {age:?}");
                assert!(rpo.breached, "zero target must flag any nonzero age");
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// A generous target with a prompt second tail: age is reported but the
    /// bound holds — and with no target at all, `breached` is always false.
    #[tokio::test]
    async fn rpo_within_target_and_no_target_do_not_breach() {
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        let target = fresh_target();
        let mut with_target = cfg();
        with_target.rpo_target = Some(Duration::from_secs(3600));
        let _ = tail_frames(&seam, &target, &with_target).await.unwrap();

        seam.append(2, 2, 0xBB);
        let out = tail_frames(&seam, &target, &with_target).await.unwrap();
        match out {
            StreamOutcome::Streamed { rpo, .. } => {
                assert!(rpo.watermark_age.is_some());
                assert!(!rpo.breached, "an hour budget can't be blown in-process");
            }
            other => panic!("expected Streamed, got {other:?}"),
        }

        // Same staged sidecar, target removed: age still reported, never
        // breached (observe-before-you-pick-a-number mode).
        seam.append(3, 3, 0xCC);
        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        match out {
            StreamOutcome::Streamed { rpo, .. } => {
                assert_eq!(rpo.target, None);
                assert!(rpo.watermark_age.is_some());
                assert!(!rpo.breached);
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// A pre-T4 two-field sidecar still parses (written_at unknown), and the
    /// next write upgrades it to the stamped three-field format.
    #[tokio::test]
    async fn rpo_legacy_two_field_sidecar_parses_and_upgrades() {
        let target = fresh_target();
        target
            .store
            .put(&target.watermark_key(), b"0 1\n".to_vec().into())
            .await
            .unwrap();
        let legacy = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(legacy.watermark, Watermark { checkpoint_seq: 0, last_frame: 1 });
        assert_eq!(legacy.written_at_nanos, None);

        // A tail against the legacy sidecar reports unknown age (not a
        // breach), uploads the frames, and re-stamps the sidecar.
        //
        // R858-B19 CHANGED THE OUTCOME HERE, deliberately: this used to assert
        // `Streamed` with `first_frame == 2` ("resumes after the legacy
        // watermark"). A two-field sidecar records no salt, so nothing says the
        // WAL it names is the WAL in front of us — and resuming at frame 2 on
        // that basis is the exact inference that spliced two generations. The
        // sidecar's RPO semantics (an *unknown* age is not a breach) are what
        // this test is about and they are untouched.
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0xAA);
        seam.append(2, 2, 0xBB);
        let mut cfg = cfg();
        cfg.rpo_target = Some(Duration::ZERO);
        let out = tail_frames(&seam, &target, &cfg).await.unwrap();
        match out {
            StreamOutcome::Restarted { rpo, first_frame, previous_generation, .. } => {
                assert_eq!(first_frame, 1, "an unverifiable watermark is re-uploaded, not resumed");
                assert_eq!(previous_generation.salt, None);
                assert_eq!(rpo.watermark_age, None);
                assert!(!rpo.breached, "unknown age is not a breach even at zero target");
            }
            other => panic!("expected Restarted, got {other:?}"),
        }
        let upgraded = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();
        assert!(upgraded.written_at_nanos.is_some(), "rewrite stamps the timestamp");
    }

    /// Pure rpo_status edge cases that don't need a staged store.
    #[test]
    fn rpo_status_truth_table() {
        let stamped = |nanos_ago: u128| PersistedWatermark {
            watermark: Watermark { checkpoint_seq: 0, last_frame: 1 },
            generation: WalGeneration::default(),
            written_at_nanos: Some(unix_nanos().saturating_sub(nanos_ago)),
            epoch: 0,
            pointer_generation: 0,
            version: None,
        };
        // No prior at all.
        let s = rpo_status(Some(Duration::from_secs(1)), None);
        assert_eq!((s.watermark_age, s.breached), (None, false));
        // Prior without a stamp (legacy sidecar).
        let legacy = PersistedWatermark {
            watermark: Watermark::default(),
            generation: WalGeneration::default(),
            written_at_nanos: None,
            epoch: 0,
            pointer_generation: 0,
            version: None,
        };
        let s = rpo_status(Some(Duration::ZERO), Some(&legacy));
        assert_eq!((s.watermark_age, s.breached), (None, false));
        // Old stamp vs tight target: breached.
        let s = rpo_status(Some(Duration::from_millis(1)), Some(&stamped(5_000_000_000)));
        assert!(s.watermark_age.unwrap() >= Duration::from_secs(4));
        assert!(s.breached);
        // Old stamp, no target: age known, never breached.
        let s = rpo_status(None, Some(&stamped(5_000_000_000)));
        assert!(s.watermark_age.is_some());
        assert!(!s.breached);
    }

    /// R761-T1: the shipped cadence default is an arithmetic consequence of
    /// the 2026-08-13 `tail_sweep_harness` table, so the arithmetic is a
    /// test rather than a claim in a comment nobody re-checks. If a future
    /// harness run moves the measured points, this test is where the
    /// mismatch surfaces — re-derive the default, do not relax the test.
    #[test]
    fn default_tail_cadence_matches_the_measured_write_op_curve() {
        // Every uploading tail_frames call writes exactly two fixed objects
        // beyond the frames: one generation manifest, one watermark CAS.
        const FIXED_OBJECTS_PER_TAIL: f64 = 2.0;
        // Measured, flat across the whole sweep — a property of the schema
        // and transaction shape, not of the cadence.
        const MEASURED_FRAMES_PER_WRITE: f64 = 2.03;
        let puts_per_write =
            |w: f64| MEASURED_FRAMES_PER_WRITE + FIXED_OBJECTS_PER_TAIL / w;

        // The model reproduces all four measured rows.
        for (writes_per_tail, measured) in [(1.0, 4.03), (5.0, 2.43), (25.0, 2.11), (100.0, 2.05)] {
            let modelled = puts_per_write(writes_per_tail);
            assert!(
                (modelled - measured).abs() < 0.005,
                "writes_per_tail={writes_per_tail}: model {modelled:.3} vs measured {measured:.3}"
            );
        }

        // The default is tailed at half the stated bound, so one missed tick
        // still lands inside the promise.
        assert_eq!(DEFAULT_RPO_TARGET, DEFAULT_TAIL_INTERVAL * 2);

        // At the burst rates the default was chosen against (~0.1-1 write/s
        // during an active session), 60s lands in the flat part of the curve:
        // the fixed-object term is under a fifth of the frame floor even at
        // the slow end, where 15s would still be paying 1.33.
        let slow_burst_writes_per_tail = 0.1 * DEFAULT_TAIL_INTERVAL.as_secs_f64();
        let fixed_term = FIXED_OBJECTS_PER_TAIL / slow_burst_writes_per_tail;
        assert!(
            fixed_term < MEASURED_FRAMES_PER_WRITE / 5.0,
            "fixed-object term {fixed_term:.3} at the slow-burst end is no longer small \
             relative to the {MEASURED_FRAMES_PER_WRITE} frame floor"
        );
        assert!(
            puts_per_write(slow_burst_writes_per_tail) < 2.4,
            "the default's worst modelled case should sit below the measured \
             writes_per_tail=5 point (2.43)"
        );

        // R761-F2 removed the frames/write term: a tail call whose frames fit
        // one batch writes the batch, the manifest and the watermark, full
        // stop. Same curve shape, no floor.
        let batched_puts_per_write = |w: f64| (1.0 + FIXED_OBJECTS_PER_TAIL) / w;
        for (writes_per_tail, pre_batching) in [(1.0, 4.03), (5.0, 2.43), (25.0, 2.11), (100.0, 2.05)]
        {
            let cut = 1.0 - batched_puts_per_write(writes_per_tail) / pre_batching;
            let want = match writes_per_tail as u32 {
                1 => 0.256,
                5 => 0.753,
                25 => 0.943,
                _ => 0.985,
            };
            assert!(
                (cut - want).abs() < 0.005,
                "writes_per_tail={writes_per_tail}: batching cuts {:.1}%, expected {:.1}%",
                cut * 100.0,
                want * 100.0,
            );
        }
        // Which is why the cadence default did not move: what is left to win
        // past 60s is now a hundredth of a PUT per write at the fast-burst end
        // of the same band, against an RPO window that would grow 5x.
        assert!(
            batched_puts_per_write(1.0 * DEFAULT_TAIL_INTERVAL.as_secs_f64())
                - batched_puts_per_write(5.0 * DEFAULT_TAIL_INTERVAL.as_secs_f64())
                < 0.05,
            "batching should have flattened the cadence lever at the fast-burst end"
        );
    }

    /// R782: `StreamOutcome::rpo()` is the accessor `tenant-streamer` pushes
    /// through — every variant that carries an `RpoStatus` gives it back, and
    /// `Fenced` (which deliberately carries none, per its own doc) gives
    /// `None` rather than a default/synthesized one.
    #[test]
    fn stream_outcome_rpo_accessor_covers_every_variant() {
        let rpo = RpoStatus { target: None, watermark_age: Some(Duration::from_secs(1)), breached: false };
        assert_eq!(StreamOutcome::Empty { watermark: Watermark::default(), rpo }.rpo(), Some(&rpo));
        assert_eq!(
            StreamOutcome::Streamed {
                generation_key: String::new(),
                first_frame: 1,
                last_frame: 1,
                checkpoint_seq: 0,
                frame_count: 1,
                backpressure: Default::default(),
                rpo,
            }
            .rpo(),
            Some(&rpo)
        );
        assert_eq!(
            StreamOutcome::Restarted {
                generation_key: String::new(),
                previous_generation: WalGeneration::default(),
                new_generation: WalGeneration {
                    checkpoint_seq: 1,
                    salt: Some(WalSalt { salt1: 1, salt2: 2 }),
                },
                first_frame: 1,
                last_frame: 1,
                frame_count: 1,
                backpressure: Default::default(),
                rpo,
            }
            .rpo(),
            Some(&rpo)
        );
        assert_eq!(
            StreamOutcome::Shed {
                checkpoint_seq: 0,
                first_frame: 1,
                last_frame: 1,
                backpressure: Default::default(),
                rpo,
            }
            .rpo(),
            Some(&rpo)
        );
        assert_eq!(
            StreamOutcome::Fenced {
                current_epoch: 2,
                our_epoch: 1,
                current_pointer_generation: 0,
                our_pointer_generation: 0,
            }
            .rpo(),
            None
        );
    }

    /// First tail with no frames yet — Empty, no manifest, no watermark.
    #[tokio::test]
    async fn empty_when_no_frames() {
        let seam = MockWal::new(4096);
        let target = fresh_target();
        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        match out {
            StreamOutcome::Empty { watermark, rpo } => {
                assert_eq!(watermark, Watermark::default());
                // No sidecar has ever been written — age is unknowable.
                assert_eq!(rpo, RpoStatus::default());
            }
            other => panic!("expected Empty, got {other:?}"),
        }
        // No manifest, no watermark sidecar.
        assert!(
            read_watermark(&target.store, &target.watermark_key())
                .await
                .unwrap()
                .is_none()
        );
    }

    /// First tail with 3 frames — Streamed, manifest written, watermark
    /// recorded, every frame object retrievable at the right key.
    #[tokio::test]
    async fn streams_initial_frames_and_records_watermark() {
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0xAA);
        seam.append(2, 0, 0xBB);
        seam.append(3, 3, 0xCC); // commit frame: db_size = 3 pages

        let target = fresh_target();
        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        let gen_key = match out {
            StreamOutcome::Streamed {
                generation_key,
                first_frame,
                last_frame,
                checkpoint_seq,
                frame_count,
                ..
            } => {
                assert_eq!(first_frame, 1);
                assert_eq!(last_frame, 3);
                assert_eq!(checkpoint_seq, 0);
                assert_eq!(frame_count, 3);
                assert!(generation_key.starts_with("backups/generations/gen-"));
                generation_key
            }
            other => panic!("expected Streamed, got {other:?}"),
        };

        // R761-F2: ONE batch object holds all three frames, at the ranged key,
        // each frame at its own offset inside it.
        let frame_size = WAL_FRAME_HEADER_SIZE + 4096;
        let bytes = target
            .store
            .get(&target.frame_batch_key(0, 0, 1, 3))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.len(), 3 * frame_size);
        for frame_no in 1..=3u64 {
            let at = (frame_no as usize - 1) * frame_size;
            let page_no = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
            assert_eq!(page_no, frame_no as u32);
        }
        // And nothing was written per-frame.
        assert!(
            target.store.get(&target.frame_key(0, 0, 1)).await.is_err(),
            "the pre-R761-F2 per-frame key must not be written any more"
        );

        // Manifest is parseable and round-trips.
        let m_bytes = target
            .store
            .get(&ObjPath::from(gen_key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let parsed = parse_generation_manifest(&String::from_utf8_lossy(&m_bytes)).unwrap();
        assert_eq!(parsed.first_frame, 1);
        assert_eq!(parsed.last_frame, 3);
        assert_eq!(parsed.checkpoint_seq, 0);
        assert_eq!(parsed.page_size, 4096);
        assert_eq!(parsed.base_snapshot_key, cfg().base_snapshot_key);
        assert_eq!(parsed.frame_batches, vec![(1, 3)], "R761-F2: one batch, indexed");

        // Watermark sidecar matches.
        let wm = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wm.watermark, Watermark { checkpoint_seq: 0, last_frame: 3 });
        assert!(wm.written_at_nanos.is_some(), "R574-T4: writes stamp the sidecar");

        // The seam was told to take WAL ownership? Not directly via
        // tail_frames — callers do that at construction (CoreWalSeam::open).
        // Verify the mock invariant separately.
        assert!(!seam.state.borrow().auto_actions_disabled);
    }

    /// Second tail with no new frames returns Empty without uploading.
    #[tokio::test]
    async fn second_tail_with_no_new_frames_is_empty() {
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &cfg()).await.unwrap();

        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        match out {
            StreamOutcome::Empty { watermark, rpo } => {
                assert_eq!(watermark, Watermark { checkpoint_seq: 0, last_frame: 1 });
                // A sidecar exists from the first tail, so age is known even
                // with no rpo_target configured — and no target means no breach.
                assert!(rpo.watermark_age.is_some());
                assert!(!rpo.breached);
            }
            other => panic!("expected Empty, got {other:?}"),
        }
    }

    /// Second tail with new frames uploads only the new ones and the
    /// generation manifest covers the incremental range. Previous frames
    /// remain at their prior keys (idempotent put on same key).
    #[tokio::test]
    async fn second_tail_uploads_only_new_frames() {
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0x11);
        seam.append(2, 2, 0x22); // commit
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &cfg()).await.unwrap();

        // Append more.
        seam.append(3, 0, 0x33);
        seam.append(4, 4, 0x44); // commit

        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        match out {
            StreamOutcome::Streamed {
                first_frame,
                last_frame,
                frame_count,
                ..
            } => {
                assert_eq!(first_frame, 3);
                assert_eq!(last_frame, 4);
                assert_eq!(frame_count, 2);
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
        assert_eq!(seam.frame_count(), 4);
        // Frames 1..=4 all retrievable under checkpoint_seq=0, as one batch
        // object per tail call — the first call's object is untouched by the
        // second (disjoint ranges, so no overwrite and no re-upload).
        for (first, last) in [(1u64, 2u64), (3, 4)] {
            target
                .store
                .get(&target.frame_batch_key(0, 0, first, last))
                .await
                .unwrap();
        }
    }

    /// WAL restart bumps checkpoint_seq; the next tail uploads under the new
    /// sequence and reports Restarted.
    #[tokio::test]
    async fn wal_restart_emits_restarted_under_new_sequence() {
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        seam.append(2, 2, 0xBB);
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &cfg()).await.unwrap();

        // Simulate the engine restarting the WAL (e.g. after a checkpoint).
        seam.restart();
        seam.append(1, 1, 0xCC);

        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        match out {
            StreamOutcome::Restarted {
                previous_generation,
                new_generation,
                first_frame,
                last_frame,
                frame_count,
                ..
            } => {
                assert_eq!(previous_generation.checkpoint_seq, 0);
                assert_eq!(new_generation.checkpoint_seq, 1);
                // R858-B19: an in-process restart moves the salt too, so this
                // regime is now caught twice over.
                assert_ne!(previous_generation.salt, new_generation.salt);
                assert_eq!(first_frame, 1);
                assert_eq!(last_frame, 1);
                assert_eq!(frame_count, 1);
            }
            other => panic!("expected Restarted, got {other:?}"),
        }

        // Old seq=0 frames still in place; new seq=1 frame under its own
        // sequence — same key shape, different directory.
        target
            .store
            .get(&target.frame_batch_key(0, 0, 1, 2))
            .await
            .unwrap();
        target
            .store
            .get(&target.frame_batch_key(0, 1, 1, 1))
            .await
            .unwrap();
    }

    /// R858-B19, the property this whole ticket turns on: **a WAL whose salt
    /// changed is a different WAL, even when `checkpoint_seq` did not move.**
    ///
    /// This is the writer-process-restart regime. The last connection closed,
    /// SQLite checkpointed and deleted the `-wal`, and the next writer built a
    /// fresh WAL back at checkpoint-sequence 0. Both tails therefore see
    /// `checkpoint_seq == 0`, which is precisely why the old
    /// `p.checkpoint_seq == current.checkpoint_seq` test resumed at
    /// `last_frame + 1` and spliced frames from two unrelated WALs into one
    /// generation chain — a restore that reported success and produced a stale
    /// or malformed image, with nothing raised anywhere.
    ///
    /// The probe (`examples/foreign_checkpoint_probe.rs -- b f`) drives the
    /// same fold end-to-end against the real system `sqlite3`. This test exists
    /// so the property is pinned here too: the probe needs an upstream sqlite3
    /// binary and half a minute, and a property this load-bearing should fail
    /// in `cargo test` when someone re-derives "the sequence is enough".
    #[tokio::test]
    async fn a_salt_change_is_a_restart_even_when_checkpoint_seq_is_unchanged() {
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        seam.append(2, 2, 0xBB);
        seam.append(3, 3, 0xCC);
        let target = fresh_target();
        let first = tail_frames(&seam, &target, &cfg()).await.unwrap();
        assert!(matches!(first, StreamOutcome::Streamed { .. }), "got {first:?}");

        // The foreign writer restarted: brand-new WAL, unrelated salt, and a
        // checkpoint-sequence that reads 0 both before and after.
        seam.recreate(WalSalt { salt1: 0x9ca8_9e29, salt2: 0x2be5_66fc });
        seam.append(1, 1, 0xDD);
        seam.append(2, 2, 0xEE);
        assert_eq!(
            seam.wal_state().unwrap().checkpoint_seq,
            0,
            "the premise: the sequence did NOT move across the recreate"
        );

        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        let StreamOutcome::Restarted {
            previous_generation,
            new_generation,
            first_frame,
            last_frame,
            ..
        } = &out
        else {
            panic!(
                "a recreated WAL must report Restarted — got {out:?}. If this is Streamed with \
                 first_frame 4, the salt check has been removed and two WAL generations are being \
                 spliced into one chain again (R858-B19)"
            );
        };
        assert_eq!(previous_generation.checkpoint_seq, new_generation.checkpoint_seq);
        assert_ne!(
            previous_generation.salt, new_generation.salt,
            "the salt is the only thing that moved, and it is what must be noticed"
        );
        assert_eq!((*first_frame, *last_frame), (1, 2), "re-upload from the top of the new WAL");

        // And the chain that results is refused rather than restored: two
        // generations both starting at frame 1 is not a stream, and restore
        // must say so instead of producing a plausible-looking wrong image.
        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        assert_eq!(manifests.len(), 2);
        let err = validate_generation_chain(&manifests).unwrap_err().to_string();
        assert!(
            err.contains("WAL") || err.contains("gap in stream"),
            "restore must refuse the post-recreate chain loudly, got: {err}"
        );
    }

    /// R858-B19 — the narrower door: a WAL recreate that lands **inside** one
    /// `tail_frames` call rather than between two. The pre-drain sample says
    /// generation A, the frames that got read are a mix of A and B, and no
    /// comparison of two watermarks can see it. The call must publish nothing.
    #[tokio::test]
    async fn a_wal_recreated_mid_drain_publishes_nothing() {
        let seam = MockWal::new(4096);
        for n in 1..=3u32 {
            seam.append(n, n, n as u8);
        }
        let target = fresh_target();
        // Read #1 is the pre-drain salt probe; reads #2..#4 are the drain. Flip
        // the WAL underneath us on the second frame of the drain.
        seam.recreate_after_reads(3);

        let err = tail_frames(&seam, &target, &cfg()).await.unwrap_err().to_string();
        assert!(err.contains("recreated while this tail was uploading"), "got: {err}");

        // The sink is untouched: no watermark to resume from, no manifest to
        // restore. Whatever frame objects landed are orphaned and unreferenced.
        assert!(read_watermark(&target.store, &target.watermark_key()).await.unwrap().is_none());
        assert!(list_and_parse_generation_manifests(&target).await.unwrap().is_empty());
    }

    /// R858-B19 — the sidecar half of the same property. A watermark persisted
    /// by a pre-salt writer says nothing about which WAL it names, so the next
    /// tail must restart rather than resume from `last_frame + 1`. Unknown is
    /// not "unchanged".
    #[tokio::test]
    async fn a_saltless_legacy_watermark_forces_a_restart_rather_than_a_resume() {
        let target = fresh_target();
        // Exactly what a pre-R858-B19 writer left behind: five positional
        // fields, no salt pair.
        target
            .store
            .put(&target.watermark_key(), format!("0 2 {} 0 0\n", unix_nanos()).into_bytes().into())
            .await
            .unwrap();

        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        seam.append(2, 2, 0xBB);
        seam.append(3, 3, 0xCC);

        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        let StreamOutcome::Restarted { previous_generation, first_frame, .. } = &out else {
            panic!(
                "an unverifiable watermark must restart, not resume — got {out:?}. Resuming here \
                 trusts a position in a WAL nobody can show is the same one (R858-B19)"
            );
        };
        assert_eq!(previous_generation.salt, None, "the prior generation is unknown, not equal");
        assert_eq!(*first_frame, 1, "re-upload everything rather than trust the old position");

        // Self-healing: the sidecar now carries a salt, so the very next tail
        // resumes normally. One redundant re-upload, not a permanent restart
        // loop.
        seam.append(4, 4, 0xDD);
        let next = tail_frames(&seam, &target, &cfg()).await.unwrap();
        let StreamOutcome::Streamed { first_frame, .. } = &next else {
            panic!("the salt is recorded now, so this must resume — got {next:?}");
        };
        assert_eq!(*first_frame, 4);
    }

    /// R858-B19 — the restore-side refusal, on the shape a pre-`v4` writer
    /// could actually leave in a bucket: a chain that looks perfectly
    /// contiguous but whose manifests never recorded which WAL they came from.
    /// `checkpoint_seq` agrees, the frame ranges tile, and it is still
    /// unrestorable, because that is exactly what a splice looks like.
    #[test]
    fn validate_chain_refuses_a_multi_generation_chain_with_no_recorded_salt() {
        let err = validate_generation_chain(&[
            mk_manifest_salted("base.db", 4096, 0, None, 1, 5),
            mk_manifest_salted("base.db", 4096, 0, None, 6, 9),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("no WAL salt"), "got: {err}");

        // One generation is not a splice — a legacy single-manifest backup
        // stays restorable, because there is nothing here to prove.
        let chain =
            validate_generation_chain(&[mk_manifest_salted("base.db", 4096, 0, None, 1, 5)])
                .unwrap();
        assert_eq!(chain.generation.salt, None);
        assert_eq!(chain.total_frames, 5);
    }

    /// R858-B19 — and the same refusal when the salts are recorded and
    /// *disagree*: a contiguous frame range across two different WALs. This is
    /// the case `checkpoint_seq` can never catch, since a recreate resets it to
    /// the value it already had.
    #[test]
    fn validate_chain_refuses_a_chain_whose_salt_changes_mid_stream() {
        let other = WalSalt { salt1: 0xea10_2175, salt2: 0xb4ff_221f };
        let err = validate_generation_chain(&[
            mk_manifest_salted("base.db", 4096, 0, Some(CHAIN_SALT), 1, 30),
            mk_manifest_salted("base.db", 4096, 0, Some(other), 31, 57),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("RECREATED mid-stream"), "got: {err}");
    }

    /// R858-B19 — manifest `v4` carries the salt, and the version guards move
    /// with it: a `v4` header without a `wal_salt` is corrupt (not legacy), and
    /// a pre-`v4` header *with* one is forged (not a newer writer).
    #[test]
    fn manifest_v4_round_trips_the_wal_salt_and_guards_both_directions() {
        let batches = [(12u64, 34u64)];
        let salt = WalSalt { salt1: 0xb83c_03f5, salt2: 0x0000_0001 };
        let text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "backups/snapshots/snapshot-1.db",
            page_size: 4096,
            checkpoint_seq: 7,
            salt: Some(salt),
            first_frame: 12,
            last_frame: 34,
            epoch: 9,
            owner: Some("node-3"),
            frame_batches: &batches,
        });
        assert!(text.starts_with(MANIFEST_HEADER_V4), "got: {text}");
        let parsed = parse_generation_manifest(&text).unwrap();
        assert_eq!(parsed.salt, Some(salt));
        assert_eq!(parsed.checkpoint_seq, 7);
        assert_eq!(parsed.frame_batches, batches);

        let no_salt = text.replace(&format!("wal_salt {}-{}\n", salt.salt1, salt.salt2), "");
        let err = parse_generation_manifest(&no_salt).unwrap_err().to_string();
        assert!(err.contains("missing `wal_salt`"), "got: {err}");

        let forged = text.replace(MANIFEST_HEADER_V4, MANIFEST_HEADER_V3);
        let err = parse_generation_manifest(&forged).unwrap_err().to_string();
        assert!(err.contains("corrupt or hand-edited"), "got: {err}");
    }

    /// R858-B19 — the sidecar's salt pair is read as a pair. Half a pair is a
    /// torn write, and downgrading it to "unknown" would hide that.
    #[tokio::test]
    async fn watermark_sidecar_round_trips_the_salt_and_rejects_half_a_pair() {
        let target = fresh_target();
        let key = target.watermark_key();
        let salt = WalSalt { salt1: 0xd492_ea8a, salt2: 0x7acf_42a3 };
        write_watermark(
            &target.store,
            &key,
            Watermark { checkpoint_seq: 2, last_frame: 11 },
            Some(salt),
            6,
            3,
            None,
        )
        .await
        .unwrap();
        let read = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!(read.generation, WalGeneration { checkpoint_seq: 2, salt: Some(salt) });

        target
            .store
            .put(&key, format!("2 11 {} 6 3 {}\n", unix_nanos(), salt.salt1).into_bytes().into())
            .await
            .unwrap();
        let err = read_watermark(&target.store, &key).await.unwrap_err().to_string();
        assert!(err.contains("salt1 but no salt2"), "got: {err}");
    }

    /// Captured frame insert: a mock [`WalInsertSeam`] records every call so
    /// tests can assert ordering and content without a real turso connection.
    struct MockInsertSeam {
        log: RefCell<Vec<MockInsertEvent>>,
    }
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MockInsertEvent {
        Begin,
        Frame { frame_no: u64, page_no: u32, db_size: u32 },
        End { force_commit: bool },
    }
    impl MockInsertSeam {
        fn new() -> Self {
            Self { log: RefCell::new(Vec::new()) }
        }
        fn events(&self) -> Vec<MockInsertEvent> {
            self.log.borrow().clone()
        }
    }
    impl WalInsertSeam for MockInsertSeam {
        fn wal_insert_begin(&self) -> Result<()> {
            self.log.borrow_mut().push(MockInsertEvent::Begin);
            Ok(())
        }
        fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<()> {
            let page_no = u32::from_be_bytes(frame[0..4].try_into().unwrap());
            let db_size = u32::from_be_bytes(frame[4..8].try_into().unwrap());
            self.log.borrow_mut().push(MockInsertEvent::Frame { frame_no, page_no, db_size });
            Ok(())
        }
        fn wal_insert_end(&self, force_commit: bool) -> Result<()> {
            self.log.borrow_mut().push(MockInsertEvent::End { force_commit });
            Ok(())
        }
    }

    fn mk_manifest(
        base: &str,
        page_size: usize,
        checkpoint_seq: u32,
        first_frame: u64,
        last_frame: u64,
    ) -> OwnedGenerationManifest {
        mk_manifest_salted(
            base,
            page_size,
            checkpoint_seq,
            Some(CHAIN_SALT),
            first_frame,
            last_frame,
        )
    }

    /// R858-B19: the salt every `mk_manifest` fixture shares, so a chain built
    /// from them is one generation unless a test deliberately says otherwise.
    const CHAIN_SALT: WalSalt = WalSalt { salt1: 0x1109_ca5e, salt2: 0x7acf_42a3 };

    /// R858-B19: `mk_manifest` with the generation salt spelled out — `None`
    /// reproduces a manifest written by a pre-`v4` writer.
    fn mk_manifest_salted(
        base: &str,
        page_size: usize,
        checkpoint_seq: u32,
        salt: Option<WalSalt>,
        first_frame: u64,
        last_frame: u64,
    ) -> OwnedGenerationManifest {
        OwnedGenerationManifest {
            base_snapshot_key: base.to_string(),
            page_size,
            checkpoint_seq,
            salt,
            first_frame,
            last_frame,
            epoch: 0,
            owner: None,
            // Chain validation is about frame ranges, not object layout, so
            // these fixtures stay on the pre-R761-F2 shape — which also keeps
            // them exercising the legacy read path.
            frame_batches: Vec::new(),
        }
    }

    /// validate_generation_chain accepts a single well-formed manifest.
    #[test]
    fn validate_chain_accepts_single_generation() {
        let chain = validate_generation_chain(&[mk_manifest("base.db", 4096, 0, 1, 5)]).unwrap();
        assert_eq!(chain.base_snapshot_key, "base.db");
        assert_eq!(chain.page_size, 4096);
        assert_eq!(chain.generation.checkpoint_seq, 0);
        assert_eq!(chain.total_frames, 5);
    }

    /// validate_generation_chain accepts a contiguous multi-generation chain
    /// and sums the frame count across generations.
    #[test]
    fn validate_chain_accepts_contiguous_multi_generation() {
        let chain = validate_generation_chain(&[
            mk_manifest("base.db", 4096, 3, 1, 5),
            mk_manifest("base.db", 4096, 3, 6, 9),
            mk_manifest("base.db", 4096, 3, 10, 12),
        ])
        .unwrap();
        assert_eq!(chain.generation.checkpoint_seq, 3);
        assert_eq!(chain.total_frames, 12);
    }

    /// Empty chain is the "no generations under prefix" signal.
    #[test]
    fn validate_chain_rejects_empty() {
        assert!(validate_generation_chain(&[]).is_err());
    }

    /// A gap between manifest ranges is corruption — fail loudly.
    #[test]
    fn validate_chain_rejects_gap() {
        let err = validate_generation_chain(&[
            mk_manifest("base.db", 4096, 0, 1, 5),
            mk_manifest("base.db", 4096, 0, 7, 9), // missing frame 6
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("gap in stream"), "err was {err}");
    }

    /// A chain that does not start at frame 1 means the base+chain don't match
    /// (some early frames were never uploaded, or the chain was truncated by a
    /// retention sweep). Refuse.
    #[test]
    fn validate_chain_rejects_non_one_start() {
        let err = validate_generation_chain(&[mk_manifest("base.db", 4096, 0, 5, 9)]).unwrap_err();
        assert!(format!("{err}").contains("gap in stream"), "err was {err}");
    }

    /// A WAL restart between generations means the source folded the WAL into
    /// main; the post-restart frames do not replay onto a pre-restart base.
    /// Refuse and direct the caller at a fresh tier-1a snapshot.
    #[test]
    fn validate_chain_rejects_restart() {
        let err = validate_generation_chain(&[
            mk_manifest("base.db", 4096, 0, 1, 5),
            mk_manifest("base.db", 4096, 1, 1, 3), // restart, new seq
        ])
        .unwrap_err();
        let s = format!("{err}");
        assert!(s.contains("WAL restart"), "err was {s}");
        assert!(s.contains("fresh tier-1a snapshot"), "err was {s}");
    }

    /// Generations referencing different bases mean the chain mixed sinks /
    /// the base was rotated mid-stream. Refuse.
    #[test]
    fn validate_chain_rejects_base_mismatch() {
        let err = validate_generation_chain(&[
            mk_manifest("base-A.db", 4096, 0, 1, 5),
            mk_manifest("base-B.db", 4096, 0, 6, 9),
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("chain spans bases"), "err was {err}");
    }

    /// Page-size mismatch across manifests is a corrupt-manifest signal.
    #[test]
    fn validate_chain_rejects_page_size_mismatch() {
        let err = validate_generation_chain(&[
            mk_manifest("base.db", 4096, 0, 1, 5),
            mk_manifest("base.db", 8192, 0, 6, 9),
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("page_size"), "err was {err}");
    }

    /// replay_frames_into walks every manifest's range in (seq, frame_no) order
    /// and pushes frames into the insert seam. The mock seam records the call
    /// log so we can verify ordering and frame content.
    #[tokio::test]
    async fn replay_walks_manifests_in_frame_order() {
        // Stage two generations' worth of frames into the object store via
        // tail_frames, then point replay at the resulting manifests.
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0xAA);
        seam.append(2, 2, 0xBB); // commit
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &cfg()).await.unwrap();
        seam.append(3, 0, 0xCC);
        seam.append(4, 4, 0xDD); // commit
        let _ = tail_frames(&seam, &target, &cfg()).await.unwrap();

        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        let insert = MockInsertSeam::new();
        let total = replay_frames_into(&target, &insert, &manifests)
            .await
            .unwrap();
        assert_eq!(total, 4);

        let events = insert.events();
        assert_eq!(events.len(), 4, "begin/end are the caller's responsibility");
        for (i, ev) in events.iter().enumerate() {
            let want_frame_no = (i + 1) as u64;
            let want_page = want_frame_no as u32;
            let want_db_size = if want_frame_no.is_multiple_of(2) { want_frame_no as u32 } else { 0 };
            assert_eq!(
                ev,
                &MockInsertEvent::Frame {
                    frame_no: want_frame_no,
                    page_no: want_page,
                    db_size: want_db_size,
                }
            );
        }
    }

    /// R761-F2 read compatibility, and the reason `frame_batches` being empty
    /// has to MEAN something rather than merely be absent: a sink written by a
    /// pre-batching writer — per-frame objects, a v2 manifest — still replays,
    /// and a chain that straddles the change replays as one stream. Written
    /// here by hand at the object level, because the writer that produced this
    /// shape no longer exists to produce it.
    #[tokio::test]
    async fn a_chain_straddling_the_batching_change_replays_as_one_stream() {
        let target = fresh_target();
        let frame_size = WAL_FRAME_HEADER_SIZE + 4096;
        let frame = |frame_no: u64, page_no: u32, db_size: u32| {
            let mut b = vec![0u8; frame_size];
            b[0..4].copy_from_slice(&page_no.to_be_bytes());
            b[4..8].copy_from_slice(&db_size.to_be_bytes());
            b[WAL_FRAME_HEADER_SIZE..].fill(frame_no as u8);
            b
        };

        // Generation 1: the old layout — one object per frame, v2 manifest
        // with no batch list.
        for frame_no in 1..=2u64 {
            target
                .store
                .put(
                    &target.frame_key(0, 0, frame_no),
                    frame(frame_no, frame_no as u32, frame_no as u32).into(),
                )
                .await
                .unwrap();
        }
        let legacy = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "b.db",
            page_size: 4096,
            checkpoint_seq: 0,
            salt: None,
            first_frame: 1,
            last_frame: 2,
            epoch: 0,
            owner: None,
            frame_batches: &[],
        });
        assert!(legacy.starts_with("TURSO-BACKUP STREAM v2\n"), "{legacy}");
        target
            .store
            .put(&target.generation_key(1), legacy.into_bytes().into())
            .await
            .unwrap();

        // Generation 2: the new layout, batched, appended to the same chain.
        let mut batch = frame(3, 3, 0);
        batch.extend_from_slice(&frame(4, 4, 4));
        target
            .store
            .put(&target.frame_batch_key(0, 0, 3, 4), batch.into())
            .await
            .unwrap();
        let batched = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "b.db",
            page_size: 4096,
            checkpoint_seq: 0,
            salt: None,
            first_frame: 3,
            last_frame: 4,
            epoch: 0,
            owner: None,
            frame_batches: &[(3, 4)],
        });
        target
            .store
            .put(&target.generation_key(2), batched.into_bytes().into())
            .await
            .unwrap();

        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        assert!(manifests[0].frame_batches.is_empty(), "gen 1 is the legacy layout");
        assert_eq!(manifests[1].frame_batches, vec![(3, 4)]);

        // R858-B19 CHANGED THIS ASSERTION, deliberately. It used to read
        // `validate_generation_chain(&manifests).expect("layout is not a chain
        // property")`. Layout still is not a chain property — that claim is
        // what the replay assertion below tests, and it still holds. What
        // changed is PROVENANCE: this fixture is a two-generation chain in
        // which neither manifest records which WAL its frames came from,
        // because both predate the `wal_salt` field. That is byte-for-byte the
        // shape a WAL recreate produced (probe F: contiguous ranges, one
        // checkpoint_seq, two different WALs), so restore can no longer tell
        // this chain from a spliced one and must refuse rather than hand back a
        // plausible wrong image. A chain this old needs a fresh tier-1a
        // snapshot; one generation of it would still restore.
        let err = validate_generation_chain(&manifests).unwrap_err().to_string();
        assert!(err.contains("no WAL salt"), "got: {err}");

        let insert = MockInsertSeam::new();
        assert_eq!(replay_frames_into(&target, &insert, &manifests).await.unwrap(), 4);
        assert_eq!(
            insert.events(),
            (1..=4u64)
                .map(|n| MockInsertEvent::Frame {
                    frame_no: n,
                    page_no: n as u32,
                    db_size: if n == 3 { 0 } else { n as u32 },
                })
                .collect::<Vec<_>>(),
            "both layouts deliver the same frames in the same order"
        );
    }

    /// restore_latest_stream errors loudly when there are no manifests under
    /// the prefix — the caller should be using tier-1a restore instead.
    #[tokio::test]
    async fn restore_errors_when_no_generations() {
        let target = fresh_target();
        let dest = std::env::temp_dir().join(format!(
            "turso-backup-restore-empty-{}-{}.db",
            std::process::id(),
            unix_nanos()
        ));
        let err = restore_latest_stream(&target, dest.to_str().unwrap())
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no generation manifests"), "err was {err}");
        // The dest file is never written when validation fails up front.
        assert!(!dest.exists());
    }

    /// A frame uploaded with a wrong byte length corrupts the chain; restore's
    /// replay path catches it before touching the destination's WAL.
    #[tokio::test]
    async fn replay_rejects_wrong_size_frame() {
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xAA);
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &cfg()).await.unwrap();

        // Overwrite the one uploaded frame object with garbage of the wrong
        // length.
        target
            .store
            .put(&target.frame_batch_key(0, 0, 1, 1), b"too short".to_vec().into())
            .await
            .unwrap();

        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        let insert = MockInsertSeam::new();
        let err = replay_frames_into(&target, &insert, &manifests)
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("expected"), "err was {err}");
    }

    /// Live-DB end-to-end fixture: a temp db path that cleans itself up
    /// (including `-wal` / `-shm` sidecars).
    struct TempDb(std::path::PathBuf);
    impl TempDb {
        fn new(tag: &str) -> Self {
            TempDb(std::env::temp_dir().join(format!(
                "turso-backup-stream-{tag}-{}-{}.db",
                std::process::id(),
                unix_nanos(),
            )))
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            for sfx in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{sfx}", self.0.display()));
            }
        }
    }

    async fn seed_rows(path: &str, start: i64, count: i64) {
        let db = turso::Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)", ())
            .await
            .unwrap();
        conn.execute("BEGIN", ()).await.unwrap();
        for i in start..start + count {
            conn.execute("INSERT INTO t (id, v) VALUES (?, ?)", (i, format!("v{i}")))
                .await
                .unwrap();
        }
        conn.execute("COMMIT", ()).await.unwrap();
    }

    async fn checkpoint_truncate(path: &str) {
        let db = turso::Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await.unwrap();
        while rows.next().await.unwrap().is_some() {}
    }

    async fn count_rows(path: &str) -> i64 {
        let db = turso::Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        let mut r = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
        let row = r.next().await.unwrap().unwrap();
        row.get::<i64>(0).unwrap()
    }

    /// End-to-end against a real turso DB and the real `CoreWalSeam`:
    /// seed → snapshot → checkpoint (resets WAL, bumps checkpoint_seq) →
    /// append more rows (creates frames under the new seq) → tail → restore →
    /// row count on the dest matches the post-append source.
    ///
    /// This is the live-DB ping-pong R005-F2's handoff named: it exercises
    /// CoreWalSeam's `wal_state` / `wal_get_frame` AND the new
    /// `wal_insert_begin` / `wal_insert_frame` / `wal_insert_end` impls, plus
    /// the full manifest discovery + replay pipeline.
    #[tokio::test]
    async fn live_db_seed_snapshot_tail_restore_round_trips() {
        let src = TempDb::new("src");
        let dest = TempDb::new("dest");

        // Seed 50 rows. These end up in the base snapshot via VACUUM INTO.
        seed_rows(src.path(), 0, 50).await;

        let target = BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: "backups".into(),
        };
        let base_key = match crate::snapshot::snapshot_and_upload(src.path(), &target)
            .await
            .unwrap()
        {
            crate::snapshot::SnapshotOutcome::Uploaded { key, .. } => key,
            other => panic!("expected Uploaded base snapshot, got {other:?}"),
        };

        // Checkpoint to fold prior WAL into main and reset the WAL header —
        // any subsequent writes land in fresh frames under a new
        // checkpoint_seq. This mirrors how a real orchestrator would hand off
        // from tier-1a (full snapshot) into tier-2 streaming.
        checkpoint_truncate(src.path()).await;

        // Append 25 more rows — these become the WAL frames we stream.
        seed_rows(src.path(), 1000, 25).await;

        // Tail the new frames into the sink via the real CoreWalSeam.
        {
            let seam = CoreWalSeam::open(src.path()).unwrap();
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                page_size: 4096,
                backpressure: BackpressureConfig::default(),
                rpo_target: None,
                epoch: 0,
                owner: None,
                pointer_generation: 0,
            };
            let outcome = tail_frames(&seam, &target, &cfg).await.unwrap();
            match outcome {
                StreamOutcome::Streamed { frame_count, .. } => {
                    assert!(frame_count > 0, "expected at least one frame uploaded");
                }
                StreamOutcome::Restarted { frame_count, .. } => {
                    // Acceptable when this is the first tail after a checkpoint:
                    // there's a prior implicit watermark via the WAL-restart bit.
                    assert!(frame_count > 0);
                }
                other => panic!("expected Streamed/Restarted, got {other:?}"),
            }
        } // drop seam (and its turso_core connection) before restore opens its own.

        // Restore into a fresh dest. Replay must produce the post-append state.
        let outcome = restore_latest_stream(&target, dest.path()).await.unwrap();
        assert_eq!(outcome.base_snapshot_key, base_key);
        assert!(outcome.frames_replayed > 0);
        assert!(outcome.generation_count >= 1);

        // The restored DB is openable via vanilla turso and has the right rows.
        // (snapshot::restore_latest's tests already cover the vanilla-sqlite3
        // exit ramp; here the contract is "turso reopens" since wal_insert_*
        // is the only consumer side that knows the engine's frame layout.)
        let restored_rows = count_rows(dest.path()).await;
        assert_eq!(restored_rows, 75, "expected 50 (base) + 25 (replayed) = 75");
    }

    /// Crash-consistency: the last frames captured by `tail_frames` are
    /// uncommitted (mid-transaction, `db_size == 0`). Restore must drop that
    /// suffix and end up at the previous commit's state — `wal_insert_end`
    /// with `force_commit = false` is the engine knob that does this.
    ///
    /// We can't easily force turso to commit-then-leak-a-partial-write in a
    /// unit test, so we drive the seam directly: a single `MockInsertSeam`-
    /// recorded restore would show the begin/end protocol, but to verify the
    /// engine actually truncates we need the real `CoreWalSeam`. The
    /// `manifest_with_uncommitted_tail_rolls_back_via_insert_end` test below
    /// stages frame bytes in the store by hand and aims them at a real DB.
    #[tokio::test]
    async fn manifest_with_uncommitted_tail_rolls_back_via_insert_end() {
        // Seed two rows via real turso so we have committed page 1 + page 2.
        // Then capture the WAL frames from the source. Then stage a *fake*
        // extra frame whose db_size = 0 (mid-transaction) in the sink, and
        // append it to the manifest range. Restore should drop that fake
        // frame and leave the dest at the 2-row state.
        let src = TempDb::new("ccsrc");
        let dest = TempDb::new("ccdest");
        seed_rows(src.path(), 0, 2).await;
        let target = BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: "backups".into(),
        };
        let base_key = match crate::snapshot::snapshot_and_upload(src.path(), &target)
            .await
            .unwrap()
        {
            crate::snapshot::SnapshotOutcome::Uploaded { key, .. } => key,
            other => panic!("expected Uploaded, got {other:?}"),
        };
        checkpoint_truncate(src.path()).await;
        seed_rows(src.path(), 100, 1).await; // one more row → at least one commit frame.

        let (checkpoint_seq, last_committed_frame, frame_size) = {
            let seam = CoreWalSeam::open(src.path()).unwrap();
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                page_size: 4096,
                backpressure: BackpressureConfig::default(),
                rpo_target: None,
                epoch: 0,
                owner: None,
                pointer_generation: 0,
            };
            let _ = tail_frames(&seam, &target, &cfg).await.unwrap();
            let w = seam.wal_state().unwrap();
            (w.checkpoint_seq, w.last_frame, WAL_FRAME_HEADER_SIZE + 4096)
        };

        // Manually upload one extra "uncommitted" frame: copy the last
        // committed frame's bytes but zero db_size in the header. This
        // simulates tail_frames having captured a mid-transaction tail.
        // R761-F2: the frames live in one batch object, so take the last
        // frame's slice out of it rather than fetching a per-frame key.
        let batch_key = target.frame_batch_key(0, checkpoint_seq, 1, last_committed_frame);
        let batch = target
            .store
            .get(&batch_key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let at = (last_committed_frame as usize - 1) * frame_size;
        let mut tail_bytes = batch[at..at + frame_size].to_vec();
        // Zero the big-endian db_size at offset 4..8 to mark this as non-commit.
        tail_bytes[4..8].copy_from_slice(&0u32.to_be_bytes());
        // Repad to ensure exact length — paranoia.
        assert_eq!(tail_bytes.len(), frame_size);
        let phantom_frame_no = last_committed_frame + 1;
        target
            .store
            .put(
                &target.frame_batch_key(0, checkpoint_seq, phantom_frame_no, phantom_frame_no),
                tail_bytes.into(),
            )
            .await
            .unwrap();

        // Extend the latest generation manifest to claim the phantom frame.
        let mut keys: Vec<_> = target
            .store
            .list_with_delimiter(Some(&join_key(&target.prefix, "generations")))
            .await
            .unwrap()
            .objects
            .into_iter()
            .map(|o| o.location)
            .collect();
        keys.sort();
        let last_manifest_key = keys.last().unwrap().clone();
        let bytes = target
            .store
            .get(&last_manifest_key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut m = parse_generation_manifest(&String::from_utf8_lossy(&bytes)).unwrap();
        m.last_frame = phantom_frame_no;
        // The phantom frame went up as its own single-frame batch object, so
        // the manifest's index has to name it too — the range alone is no
        // longer enough to find a frame (R761-F2).
        m.frame_batches.push((phantom_frame_no, phantom_frame_no));
        let new_text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: &m.base_snapshot_key,
            page_size: m.page_size,
            checkpoint_seq: m.checkpoint_seq,
            salt: None,
            first_frame: m.first_frame,
            last_frame: m.last_frame,
            epoch: m.epoch,
            owner: m.owner.as_deref(),
            frame_batches: &m.frame_batches,
        });
        target
            .store
            .put(&last_manifest_key, new_text.into_bytes().into())
            .await
            .unwrap();

        // Restore. The phantom uncommitted frame must be dropped by
        // wal_insert_end(false); the row count reflects the last *commit*.
        let _ = restore_latest_stream(&target, dest.path()).await.unwrap();
        let restored = count_rows(dest.path()).await;
        assert_eq!(restored, 3, "expected 2 (base) + 1 (committed) — phantom rolled back");
    }

    /// `replay_wal_onto_main` with an empty WAL returns the main bytes
    /// untouched — the main file alone is the committed image.
    #[test]
    fn replay_empty_wal_returns_main_unchanged() {
        let seam = MockWal::new(4096);
        let mut main = vec![0u8; 4096 * 3];
        main[0..16].copy_from_slice(b"SQLite format 3\0");
        let img = replay_wal_onto_main(&seam, main.clone(), 4096).unwrap();
        assert_eq!(img, main);
    }

    /// A single commit frame updates its page slot and the final image is
    /// sized to the commit's `db_size`. Unrelated pages in `main` are left
    /// in place.
    #[test]
    fn replay_single_commit_frame_applies_page() {
        let seam = MockWal::new(4096);
        // Frame 1: page 2, commit at db_size = 3 pages.
        seam.append(2, 3, 0xCC);
        let mut main = vec![0u8; 4096 * 3];
        main[0..16].copy_from_slice(b"SQLite format 3\0");

        let img = replay_wal_onto_main(&seam, main, 4096).unwrap();
        assert_eq!(img.len(), 4096 * 3);
        // Page 1 untouched (still has the magic + zeros).
        assert!(img.starts_with(b"SQLite format 3\0"));
        // Page 2 = 0xCC fill from the WAL frame.
        assert!(img[4096..4096 * 2].iter().all(|&b| b == 0xCC));
        // Page 3 untouched.
        assert!(img[4096 * 2..].iter().all(|&b| b == 0));
    }

    /// Uncommitted frames past the last commit are dropped and the image is
    /// truncated to the last commit's `db_size` — the crash-consistency story
    /// matching restore's `wal_insert_end(false)`.
    #[test]
    fn replay_drops_uncommitted_tail() {
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0x11); // mid-txn
        seam.append(2, 2, 0x22); // commit at db_size = 2
        seam.append(3, 0, 0x33); // uncommitted (dropped)
        seam.append(4, 0, 0x44); // uncommitted (dropped)

        let main = vec![0u8; 4096 * 4]; // pre-grown so a buggy replay would keep junk
        let img = replay_wal_onto_main(&seam, main, 4096).unwrap();
        assert_eq!(img.len(), 4096 * 2, "image must be truncated to db_size=2");
        assert!(img[0..4096].iter().all(|&b| b == 0x11));
        assert!(img[4096..].iter().all(|&b| b == 0x22));
    }

    /// Multi-commit chain: the full commit prefix is applied and the image is
    /// grown to the final commit's `db_size`. Intermediate uncommitted frames
    /// between commits are also applied (they are part of the committed
    /// suffix once a later frame in the same batch becomes a commit).
    #[test]
    fn replay_applies_full_commit_prefix_and_grows_image() {
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0xAA);
        seam.append(2, 2, 0xBB); // first commit
        seam.append(3, 0, 0xCC);
        seam.append(4, 4, 0xDD); // second commit

        let main = vec![0u8; 4096 * 2];
        let img = replay_wal_onto_main(&seam, main, 4096).unwrap();
        assert_eq!(img.len(), 4096 * 4, "image grown to db_size=4");
        assert!(img[0..4096].iter().all(|&b| b == 0xAA));
        assert!(img[4096..4096 * 2].iter().all(|&b| b == 0xBB));
        assert!(img[4096 * 2..4096 * 3].iter().all(|&b| b == 0xCC));
        assert!(img[4096 * 3..].iter().all(|&b| b == 0xDD));
    }

    /// WAL has frames but none are committed (writer mid-transaction at the
    /// instant we sampled). The main file alone is the image; the uncommitted
    /// suffix is dropped wholesale.
    #[test]
    fn replay_returns_main_when_only_uncommitted_frames_present() {
        let seam = MockWal::new(4096);
        seam.append(1, 0, 0x11);
        seam.append(2, 0, 0x22);
        let mut main = vec![0u8; 4096 * 3];
        main[0..16].copy_from_slice(b"SQLite format 3\0");
        let img = replay_wal_onto_main(&seam, main.clone(), 4096).unwrap();
        assert_eq!(img, main);
    }

    /// End-to-end against a real turso DB and `CoreWalSeam`: seed, fold the
    /// first batch into main via TRUNCATE, then seed more rows so the WAL has
    /// uncheckpointed committed frames. `raw_consistent_copy_live` must
    /// reproduce the full row count without itself calling TRUNCATE — that is
    /// the live-writer contract a TRUNCATE-busy concurrent writer would
    /// otherwise block.
    #[tokio::test]
    async fn live_db_consistent_copy_without_truncate_round_trips() {
        let src = TempDb::new("live-cc-src");
        seed_rows(src.path(), 0, 25).await;
        checkpoint_truncate(src.path()).await; // first batch into main, WAL reset
        seed_rows(src.path(), 1000, 10).await; // second batch lives in WAL

        // No TRUNCATE here — this is the live-writer path.
        let image = raw_consistent_copy_live(src.path(), 4096).await.unwrap();
        assert!(image.starts_with(b"SQLite format 3\0"));

        // Write the image to a fresh path (no -wal sidecar — the replay
        // folded WAL in) and re-open to confirm all 35 rows are present.
        let restored = TempDb::new("live-cc-restored");
        std::fs::write(restored.path(), &image).unwrap();
        let rows = count_rows(restored.path()).await;
        assert_eq!(
            rows, 35,
            "expected 25 (checkpointed) + 10 (replayed from WAL)"
        );
    }

    // ── R858-B18: reading a source nobody will lock for us ────────────────
    //
    // The property under test throughout: the copy is either a validated
    // point in time or a refusal, never a plausible wrong image.

    /// The two salt readers must agree. [`read_wal_salt`] pulls it out of frame
    /// 1 through the [`WalSeam`] trait (the only path a `from_conn` seam has);
    /// [`WalFileHeader::read`] pulls it out of the 32-byte `-wal` header with no
    /// engine at all. They are separate code paths against separate byte
    /// offsets, and R858-B18 depends on them naming the same generation — if
    /// they could disagree, validation would compare a salt the streamer never
    /// recorded.
    #[tokio::test]
    async fn wal_file_header_salt_matches_the_salt_read_through_the_seam() {
        let src = TempDb::new("b18-salt-agree");
        seed_rows(src.path(), 0, 5).await;

        let seam = CoreWalSeam::open_reader(src.path()).unwrap();
        let state = seam.wal_state().unwrap();
        assert!(state.last_frame > 0, "seed should leave frames in the WAL");
        let via_seam = read_wal_salt(&seam, 4096, state.last_frame).unwrap().unwrap();
        drop(seam);

        let via_file = WalFileHeader::read(src.path()).unwrap().unwrap();
        assert_eq!(via_file.salt, via_seam, "frame 1 carries the WAL header's salt verbatim");
        assert_eq!(
            via_file.checkpoint_seq, state.checkpoint_seq,
            "and the on-disk sequence is the one the engine reports"
        );
    }

    /// A `-wal` shorter than one header names no generation — an honest
    /// unknown, not an error (same posture as `WalGeneration { salt: None }`).
    #[test]
    fn wal_file_header_parse_needs_a_whole_header() {
        assert!(WalFileHeader::parse(&[0u8; WalFileHeader::SIZE - 1]).is_none());
        let mut hdr = [0u8; WalFileHeader::SIZE];
        hdr[8..12].copy_from_slice(&4096u32.to_be_bytes());
        hdr[12..16].copy_from_slice(&7u32.to_be_bytes());
        hdr[16..20].copy_from_slice(&0xb83c_03f5u32.to_be_bytes());
        hdr[20..24].copy_from_slice(&0x7acf_42a3u32.to_be_bytes());
        let parsed = WalFileHeader::parse(&hdr).unwrap();
        assert_eq!(parsed.page_size, 4096);
        assert_eq!(parsed.checkpoint_seq, 7);
        assert_eq!(parsed.salt, WalSalt { salt1: 0xb83c_03f5, salt2: 0x7acf_42a3 });
    }

    /// The central asymmetry of [`SourceFingerprint::stable_across`]: a plain
    /// append is NOT movement (frames `1..=max_frame` are immutable within a
    /// generation, so our image is merely an earlier point in time), while a
    /// checkpoint IS (it rewrites the main file our copy already read).
    ///
    /// Getting this backwards is not a small error in either direction: treat
    /// an append as movement and every copy of a database that is actually in
    /// use is refused; treat a checkpoint as harmless and we hand back spliced
    /// state that passes `integrity_check`.
    #[tokio::test]
    async fn fingerprint_ignores_an_append_and_catches_a_checkpoint() {
        let src = TempDb::new("b18-fingerprint");
        seed_rows(src.path(), 0, 5).await;

        let before = SourceFingerprint::read(src.path()).unwrap();
        seed_rows(src.path(), 100, 5).await;
        let appended = SourceFingerprint::read(src.path()).unwrap();
        assert!(
            appended.wal_len > before.wal_len,
            "the append must actually have grown the WAL, or this proves nothing \
             (before {}B, after {}B)",
            before.wal_len,
            appended.wal_len
        );
        assert!(
            before.stable_across(&appended),
            "an append is not movement: {} -> {}",
            before.describe(),
            appended.describe()
        );

        checkpoint_truncate(src.path()).await;
        let folded = SourceFingerprint::read(src.path()).unwrap();
        assert!(
            !before.stable_across(&folded),
            "a checkpoint IS movement and must be caught: {} -> {}",
            before.describe(),
            folded.describe()
        );
    }

    /// The protocol accepts when the source holds still, and the accepted value
    /// is the one the attempt produced.
    #[tokio::test]
    async fn validation_accepts_a_quiescent_source() {
        let src = TempDb::new("b18-quiescent");
        seed_rows(src.path(), 0, 3).await;
        let got = validated_against_source(src.path(), "test read", || async { Ok(41 + 1) })
            .await
            .unwrap();
        assert_eq!(got, 42);
    }

    /// A source that moves under every attempt yields a REFUSAL, not a value —
    /// and the message names both samples so an operator can see what moved.
    #[tokio::test]
    async fn validation_refuses_when_the_source_moves_under_every_attempt() {
        let src = TempDb::new("b18-moving");
        seed_rows(src.path(), 0, 3).await;
        let path = src.path().to_string();
        let attempts = std::cell::Cell::new(0u32);
        let err = validated_against_source(src.path(), "test read", || {
            // Grow the main file inside the attempt window, which is exactly
            // the shape of a foreign checkpoint folding pages into it.
            attempts.set(attempts.get() + 1);
            let path = path.clone();
            async move {
                let mut f = std::fs::OpenOptions::new().append(true).open(&path)?;
                std::io::Write::write_all(&mut f, &[0u8; 4096])?;
                Ok(())
            }
        })
        .await
        .unwrap_err();
        assert_eq!(
            attempts.get(),
            COPY_VALIDATION_ATTEMPTS,
            "every attempt in the budget must be spent before refusing"
        );
        let msg = format!("{err:#}");
        assert!(msg.contains("refusing a test read"), "{msg}");
        assert!(msg.contains("before and"), "message must name both samples: {msg}");
    }

    /// An attempt that fails against a source that did NOT move is a real
    /// error, not a race — it is returned as itself on the first attempt rather
    /// than retried and then reported as concurrency.
    #[tokio::test]
    async fn validation_surfaces_a_real_error_without_burning_the_budget() {
        let src = TempDb::new("b18-real-error");
        seed_rows(src.path(), 0, 3).await;
        let attempts = std::cell::Cell::new(0u32);
        let err = validated_against_source(src.path(), "test read", || {
            attempts.set(attempts.get() + 1);
            async { Err::<(), _>(anyhow::anyhow!("page 3 checksum mismatch")) }
        })
        .await
        .unwrap_err();
        assert_eq!(attempts.get(), 1, "a stable source means retrying cannot help");
        let msg = format!("{err:#}");
        assert!(msg.contains("did NOT move"), "{msg}");
        assert!(msg.contains("page 3 checksum mismatch"), "{msg}");
    }

    /// The reader open is the one the backup path uses, and it must produce the
    /// same watermark the writable open does. (Cross-process non-exclusivity —
    /// the point of the flag — is measured in `examples/foreign_checkpoint_probe.rs`
    /// probes G2r/G3c, since it needs a second process.)
    #[tokio::test]
    async fn read_only_seam_reports_the_same_watermark_as_the_writable_one() {
        let src = TempDb::new("b18-reader-watermark");
        seed_rows(src.path(), 0, 4).await;
        let writable = CoreWalSeam::open(src.path()).unwrap().wal_state().unwrap();
        let reader = CoreWalSeam::open_reader(src.path()).unwrap().wal_state().unwrap();
        assert_eq!(writable, reader);
    }

    /// A second call to `raw_consistent_copy_live` after more writes captures
    /// the new state — the primitive is callable repeatedly without per-call
    /// setup, mirroring how a snapshot loop would drive it.
    #[tokio::test]
    async fn live_db_consistent_copy_reflects_new_writes() {
        let src = TempDb::new("live-cc-incr-src");
        seed_rows(src.path(), 0, 5).await;
        let img1 = raw_consistent_copy_live(src.path(), 4096).await.unwrap();
        let r1 = TempDb::new("live-cc-incr-r1");
        std::fs::write(r1.path(), &img1).unwrap();
        assert_eq!(count_rows(r1.path()).await, 5);

        seed_rows(src.path(), 100, 7).await;
        let img2 = raw_consistent_copy_live(src.path(), 4096).await.unwrap();
        let r2 = TempDb::new("live-cc-incr-r2");
        std::fs::write(r2.path(), &img2).unwrap();
        assert_eq!(count_rows(r2.path()).await, 12);
    }

    // ── R732-F2 (W245): tenant fencing epochs on the R2 write path ────────
    //
    // The property under test throughout: a writer holding a stale fencing
    // token is REJECTED, not merely unlucky. Every test below names the
    // split-brain it rules out.

    fn cfg_at_epoch(epoch: u64, owner: Option<&'static str>) -> StreamConfig<'static> {
        StreamConfig { epoch, owner, ..cfg() }
    }

    /// R736-T2: like `cfg_at_epoch`, but also names the cross-cell pointer
    /// generation this writer believes it holds.
    fn cfg_at(epoch: u64, pointer_generation: u64, owner: Option<&'static str>) -> StreamConfig<'static> {
        StreamConfig { epoch, pointer_generation, owner, ..cfg() }
    }

    /// THE canonical F2 test, and the reason the whole relay exists: two
    /// owners tailing the same tenant. The one at the older epoch bounces and
    /// writes nothing; the sink is byte-for-byte what the newer owner left.
    #[tokio::test]
    async fn a_stale_writer_is_fenced_and_writes_zero_frames() {
        let target = fresh_target();

        // The real owner (epoch 2) streams three frames.
        let winner = MockWal::new(4096);
        for i in 1..=3u32 {
            winner.append(i, i, i as u8);
        }
        let out = tail_frames(&winner, &target, &cfg_at_epoch(2, Some("node-2")))
            .await
            .unwrap();
        assert!(matches!(out, StreamOutcome::Streamed { frame_count: 3, .. }));

        let manifests_before = list_and_parse_generation_manifests(&target).await.unwrap();
        let watermark_before = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();

        // The partitioned old owner (epoch 1) wakes up with its own frames and
        // tails the same sink, unaware it has been transferred away.
        let loser = MockWal::new(4096);
        for i in 1..=9u32 {
            loser.append(i, i, 0xff);
        }
        let out = tail_frames(&loser, &target, &cfg_at_epoch(1, Some("node-1")))
            .await
            .unwrap();
        assert_eq!(
            out,
            StreamOutcome::Fenced {
                current_epoch: 2,
                our_epoch: 1,
                current_pointer_generation: 0,
                our_pointer_generation: 0,
            },
            "the stale owner must be told it lost, with both epochs"
        );

        // Zero side effects: no frames under its own epoch prefix, no extra
        // generation, and the watermark still names the winner.
        // Nothing at all under the loser's epoch prefix — asserted by listing
        // rather than by probing one key, so it holds whatever object layout
        // the writer would have used (R761-F2).
        let loser_prefix = "backups/frames/00000000000000000001/";
        assert!(
            !objects_under(&target)
                .await
                .iter()
                .any(|k| k.starts_with(loser_prefix)),
            "a fenced writer must not upload a single frame"
        );
        let manifests_after = list_and_parse_generation_manifests(&target).await.unwrap();
        assert_eq!(
            manifests_after, manifests_before,
            "a fenced writer must not write a generation manifest"
        );
        let watermark_after = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watermark_after.epoch, 2);
        assert_eq!(watermark_after.watermark, watermark_before.watermark);
    }

    /// An UNFENCED (epoch 0) legacy streamer is fenced by a sink that has been
    /// claimed. This is the migration case: a node still running the old
    /// single-writer configuration must not be allowed to scribble over a
    /// tenant that yubaba has since handed to someone else.
    #[tokio::test]
    async fn an_unfenced_writer_is_fenced_by_a_claimed_sink() {
        let target = fresh_target();
        let owner = MockWal::new(4096);
        owner.append(1, 1, 1);
        tail_frames(&owner, &target, &cfg_at_epoch(1, None)).await.unwrap();

        let legacy = MockWal::new(4096);
        legacy.append(1, 1, 2);
        assert_eq!(
            tail_frames(&legacy, &target, &cfg_at_epoch(0, None)).await.unwrap(),
            StreamOutcome::Fenced {
                current_epoch: 1,
                our_epoch: 0,
                current_pointer_generation: 0,
                our_pointer_generation: 0,
            }
        );
    }

    // ── R736-T2 (W250): the second, cross-cell fence ───────────────────────
    //
    // The epoch alone is a *local* raft counter — it cannot see a tenant
    // that moved to a different cell's independent raft group. These tests
    // prove the pointer generation catches exactly the case the epoch can't:
    // a stale cell that is current on its own epoch.

    /// THE canonical T2 test: a writer whose epoch is perfectly current for
    /// its own (now-stale) cell still bounces, because the global pointer
    /// says ownership moved elsewhere. Proves the epoch is necessary but not
    /// sufficient — this is the gap W250 exists to close.
    #[tokio::test]
    async fn a_stale_pointer_generation_fences_even_at_a_current_epoch() {
        let target = fresh_target();

        // The new cell streams at generation 2, epoch 1 (its own local raft
        // is fresh — it just took ownership).
        let winner = MockWal::new(4096);
        for i in 1..=3u32 {
            winner.append(i, i, i as u8);
        }
        let out = tail_frames(&winner, &target, &cfg_at(1, 2, Some("cell-b/node-1")))
            .await
            .unwrap();
        assert!(matches!(out, StreamOutcome::Streamed { frame_count: 3, .. }));

        // The old cell's writer wakes up unaware of the move. Its own local
        // epoch (1) is perfectly current for its own raft group — nothing
        // local told it to step down — but its pointer generation (1) is
        // behind the sink's (2).
        let loser = MockWal::new(4096);
        for i in 1..=9u32 {
            loser.append(i, i, 0xff);
        }
        let out = tail_frames(&loser, &target, &cfg_at(1, 1, Some("cell-a/node-1")))
            .await
            .unwrap();
        assert_eq!(
            out,
            StreamOutcome::Fenced {
                current_epoch: 1,
                our_epoch: 1,
                current_pointer_generation: 2,
                our_pointer_generation: 1,
            },
            "an equal, non-stale epoch must not mask a stale pointer generation"
        );

        // Zero side effects, exactly like the epoch-only fence.
        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        assert_eq!(manifests.len(), 1, "only the winner's generation was written");
    }

    /// The watermark sidecar carries the pointer generation as a fifth
    /// positional field, and a four-field sidecar (pre-R736-T2 writer) reads
    /// back generation 0 — unfenced, exactly like a pre-R732 sidecar reads
    /// back epoch 0.
    #[tokio::test]
    async fn watermark_sidecar_round_trips_the_pointer_generation() {
        let target = fresh_target();
        let key = target.watermark_key();
        write_watermark(
            &target.store,
            &key,
            Watermark { checkpoint_seq: 2, last_frame: 11 },
            None,
            6,
            3,
            None,
        )
        .await
        .unwrap();
        let read = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!((read.epoch, read.pointer_generation), (6, 3));

        // A four-field sidecar (epoch, no generation) — the R732-F2 shape.
        target
            .store
            .put(&key, b"2 11 12345 6\n".to_vec().into())
            .await
            .unwrap();
        let legacy = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!(legacy.epoch, 6);
        assert_eq!(legacy.pointer_generation, 0, "a pre-R736-T2 sidecar fences nobody on generation");
    }

    /// R869: `read_fence_state` reports exactly the two comparands
    /// [`tail_frames`] checks — including the pre-fencing sidecar shapes, which
    /// must read back as unfenced rather than erroring, since a rebuild will
    /// meet them on any sink written before R732-F2.
    #[tokio::test]
    async fn read_fence_state_reports_what_tail_frames_would_check() {
        let target = fresh_target();
        assert_eq!(
            read_fence_state(&target).await.unwrap(),
            None,
            "no sidecar means no fence at all, which is not the same as a zero fence"
        );

        write_watermark(
            &target.store,
            &target.watermark_key(),
            Watermark {
                checkpoint_seq: 2,
                last_frame: 11,
            },
            None,
            6,
            3,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            read_fence_state(&target).await.unwrap(),
            Some(FenceState {
                epoch: 6,
                pointer_generation: 3
            })
        );

        // A two-field sidecar — the original pre-fencing shape. Both comparands
        // read back 0, so a rebuild sees "unfenced" rather than a parse error.
        target
            .store
            .put(&target.watermark_key(), b"2 11\n".to_vec().into())
            .await
            .unwrap();
        assert_eq!(
            read_fence_state(&target).await.unwrap(),
            Some(FenceState::default())
        );
    }

    /// The property a rebuild's epoch floor rests on, stated as a test rather
    /// than as a comment: seeding one above `read_fence_state().epoch` is
    /// exactly enough to stop being fenced, and one *below* it is not.
    #[tokio::test]
    async fn a_floor_taken_from_read_fence_state_is_what_unfences_a_rebuild() {
        let target = fresh_target();
        let seam = MockWal::new(4096);
        for i in 1..=3u32 {
            seam.append(i, i, i as u8);
        }
        tail_frames(&seam, &target, &cfg_at_epoch(5, Some("dead-fleet")))
            .await
            .unwrap();

        let fence = read_fence_state(&target).await.unwrap().unwrap();
        assert_eq!(fence.epoch, 5);

        // A rebuilt cluster that restarted its epochs at 1 is refused.
        seam.append(4, 4, 4);
        let out = tail_frames(&seam, &target, &cfg_at_epoch(1, Some("rebuilt")))
            .await
            .unwrap();
        assert!(matches!(out, StreamOutcome::Fenced { .. }), "got {out:?}");

        // Seeded from the fence, it is not.
        let out = tail_frames(
            &seam,
            &target,
            &cfg_at_epoch(fence.epoch + 1, Some("rebuilt")),
        )
        .await
        .unwrap();
        assert!(matches!(out, StreamOutcome::Streamed { .. }), "got {out:?}");
    }

    /// The same owner resuming at the same epoch is NOT fenced — fencing is
    /// strictly "someone newer exists", not "someone else wrote here". An
    /// owner that restarts under an unchanged token must keep streaming, or
    /// every process restart would wedge the tenant.
    #[tokio::test]
    async fn an_equal_epoch_writer_resumes_normally() {
        let target = fresh_target();
        let seam = MockWal::new(4096);
        seam.append(1, 1, 1);
        tail_frames(&seam, &target, &cfg_at_epoch(3, None)).await.unwrap();
        seam.append(2, 2, 2);
        let out = tail_frames(&seam, &target, &cfg_at_epoch(3, None)).await.unwrap();
        match out {
            StreamOutcome::Streamed { first_frame, last_frame, .. } => {
                assert_eq!((first_frame, last_frame), (2, 2), "resumes after the watermark");
            }
            other => panic!("expected Streamed, got {other:?}"),
        }
    }

    /// Frame keys are namespaced by epoch, and epoch 0 keeps the pre-fencing
    /// two-level layout so existing backups stay addressable.
    #[test]
    fn frame_keys_are_namespaced_by_epoch_with_zero_keeping_the_legacy_layout() {
        let target = fresh_target();
        assert_eq!(
            target.frame_key(0, 7, 42).to_string(),
            "backups/frames/0000000007/00000000000000000042",
            "epoch 0 must keep the original key shape"
        );
        assert_eq!(
            target.frame_key(5, 7, 42).to_string(),
            "backups/frames/00000000000000000005/0000000007/00000000000000000042"
        );
        assert_ne!(target.frame_key(5, 7, 42), target.frame_key(6, 7, 42));
    }

    /// R761-F2: a batch key carries its frame range, keeps the epoch
    /// namespacing and the zero-padding (so lexical order is still frame
    /// order), and cannot be confused with a pre-batching per-frame key even
    /// when the batch holds exactly one frame.
    #[test]
    fn batch_keys_carry_the_range_and_never_collide_with_a_per_frame_key() {
        let target = fresh_target();
        assert_eq!(
            target.frame_batch_key(0, 7, 42, 99).to_string(),
            "backups/frames/0000000007/00000000000000000042-00000000000000000099",
            "epoch 0 keeps the two-level layout for batches too"
        );
        assert_eq!(
            target.frame_batch_key(5, 7, 42, 99).to_string(),
            "backups/frames/00000000000000000005/0000000007/00000000000000000042-00000000000000000099"
        );
        assert_ne!(
            target.frame_batch_key(0, 7, 42, 42),
            target.frame_key(0, 7, 42),
            "a one-frame batch is still a batch — the two layouts must stay distinguishable"
        );
        // Lexical order matches frame order for the batches of one stream,
        // which never overlap.
        assert!(
            target.frame_batch_key(0, 7, 1, 8) < target.frame_batch_key(0, 7, 9, 16),
            "zero-padding must keep batches lexically ordered by first frame"
        );
    }

    /// A takeover mid-stream leaves both owners' frames intact under their own
    /// prefixes — the key namespacing is the backstop behind the epoch check.
    #[tokio::test]
    async fn a_takeover_writes_under_its_own_epoch_prefix_without_disturbing_the_old_one() {
        let target = fresh_target();
        let seam = MockWal::new(4096);
        seam.append(1, 1, 0xaa);
        tail_frames(&seam, &target, &cfg_at_epoch(1, None)).await.unwrap();
        seam.append(2, 2, 0xbb);
        tail_frames(&seam, &target, &cfg_at_epoch(2, None)).await.unwrap();

        let first = target.store.get(&target.frame_batch_key(1, 0, 1, 1)).await.unwrap();
        assert_eq!(first.bytes().await.unwrap().len(), WAL_FRAME_HEADER_SIZE + 4096);
        target
            .store
            .get(&target.frame_batch_key(2, 0, 2, 2))
            .await
            .expect("the new owner's frame lives under its own epoch");
        assert!(
            target.store.get(&target.frame_batch_key(1, 0, 2, 2)).await.is_err(),
            "the new owner must not write into the old owner's prefix"
        );
    }

    /// Restore refuses a chain whose epoch goes backwards: a generation
    /// written by an owner that had already been fenced. Replaying it would
    /// interleave a stale owner's frames into the live stream.
    #[test]
    fn validate_chain_refuses_an_epoch_regression() {
        let mut newer = mk_manifest("base.db", 4096, 0, 1, 5);
        newer.epoch = 4;
        let mut stale = mk_manifest("base.db", 4096, 0, 6, 9);
        stale.epoch = 3;
        let err = validate_generation_chain(&[newer, stale]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("epoch 3"), "err was {msg}");
        assert!(msg.contains("fenced"), "err must name the cause: {msg}");
    }

    /// …but a chain that spans an ownership TRANSFER is fine. Epochs may
    /// advance mid-stream; only regression is corruption.
    #[test]
    fn validate_chain_accepts_a_transfer_mid_chain() {
        let mut first = mk_manifest("base.db", 4096, 0, 1, 5);
        first.epoch = 3;
        let mut second = mk_manifest("base.db", 4096, 0, 6, 9);
        second.epoch = 4;
        let chain = validate_generation_chain(&[first, second]).unwrap();
        assert_eq!(chain.total_frames, 9);
        assert_eq!(chain.epoch, 4, "the chain reports the most recent owner");
    }

    /// A pre-fencing chain still validates and reports epoch 0.
    #[test]
    fn validate_chain_of_pre_fencing_manifests_reports_epoch_zero() {
        let chain = validate_generation_chain(&[mk_manifest("base.db", 4096, 0, 1, 5)]).unwrap();
        assert_eq!(chain.epoch, 0);
    }

    /// Manifest v2 round-trips the epoch and the owner label.
    #[test]
    fn manifest_v2_round_trips_epoch_and_owner() {
        let text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "backups/snapshots/snapshot-1.db",
            page_size: 4096,
            checkpoint_seq: 7,
            salt: None,
            first_frame: 12,
            last_frame: 34,
            epoch: 9,
            owner: Some("node-3"),
            frame_batches: &[],
        });
        assert!(text.starts_with("TURSO-BACKUP STREAM v2\n"), "{text}");
        let parsed = parse_generation_manifest(&text).unwrap();
        assert_eq!(parsed.epoch, 9);
        assert_eq!(parsed.owner.as_deref(), Some("node-3"));

        // No owner label → the key is omitted entirely, not written empty.
        let text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "b.db",
            page_size: 4096,
            checkpoint_seq: 0,
            salt: None,
            first_frame: 1,
            last_frame: 1,
            epoch: 1,
            owner: None,
            frame_batches: &[],
        });
        assert!(!text.contains("owner"), "{text}");
        assert_eq!(parse_generation_manifest(&text).unwrap().owner, None);
    }

    /// R761-F2: a batch list round-trips, and its presence is what moves the
    /// header to v3 — the version and the layout are one fact, so a reader can
    /// never see a v3 header without an index or a v2 header with one.
    #[test]
    fn manifest_v3_round_trips_the_frame_batch_list() {
        let batches = [(12u64, 20u64), (21, 34)];
        let text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "backups/snapshots/snapshot-1.db",
            page_size: 4096,
            checkpoint_seq: 7,
            salt: None,
            first_frame: 12,
            last_frame: 34,
            epoch: 9,
            owner: Some("node-3"),
            frame_batches: &batches,
        });
        assert!(text.starts_with("TURSO-BACKUP STREAM v3\n"), "{text}");
        let parsed = parse_generation_manifest(&text).unwrap();
        assert_eq!(parsed.frame_batches, batches.to_vec());
        assert_eq!(parsed.first_frame, 12);
        assert_eq!(parsed.last_frame, 34);
        assert_eq!(parsed.owner.as_deref(), Some("node-3"));
    }

    /// A v3 batch list that does not exactly tile the manifest's own frame
    /// range is corruption, and it has to fail at parse: the list IS the frame
    /// index, so a hole in it becomes a 404 halfway through a replay — after
    /// the destination has already been overwritten with the base snapshot.
    #[test]
    fn a_v3_manifest_whose_batches_do_not_tile_its_range_is_rejected() {
        let head = "TURSO-BACKUP STREAM v3\nbase_snapshot b.db\npage_size 4096\ncheckpoint_seq 0\nepoch 0\n";
        for (batches, want, why) in [
            ("frame_batch 1-3\nframe_batch 5-9\n", "does not continue", "gap"),
            ("frame_batch 1-3\n", "but the manifest claims", "short"),
            ("frame_batch 2-9\n", "does not continue", "wrong start"),
            ("frame_batch 1-4\nframe_batch 4-9\n", "does not continue", "overlap"),
            ("", "no `frame_batch` lines", "missing index"),
        ] {
            let text = format!("{head}first_frame 1\nlast_frame 9\n{batches}");
            let err = parse_generation_manifest(&text).unwrap_err();
            assert!(
                format!("{err}").contains(want),
                "{why}: expected {want:?}, err was {err}"
            );
        }
    }

    /// The other direction: a `frame_batch` line under a v1/v2 header is
    /// corrupt too. Silently honouring it would let a hand-edited manifest
    /// claim a layout its header says it does not have.
    #[test]
    fn a_pre_v3_manifest_carrying_a_batch_line_is_rejected() {
        let bad = "TURSO-BACKUP STREAM v2\nbase_snapshot b.db\npage_size 4096\ncheckpoint_seq 0\nfirst_frame 1\nlast_frame 3\nepoch 0\nframe_batch 1-3\n";
        let err = parse_generation_manifest(bad).unwrap_err();
        assert!(format!("{err}").contains("corrupt or hand-edited"), "err was {err}");
    }

    /// A v1 manifest — one written before fencing existed — still parses, as
    /// epoch 0. Those backups have to stay restorable.
    #[test]
    fn a_v1_manifest_parses_as_epoch_zero() {
        let legacy = "TURSO-BACKUP STREAM v1\nbase_snapshot b.db\npage_size 4096\ncheckpoint_seq 0\nfirst_frame 1\nlast_frame 3\n";
        let parsed = parse_generation_manifest(legacy).unwrap();
        assert_eq!(parsed.epoch, 0);
        assert_eq!(parsed.owner, None);
        assert_eq!(parsed.last_frame, 3);
    }

    /// A v2 manifest missing its epoch is corrupt, not legacy. Defaulting it
    /// to 0 would silently demote a fenced generation to unfenced — the one
    /// direction this mechanism must never fail in.
    #[test]
    fn a_v2_manifest_without_an_epoch_is_rejected() {
        let bad = "TURSO-BACKUP STREAM v2\nbase_snapshot b.db\npage_size 4096\ncheckpoint_seq 0\nfirst_frame 1\nlast_frame 3\n";
        let err = parse_generation_manifest(bad).unwrap_err();
        assert!(format!("{err}").contains("missing `epoch`"), "err was {err}");
    }

    /// The watermark sidecar carries the epoch as a fourth positional field,
    /// and a three-field sidecar (pre-R732 writer) reads back as epoch 0.
    #[tokio::test]
    async fn watermark_sidecar_round_trips_the_epoch() {
        let target = fresh_target();
        let key = target.watermark_key();
        write_watermark(
            &target.store,
            &key,
            Watermark { checkpoint_seq: 2, last_frame: 11 },
            None,
            6,
            0,
            None,
        )
        .await
        .unwrap();
        let read = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!(read.epoch, 6);
        assert_eq!(read.watermark.last_frame, 11);
        assert!(read.written_at_nanos.is_some());

        target
            .store
            .put(&key, b"2 11 12345\n".to_vec().into())
            .await
            .unwrap();
        let legacy = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!(legacy.epoch, 0, "a pre-R732 sidecar fences nobody");
        assert_eq!(legacy.written_at_nanos, Some(12345));
    }

    /// End-to-end on a real DB: an ownership transfer happens mid-stream and
    /// the restore is clean — every frame from both owners replays, and the
    /// outcome reports the winning epoch. This is the "the N+1 writer wins;
    /// restore is clean" half of the ticket's ask, run against turso_core
    /// rather than a mock.
    #[tokio::test]
    async fn a_transfer_mid_stream_restores_cleanly_and_reports_the_new_epoch() {
        let src = TempDb::new("src-epoch");
        let dest = TempDb::new("dest-epoch");
        seed_rows(src.path(), 0, 50).await;

        let target = fresh_target();
        let base_key = match crate::snapshot::snapshot_and_upload(src.path(), &target)
            .await
            .unwrap()
        {
            crate::snapshot::SnapshotOutcome::Uploaded { key, .. } => key,
            other => panic!("expected Uploaded base snapshot, got {other:?}"),
        };
        checkpoint_truncate(src.path()).await;

        // Owner A (epoch 1) streams the first batch of writes.
        seed_rows(src.path(), 1000, 10).await;
        {
            let seam = CoreWalSeam::open(src.path()).unwrap();
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                epoch: 1,
                owner: Some("node-a"),
                ..cfg()
            };
            let out = tail_frames(&seam, &target, &cfg).await.unwrap();
            assert!(
                matches!(
                    out,
                    StreamOutcome::Streamed { .. } | StreamOutcome::Restarted { .. }
                ),
                "owner A should have streamed, got {out:?}"
            );
        }

        // Ownership transfers. Owner B (epoch 2) picks up where A stopped.
        seed_rows(src.path(), 2000, 15).await;
        {
            let seam = CoreWalSeam::open(src.path()).unwrap();
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                epoch: 2,
                owner: Some("node-b"),
                ..cfg()
            };
            let out = tail_frames(&seam, &target, &cfg).await.unwrap();
            assert!(
                matches!(
                    out,
                    StreamOutcome::Streamed { .. } | StreamOutcome::Restarted { .. }
                ),
                "owner B should have streamed, got {out:?}"
            );
        }

        let outcome = restore_latest_stream(&target, dest.path()).await.unwrap();
        assert_eq!(outcome.base_snapshot_key, base_key);
        assert_eq!(outcome.epoch, 2, "restore reports the most recent owner");
        assert_eq!(
            count_rows(dest.path()).await,
            75,
            "50 (base) + 10 (owner A) + 15 (owner B)"
        );
    }

    // ── R732-T3 (W245): the watermark advance is a compare-and-swap ───────

    /// Bootstrapping a fresh sink uses `PutMode::Create`, so two writers
    /// racing to claim a brand-new tenant cannot both succeed. Without this
    /// the very first write — the one with no prior version to swap on —
    /// would be the one unguarded moment in the whole protocol.
    #[tokio::test]
    async fn a_second_bootstrap_of_a_fresh_sink_is_contended() {
        let target = fresh_target();
        let key = target.watermark_key();
        let w = Watermark { checkpoint_seq: 0, last_frame: 1 };
        assert_eq!(
            write_watermark(&target.store, &key, w, None, 1, 0, None).await.unwrap(),
            WatermarkCas::Advanced
        );
        assert_eq!(
            write_watermark(&target.store, &key, w, None, 1, 0, None).await.unwrap(),
            WatermarkCas::Contended,
            "the sink already exists — Create must not silently overwrite it"
        );
    }

    /// An advance is conditional on the version actually read. A writer
    /// holding a version somebody else has already replaced loses.
    #[tokio::test]
    async fn an_advance_on_a_replaced_version_is_contended() {
        let target = fresh_target();
        let key = target.watermark_key();
        write_watermark(
            &target.store,
            &key,
            Watermark { checkpoint_seq: 0, last_frame: 1 },
            None,
            1,
            0,
            None,
        )
        .await
        .unwrap();

        // Our writer reads the sidecar and holds onto that version…
        let stale = read_watermark(&target.store, &key).await.unwrap().unwrap();
        // …while somebody else advances it out from under us.
        write_watermark(
            &target.store,
            &key,
            Watermark { checkpoint_seq: 0, last_frame: 5 },
            None,
            2,
            0,
            read_watermark(&target.store, &key)
                .await
                .unwrap()
                .unwrap()
                .version
                .as_ref(),
        )
        .await
        .unwrap();

        assert_eq!(
            write_watermark(
                &target.store,
                &key,
                Watermark { checkpoint_seq: 0, last_frame: 2 },
                None,
                1,
                0,
                stale.version.as_ref(),
            )
            .await
            .unwrap(),
            WatermarkCas::Contended,
            "the stale version must not be allowed to overwrite the newer one"
        );
        // And the sink still names the winner, not us.
        let now = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!((now.epoch, now.watermark.last_frame), (2, 5));
    }

    /// Re-reading before advancing works: the point is the version, not the
    /// identity of the writer.
    #[tokio::test]
    async fn an_advance_on_the_current_version_succeeds() {
        let target = fresh_target();
        let key = target.watermark_key();
        write_watermark(
            &target.store,
            &key,
            Watermark { checkpoint_seq: 0, last_frame: 1 },
            None,
            1,
            0,
            None,
        )
        .await
        .unwrap();
        let cur = read_watermark(&target.store, &key).await.unwrap().unwrap();
        assert_eq!(
            write_watermark(
                &target.store,
                &key,
                Watermark { checkpoint_seq: 0, last_frame: 7 },
                None,
                1,
                0,
                cur.version.as_ref(),
            )
            .await
            .unwrap(),
            WatermarkCas::Advanced
        );
    }

    /// A store that lets a test slip a competing writer in between our read
    /// of the watermark and our conditional write of it — the interleaving
    /// that a sequential epoch check cannot catch and the CAS must.
    ///
    /// On the first `put_opts` aimed at the watermark key it writes a rival
    /// sidecar (at `rival_epoch`) straight through to the inner store, then
    /// forwards our conditional put, which now finds a version it does not
    /// hold.
    struct RacingStore {
        inner: Arc<dyn ObjectStore>,
        watermark: ObjPath,
        rival_epoch: u64,
        /// R736-T2: the rival's pointer generation, so the same interleaving
        /// can be exercised for the cross-cell fence too.
        rival_pointer_generation: u64,
        fired: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for RacingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RacingStore({})", self.inner)
        }
    }
    impl std::fmt::Debug for RacingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RacingStore({:?})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RacingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location == &self.watermark
                && !self
                    .fired
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let rival =
                    format!("0 99 1 {} {}\n", self.rival_epoch, self.rival_pointer_generation);
                self.inner
                    .put(location, rival.into_bytes().into())
                    .await?;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn racing_target(rival_epoch: u64) -> BackupTarget {
        racing_target_at(rival_epoch, 0)
    }

    /// R736-T2: like `racing_target`, but also names the rival's pointer
    /// generation.
    fn racing_target_at(rival_epoch: u64, rival_pointer_generation: u64) -> BackupTarget {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "backups".to_string();
        let watermark = join_key(&prefix, "latest.stream-watermark");
        BackupTarget {
            store: Arc::new(RacingStore {
                inner,
                watermark,
                rival_epoch,
                rival_pointer_generation,
                fired: std::sync::atomic::AtomicBool::new(false),
            }),
            prefix,
        }
    }

    /// The race the sequential check cannot see: a newer owner claims the
    /// sink *after* we read it and *before* we write it. The CAS catches it,
    /// and we report Fenced without publishing a manifest — which is why the
    /// manifest write had to move after the watermark advance.
    #[tokio::test]
    async fn losing_the_watermark_race_to_a_newer_owner_fences_us_before_we_publish() {
        let target = racing_target(9);
        let seam = MockWal::new(4096);
        for i in 1..=2u32 {
            seam.append(i, i, i as u8);
        }
        let out = tail_frames(&seam, &target, &cfg_at_epoch(4, Some("node-loser")))
            .await
            .unwrap();
        assert_eq!(
            out,
            StreamOutcome::Fenced {
                current_epoch: 9,
                our_epoch: 4,
                current_pointer_generation: 0,
                our_pointer_generation: 0,
            }
        );

        // No manifest published — the chain stays clean, so restore is not
        // poisoned by a regressed generation from a writer that lost.
        assert!(
            list_and_parse_generation_manifests(&target)
                .await
                .unwrap()
                .is_empty(),
            "a writer that loses the CAS must publish nothing"
        );
        // The rival's sidecar survived untouched.
        let now = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(now.epoch, 9);
    }

    /// R736-T2: the same race, but the rival is a different cell claiming the
    /// tenant via a pointer CAS — our epoch is unchanged (we were never told
    /// to step down locally) but the generation moved under us mid-write.
    #[tokio::test]
    async fn losing_the_watermark_race_to_a_cross_cell_move_fences_us_before_we_publish() {
        let target = racing_target_at(4, 2);
        let seam = MockWal::new(4096);
        for i in 1..=2u32 {
            seam.append(i, i, i as u8);
        }
        let out = tail_frames(&seam, &target, &cfg_at(4, 1, Some("cell-a/node-loser")))
            .await
            .unwrap();
        assert_eq!(
            out,
            StreamOutcome::Fenced {
                current_epoch: 4,
                our_epoch: 4,
                current_pointer_generation: 2,
                our_pointer_generation: 1,
            },
            "an unchanged epoch must not mask a generation that moved under us"
        );
        assert!(
            list_and_parse_generation_manifests(&target)
                .await
                .unwrap()
                .is_empty(),
            "a writer that loses the CAS on generation must publish nothing"
        );
    }

    /// Losing the race to a writer at our own epoch is NOT a fencing event —
    /// it means two streamers were handed the same token. That is a caller
    /// bug and must surface as a loud error, never be retried into success.
    #[tokio::test]
    async fn losing_the_race_to_an_equal_epoch_writer_is_a_loud_error() {
        let target = racing_target(4);
        let seam = MockWal::new(4096);
        seam.append(1, 1, 1);
        let err = tail_frames(&seam, &target, &cfg_at_epoch(4, None))
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("two streamers share one fencing token"),
            "err was {msg}"
        );
    }

    /// Generation manifest round-trip and rejection of garbage.
    #[test]
    fn manifest_round_trip_and_rejection() {
        let text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "backups/snapshots/snapshot-1.db",
            page_size: 4096,
            checkpoint_seq: 7,
            salt: None,
            first_frame: 12,
            last_frame: 34,
            epoch: 0,
            owner: None,
            frame_batches: &[],
        });
        let parsed = parse_generation_manifest(&text).unwrap();
        assert_eq!(parsed.base_snapshot_key, "backups/snapshots/snapshot-1.db");
        assert_eq!(parsed.page_size, 4096);
        assert_eq!(parsed.checkpoint_seq, 7);
        assert_eq!(parsed.first_frame, 12);
        assert_eq!(parsed.last_frame, 34);

        assert!(parse_generation_manifest("not a manifest").is_err());
        assert!(parse_generation_manifest("TURSO-BACKUP STREAM v1\nunknown 1").is_err());
        assert!(parse_generation_manifest("TURSO-BACKUP STREAM v1\nbase_snapshot k").is_err()); // missing fields
    }
}
