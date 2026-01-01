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
//! frames/{checkpoint_seq:010}/{frame_no:020}    raw WAL frame (24-byte header + page)
//! generations/gen-{unix_nanos:020}.manifest     one per `tail_frames` call that uploaded
//! latest.stream-watermark                       text sidecar: "<checkpoint_seq> <last_frame>"
//! ```
//!
//! The generation manifest names the base snapshot key, the page size, and the
//! frame range covered. Object keys are zero-padded so lexical order matches
//! chronological order (same convention as tier 1a snapshots / tier 1b
//! manifests; clock-skew-immune).
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

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
/// fires), but `checkpoint_seq_no` increments monotonically across restarts.
/// So this pair is the right primary key for sink objects — not raw frame_no.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermark {
    pub checkpoint_seq: u32,
    pub last_frame: u64,
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
    /// Open a connection at `path` and disable auto-checkpoint / auto-restart
    /// so the caller owns WAL maintenance. Requires `turso_core` with
    /// `features = ["conn_raw_api"]` (set in this crate's Cargo.toml).
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
}

/// What a [`tail_frames`] call did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOutcome {
    /// No new frames since the last tail — sink is already current.
    Empty { watermark: Watermark },
    /// Uploaded a contiguous range of frames and wrote a generation manifest.
    Streamed {
        generation_key: String,
        first_frame: u64,
        last_frame: u64,
        checkpoint_seq: u32,
        frame_count: u64,
    },
    /// The WAL header restarted since the last tail (`checkpoint_seq`
    /// advanced). Frames `1..N` in the new sequence are uploaded; the sink
    /// records both the old and new sequences. Restore replays in
    /// (`checkpoint_seq`, `frame_no`) order.
    Restarted {
        generation_key: String,
        previous_checkpoint_seq: u32,
        new_checkpoint_seq: u32,
        first_frame: u64,
        last_frame: u64,
        frame_count: u64,
    },
}

impl BackupTarget {
    pub(crate) fn watermark_key(&self) -> ObjPath {
        join_key(&self.prefix, "latest.stream-watermark")
    }

    pub(crate) fn frame_key(&self, checkpoint_seq: u32, frame_no: u64) -> ObjPath {
        join_key(
            &self.prefix,
            &format!("frames/{checkpoint_seq:010}/{frame_no:020}"),
        )
    }

    pub(crate) fn generation_key(&self, unix_nanos: u128) -> ObjPath {
        join_key(
            &self.prefix,
            &format!("generations/gen-{unix_nanos:020}.manifest"),
        )
    }
}

/// Tail new WAL frames from `seam` into `target`, anchored to a base
/// snapshot. Idempotent and resumable: on the second call only frames after
/// the recorded watermark are uploaded.
///
/// Ordering within a single call:
/// 1. Read [`Watermark`] from the seam (snapshot the current `(checkpoint_seq,
///    max_frame)`).
/// 2. Read the prior watermark sidecar (if any).
/// 3. If the seam's `checkpoint_seq` advanced, treat this as a restart:
///    upload frames `1..=max_frame` under the new sequence.
/// 4. Otherwise upload frames `prior.last_frame+1..=max_frame`.
/// 5. Write a generation manifest pointing at the base snapshot + frame
///    range, then update the watermark sidecar. Manifest is written **last**
///    so a manifest never references a missing frame.
///
/// Returns [`StreamOutcome::Empty`] if there is nothing to do (max_frame
/// hasn't advanced and checkpoint_seq is unchanged).
pub async fn tail_frames<S: WalSeam>(
    seam: &S,
    target: &BackupTarget,
    cfg: &StreamConfig<'_>,
) -> Result<StreamOutcome> {
    let current = seam.wal_state()?;
    let prior = read_watermark(&target.store, &target.watermark_key()).await?;

    // Decide the range to upload.
    let (start_frame, restarted) = match prior {
        Some(p) if p.checkpoint_seq == current.checkpoint_seq => (p.last_frame + 1, false),
        Some(_) => (1, true),
        None => (1, false),
    };

    if current.last_frame < start_frame {
        return Ok(StreamOutcome::Empty { watermark: current });
    }

    // Upload frames in ascending order so a partial failure leaves a prefix
    // (the manifest is written last, so a prefix without a manifest is
    // invisible to restore — the next tail just overwrites the same keys).
    let frame_size = WAL_FRAME_HEADER_SIZE + cfg.page_size;
    let mut buf = vec![0u8; frame_size];
    for frame_no in start_frame..=current.last_frame {
        seam.wal_get_frame(frame_no, &mut buf)
            .with_context(|| format!("reading wal frame {frame_no}"))?;
        let key = target.frame_key(current.checkpoint_seq, frame_no);
        target
            .store
            .put(&key, buf.clone().into())
            .await
            .with_context(|| format!("uploading wal frame {frame_no} to {key}"))?;
    }

    // Write the generation manifest, then update the watermark sidecar.
    let nanos = unix_nanos();
    let gen_key = target.generation_key(nanos);
    let manifest = format_generation_manifest(GenerationManifest {
        base_snapshot_key: cfg.base_snapshot_key,
        page_size: cfg.page_size,
        checkpoint_seq: current.checkpoint_seq,
        first_frame: start_frame,
        last_frame: current.last_frame,
    });
    target
        .store
        .put(&gen_key, manifest.into_bytes().into())
        .await
        .with_context(|| format!("writing generation manifest {gen_key}"))?;
    write_watermark(&target.store, &target.watermark_key(), current).await?;

    let frame_count = current.last_frame - start_frame + 1;
    let gen_key = gen_key.to_string();
    if restarted {
        Ok(StreamOutcome::Restarted {
            generation_key: gen_key,
            previous_checkpoint_seq: prior.map(|p| p.checkpoint_seq).unwrap_or(0),
            new_checkpoint_seq: current.checkpoint_seq,
            first_frame: start_frame,
            last_frame: current.last_frame,
            frame_count,
        })
    } else {
        Ok(StreamOutcome::Streamed {
            generation_key: gen_key,
            first_frame: start_frame,
            last_frame: current.last_frame,
            checkpoint_seq: current.checkpoint_seq,
            frame_count,
        })
    }
}

/// Take a raw, point-in-time-consistent byte image of the database at `db_path`
/// WITHOUT folding the WAL via a `TRUNCATE` checkpoint. The live-writer pair of
/// [`crate::dedup::raw_consistent_copy`], which a concurrent writer can make
/// return `busy`.
///
/// Algorithm — the read-only "copy main + replay WAL frames ourselves" path
/// flagged in the working doc and the R005-T1 spike:
///
/// 1. Open a fresh `turso_core::Connection` via [`CoreWalSeam::open`], which
///    calls `wal_auto_actions_disable` so our seam can't auto-checkpoint or
///    restart the WAL header mid-read on our connection.
/// 2. Snapshot the watermark `(checkpoint_seq, max_frame)` via
///    [`WalSeam::wal_state`].
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
///
/// Returned bytes are a self-contained vanilla-SQLite image (page-offset
/// stable, no `-wal` sidecar required), ready to feed
/// [`crate::dedup::snapshot_dedup`]'s content-addressed chunking under a
/// concurrent writer.
pub async fn raw_consistent_copy_live(db_path: &str, page_size: usize) -> Result<Vec<u8>> {
    let seam = CoreWalSeam::open(db_path)
        .with_context(|| format!("opening WAL seam on {db_path}"))?;
    let main_bytes = std::fs::read(db_path)
        .with_context(|| format!("reading main db file {db_path}"))?;
    let image = replay_wal_onto_main(&seam, main_bytes, page_size)?;
    anyhow::ensure!(
        image.starts_with(b"SQLite format 3\0"),
        "live consistent copy of {db_path} is not a SQLite database"
    );
    Ok(image)
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
}

/// A consistency-checked sequence of generation manifests, ready to drive a
/// replay. The fields are the single base / page size / checkpoint sequence
/// shared by every manifest in the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedChain {
    pub base_snapshot_key: String,
    pub page_size: usize,
    pub checkpoint_seq: u32,
    pub total_frames: u64,
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
pub(crate) fn validate_generation_chain(
    manifests: &[OwnedGenerationManifest],
) -> Result<ValidatedChain> {
    let first = manifests
        .first()
        .context("validate_generation_chain: empty manifest list")?;
    let mut expected_next_frame: u64 = 1;
    for (i, m) in manifests.iter().enumerate() {
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
        checkpoint_seq: first.checkpoint_seq,
        total_frames: expected_next_frame - 1,
    })
}

/// Replay every frame named by `manifests` into `seam`, in (checkpoint_seq,
/// frame_no) order. Caller must have already called `wal_insert_begin` on the
/// seam; `wal_insert_end` is also the caller's responsibility (so a test or a
/// future fault-injection path can choose `force_commit`).
async fn replay_frames_into<S: WalInsertSeam>(
    target: &BackupTarget,
    seam: &S,
    manifests: &[OwnedGenerationManifest],
    page_size: usize,
) -> Result<u64> {
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let mut total: u64 = 0;
    for m in manifests {
        for frame_no in m.first_frame..=m.last_frame {
            let key = target.frame_key(m.checkpoint_seq, frame_no);
            let bytes = target
                .store
                .get(&key)
                .await
                .with_context(|| format!("fetching frame {key}"))?
                .bytes()
                .await
                .with_context(|| format!("reading frame body {key}"))?;
            if bytes.len() != frame_size {
                anyhow::bail!(
                    "frame {key} is {} bytes, expected {frame_size} (24 header + {page_size} page)",
                    bytes.len(),
                );
            }
            seam.wal_insert_frame(frame_no, &bytes)
                .with_context(|| format!("inserting frame {frame_no} (key {key})"))?;
            total += 1;
        }
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
pub async fn restore_latest_stream(
    target: &BackupTarget,
    dest_path: &str,
) -> Result<RestoreOutcome> {
    let manifests = list_and_parse_generation_manifests(target).await?;
    if manifests.is_empty() {
        anyhow::bail!(
            "no generation manifests under {} — restore tier-1a directly via snapshot::restore_latest",
            join_key(&target.prefix, "generations"),
        );
    }
    let chain = validate_generation_chain(&manifests)?;

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
    let frames = match replay_frames_into(target, &seam, &manifests, chain.page_size).await {
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
        checkpoint_seq: chain.checkpoint_seq,
        generation_count: manifests.len(),
        frames_replayed: frames,
        last_frame: chain.total_frames,
    })
}

async fn list_and_parse_generation_manifests(
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

/// In-memory shape of a generation manifest. Format on disk:
///
/// ```text
/// TURSO-BACKUP STREAM v1
/// base_snapshot <key>
/// page_size <n>
/// checkpoint_seq <n>
/// first_frame <n>
/// last_frame <n>
/// ```
///
/// Text, dependency-free (no serde), one field per line. Mirrors the
/// `dedup::Manifest` convention so the crate stays consistent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationManifest<'a> {
    pub base_snapshot_key: &'a str,
    pub page_size: usize,
    pub checkpoint_seq: u32,
    pub first_frame: u64,
    pub last_frame: u64,
}

pub(crate) fn format_generation_manifest(m: GenerationManifest<'_>) -> String {
    format!(
        "TURSO-BACKUP STREAM v1\nbase_snapshot {}\npage_size {}\ncheckpoint_seq {}\nfirst_frame {}\nlast_frame {}\n",
        m.base_snapshot_key, m.page_size, m.checkpoint_seq, m.first_frame, m.last_frame,
    )
}

/// Parse a generation manifest. Tolerant to trailing whitespace; rejects any
/// other shape (so a corrupt manifest fails loudly during restore, doesn't
/// silently degrade).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedGenerationManifest {
    pub base_snapshot_key: String,
    pub page_size: usize,
    pub checkpoint_seq: u32,
    pub first_frame: u64,
    pub last_frame: u64,
}

pub fn parse_generation_manifest(text: &str) -> Result<OwnedGenerationManifest> {
    let mut lines = text.lines();
    let header = lines.next().context("empty generation manifest")?;
    if header.trim() != "TURSO-BACKUP STREAM v1" {
        anyhow::bail!("unexpected manifest header: {header:?}");
    }
    let mut base_snapshot_key: Option<String> = None;
    let mut page_size: Option<usize> = None;
    let mut checkpoint_seq: Option<u32> = None;
    let mut first_frame: Option<u64> = None;
    let mut last_frame: Option<u64> = None;
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
            "first_frame" => first_frame = Some(v.parse().context("first_frame")?),
            "last_frame" => last_frame = Some(v.parse().context("last_frame")?),
            other => anyhow::bail!("unknown manifest key: {other}"),
        }
    }
    Ok(OwnedGenerationManifest {
        base_snapshot_key: base_snapshot_key.context("missing base_snapshot")?,
        page_size: page_size.context("missing page_size")?,
        checkpoint_seq: checkpoint_seq.context("missing checkpoint_seq")?,
        first_frame: first_frame.context("missing first_frame")?,
        last_frame: last_frame.context("missing last_frame")?,
    })
}

async fn read_watermark(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
) -> Result<Option<Watermark>> {
    match store.get(key).await {
        Ok(res) => {
            let bytes = res.bytes().await.context("reading watermark sidecar")?;
            let s = String::from_utf8_lossy(&bytes);
            let (seq, frame) = s
                .trim()
                .split_once(' ')
                .context("watermark sidecar must be '<checkpoint_seq> <last_frame>'")?;
            Ok(Some(Watermark {
                checkpoint_seq: seq.parse().context("watermark checkpoint_seq")?,
                last_frame: frame.parse().context("watermark last_frame")?,
            }))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).context("fetching watermark sidecar"),
    }
}

async fn write_watermark(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
    w: Watermark,
) -> Result<()> {
    let body = format!("{} {}\n", w.checkpoint_seq, w.last_frame);
    store
        .put(key, body.into_bytes().into())
        .await
        .with_context(|| format!("writing watermark sidecar {key}"))?;
    Ok(())
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
    use object_store::memory::InMemory;
    use std::cell::RefCell;

    /// In-memory WAL seam: a fixed page_size, a vector of frames the test
    /// appends to, and an advancing checkpoint_seq the test can bump.
    struct MockWal {
        page_size: usize,
        state: RefCell<MockState>,
    }
    struct MockState {
        checkpoint_seq: u32,
        frames: Vec<MockFrame>,
        auto_actions_disabled: bool,
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
                    frames: Vec::new(),
                    auto_actions_disabled: false,
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
        /// Simulate a WAL restart: bump checkpoint_seq, reset frames.
        fn restart(&self) {
            let mut s = self.state.borrow_mut();
            s.checkpoint_seq += 1;
            s.frames.clear();
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
            let s = self.state.borrow();
            let idx = frame_no
                .checked_sub(1)
                .context("frame_no must be >= 1")? as usize;
            let f = s
                .frames
                .get(idx)
                .with_context(|| format!("frame {frame_no} out of range"))?;
            // Synthesize a 24-byte header: big-endian page_no, db_size, then
            // zeros for salts/checksums (we never validate those).
            buf[0..4].copy_from_slice(&f.info.page_no.to_be_bytes());
            buf[4..8].copy_from_slice(&f.info.db_size.to_be_bytes());
            buf[8..WAL_FRAME_HEADER_SIZE].fill(0);
            buf[WAL_FRAME_HEADER_SIZE..].copy_from_slice(&f.page_bytes);
            Ok(f.info)
        }
        fn wal_auto_actions_disable(&self) {
            self.state.borrow_mut().auto_actions_disabled = true;
        }
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
        }
    }

    /// First tail with no frames yet — Empty, no manifest, no watermark.
    #[tokio::test]
    async fn empty_when_no_frames() {
        let seam = MockWal::new(4096);
        let target = fresh_target();
        let out = tail_frames(&seam, &target, &cfg()).await.unwrap();
        match out {
            StreamOutcome::Empty { watermark } => {
                assert_eq!(watermark, Watermark::default());
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

        // Frame keys exist + carry the right bytes.
        for frame_no in 1..=3u64 {
            let bytes = target
                .store
                .get(&target.frame_key(0, frame_no))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(bytes.len(), WAL_FRAME_HEADER_SIZE + 4096);
            let page_no = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
            assert_eq!(page_no, frame_no as u32);
        }

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

        // Watermark sidecar matches.
        let wm = read_watermark(&target.store, &target.watermark_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wm, Watermark { checkpoint_seq: 0, last_frame: 3 });

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
            StreamOutcome::Empty { watermark } => assert_eq!(
                watermark,
                Watermark { checkpoint_seq: 0, last_frame: 1 },
            ),
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
        // Frames 1..=4 all retrievable under checkpoint_seq=0.
        for frame_no in 1..=4u64 {
            target
                .store
                .get(&target.frame_key(0, frame_no))
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
                previous_checkpoint_seq,
                new_checkpoint_seq,
                first_frame,
                last_frame,
                frame_count,
                ..
            } => {
                assert_eq!(previous_checkpoint_seq, 0);
                assert_eq!(new_checkpoint_seq, 1);
                assert_eq!(first_frame, 1);
                assert_eq!(last_frame, 1);
                assert_eq!(frame_count, 1);
            }
            other => panic!("expected Restarted, got {other:?}"),
        }

        // Old seq=0 frames still in place; new seq=1 frame at its own key.
        target
            .store
            .get(&target.frame_key(0, 1))
            .await
            .unwrap();
        target
            .store
            .get(&target.frame_key(1, 1))
            .await
            .unwrap();
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
        OwnedGenerationManifest {
            base_snapshot_key: base.to_string(),
            page_size,
            checkpoint_seq,
            first_frame,
            last_frame,
        }
    }

    /// validate_generation_chain accepts a single well-formed manifest.
    #[test]
    fn validate_chain_accepts_single_generation() {
        let chain = validate_generation_chain(&[mk_manifest("base.db", 4096, 0, 1, 5)]).unwrap();
        assert_eq!(chain.base_snapshot_key, "base.db");
        assert_eq!(chain.page_size, 4096);
        assert_eq!(chain.checkpoint_seq, 0);
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
        assert_eq!(chain.checkpoint_seq, 3);
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
        let total = replay_frames_into(&target, &insert, &manifests, 4096)
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

        // Overwrite the one uploaded frame with garbage of the wrong length.
        target
            .store
            .put(&target.frame_key(0, 1), b"too short".to_vec().into())
            .await
            .unwrap();

        let manifests = list_and_parse_generation_manifests(&target).await.unwrap();
        let insert = MockInsertSeam::new();
        let err = replay_frames_into(&target, &insert, &manifests, 4096)
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
            };
            let _ = tail_frames(&seam, &target, &cfg).await.unwrap();
            let w = seam.wal_state().unwrap();
            (w.checkpoint_seq, w.last_frame, WAL_FRAME_HEADER_SIZE + 4096)
        };

        // Manually upload one extra "uncommitted" frame: copy the last
        // committed frame's bytes but zero db_size in the header. This
        // simulates tail_frames having captured a mid-transaction tail.
        let last_key = target.frame_key(checkpoint_seq, last_committed_frame);
        let mut tail_bytes = target
            .store
            .get(&last_key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        // Zero the big-endian db_size at offset 4..8 to mark this as non-commit.
        tail_bytes[4..8].copy_from_slice(&0u32.to_be_bytes());
        // Repad to ensure exact length — paranoia.
        assert_eq!(tail_bytes.len(), frame_size);
        let phantom_frame_no = last_committed_frame + 1;
        target
            .store
            .put(&target.frame_key(checkpoint_seq, phantom_frame_no), tail_bytes.into())
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
        let new_text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: &m.base_snapshot_key,
            page_size: m.page_size,
            checkpoint_seq: m.checkpoint_seq,
            first_frame: m.first_frame,
            last_frame: m.last_frame,
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

    /// Generation manifest round-trip and rejection of garbage.
    #[test]
    fn manifest_round_trip_and_rejection() {
        let text = format_generation_manifest(GenerationManifest {
            base_snapshot_key: "backups/snapshots/snapshot-1.db",
            page_size: 4096,
            checkpoint_seq: 7,
            first_frame: 12,
            last_frame: 34,
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
