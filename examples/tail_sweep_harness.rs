//! `tail_sweep_harness` (example) — W313 §9(0)'s required measurement:
//! how many object-store write ops does one application write actually
//! cost, and does batching a tail call's frames into one object change the
//! bill?
//!
//! Backing doc: @arch:see(.yah/docs/working/W313-roadcase-capability-cells.md) §3.1, §9(0)
//!
//! §3.1's ~4x-over-storage projection assumed ~3 frames/transaction, which
//! was an estimate, not a measurement. This harness gets the number: it
//! models a **casual app** — many small, single-row transactions, with
//! [`tail_frames`] called on an interval between them, exactly what an RPO
//! scheduler does — and sweeps that interval, because the interval is the
//! knob §3.1's remedy (batch frames per tail call) would turn.
//!
//! **R761-F2 landed that remedy**, so the sign of the comparison flipped:
//! the measured numbers are the batched layout's, and the counterfactual is
//! the pre-batching one-object-per-frame cost (`unbatched_puts_per_txn`,
//! computed from the frame count this workload actually produced). The
//! headline reduction still means the same thing — what batching is worth on
//! this workload — and `frame_puts_per_tail_call` is the direct check that
//! the sink really did batch: it should be 1, or a small integer when a call
//! overflows the spill buffer.
//!
//! ## What one step does
//!
//! For each `writes_per_tail` in [`SWEEP_INTERVALS`]:
//! 1. Open a fresh turso DB, create a small table **with a secondary
//!    index** (`cells(id, k, v)` + `idx_cells_k` on `k`) — a bare rowid
//!    table would hide the extra frames an index split produces, and
//!    that's exactly the frame count §3.1 guessed at. Seed a handful of
//!    warm rows so the index has real structure before the measured phase
//!    starts.
//! 2. Snapshot it ([`snapshot_and_upload`]) to a per-config in-memory
//!    object store, then [`checkpoint_truncate`] — same tier-1a-then-tier-2
//!    handoff `rss_harness.rs` uses, and the point at which the *measured*
//!    phase begins: everything from here on is wrapped in a
//!    [`CountingStore`], so the base snapshot's own PUT never pollutes the
//!    per-write cost this harness reports.
//! 3. Run `TAIL_SWEEP_TXNS` single-row `BEGIN; INSERT; COMMIT` transactions
//!    against the DB, calling [`tail_frames`] every `writes_per_tail`
//!    transactions — the RPO scheduler's cadence, made explicit. `id` is a
//!    monotonic counter (a realistic autoincrement key); `k` is a
//!    multiplicative hash of `id`, so index inserts scatter across the
//!    btree instead of only ever appending at the tail, which would hide
//!    the index-split frames a real workload produces.
//! 4. [`CountingStore`] wraps the sink's `object_store::ObjectStore` and
//!    counts every `put_opts` call, classified by key prefix
//!    (`frames/`, `generations/`, `latest.stream-watermark`,
//!    `snapshots/`). `put_opts` catches everything: `put_with_backoff`'s
//!    per-frame `store.put()` delegates to it (`ObjectStoreExt::put`'s
//!    default impl), the generation manifest write calls it directly, and
//!    so does the watermark CAS — one counter answers "which key class
//!    dominates the bill" with no separate instrumentation per call site.
//!
//! ## A connection-lifetime finding this harness inherits rather than
//! rediscovers
//!
//! The plan was to hold a writer connection open across the whole measured
//! phase and open a fresh [`CoreWalSeam`] per tail call. `stream.rs`'s own
//! `R005-F3` handoff already answers whether that's possible: *"the live
//! tests use `turso::Builder` for writes/reads + `CoreWalSeam` for tailing
//! — exclusive WAL lock means we drop the high-level conn before opening
//! the low-level seam"* — and every one of `stream.rs`'s live-DB test
//! helpers (`seed_rows`, `checkpoint_truncate`) does exactly that: open,
//! act, let it drop, before the next `CoreWalSeam::open`. So this harness
//! opens a `turso::Builder` connection for a whole batch of
//! `writes_per_tail` transactions (holding it open *within* the batch is
//! fine — nothing else contends for the lock there), drops it, opens a
//! fresh `CoreWalSeam` for the tail call, drops that, and reopens the
//! writer for the next batch. Open/close per tail interval, not per
//! transaction — cheaper than the naive per-transaction-reopen fallback,
//! and it's what the crate's own tests prove is required rather than a
//! shape this harness had to discover by crashing first.
//!
//! ## Output
//!
//! NDJSON to stdout: one line per tail call
//! (`{"writes_per_tail":W,"tail_call":N,"txns_so_far":T,"outcome":"...",
//! "frame_count":F,"puts":{"frames":a,"generations":b,"watermark":c,
//! "snapshots":d,"other":e},"bytes":{...},"elapsed_ms":T}`), one summary
//! line per `writes_per_tail` config, and a trailing overall summary line
//! that answers the acceptance question directly.
//!
//! ## Running
//!
//! ```text
//! cargo run -p turso-backup --example tail_sweep_harness
//! TAIL_SWEEP_INTERVALS=1,5,25,100 TAIL_SWEEP_TXNS=200 TAIL_SWEEP_WRITES_PER_DAY=10 \
//!   cargo run -p turso-backup --example tail_sweep_harness
//! ```
//!
//! Env overrides (all optional):
//! - `TAIL_SWEEP_INTERVALS` (default `1,5,25,100`) — comma-separated
//!   `writes_per_tail` values to sweep.
//! - `TAIL_SWEEP_TXNS` (default `200`) — total single-row transactions run
//!   per interval.
//! - `TAIL_SWEEP_WRITES_PER_DAY` (default `10`) — writes/day used for the
//!   $/cell/month projection, matching W313 §3.1's "~10 writes/day" table
//!   so the two are directly comparable.
//! - `TAIL_SWEEP_WORKDIR` (default a fresh
//!   `$TMPDIR/turso-backup-tail-sweep-<pid>`).

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use turso_backup::backpressure::BackpressureConfig;
use turso_backup::snapshot::{snapshot_and_upload, BackupTarget, SnapshotOutcome};
use turso_backup::stream::{tail_frames, CoreWalSeam, StreamConfig, StreamOutcome};

/// Matches the WAL page size the live-DB tests in `stream.rs` use and
/// `rss_harness.rs`'s own constant; turso's default page size for a
/// freshly created database.
const PAGE_SIZE: usize = 4096;

/// R2 Class A price per the pricing W313 §3.1 costs against.
const R2_CLASS_A_USD_PER_MILLION_OPS: f64 = 4.50;

/// Object-store key classes this harness distinguishes — mirrors the
/// object layout documented at the top of `stream.rs`.
const CLASSES: [&str; 5] = ["frames", "generations", "watermark", "snapshots", "other"];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let intervals = env_intervals("TAIL_SWEEP_INTERVALS", &[1, 5, 25, 100]);
    let total_txns = env_usize("TAIL_SWEEP_TXNS", 200);
    let writes_per_day = env_usize("TAIL_SWEEP_WRITES_PER_DAY", 10) as f64;
    let workdir = std::env::var("TAIL_SWEEP_WORKDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("turso-backup-tail-sweep-{}", std::process::id()))
        });
    std::fs::create_dir_all(&workdir)
        .with_context(|| format!("creating workdir {}", workdir.display()))?;

    let mut config_summaries: Vec<ConfigSummary> = Vec::with_capacity(intervals.len());

    for writes_per_tail in intervals {
        let summary =
            run_sweep_point(&workdir, writes_per_tail, total_txns, writes_per_day).await?;
        config_summaries.push(summary);
    }

    print_overall_summary(&config_summaries, writes_per_day);

    let _ = std::fs::remove_dir_all(&workdir);
    Ok(())
}

struct ConfigSummary {
    writes_per_tail: usize,
    total_txns: usize,
    total_frames: u64,
    puts: HashMap<&'static str, ClassStats>,
    tail_calls_with_frames: u64,
}

impl ConfigSummary {
    fn total_puts(&self) -> u64 {
        self.puts.values().map(|s| s.puts).sum()
    }

    fn frames_per_txn(&self) -> f64 {
        self.total_frames as f64 / self.total_txns as f64
    }

    fn puts_per_txn(&self) -> f64 {
        self.total_puts() as f64 / self.total_txns as f64
    }

    /// The PUT count this sink WOULD cost on the pre-R761-F2 layout: one
    /// object per WAL frame, plus the manifest and watermark PUTs, which the
    /// layout change does not touch.
    ///
    /// This was the counterfactual in the other direction until R761-F2
    /// landed — "what would batching save?" — and batching is now what the
    /// crate does, so the hypothetical is the *old* layout and the measured
    /// number is the batched one. Keeping the comparison (rather than
    /// deleting it) is what lets a re-run still answer §9(0)'s question
    /// against a fresh workload instead of citing the 2026-08-13 table
    /// forever.
    fn unbatched_total_puts(&self) -> u64 {
        let generations = self.puts.get("generations").map(|s| s.puts).unwrap_or(0);
        let watermark = self.puts.get("watermark").map(|s| s.puts).unwrap_or(0);
        self.total_frames + generations + watermark
    }

    fn unbatched_puts_per_txn(&self) -> f64 {
        self.unbatched_total_puts() as f64 / self.total_txns as f64
    }

    /// Sanity check on the layout the sink actually wrote: with batching, the
    /// `frames` PUT count is one per tail call that produced frames (or a few,
    /// if a call overflowed the spill buffer) — never one per frame.
    fn frame_puts_per_tail_call(&self) -> f64 {
        let frames = self.puts.get("frames").map(|s| s.puts).unwrap_or(0);
        frames as f64 / self.tail_calls_with_frames.max(1) as f64
    }

    fn projected_usd_per_cell_month(&self, writes_per_day: f64) -> f64 {
        self.puts_per_txn() * writes_per_day * 30.0 / 1_000_000.0 * R2_CLASS_A_USD_PER_MILLION_OPS
    }

    fn unbatched_projected_usd_per_cell_month(&self, writes_per_day: f64) -> f64 {
        self.unbatched_puts_per_txn() * writes_per_day * 30.0 / 1_000_000.0
            * R2_CLASS_A_USD_PER_MILLION_OPS
    }
}

async fn run_sweep_point(
    workdir: &std::path::Path,
    writes_per_tail: usize,
    total_txns: usize,
    writes_per_day: f64,
) -> Result<ConfigSummary> {
    let started = Instant::now();
    let db_path = workdir.join(format!("cell-{writes_per_tail}.db"));
    let db_path = db_path.to_str().context("db path is not valid UTF-8")?;

    // Schema + a handful of warm rows, seeded before the base snapshot so
    // the index has real structure before the measured phase starts — a
    // fresh empty index would understate the split cost a live app sees.
    seed_schema_and_rows(db_path, 0, 20)
        .await
        .with_context(|| format!("seeding schema for writes_per_tail={writes_per_tail}"))?;

    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base_target = BackupTarget { store: raw_store.clone(), prefix: format!("cell-{writes_per_tail}") };
    let base_key = match snapshot_and_upload(db_path, &base_target)
        .await
        .with_context(|| format!("snapshot_and_upload for writes_per_tail={writes_per_tail}"))?
    {
        SnapshotOutcome::Uploaded { key, .. } => key,
        other => anyhow::bail!("expected a fresh Uploaded base snapshot, got {other:?}"),
    };
    checkpoint_truncate(db_path)
        .await
        .with_context(|| format!("checkpointing writes_per_tail={writes_per_tail}"))?;

    // Everything from here counts toward the per-write cost: the base
    // snapshot's PUT above went through `raw_store` directly and is
    // deliberately not seen by `counting`.
    let counting = Arc::new(CountingStore::new(raw_store));
    let counted_target = BackupTarget { store: counting.clone(), prefix: base_target.prefix.clone() };

    let mut txns_done = 0usize;
    let mut tail_call = 0u64;
    let mut total_frames = 0u64;
    let mut tail_calls_with_frames = 0u64;
    let mut next_row_id: i64 = 1000; // past the warm-seed range

    while txns_done < total_txns {
        let batch = writes_per_tail.min(total_txns - txns_done);
        let step_started = Instant::now();
        run_transactions(db_path, next_row_id, batch)
            .await
            .with_context(|| format!("running {batch} transactions for writes_per_tail={writes_per_tail}"))?;
        next_row_id += batch as i64;
        txns_done += batch;

        let before = counting.snapshot();
        let outcome = {
            let seam = CoreWalSeam::open(db_path)
                .with_context(|| format!("opening tail seam for writes_per_tail={writes_per_tail}"))?;
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                page_size: PAGE_SIZE,
                backpressure: BackpressureConfig::default(),
                rpo_target: None,
                epoch: 0,
                owner: Some("tail-sweep-harness"),
                pointer_generation: 0,
            };
            tail_frames(&seam, &counted_target, &cfg)
                .await
                .with_context(|| format!("tail_frames for writes_per_tail={writes_per_tail}"))?
        };
        let after = counting.snapshot();
        tail_call += 1;

        let frame_count = match &outcome {
            StreamOutcome::Streamed { frame_count, .. } => *frame_count,
            // Auto-checkpoint can fire mid-sweep and roll the WAL under a new
            // checkpoint_seq — still real frames uploaded, not an error.
            StreamOutcome::Restarted { frame_count, .. } => *frame_count,
            StreamOutcome::Empty { .. } => 0,
            StreamOutcome::Shed { .. } => 0,
            StreamOutcome::Fenced { current_epoch, our_epoch, .. } => anyhow::bail!(
                "unexpected Fenced outcome (epoch {our_epoch} vs sink epoch {current_epoch}) — this harness is single-writer at epoch 0"
            ),
        };
        total_frames += frame_count;
        if frame_count > 0 {
            tail_calls_with_frames += 1;
        }

        print_step_line(writes_per_tail, tail_call, txns_done, &outcome, frame_count, &before, &after, step_started);
    }

    let summary = ConfigSummary {
        writes_per_tail,
        total_txns,
        total_frames,
        puts: counting.snapshot(),
        tail_calls_with_frames,
    };
    print_config_summary_line(&summary, writes_per_day, started);
    Ok(summary)
}

async fn seed_schema_and_rows(path: &str, start_id: i64, count: i64) -> Result<()> {
    let db = turso::Builder::new_local(path)
        .build()
        .await
        .with_context(|| format!("opening {path}"))?;
    let conn = db.connect().with_context(|| format!("connecting to {path}"))?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cells (id INTEGER PRIMARY KEY, k TEXT NOT NULL, v TEXT)",
        (),
    )
    .await
    .context("create table")?;
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cells_k ON cells(k)", ())
        .await
        .context("create index")?;
    conn.execute("BEGIN", ()).await.context("begin")?;
    for id in start_id..start_id + count {
        conn.execute(
            "INSERT INTO cells (id, k, v) VALUES (?, ?, ?)",
            (id, scatter_key(id), format!("v{id}")),
        )
        .await
        .with_context(|| format!("seed insert row {id}"))?;
    }
    conn.execute("COMMIT", ()).await.context("commit")?;
    Ok(())
}

/// One "application write": a single-row insert in its own transaction —
/// the casual-app shape §9(0) asks for, not `rss_harness`'s one big
/// transaction. `writer` is opened once per batch (see the module doc's
/// connection-lifetime note) and reused across every transaction in the
/// batch; each transaction still gets its own `BEGIN`/`COMMIT` boundary
/// because that's what produces a distinct WAL commit frame per write.
async fn run_transactions(path: &str, start_id: i64, count: usize) -> Result<()> {
    let db = turso::Builder::new_local(path)
        .build()
        .await
        .with_context(|| format!("opening writer at {path}"))?;
    let conn = db.connect().with_context(|| format!("connecting writer at {path}"))?;
    for i in 0..count {
        let id = start_id + i as i64;
        conn.execute("BEGIN", ()).await.context("begin")?;
        conn.execute(
            "INSERT INTO cells (id, k, v) VALUES (?, ?, ?)",
            (id, scatter_key(id), format!("v{id}")),
        )
        .await
        .with_context(|| format!("insert row {id}"))?;
        conn.execute("COMMIT", ()).await.context("commit")?;
    }
    Ok(())
}

async fn checkpoint_truncate(path: &str) -> Result<()> {
    let db = turso::Builder::new_local(path)
        .build()
        .await
        .with_context(|| format!("opening {path} for checkpoint"))?;
    let conn = db.connect().with_context(|| format!("connecting to {path} for checkpoint"))?;
    let mut rows = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await
        .context("wal_checkpoint(TRUNCATE)")?;
    while rows.next().await.context("draining checkpoint rows")?.is_some() {}
    Ok(())
}

/// Multiplicative hash of `n` (Knuth's constant), formatted as fixed-width
/// hex — spreads index keys pseudo-randomly across the btree's lexical
/// range instead of only ever appending at the tail. A sequential key would
/// understate index-split cost; real app keys (usernames, UUIDs, slugs)
/// scatter like this one does.
fn scatter_key(n: i64) -> String {
    let h = (n as u64).wrapping_mul(2_654_435_761);
    format!("{h:016x}")
}

// --- CountingStore: wraps the sink and counts put_opts by key class ------

#[derive(Debug, Default, Clone, Copy)]
struct ClassStats {
    puts: u64,
    bytes: u64,
}

/// Classify an object-store key by the layout documented at the top of
/// `stream.rs`. `frames/{epoch}/{checkpoint_seq}/{first}-{last}` (or the
/// unfenced two-level form, or a pre-R761-F2 per-frame key) matches
/// `"frames"`; everything else matches on its own literal path segment. The
/// prefix is what identifies the class, so batching did not change this —
/// only how many keys land under it.
fn classify(key: &str) -> &'static str {
    if key.contains("frames/") {
        "frames"
    } else if key.contains("generations/") {
        "generations"
    } else if key.ends_with("latest.stream-watermark") {
        "watermark"
    } else if key.contains("snapshots/") {
        "snapshots"
    } else {
        "other"
    }
}

/// Wraps an inner store and counts `put_opts` calls, classified by key
/// prefix and accumulating payload bytes per class. Mirrors this crate's
/// own `CountingStore` (`puller.rs`, get-side) and `FaultyStore`
/// (`backpressure.rs`) boilerplate: delegate everything, instrument one
/// method. `put_opts` is the single instrumentation point because every
/// write in `stream.rs` funnels through it — `put_with_backoff`'s
/// `store.put()` delegates via `ObjectStoreExt`'s default impl, and the
/// manifest write and watermark CAS both call `put_opts` directly.
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    stats: Mutex<HashMap<&'static str, ClassStats>>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, stats: Mutex::new(HashMap::new()) }
    }

    fn snapshot(&self) -> HashMap<&'static str, ClassStats> {
        self.stats.lock().unwrap().clone()
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}
impl fmt::Debug for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({:?})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let class = classify(location.as_ref());
        let len = payload.content_length() as u64;
        {
            let mut stats = self.stats.lock().unwrap();
            let entry = stats.entry(class).or_default();
            entry.puts += 1;
            entry.bytes += len;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<ObjPath>>,
    ) -> BoxStream<'static, OsResult<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjPath, to: &ObjPath, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// --- output ---------------------------------------------------------------

fn class_json(stats: &HashMap<&'static str, ClassStats>) -> String {
    let parts: Vec<String> = CLASSES
        .iter()
        .map(|c| {
            let s = stats.get(c).copied().unwrap_or_default();
            format!("\"{c}\":{{\"puts\":{},\"bytes\":{}}}", s.puts, s.bytes)
        })
        .collect();
    format!("{{{}}}", parts.join(","))
}

fn delta(
    before: &HashMap<&'static str, ClassStats>,
    after: &HashMap<&'static str, ClassStats>,
) -> HashMap<&'static str, ClassStats> {
    CLASSES
        .iter()
        .map(|c| {
            let b = before.get(c).copied().unwrap_or_default();
            let a = after.get(c).copied().unwrap_or_default();
            (*c, ClassStats { puts: a.puts - b.puts, bytes: a.bytes - b.bytes })
        })
        .collect()
}

fn outcome_name(o: &StreamOutcome) -> &'static str {
    match o {
        StreamOutcome::Empty { .. } => "Empty",
        StreamOutcome::Streamed { .. } => "Streamed",
        StreamOutcome::Restarted { .. } => "Restarted",
        StreamOutcome::Shed { .. } => "Shed",
        StreamOutcome::Fenced { .. } => "Fenced",
    }
}

#[allow(clippy::too_many_arguments)]
fn print_step_line(
    writes_per_tail: usize,
    tail_call: u64,
    txns_so_far: usize,
    outcome: &StreamOutcome,
    frame_count: u64,
    before: &HashMap<&'static str, ClassStats>,
    after: &HashMap<&'static str, ClassStats>,
    step_started: Instant,
) {
    let step = delta(before, after);
    println!(
        "{{\"writes_per_tail\":{writes_per_tail},\"tail_call\":{tail_call},\"txns_so_far\":{txns_so_far},\"outcome\":\"{}\",\"frame_count\":{frame_count},\"puts\":{},\"elapsed_ms\":{}}}",
        outcome_name(outcome),
        class_json(&step),
        step_started.elapsed().as_millis(),
    );
}

fn print_config_summary_line(summary: &ConfigSummary, writes_per_day: f64, started: Instant) {
    println!(
        "{{\"config_summary\":true,\"writes_per_tail\":{},\"total_txns\":{},\"total_frames\":{},\"frames_per_txn\":{:.3},\"total_puts\":{},\"puts_per_txn\":{:.3},\"puts_by_class\":{},\"frame_puts_per_tail_call\":{:.3},\"unbatched_puts_per_txn\":{:.3},\"projected_usd_per_cell_month\":{:.6},\"unbatched_projected_usd_per_cell_month\":{:.6},\"total_elapsed_ms\":{}}}",
        summary.writes_per_tail,
        summary.total_txns,
        summary.total_frames,
        summary.frames_per_txn(),
        summary.total_puts(),
        summary.puts_per_txn(),
        class_json(&summary.puts),
        summary.frame_puts_per_tail_call(),
        summary.unbatched_puts_per_txn(),
        summary.projected_usd_per_cell_month(writes_per_day),
        summary.unbatched_projected_usd_per_cell_month(writes_per_day),
        started.elapsed().as_millis(),
    );
}

fn print_overall_summary(configs: &[ConfigSummary], writes_per_day: f64) {
    let avg_frames_per_txn: f64 =
        configs.iter().map(|c| c.frames_per_txn()).sum::<f64>() / configs.len() as f64;

    // Worst case for PUT overhead is the shortest tail interval (manifest +
    // watermark amortize over the fewest writes); best case is the longest.
    let shortest = configs.iter().min_by_key(|c| c.writes_per_tail).unwrap();
    let longest = configs.iter().max_by_key(|c| c.writes_per_tail).unwrap();

    // R761-F2 landed the batched layout, so the measured numbers ARE the
    // batched ones and the reduction is stated against what the pre-batching
    // layout would have cost on this same workload.
    let reduction_pct_shortest =
        (1.0 - shortest.puts_per_txn() / shortest.unbatched_puts_per_txn()) * 100.0;
    let reduction_pct_longest =
        (1.0 - longest.puts_per_txn() / longest.unbatched_puts_per_txn()) * 100.0;

    let materially = reduction_pct_longest > 25.0;

    println!(
        "{{\"summary\":true,\"avg_frames_per_write\":{avg_frames_per_txn:.3},\"puts_per_write\":{{\"writes_per_tail_{}\":{:.3},\"writes_per_tail_{}\":{:.3}}},\"unbatched_puts_per_write\":{{\"writes_per_tail_{}\":{:.3},\"writes_per_tail_{}\":{:.3}}},\"batching_reduction_pct\":{{\"writes_per_tail_{}\":{reduction_pct_shortest:.1},\"writes_per_tail_{}\":{reduction_pct_longest:.1}}},\"projected_usd_per_cell_month\":{{\"writes_per_tail_{}\":{:.6},\"writes_per_tail_{}\":{:.6}}},\"writes_per_day_assumed\":{writes_per_day},\"batching_materially_changes_bill\":{materially},\"answer\":\"frames/write ~= {avg_frames_per_txn:.2}; PUTs/write (batched, R761-F2) ranges {:.2} (writes_per_tail={}) to {:.2} (writes_per_tail={}); the pre-batching one-object-per-frame layout would cost {:.2} and {:.2} on the same workload, so batching cuts PUTs by {reduction_pct_shortest:.0}% and {reduction_pct_longest:.0}% -- {} a layout break\"}}",
        shortest.writes_per_tail, shortest.puts_per_txn(),
        longest.writes_per_tail, longest.puts_per_txn(),
        shortest.writes_per_tail, shortest.unbatched_puts_per_txn(),
        longest.writes_per_tail, longest.unbatched_puts_per_txn(),
        shortest.writes_per_tail,
        longest.writes_per_tail,
        shortest.writes_per_tail, shortest.projected_usd_per_cell_month(writes_per_day),
        longest.writes_per_tail, longest.projected_usd_per_cell_month(writes_per_day),
        shortest.puts_per_txn(), shortest.writes_per_tail,
        longest.puts_per_txn(), longest.writes_per_tail,
        shortest.unbatched_puts_per_txn(), longest.unbatched_puts_per_txn(),
        if materially { "worth" } else { "not worth" },
    );
}

// --- env parsing ------------------------------------------------------------

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_intervals(key: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(key) {
        Ok(v) => v.split(',').filter_map(|s| s.trim().parse().ok()).collect(),
        Err(_) => default.to_vec(),
    }
}
