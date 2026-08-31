//! `rss_harness` (example) — the §8 capacity measurement: open N Turso
//! DBs, attach a warm-applier connection to each, stream WAL through it, and
//! record the process RSS after every step.
//!
//! R574-T1 (phase P1 of the WAL→R2 streamer hardening relay). Backing docs:
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)
//! @arch:see(.yah/docs/working/W253-tenant-db-platform-architecture.md) §8
//! "Capacity & economics model": *"Required measurement (do before sizing):
//! open N Turso DBs, attach the applier, stream WAL, watch RSS. The slope =
//! per-replica memory cost; the intercept = engine/runtime baseline. This
//! single curve decides 100 vs 1000+ replicas/box and whether
//! warm-for-everyone is viable."*
//!
//! ## What one step does
//!
//! For `db_count` = 1..=`RSS_HARNESS_MAX_DBS`, open one more (source,
//! applier) pair:
//! 1. Seed a fresh local turso DB with `RSS_HARNESS_ROWS` rows.
//! 2. Tier-1a snapshot it ([`snapshot_and_upload`]) to a per-step in-memory
//!    object store — this crate's own R2-shaped sink, so the harness
//!    exercises the real code path, not a stand-in.
//! 3. Checkpoint, seed half as many rows again (fresh WAL frames), and tail
//!    them into the sink ([`tail_frames`]) via a [`CoreWalSeam`] on the
//!    source — the source connection is dropped immediately after streaming;
//!    only the *applier* side counts toward the curve, matching a real
//!    topology where the tenant master lives on a different box.
//! 4. Materialize the destination via the crate's own restore path
//!    ([`restore_latest_stream`]), then open a **new**, resident
//!    [`CoreWalSeam`] on it and push it into a `Vec` that is never drained —
//!    this is "attach applier": a standing connection representing a warm
//!    standby between tail calls, exactly as §5 describes it. `db_count`
//!    appliers are open and resident by the time step `db_count` is
//!    measured.
//! 5. Delete the source DB's files (it's not part of the standing cost) and
//!    shell out to `ps -o rss= -p <pid>` — portable across the macOS dev box
//!    and Linux prod nodes with zero new dependency (no `/proc` on Darwin,
//!    no `getrusage` binding in this crate's deps).
//!
//! Deliberately **not** a `#[test]`/bench target: like the
//! `turso-backup-snapshot` CLI bin, this wants to be a clean, killable process
//! (an OOM or FD exhaustion at DB #400 shouldn't take out `cargo test`), and
//! a human watching the curve live (or piping it to a file) is the point.
//!
//! No cache-trimming is applied to the applier connections here — that is
//! R574-F3's job ("warm applier … trimmed cache"). This harness measures the
//! *current*, untrimmed baseline so F3 has a real "before" curve to improve
//! on.
//!
//! ## Running
//!
//! ```text
//! cargo run -p turso-backup --release --example rss_harness
//! RSS_HARNESS_MAX_DBS=100 RSS_HARNESS_ROWS=200 cargo run -p turso-backup --release --example rss_harness
//! ```
//!
//! Env overrides (all optional):
//! - `RSS_HARNESS_MAX_DBS` (default `40`) — how many (source, applier) pairs
//!   to open.
//! - `RSS_HARNESS_ROWS` (default `40`) — rows seeded into the tier-1a base
//!   snapshot per DB; half that many again are seeded post-checkpoint to
//!   produce the streamed WAL frames.
//! - `RSS_HARNESS_WORKDIR` (default a fresh `$TMPDIR/turso-backup-rss-harness-<pid>`)
//!   — where the source/dest `.db` files live.
//!
//! ## Output
//!
//! NDJSON to stdout, one line per step:
//! `{"db_count":N,"rss_kb":X,"delta_kb":Y|null,"frames_replayed":F,"elapsed_ms":T,"total_elapsed_ms":TT}`
//! followed by a trailing summary line once the run completes:
//! `{"summary":true,"slope_kb_per_db":S,"intercept_kb":I,"samples":N}` — an
//! ordinary-least-squares fit over every recorded `(db_count, rss_kb)` point.

use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use turso_backup::snapshot::{snapshot_and_upload, BackupTarget, SnapshotOutcome};
use turso_backup::stream::{restore_latest_stream, tail_frames, CoreWalSeam, StreamConfig};

/// Matches the WAL page size the live-DB tests in `stream.rs` use; turso's
/// default page size for a freshly created database.
const PAGE_SIZE: usize = 4096;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let max_dbs = env_usize("RSS_HARNESS_MAX_DBS", 40);
    let rows_per_db = env_usize("RSS_HARNESS_ROWS", 40).max(2);
    let workdir = std::env::var("RSS_HARNESS_WORKDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("turso-backup-rss-harness-{}", std::process::id()))
        });
    std::fs::create_dir_all(&workdir)
        .with_context(|| format!("creating workdir {}", workdir.display()))?;

    // Appliers accumulate here and are never dropped mid-run: RSS after step
    // N reflects N concurrently-open, attached warm-standby connections.
    let mut appliers: Vec<CoreWalSeam> = Vec::with_capacity(max_dbs);
    let mut samples: Vec<(f64, f64)> = Vec::with_capacity(max_dbs);
    let mut prev_rss_kb: Option<i64> = None;
    let started = Instant::now();

    for i in 1..=max_dbs {
        let step_started = Instant::now();
        let src_path = workdir.join(format!("src-{i}.db"));
        let dest_path = workdir.join(format!("dest-{i}.db"));
        let src_path = src_path.to_str().context("src path is not valid UTF-8")?;
        let dest_path = dest_path.to_str().context("dest path is not valid UTF-8")?;

        seed_rows(src_path, 0, rows_per_db as i64)
            .await
            .with_context(|| format!("seeding base rows for db #{i}"))?;

        // Fresh in-memory sink per step, scoped to this loop body only — it
        // drops at the end of the iteration, so its bytes never accumulate
        // in the RSS curve we're trying to attribute to the applier side.
        let target = BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: format!("tenant-{i}"),
        };
        let base_key = match snapshot_and_upload(src_path, &target)
            .await
            .with_context(|| format!("snapshot_and_upload for db #{i}"))?
        {
            SnapshotOutcome::Uploaded { key, .. } => key,
            other => anyhow::bail!("db #{i}: expected a fresh Uploaded snapshot, got {other:?}"),
        };

        checkpoint_truncate(src_path)
            .await
            .with_context(|| format!("checkpointing db #{i}"))?;
        seed_rows(src_path, 1_000_000 * i as i64, (rows_per_db / 2).max(1) as i64)
            .await
            .with_context(|| format!("seeding post-checkpoint rows for db #{i}"))?;

        {
            // Source connection is scoped to this block: only the applier
            // side is meant to represent standing per-replica cost.
            let seam = CoreWalSeam::open(src_path)
                .with_context(|| format!("opening source WAL seam for db #{i}"))?;
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                page_size: PAGE_SIZE,
                backpressure: turso_backup::backpressure::BackpressureConfig::default(),
                rpo_target: None,
                epoch: 0,
                owner: None,
                pointer_generation: 0,
            };
            tail_frames(&seam, &target, &cfg)
                .await
                .with_context(|| format!("tail_frames for db #{i}"))?;
        }

        // "Attach applier": materialize the destination through the crate's
        // own restore path, then open a second, resident seam on it and
        // hold it for the rest of the run.
        let outcome = restore_latest_stream(&target, dest_path)
            .await
            .with_context(|| format!("restore_latest_stream for db #{i}"))?;
        let applier = CoreWalSeam::open(dest_path)
            .with_context(|| format!("attaching applier for db #{i}"))?;
        appliers.push(applier);

        // The source DB isn't part of the standing cost once its frames are
        // streamed — remove it so disk pressure doesn't confound the RSS
        // reading on long runs.
        for sfx in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{src_path}{sfx}"));
        }

        let rss_kb = read_rss_kb().context("reading process RSS via `ps`")?;
        let delta_kb = prev_rss_kb.map(|p| rss_kb - p);
        prev_rss_kb = Some(rss_kb);
        samples.push((i as f64, rss_kb as f64));

        println!(
            "{{\"db_count\":{i},\"rss_kb\":{rss_kb},\"delta_kb\":{},\"frames_replayed\":{},\"elapsed_ms\":{},\"total_elapsed_ms\":{}}}",
            delta_kb.map(|d| d.to_string()).unwrap_or_else(|| "null".to_string()),
            outcome.frames_replayed,
            step_started.elapsed().as_millis(),
            started.elapsed().as_millis(),
        );
    }

    if let Some((slope, intercept)) = linear_fit(&samples) {
        println!(
            "{{\"summary\":true,\"slope_kb_per_db\":{slope:.3},\"intercept_kb\":{intercept:.3},\"samples\":{}}}",
            samples.len()
        );
    }

    // Close every applier connection before removing its file.
    drop(appliers);
    for i in 1..=max_dbs {
        for sfx in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(workdir.join(format!("dest-{i}.db{sfx}")));
        }
    }
    let _ = std::fs::remove_dir(&workdir);

    Ok(())
}

async fn seed_rows(path: &str, start: i64, count: i64) -> Result<()> {
    let db = turso::Builder::new_local(path)
        .build()
        .await
        .with_context(|| format!("opening {path}"))?;
    let conn = db.connect().with_context(|| format!("connecting to {path}"))?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)",
        (),
    )
    .await
    .context("create table")?;
    conn.execute("BEGIN", ()).await.context("begin")?;
    for i in start..start + count {
        conn.execute("INSERT INTO t (id, v) VALUES (?, ?)", (i, format!("v{i}")))
            .await
            .with_context(|| format!("insert row {i}"))?;
    }
    conn.execute("COMMIT", ()).await.context("commit")?;
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

/// Read this process's resident set size in KB by shelling out to `ps`.
/// Portable across the macOS dev box and Linux prod nodes (`-o rss=` is
/// specified in KB on both BSD/Darwin and GNU `ps`) without pulling in a
/// `/proc`-only or `libc`/`getrusage` dependency for one measurement.
fn read_rss_kb() -> Result<i64> {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .context("spawning `ps`")?;
    anyhow::ensure!(out.status.success(), "`ps` exited with {:?}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    text.trim()
        .parse::<i64>()
        .with_context(|| format!("parsing `ps -o rss=` output {text:?}"))
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Ordinary-least-squares fit of `y = slope * x + intercept` over
/// `(db_count, rss_kb)` points. `None` if there are fewer than two points or
/// every point shares the same `x` (a degenerate, vertical "line").
fn linear_fit(points: &[(f64, f64)]) -> Option<(f64, f64)> {
    let n = points.len() as f64;
    if points.len() < 2 {
        return None;
    }
    let sum_x: f64 = points.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let sum_xx: f64 = points.iter().map(|(x, _)| x * x).sum();
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        return None;
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;
    Some((slope, intercept))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_fit_recovers_a_known_line() {
        // y = 2x + 10, no noise.
        let points: Vec<(f64, f64)> = (1..=10).map(|x| (x as f64, 2.0 * x as f64 + 10.0)).collect();
        let (slope, intercept) = linear_fit(&points).unwrap();
        assert!((slope - 2.0).abs() < 1e-9, "slope = {slope}");
        assert!((intercept - 10.0).abs() < 1e-9, "intercept = {intercept}");
    }

    #[test]
    fn linear_fit_tolerates_noise_and_reports_a_reasonable_slope() {
        // Roughly y = 5x + 100, +/- small jitter -- the shape a real RSS
        // curve takes (per-replica cost with some measurement noise).
        let points = vec![
            (1.0, 105.0),
            (2.0, 109.0),
            (3.0, 116.0),
            (4.0, 119.0),
            (5.0, 126.0),
        ];
        let (slope, intercept) = linear_fit(&points).unwrap();
        assert!((4.0..6.0).contains(&slope), "slope = {slope}");
        assert!((90.0..110.0).contains(&intercept), "intercept = {intercept}");
    }

    #[test]
    fn linear_fit_none_below_two_points() {
        assert_eq!(linear_fit(&[]), None);
        assert_eq!(linear_fit(&[(1.0, 1.0)]), None);
    }

    #[test]
    fn linear_fit_none_for_degenerate_vertical_points() {
        // Every point shares the same x -- no well-defined slope.
        assert_eq!(linear_fit(&[(3.0, 1.0), (3.0, 5.0), (3.0, 9.0)]), None);
    }

    #[test]
    fn env_usize_uses_default_when_unset() {
        assert_eq!(env_usize("RSS_HARNESS_TEST_VAR_NOT_SET", 7), 7);
    }

    #[test]
    fn env_usize_parses_a_set_value() {
        // SAFETY-equivalent: `env::set_var` is fine in a single-threaded
        // `#[test]` that doesn't race other env mutators in this file.
        std::env::set_var("RSS_HARNESS_TEST_VAR_SET", "99");
        assert_eq!(env_usize("RSS_HARNESS_TEST_VAR_SET", 7), 99);
        std::env::remove_var("RSS_HARNESS_TEST_VAR_SET");
    }

    /// Smoke test: `ps -o rss=` on our own live process should succeed and
    /// return a plausible, positive resident-set size on both macOS
    /// (dev box) and Linux (CI/prod) `ps`.
    #[test]
    fn read_rss_kb_returns_a_positive_value() {
        let rss = read_rss_kb().unwrap();
        assert!(rss > 0, "expected a positive RSS reading, got {rss}");
    }
}
