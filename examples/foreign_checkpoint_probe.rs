//! R858-S6 — measure what a turso-backup stream does when the WAL is written
//! *and checkpointed* by an upstream C SQLite process we neither trigger nor
//! observe (headscale's Go driver is the motivating case).
//!
//! The cross-engine WAL *format* question was answered before this harness
//! existed: `raw_consistent_copy_live` parses and replays frames written by
//! upstream C SQLite. What was never exercised is the **checkpoint race**.
//! `tail_frames` resumes from a `(checkpoint_seq, max_frame)` watermark, and a
//! foreign writer can fold the WAL into the main file between two tail calls.
//! `tail_frames` handles a `checkpoint_seq` advance by treating it as a restart
//! and re-uploading `1..=max_frame`. Whether that detection actually fires
//! against a foreign checkpointer is what this measures.
//!
//! ## Two traps this harness exists to avoid
//!
//! 1. **The writer must be the system `sqlite3` binary.** Driving the WAL with
//!    turso and reading it back with turso proves nothing about interop.
//! 2. **SQLite checkpoints on a clean close.** A `sqlite3 db "INSERT ..."`
//!    one-liner leaves a zero-byte WAL and every probe built on it is vacuous.
//!    So every writer that must leave frames behind is SIGKILLed after a
//!    sentinel round-trip proves its statements ran ([`sqlite3_kill9`]) — and
//!    every *read* of a live source goes through [`rows_via_copy`], which
//!    snapshots the `db`/`-wal`/`-shm` trio first, because reading the source
//!    directly would fold the WAL and destroy the condition under test.
//!
//! ## Two foreign-checkpoint regimes, which behave differently
//!
//! Probe A and probe E disagree, and the disagreement is the finding rather
//! than a bug in either:
//!
//! - **Process restart** (probe A): when the *last* connection to a DB closes,
//!   SQLite checkpoints and **deletes the WAL file**. The next writer creates a
//!   fresh WAL whose checkpoint-sequence is back to `0` with a brand-new random
//!   salt. So `checkpoint_seq` does NOT advance across this — it resets.
//! - **In-process autocheckpoint** (probe E): a long-lived connection folding
//!   its own WAL at the default 1000-page threshold reuses the WAL file and
//!   **does** increment `checkpoint_seq` (and `salt1`) monotonically.
//!
//! `tail_frames` keys its restart detection on `checkpoint_seq` alone, so it is
//! correct in the second regime and blind in the first. The WAL **salt** moves
//! in both, which is why probes here print it alongside.
//!
//! ## Probes
//!
//! - **A** — the process-restart regime: is a foreign checkpoint whose writer
//!   then closes visible to turso's watermark? Cross-checked against the WAL
//!   header's own checkpoint-sequence and salt fields ([`wal_hdr`]), read
//!   straight off disk, so a negative result is attributable to the engine
//!   rather than to the seam.
//! - **B** — tail, foreign fold, tail, restore, where the *new* WAL is
//!   SHORTER than the old watermark. Failure mode: a stalled stream.
//! - **F** — same, but the new WAL grows PAST the old watermark. Failure mode:
//!   frames from two different WAL generations spliced into one chain.
//! - **C** — hydration (`raw_consistent_copy_live`) against a foreign-written
//!   uncheckpointed WAL, restored and read back with `sqlite3`.
//! - **D** — liveness: can a foreign `sqlite3` writer write, and checkpoint,
//!   while a `CoreWalSeam` is open on the same file? Watchdogged, because the
//!   first run of this probe hung for ten minutes.
//! - **G** — the prior question D assumes away: can turso open the file *at
//!   all* while a foreign connection merely holds it open? This is headscale's
//!   actual posture and it decides whether streaming is possible at all. G2r and
//!   G3c are R858-B18's answer, each differing from its predecessor in exactly
//!   one variable (`OpenFlags::ReadOnly` instead of the write default; the flag
//!   instead of `LIMBO_DISABLE_FILE_LOCK=1`).
//! - **H** — R858-B18: does the validation actually catch a torn read, or hand
//!   back a plausible wrong image? Runs a foreign writer CONCURRENTLY with the
//!   copy, in two regimes, because one write rate can only prove half of it.
//! - **S** — R858-B18: the same question for the *snapshot* tier, which reaches
//!   the source through `turso::Builder` and was blocked for the same reason.
//! - **E** — cost: how many transactions before SQLite's *default*
//!   autocheckpoint folds the WAL, i.e. how often the restart path fires.
//!
//! Run: `cargo run -p turso-backup --example foreign_checkpoint_probe`
//! Pass probe letters to select (`... -- a d`). Override the writer with
//! `PROBE_SQLITE3=/path/to/sqlite3`; the probe prints the version it used, and
//! a non-upstream binary invalidates the whole run.

use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso_backup::backpressure::BackpressureConfig;
use turso_backup::snapshot::{snapshot_and_upload, upload_base_snapshot, BackupTarget};
use turso_backup::stream::{
    raw_consistent_copy_live, restore_latest_stream, tail_frames, CoreWalSeam, SourceFingerprint,
    StreamConfig, StreamOutcome, WalSeam, Watermark,
};

const PAGE_SIZE: usize = 4096;

// ── foreign-writer plumbing ──────────────────────────────────────────────────

fn sqlite3_bin() -> String {
    std::env::var("PROBE_SQLITE3").unwrap_or_else(|_| "/usr/bin/sqlite3".to_string())
}

/// What a watchdogged foreign-writer run did. Fields are reported through
/// `Debug` in the probe output, not read programmatically.
#[derive(Debug)]
#[allow(dead_code)]
enum ForeignRun {
    /// Statements executed; the child was SIGKILLed before it could checkpoint.
    Ran { elapsed: Duration },
    /// The child never reached the sentinel before the deadline — it was
    /// blocked, not merely erroring (an error still lets the sentinel print).
    Blocked { waited: Duration, stderr: String },
    /// The child exited on its own before the sentinel.
    Died { stderr: String },
}

/// Run `sql` through a `sqlite3` child and **SIGKILL it** once a sentinel
/// `SELECT` proves the statements executed. The kill is the point: a clean
/// close checkpoints the WAL, which would erase the very condition every probe
/// here needs (committed frames still sitting in the WAL).
///
/// `deadline` bounds the sentinel wait. Without it this function hangs forever
/// when the child is blocked on a lock — which is exactly what probe D found.
fn sqlite3_kill9(db: &str, sql: &str, deadline: Duration) -> Result<ForeignRun> {
    let started = Instant::now();
    let mut child = Command::new(sqlite3_bin())
        .arg(db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning {}", sqlite3_bin()))?;
    let pid = child.id();
    let mut sin = child.stdin.take().expect("piped stdin");
    let sout = child.stdout.take().expect("piped stdout");

    // Watchdog: SIGKILL by pid after the deadline. `read_line` below has no
    // timeout of its own, so this thread is the only thing that can unblock it.
    let wd = std::thread::spawn(move || {
        std::thread::sleep(deadline);
        let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
    });

    writeln!(sin, "{sql}")?;
    writeln!(sin, "SELECT 'PROBE-SENTINEL';")?;
    sin.flush()?;

    let mut rd = BufReader::new(sout);
    let mut line = String::new();
    let mut saw_sentinel = false;
    loop {
        line.clear();
        // EOF here means the watchdog killed the child, or it exited on its own.
        if rd.read_line(&mut line)? == 0 {
            break;
        }
        if line.contains("PROBE-SENTINEL") {
            saw_sentinel = true;
            break;
        }
    }
    let elapsed = started.elapsed();
    // stdin is still open, so a child that reached the sentinel is parked
    // waiting for more input and has had no chance to checkpoint on close.
    let _ = child.kill();
    let _ = child.wait();
    drop(sin);
    let mut stderr = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut stderr);
    }
    let _ = wd.join();

    Ok(if saw_sentinel {
        ForeignRun::Ran { elapsed }
    } else if elapsed >= deadline.saturating_sub(Duration::from_millis(250)) {
        ForeignRun::Blocked { waited: elapsed, stderr: stderr.trim().to_string() }
    } else {
        ForeignRun::Died { stderr: stderr.trim().to_string() }
    })
}

/// Run `sql` to completion in a `sqlite3` child that closes cleanly — which
/// means it **also checkpoints on exit**. Only for setup and for reads of
/// throwaway copies.
fn sqlite3_clean(db: &str, sql: &str) -> Result<String> {
    let out = Command::new(sqlite3_bin())
        .arg(db)
        .arg(sql)
        .output()
        .with_context(|| format!("running {} {db}", sqlite3_bin()))?;
    anyhow::ensure!(
        out.status.success(),
        "sqlite3 failed on {sql:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn row_count(db: &str) -> Result<i64> {
    Ok(sqlite3_clean(db, "SELECT count(*) FROM t;")?.parse()?)
}

/// Row count of a LIVE source, without perturbing it. Reading the source
/// directly with `sqlite3` folds its WAL on close — which silently destroys the
/// uncheckpointed-frames condition every probe here depends on. So copy the
/// `db`/`-wal`/`-shm` trio and count in the copy.
fn rows_via_copy(db: &str, tag: &str) -> Result<i64> {
    let c = format!("{db}.rd-{tag}");
    for sfx in ["", "-wal", "-shm"] {
        let _ = std::fs::copy(format!("{db}{sfx}"), format!("{c}{sfx}"));
    }
    let n = row_count(&c);
    for sfx in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{c}{sfx}"));
    }
    n
}

/// The 32-byte WAL header, read straight off disk. Ground truth independent of
/// any engine: `ckpt_seq` is the field `tail_frames`'s restart detection is
/// ultimately asking about, and `salt1`/`salt2` are what SQLite actually
/// re-rolls on every WAL reset.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct WalHdr {
    pgsz: u32,
    ckpt_seq: u32,
    salt1: u32,
    salt2: u32,
}

fn wal_hdr(db: &str) -> Option<WalHdr> {
    let b = std::fs::read(format!("{db}-wal")).ok()?;
    if b.len() < 32 {
        return None;
    }
    let be = |o: usize| u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
    Some(WalHdr { pgsz: be(8), ckpt_seq: be(12), salt1: be(16), salt2: be(20) })
}

fn hdr_str(db: &str) -> String {
    match wal_hdr(db) {
        Some(h) => format!(
            "pgsz {} ckpt_seq {} salt {:08x}/{:08x}",
            h.pgsz, h.ckpt_seq, h.salt1, h.salt2
        ),
        None => "<no WAL header (file absent or truncated)>".to_string(),
    }
}

fn file_len(p: &str) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

/// Watermark as turso sees it right now. Opens and drops a seam per call so we
/// never hold WAL ownership across a foreign writer's turn.
fn turso_watermark(db: &str) -> Result<Watermark> {
    CoreWalSeam::open(db)?.wal_state()
}

fn state_line(db: &str, tag: &str) -> Result<String> {
    let w = turso_watermark(db)?;
    Ok(format!(
        "{tag}: turso (seq {}, max_frame {}) | wal-hdr {} | wal {}B main {}B",
        w.checkpoint_seq,
        w.last_frame,
        hdr_str(db),
        file_len(&format!("{db}-wal")),
        file_len(db)
    ))
}

struct Probe {
    dir: std::path::PathBuf,
}

impl Probe {
    fn new(tag: &str) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!(
            "turso-backup-foreign-probe-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }
    fn path(&self, name: &str) -> String {
        self.dir.join(name).to_str().unwrap().to_string()
    }
}

fn cfg(base_snapshot_key: &str) -> StreamConfig<'_> {
    StreamConfig {
        base_snapshot_key,
        page_size: PAGE_SIZE,
        backpressure: BackpressureConfig::default(),
        rpo_target: None,
        epoch: 0,
        owner: Some("R858-S6-probe"),
        pointer_generation: 0,
    }
}

fn target() -> BackupTarget {
    BackupTarget { store: Arc::new(InMemory::new()), prefix: "probe".into() }
}

/// Seed a fresh WAL-mode DB with `n` rows through the foreign writer, closing
/// cleanly so the WAL starts folded and the base snapshot is unambiguous.
fn seed_clean(db: &str, n: i64) -> Result<()> {
    let mut sql = String::from(
        "PRAGMA journal_mode=WAL;\nCREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT);\nBEGIN;\n",
    );
    for i in 0..n {
        sql.push_str(&format!("INSERT INTO t(id,v) VALUES({i},'v{i}');\n"));
    }
    sql.push_str("COMMIT;\n");
    sqlite3_clean(db, &sql)?;
    Ok(())
}

/// Append `n` rows in `txns` separate transactions through the foreign writer,
/// then SIGKILL it — leaving the frames committed-but-uncheckpointed.
/// `pad` bytes per row, to control how many WAL pages the writes dirty.
fn append_dirty(db: &str, start: i64, n: i64, txns: i64, pad: usize) -> Result<ForeignRun> {
    let mut sql = String::new();
    let per = (n / txns).max(1);
    let mut i = start;
    while i < start + n {
        sql.push_str("BEGIN;\n");
        for j in i..(i + per).min(start + n) {
            sql.push_str(&format!("INSERT INTO t(id,v) VALUES({j},'{}');\n", "x".repeat(pad)));
        }
        sql.push_str("COMMIT;\n");
        i += per;
    }
    sqlite3_kill9(db, &sql, Duration::from_secs(30))
}

// ── probes ───────────────────────────────────────────────────────────────────

/// A — is a foreign checkpoint visible to turso as a `checkpoint_seq` advance?
fn probe_a() -> Result<()> {
    println!("\n=== PROBE A: does turso observe a foreign checkpoint? (process-restart regime) ===");
    println!("NOTE: the checkpointing connection here CLOSES, which makes SQLite delete the WAL.");
    println!("      The mode label is therefore not the variable under test — see probe E for the");
    println!("      in-process autocheckpoint regime, where checkpoint_seq DOES advance.");
    for mode in ["TRUNCATE", "RESTART", "FULL"] {
        let p = Probe::new(&format!("a-{mode}"))?;
        let db = p.path("a.db");
        seed_clean(&db, 3)?;
        append_dirty(&db, 100, 3, 1, 8)?;
        let before = turso_watermark(&db)?;
        let hb = wal_hdr(&db);
        println!("{}", state_line(&db, &format!("A1[{mode}] pre-fold "))?);

        let r = sqlite3_clean(&db, &format!("PRAGMA wal_checkpoint({mode});"))?;
        println!("A2[{mode}] foreign `wal_checkpoint({mode})` returned {r}");
        println!("{}", state_line(&db, &format!("A3[{mode}] post-fold"))?);

        append_dirty(&db, 200, 3, 1, 8)?;
        let after = turso_watermark(&db)?;
        let ha = wal_hdr(&db);
        println!("{}", state_line(&db, &format!("A4[{mode}] post-fold+write"))?);
        println!(
            "A[{mode}] VERDICT: turso checkpoint_seq {} -> {} (advanced: {}) | on-disk ckpt_seq {:?} -> {:?} | salt changed: {}",
            before.checkpoint_seq,
            after.checkpoint_seq,
            after.checkpoint_seq > before.checkpoint_seq,
            hb.map(|h| h.ckpt_seq),
            ha.map(|h| h.ckpt_seq),
            hb.map(|h| (h.salt1, h.salt2)) != ha.map(|h| (h.salt1, h.salt2)),
        );
    }
    Ok(())
}

/// Shared driver for B (new WAL shorter than the watermark) and F (longer).
///
/// `orphan_early`: immediately after the fold, write a SECOND table whose pages
/// no later frame touches again. Without this, F's dropped prefix happens to be
/// harmless — the same leaf page keeps getting rewritten, so the last frame
/// wins and the image looks right. That is luck, not a guarantee, and this flag
/// removes the luck.
async fn tail_across_fold(
    tag: &str,
    pre_fold_rows: i64,
    pre_fold_txns: i64,
    post_fold_rows: i64,
    post_fold_txns: i64,
    orphan_early: bool,
) -> Result<()> {
    let p = Probe::new(tag)?;
    let db = p.path("s.db");
    let tgt = target();

    seed_clean(&db, 3)?;
    // Base snapshot from the page-offset-stable live copy, not VACUUM INTO:
    // frames are page images keyed by page number, so the base must carry the
    // source's own page layout.
    let base = upload_base_snapshot(&tgt, &raw_consistent_copy_live(&db, PAGE_SIZE).await?).await?;

    append_dirty(&db, 100, pre_fold_rows, pre_fold_txns, 400)?;
    println!("{}", state_line(&db, "  pre-tail#1")?);
    let o1 = tail_frames(&CoreWalSeam::open(&db)?, &tgt, &cfg(&base)).await?;
    println!("  tail#1 -> {}", short(&o1));

    // The foreign process folds the WAL between our two tail calls, then keeps
    // writing. This is exactly the race the ticket names.
    sqlite3_clean(&db, "PRAGMA wal_checkpoint(TRUNCATE);")?;
    if orphan_early {
        // Lands in the frame range tail#2 will skip, and nothing later rewrites
        // these pages.
        let mut sql =
            String::from("CREATE TABLE IF NOT EXISTS u(id INTEGER PRIMARY KEY, v TEXT);\nBEGIN;\n");
        for i in 0..40 {
            sql.push_str(&format!("INSERT INTO u(id,v) VALUES({i},'{}');\n", "u".repeat(400)));
        }
        sql.push_str("COMMIT;\n");
        sqlite3_kill9(&db, &sql, Duration::from_secs(30))?;
        println!("{}", state_line(&db, "  post-fold, after the orphan-prefix write")?);
    }
    append_dirty(&db, 200, post_fold_rows, post_fold_txns, 400)?;
    println!("{}", state_line(&db, "  post-fold")?);

    let o2 = tail_frames(&CoreWalSeam::open(&db)?, &tgt, &cfg(&base)).await?;
    println!("  tail#2 (across the fold) -> {}", short(&o2));
    println!(
        "  restart signalled? {}",
        matches!(o2, StreamOutcome::Restarted { .. })
    );

    let src = rows_via_copy(&db, "final")?;
    let src_u = if orphan_early {
        sqlite3_clean(&p.path("s.db.rd-u"), "SELECT 1").ok();
        let c = format!("{db}.rd-u");
        for sfx in ["", "-wal", "-shm"] {
            let _ = std::fs::copy(format!("{db}{sfx}"), format!("{c}{sfx}"));
        }
        let n = sqlite3_clean(&c, "SELECT count(*) FROM u;").ok();
        for sfx in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{c}{sfx}"));
        }
        n
    } else {
        None
    };

    let dest = p.path("restored.db");
    match restore_latest_stream(&tgt, &dest).await {
        Ok(out) => {
            let integrity = sqlite3_clean(&dest, "PRAGMA integrity_check;");
            let n = row_count(&dest);
            let n_u = if orphan_early {
                sqlite3_clean(&dest, "SELECT count(*) FROM u;").ok()
            } else {
                None
            };
            println!(
                "  restore SUCCEEDED (frames_replayed {}, generations {}); integrity_check {:?}; t rows {:?} vs source {src}; u rows {:?} vs source {:?}",
                out.frames_replayed, out.generation_count, integrity, n, n_u, src_u
            );
            let t_ok = matches!(n, Ok(n) if n == src);
            let u_ok = !orphan_early || (n_u.is_some() && n_u == src_u);
            match (&n, t_ok && u_ok && matches!(integrity.as_deref(), Ok("ok"))) {
                (Ok(_), true) => println!("  VERDICT[{tag}]: CORRECT"),
                (Ok(n), false) => println!(
                    "  VERDICT[{tag}]: SILENT WRONG IMAGE — restored t={n}/{src}, u={n_u:?}/{src_u:?}, integrity {integrity:?}. No error was raised anywhere."
                ),
                (Err(e), _) => {
                    println!("  VERDICT[{tag}]: restored image UNREADABLE by upstream sqlite3: {e}")
                }
            }
        }
        Err(e) => println!("  restore REFUSED (fail-loud): {e:#}\n  VERDICT[{tag}]: safe-but-stalled"),
    }
    Ok(())
}

fn short(o: &StreamOutcome) -> String {
    match o {
        StreamOutcome::Empty { watermark, .. } => format!(
            "Empty (seq {}, last_frame {})",
            watermark.checkpoint_seq, watermark.last_frame
        ),
        StreamOutcome::Streamed { first_frame, last_frame, checkpoint_seq, frame_count, .. } => {
            format!("Streamed seq {checkpoint_seq} frames {first_frame}..={last_frame} ({frame_count})")
        }
        StreamOutcome::Restarted {
            previous_generation, new_generation, first_frame, last_frame, ..
        } => format!(
            "Restarted gen (seq {}, salt {})->(seq {}, salt {}) frames {first_frame}..={last_frame}",
            previous_generation.checkpoint_seq,
            previous_generation.salt.map_or("<unknown>".to_string(), |s| s.to_string()),
            new_generation.checkpoint_seq,
            new_generation.salt.map_or("<unknown>".to_string(), |s| s.to_string()),
        ),
        other => format!("{other:?}"),
    }
}

/// C — hydration: the page-stable live copy of a foreign-written DB.
async fn probe_c() -> Result<()> {
    println!("\n=== PROBE C: hydration via raw_consistent_copy_live ===");
    let p = Probe::new("c")?;
    let db = p.path("c.db");
    seed_clean(&db, 5)?;
    append_dirty(&db, 100, 4, 2, 400)?;
    println!("{}", state_line(&db, "C1 foreign source")?);
    let src = rows_via_copy(&db, "pre")?;

    let img = raw_consistent_copy_live(&db, PAGE_SIZE).await?;
    let out = p.path("c-image.db");
    std::fs::write(&out, &img)?;
    let integrity = sqlite3_clean(&out, "PRAGMA integrity_check;");
    let rows = row_count(&out);
    println!(
        "C2 image {} bytes; header ok {}; NO -wal sidecar written: {}; integrity_check {:?}; rows {:?} vs live source {src}",
        img.len(),
        img.starts_with(b"SQLite format 3\0"),
        !std::path::Path::new(&format!("{out}-wal")).exists(),
        integrity,
        rows
    );
    println!(
        "C VERDICT: {}",
        if rows.as_ref().ok() == Some(&src) && matches!(integrity.as_deref(), Ok("ok")) {
            "CORRECT — a foreign-written uncheckpointed WAL hydrates to a vanilla-SQLite image upstream sqlite3 reads"
        } else {
            "WRONG"
        }
    );
    println!("{}", state_line(&db, "C3 source after the copy (must be unperturbed)")?);
    Ok(())
}

/// D — liveness under a held seam. Watchdogged: the first run of this probe
/// hung for ten minutes, which is itself the answer.
fn probe_d() -> Result<()> {
    println!("\n=== PROBE D: foreign writer liveness while a CoreWalSeam is open ===");
    let p = Probe::new("d")?;
    let db = p.path("d.db");
    seed_clean(&db, 3)?;

    // Control: the same write with NO seam held, so a failure below is
    // attributable to the seam and not to the harness.
    let ctl = append_dirty(&db, 50, 3, 1, 8)?;
    println!("D0 control (no seam held): {ctl:?}");

    let seam = CoreWalSeam::open(&db)?;
    let before = seam.wal_state()?;
    println!("D1 seam open; wal_state = (seq {}, max_frame {})", before.checkpoint_seq, before.last_frame);

    let w = sqlite3_kill9(
        &db,
        "PRAGMA busy_timeout=2000;\nBEGIN IMMEDIATE;\nINSERT INTO t(id,v) VALUES(999,'x');\nCOMMIT;",
        Duration::from_secs(10),
    )?;
    println!("D2 foreign WRITE while seam held: {w:?}");

    let started = Instant::now();
    let ck = Command::new(sqlite3_bin())
        .arg(&db)
        .arg("PRAGMA busy_timeout=2000; PRAGMA wal_checkpoint(TRUNCATE);")
        .output()?;
    println!(
        "D3 foreign CHECKPOINT while seam held after {:?}: status {} stdout {:?} stderr {:?}",
        started.elapsed(),
        ck.status,
        String::from_utf8_lossy(&ck.stdout).trim(),
        String::from_utf8_lossy(&ck.stderr).trim()
    );

    let after = seam.wal_state()?;
    println!(
        "D4 the SAME never-reopened seam now reports (seq {}, max_frame {}) — it {} the foreign activity",
        after.checkpoint_seq,
        after.last_frame,
        if after == before { "does NOT see" } else { "sees" }
    );
    drop(seam);
    println!("{}", state_line(&db, "D5 after dropping the seam")?);
    Ok(())
}

/// G — mutual exclusion. Not a race but a hard lock: can turso open the file at
/// all while a foreign connection merely holds it OPEN (idle, no transaction)?
/// This is headscale's actual posture — one long-lived connection for the
/// process lifetime — so it decides whether streaming is possible at all.
async fn probe_g() -> Result<()> {
    println!("\n=== PROBE G: can turso open a DB a foreign process merely holds open? ===");
    let p = Probe::new("g")?;
    let db = p.path("g.db");
    seed_clean(&db, 3)?;

    // Foreign connection open and IDLE — no transaction, nothing written since.
    let mut child = Command::new(sqlite3_bin())
        .arg(&db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut sin = child.stdin.take().expect("piped stdin");
    let sout = child.stdout.take().expect("piped stdout");
    let mut rd = BufReader::new(sout);
    // Force the connection to actually attach to the file.
    writeln!(sin, "SELECT count(*) FROM t;\nSELECT 'PROBE-SENTINEL';")?;
    sin.flush()?;
    let mut line = String::new();
    loop {
        line.clear();
        anyhow::ensure!(rd.read_line(&mut line)? > 0, "foreign holder died early");
        if line.contains("PROBE-SENTINEL") {
            break;
        }
    }
    println!("G1 foreign sqlite3 connection is open and idle on the DB");

    match CoreWalSeam::open(&db) {
        Ok(s) => println!(
            "G2 CoreWalSeam::open SUCCEEDED alongside it -> {:?}",
            s.wal_state()?
        ),
        Err(e) => println!("G2 CoreWalSeam::open REFUSED: {e:#}"),
    }
    // R858-B18: the same open differing in ONE variable — OpenFlags::ReadOnly,
    // no env var. If this succeeds where G2 was refused, the lock is a
    // per-handle guard and the backup path can decline to take it.
    match CoreWalSeam::open_reader(&db) {
        Ok(s) => println!(
            "G2r CoreWalSeam::open_reader (OpenFlags::ReadOnly, no env var) SUCCEEDED alongside it -> {:?}",
            s.wal_state()?
        ),
        Err(e) => println!("G2r CoreWalSeam::open_reader REFUSED: {e:#}"),
    }
    match raw_consistent_copy_live(&db, PAGE_SIZE).await {
        Ok(img) => println!("G3 raw_consistent_copy_live SUCCEEDED ({} bytes)", img.len()),
        Err(e) => println!("G3 raw_consistent_copy_live REFUSED: {e:#}"),
    }

    // turso_core has an escape hatch (io/common.rs: LIMBO_DISABLE_FILE_LOCK,
    // plus OpenFlags::NoLock / ReadOnly). Does bypassing the lock produce a
    // CORRECT copy, or just an unguarded one? The distinction decides whether
    // the follow-on work is "pass a flag" or "the engines share no locking
    // protocol". Have the holder write something first, so there is state that
    // only a genuinely-coordinated reader would see.
    writeln!(sin, "INSERT INTO t(id,v) VALUES(4242,'after-hold');\nSELECT 'PROBE-SENTINEL';")?;
    sin.flush()?;
    loop {
        line.clear();
        anyhow::ensure!(rd.read_line(&mut line)? > 0, "foreign holder died mid-write");
        if line.contains("PROBE-SENTINEL") {
            break;
        }
    }
    // R858-B18 G3c — the measurement this ticket's fix rests on. Differs from
    // G3b below in EXACTLY ONE variable: the per-handle OpenFlags::ReadOnly
    // (via CoreWalSeam::open_reader, which raw_consistent_copy_live now uses)
    // instead of the process-wide LIMBO_DISABLE_FILE_LOCK=1. Same source state,
    // same 4 rows, same assertions — so a matching verdict proves the flag
    // reproduces G3b rather than us assuming it does. Runs BEFORE the env var
    // is ever set, so the env var cannot be the explanation.
    let g3c = raw_consistent_copy_live(&db, PAGE_SIZE).await;
    match &g3c {
        Ok(img) => {
            let out = p.path("g-readonly.db");
            std::fs::write(&out, img)?;
            println!(
                "G3c with OpenFlags::ReadOnly (no env var), copy SUCCEEDED ({} bytes); integrity_check {:?}; rows {:?} (source has 4, incl. the row written while we read)",
                img.len(),
                sqlite3_clean(&out, "PRAGMA integrity_check;"),
                row_count(&out)
            );
        }
        Err(e) => println!("G3c with OpenFlags::ReadOnly (no env var), copy REFUSED: {e:#}"),
    }
    std::env::set_var("LIMBO_DISABLE_FILE_LOCK", "1");
    match raw_consistent_copy_live(&db, PAGE_SIZE).await {
        Ok(img) => {
            let out = p.path("g-nolock.db");
            std::fs::write(&out, &img)?;
            println!(
                "G3b with LIMBO_DISABLE_FILE_LOCK=1, copy SUCCEEDED ({} bytes); integrity_check {:?}; rows {:?} (source has 4, incl. the row written while we read)",
                img.len(),
                sqlite3_clean(&out, "PRAGMA integrity_check;"),
                row_count(&out)
            );
        }
        Err(e) => println!("G3b with LIMBO_DISABLE_FILE_LOCK=1, copy still REFUSED: {e:#}"),
    }
    std::env::remove_var("LIMBO_DISABLE_FILE_LOCK");

    // R858-B18 G6 — the ticket's TITLE claim, which G2/G3 only approached:
    // can turso-backup *tail* (not merely copy) a database a foreign process is
    // holding? Base snapshot via the reader path, then tail_frames through a
    // ReadOnly seam, with the holder still attached. A locking error here would
    // mean the flag fixed the snapshot tier and not the stream tier.
    if let Ok(img) = &g3c {
        let tgt = target();
        let base = upload_base_snapshot(&tgt, img).await?;
        match CoreWalSeam::open_reader(&db) {
            Ok(seam) => match tail_frames(&seam, &tgt, &cfg(&base)).await {
                Ok(o) => println!(
                    "G6 tail_frames through a ReadOnly seam, foreign holder STILL attached: {}",
                    short(&o)
                ),
                Err(e) => println!("G6 tail_frames through a ReadOnly seam FAILED: {e:#}"),
            },
            Err(e) => println!("G6 could not open a ReadOnly seam to tail: {e:#}"),
        }
    }

    let _ = Command::new("kill").arg("-9").arg(child.id().to_string()).status();
    let _ = child.wait();
    drop(sin);

    println!("G4 foreign holder killed; retrying with nobody else on the file:");
    match CoreWalSeam::open(&db) {
        Ok(s) => println!("G5 CoreWalSeam::open SUCCEEDED -> {:?}", s.wal_state()?),
        Err(e) => println!("G5 CoreWalSeam::open still REFUSED: {e:#}"),
    }
    println!(
        "G VERDICT: A WRITABLE turso open and upstream C SQLite are mutually exclusive on one file — \
         turso_core takes a whole-file exclusive fcntl lock at open (io/unix.rs lock_file(true)), \
         so whichever opens first locks the other out, and headscale holds its connection for its \
         whole process lifetime (G2). That is a GUARD, not a format incompatibility, and R858-B18 \
         resolved it AT THE HANDLE: OpenFlags::ReadOnly skips the lock for that one handle with no \
         env var and no process-wide effect (G2r), and the resulting copy is byte-for-byte as good \
         as the env-var one — G3c and G3b agree exactly, same size, integrity_check ok, same 4 rows \
         including the one the holder wrote after we had already been refused. So LIMBO_DISABLE_FILE_LOCK \
         is never needed and should never be used: it removes the guard for EVERY open in the \
         process, including the writable ones. THE FLAG IS ONLY HALF: neither engine observes the \
         other's locks (turso whole-file fcntl, C SQLite byte-range + the -shm WAL index), so \
         getting in without a lock is not coordination — see probe H for the optimistic validation \
         that turns 'may read torn state' into 'detects torn state and refuses', and probe S for \
         the same two-part fix on the snapshot tier."
    );
    Ok(())
}

/// H — R858-B18: does the optimistic validation actually CATCH a torn read, or
/// does it hand back a plausible wrong image? Probe G proved the ReadOnly open
/// gets us in the door; this one drives a foreign writer **concurrently with the
/// copy** and asks what comes out.
///
/// The three outcomes this distinguishes, which is the whole point:
///
/// - `Ok` with a valid image — validation held, the copy is a real point in time.
/// - `Err` — validation caught movement and REFUSED. This is the outcome the
///   ticket demands exist; without it, ReadOnly alone is the "may read torn
///   state" trade that is strictly worse than refusing to open.
/// - `Ok` with a corrupt image — the failure mode this probe exists to rule out.
///   Every accepted image is integrity-checked and row-counted here, so this
///   would show up as a WRONG verdict rather than as silence.
///
/// It also measures the cost of the *stricter* rule this crate deliberately does
/// NOT use: how many of the same copies would have been refused had `wal_len`
/// (i.e. a plain append) counted as movement. That number is the justification
/// for [`SourceFingerprint::stable_across`] being narrower than field equality.
///
/// Run in TWO regimes, because a single write rate can only prove half of it.
/// HOT (folds far more often than a copy takes) must show REFUSALS; CALM (the
/// headscale-shaped rate: a small database, occasional single-row commits, the
/// default autocheckpoint) must show ACCEPTS. Neither may ever show a corrupt
/// accepted image.
async fn probe_h() -> Result<()> {
    println!("\n=== PROBE H: validation under a foreign writer running CONCURRENTLY with the copy ===");
    let hot = h_regime(
        "HOT",
        HRegime { seed_rows: 4000, autockpt: 16, batch: 40, pad: 400, pause_ms: 0, rounds: 12 },
    )
    .await?;
    let calm = h_regime(
        "CALM",
        // headscale.db measures 94 KB, and a coordination server's node/route
        // churn is single-row commits, not a bulk load. Default autocheckpoint
        // (1000 pages) is what the foreign process actually runs.
        HRegime { seed_rows: 200, autockpt: 1000, batch: 1, pad: 60, pause_ms: 15, rounds: 12 },
    )
    .await?;

    println!(
        "H VERDICT: {}",
        if hot.corrupt > 0 || calm.corrupt > 0 {
            "WRONG — an image was accepted that should not have been; the validation is not sufficient."
        } else if hot.refused == 0 {
            "UNPROVEN (refuse half) — no refusal was observed even in the HOT regime, so this run did not exercise the detector."
        } else if calm.accepted == 0 {
            "UNPROVEN (accept half) — the detector refuses, but even the CALM regime never produced a validated image, so the protocol may be too strict to back up a live database at all."
        } else {
            "CORRECT — under concurrent foreign writes the copy either returns a validated point-in-time image or REFUSES, and never returned a torn image. The CALM (headscale-shaped) regime backs up successfully WHILE the foreign process writes; the HOT regime is correctly reported as unbackupable rather than silently mis-copied."
        }
    );
    Ok(())
}

/// One probe-H write regime. `pause_ms` throttles the foreign writer between
/// commits; `autockpt` is the `wal_autocheckpoint` the *writer's* connection runs
/// (the pragma is per-connection, so setting it anywhere else measures nothing).
struct HRegime {
    seed_rows: i64,
    autockpt: i64,
    batch: i64,
    pad: usize,
    pause_ms: u64,
    rounds: u32,
}

/// What one regime measured.
struct HResult {
    accepted: u32,
    refused: u32,
    corrupt: u32,
}

async fn h_regime(tag: &str, r: HRegime) -> Result<HResult> {
    let p = Probe::new(&format!("h-{}", tag.to_lowercase()))?;
    let db = p.path("h.db");
    seed_clean(&db, r.seed_rows)?;
    println!(
        "\nH0[{tag}] seeded {} rows: main {}B | {} | writer autocheckpoint {} pages, {}-row commits every {}ms",
        r.seed_rows,
        file_len(&db),
        hdr_str(&db),
        r.autockpt,
        r.batch,
        r.pause_ms
    );

    // Long-lived foreign writer. No sentinel round-trip: the point is for it to
    // be mid-flight while we copy, not to be synchronised with us.
    let mut child = Command::new(sqlite3_bin())
        .arg(&db)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut sin = child.stdin.take().expect("piped stdin");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let feeder_stop = stop.clone();
    let (autockpt, batch, pad, pause_ms) = (r.autockpt, r.batch, r.pad, r.pause_ms);
    let feeder = std::thread::spawn(move || {
        let mut i = 100_000i64;
        let _ = writeln!(sin, "PRAGMA wal_autocheckpoint={autockpt};");
        while !feeder_stop.load(std::sync::atomic::Ordering::Relaxed) {
            let mut sql = String::from("BEGIN;\n");
            for _ in 0..batch {
                sql.push_str(&format!(
                    "INSERT INTO t(id,v) VALUES({i},'{}');\n",
                    "y".repeat(pad)
                ));
                i += 1;
            }
            sql.push_str("COMMIT;\n");
            if writeln!(sin, "{sql}").is_err() {
                break;
            }
            let _ = sin.flush();
            if pause_ms > 0 {
                std::thread::sleep(Duration::from_millis(pause_ms));
            }
        }
        drop(sin);
    });

    let mut res = HResult { accepted: 0, refused: 0, corrupt: 0 };
    let mut strict_would_refuse = 0u32;
    let mut first_refusal: Option<String> = None;
    let mut last_rows: i64 = 0;
    for round in 0..r.rounds {
        let before = SourceFingerprint::read(&db)?;
        let started = Instant::now();
        let got = raw_consistent_copy_live(&db, PAGE_SIZE).await;
        let elapsed = started.elapsed();
        let after = SourceFingerprint::read(&db)?;
        if before.wal_len != after.wal_len {
            strict_would_refuse += 1;
        }
        match got {
            Ok(img) => {
                res.accepted += 1;
                let out = p.path(&format!("h-img-{round}.db"));
                std::fs::write(&out, &img)?;
                let integrity = sqlite3_clean(&out, "PRAGMA integrity_check;");
                let rows = row_count(&out);
                let ok = matches!(integrity.as_deref(), Ok("ok"))
                    && rows.as_ref().is_ok_and(|n| *n >= last_rows);
                if ok {
                    last_rows = *rows.as_ref().unwrap();
                } else {
                    res.corrupt += 1;
                    println!(
                        "H1[{tag}/{round}] ACCEPTED a BAD image after {elapsed:?}: {} bytes; integrity {:?}; rows {:?} (previous accepted image had {last_rows})",
                        img.len(),
                        integrity,
                        rows
                    );
                }
                let _ = std::fs::remove_file(&out);
            }
            Err(e) => {
                res.refused += 1;
                if first_refusal.is_none() {
                    first_refusal = Some(format!("{e:#}"));
                }
            }
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = Command::new("kill").arg("-9").arg(child.id().to_string()).status();
    let _ = child.wait();
    let _ = feeder.join();

    println!(
        "H2[{tag}] {} copies under a concurrent foreign writer: {} accepted, {} refused-and-validated, {} accepted-but-corrupt",
        r.rounds, res.accepted, res.refused, res.corrupt
    );
    println!(
        "H3[{tag}] every accepted image passed PRAGMA integrity_check and was row-count-monotonic: {} (last accepted image had {last_rows} rows)",
        res.corrupt == 0
    );
    match &first_refusal {
        Some(m) => println!("H4[{tag}] first refusal message, verbatim: {m}"),
        None => println!("H4[{tag}] no refusal was observed in {} rounds", r.rounds),
    }
    println!(
        "H5[{tag}] the STRICTER rule this crate does not use (wal_len counts as movement) would have refused {strict_would_refuse}/{} of the same copies",
        r.rounds
    );
    Ok(res)
}

/// S — R858-B18: the SNAPSHOT tier (tier 1a), which the ticket's gotcha names as
/// the second blocked source open and which probe G never touched. `snapshot.rs`
/// reaches the source through `turso::Builder::new_local` — the friendly wrapper,
/// which exposes no `OpenFlags` — so it takes the same whole-file exclusive lock
/// that refused `CoreWalSeam::open`.
///
/// Four measurements against one live foreign holder:
///
/// - **S1** `snapshot_and_upload`, the shipping entry point. Before R858-B18 this
///   was refused outright with `Locking error: File is locked by another process`
///   (measured 2026-09-10, before the change); it now goes through the read-only
///   path and should succeed.
/// - **S1w** the control that keeps S1 honest: the same `VACUUM INTO` through a
///   **writable** open, which is what `snapshot.rs` used to do. Still expected to
///   be refused — so the difference between S1w and S2 is the flag and nothing
///   else.
/// - **S2** `VACUUM INTO` driven through a `turso_core` connection opened
///   `OpenFlags::ReadOnly`. This is the question that decides whether tier 1a can
///   keep `VACUUM INTO` at all: the flag gets the file open, but VACUUM INTO is a
///   statement, and a read-only connection might refuse to run it.
/// - **S3** the tier-2-compatible reader path — `raw_consistent_copy_live` +
///   `upload_base_snapshot`, which publishes a page-preserving image under the
///   same key layout (`VACUUM INTO` repacks, so its output is not a valid tier-2
///   base).
async fn probe_s() -> Result<()> {
    println!("\n=== PROBE S: the snapshot tier against a live foreign holder ===");
    let p = Probe::new("s")?;
    let db = p.path("s.db");
    seed_clean(&db, 50)?;

    let mut child = Command::new(sqlite3_bin())
        .arg(&db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut sin = child.stdin.take().expect("piped stdin");
    let sout = child.stdout.take().expect("piped stdout");
    let mut rd = BufReader::new(sout);
    writeln!(sin, "SELECT count(*) FROM t;\nSELECT 'PROBE-SENTINEL';")?;
    sin.flush()?;
    let mut line = String::new();
    loop {
        line.clear();
        anyhow::ensure!(rd.read_line(&mut line)? > 0, "foreign holder died early");
        if line.contains("PROBE-SENTINEL") {
            break;
        }
    }
    println!("S0 foreign sqlite3 connection is open and idle on the DB");

    let tgt = target();
    match snapshot_and_upload(&db, &tgt).await {
        Ok(o) => println!("S1 snapshot_and_upload SUCCEEDED -> {o:?}"),
        Err(e) => println!("S1 snapshot_and_upload REFUSED: {e:#}"),
    }

    // S1w / S2: the same VACUUM INTO, differing ONLY in the open flags. Note
    // VACUUM INTO is NOT gated by DatabaseOpts::enable_vacuum (only bare VACUUM
    // is — turso_core translate/vacuum.rs:39), so plain DatabaseOpts::new() is
    // the right opts for both and any difference is attributable to the flag.
    for (tag, flags) in [
        ("S1w VACUUM INTO via write flags (what snapshot.rs did before)", turso_core::OpenFlags::default()),
        ("S2 VACUUM INTO via OpenFlags::ReadOnly", turso_core::OpenFlags::ReadOnly),
    ] {
        let vac = p.path(&format!("s-vacuum-{:?}.db", flags));
        let _ = std::fs::remove_file(&vac);
        let got = (|| -> anyhow::Result<()> {
            let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new()?);
            let core = turso_core::Database::open_file_with_flags(
                io,
                &db,
                flags,
                turso_core::DatabaseOpts::new(),
                None,
            )?;
            let conn = core.connect()?;
            conn.execute(format!("VACUUM INTO '{}'", vac.replace('\'', "''")))?;
            Ok(())
        })();
        match got {
            Ok(()) => println!(
                "{tag} SUCCEEDED -> {}B; integrity_check {:?}; rows {:?}",
                file_len(&vac),
                sqlite3_clean(&vac, "PRAGMA integrity_check;"),
                row_count(&vac)
            ),
            Err(e) => println!("{tag} REFUSED: {e:#}"),
        }
    }

    // S3: the reader path. Same published key layout, no writable source open.
    match raw_consistent_copy_live(&db, PAGE_SIZE).await {
        Ok(img) => {
            let key = upload_base_snapshot(&tgt, &img).await?;
            let out = p.path("s-reader.db");
            std::fs::write(&out, &img)?;
            println!(
                "S3 raw_consistent_copy_live + upload_base_snapshot SUCCEEDED -> {key} ({}B); integrity_check {:?}; rows {:?}",
                img.len(),
                sqlite3_clean(&out, "PRAGMA integrity_check;"),
                row_count(&out)
            );
        }
        Err(e) => println!("S3 reader path REFUSED: {e:#}"),
    }
    println!(
        "S VERDICT: tier 1a was blocked for exactly the same reason as tier 2 and is fixed the same \
         way — S1w (write flags) is still refused while S2 (OpenFlags::ReadOnly) vacuums the same \
         live source successfully, so VACUUM INTO does run on a read-only connection and tier 1a \
         keeps its byte-deterministic gate-2 hash. snapshot.rs now opens through turso_core with \
         that flag instead of turso::Builder (which exposes no OpenFlags), wrapped in the same \
         optimistic validation, which is why S1 succeeds where it used to report a locking error."
    );

    let _ = Command::new("kill").arg("-9").arg(child.id().to_string()).status();
    let _ = child.wait();
    drop(sin);
    Ok(())
}

/// E — cost: transactions before the *default* autocheckpoint folds the WAL.
fn probe_e() -> Result<()> {
    println!("\n=== PROBE E: default-autocheckpoint fold rate (how often the restart path fires) ===");
    let p = Probe::new("e")?;
    let db = p.path("e.db");
    println!(
        "E0 writer default wal_autocheckpoint = {} pages",
        sqlite3_clean(&db, "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint;")?
            .lines()
            .last()
            .unwrap_or("?")
    );
    sqlite3_clean(
        &db,
        "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, v TEXT); CREATE INDEX IF NOT EXISTS ix ON t(v);",
    )?;

    // One long-lived writer doing many small transactions — the shape a
    // coordination server's node/route churn produces. A PASSIVE autocheckpoint
    // does not truncate the WAL file, so the fold is detected from the on-disk
    // header (salt re-roll / ckpt_seq bump), not from the file size.
    let mut child = Command::new(sqlite3_bin())
        .arg(&db)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut sin = child.stdin.take().expect("piped stdin");
    let sout = child.stdout.take().expect("piped stdout");
    let mut rd = BufReader::new(sout);
    let mut line = String::new();

    let total = 3000i64;
    let mut hdr = wal_hdr(&db);
    let mut folds: Vec<i64> = Vec::new();
    for i in 0..total {
        writeln!(sin, "INSERT INTO t(id,v) VALUES({i},'{}');\nSELECT 'PROBE-SENTINEL';", "x".repeat(200))?;
        sin.flush()?;
        loop {
            line.clear();
            anyhow::ensure!(rd.read_line(&mut line)? > 0, "writer died at row {i}");
            if line.contains("PROBE-SENTINEL") {
                break;
            }
        }
        let h = wal_hdr(&db);
        if h != hdr {
            if hdr.is_some() {
                folds.push(i);
            }
            hdr = h;
        }
        if i % 500 == 0 {
            // NB: no turso_watermark() in this loop. Probe G measures why —
            // opening a seam while the foreign writer holds the file is refused
            // outright, so asking turso here would abort the measurement.
            println!(
                "E1 after {i} single-row txns: wal {}B main {}B | {}",
                file_len(&format!("{db}-wal")),
                file_len(&db),
                hdr_str(&db),
            );
        }
    }
    println!(
        "E2 after {total} single-row txns: wal {}B main {}B | {}",
        file_len(&format!("{db}-wal")),
        file_len(&db),
        hdr_str(&db),
    );
    println!("E3 WAL-header changes (folds/restarts) at txn #: {folds:?}");
    // Only now that the writer is gone can turso open the file at all.
    let _ = Command::new("kill").arg("-9").arg(child.id().to_string()).status();
    let _ = child.wait();
    drop(sin);
    let w = turso_watermark(&db)?;
    println!(
        "E4 with the writer gone, turso reports (seq {}, max_frame {})",
        w.checkpoint_seq, w.last_frame
    );
    println!(
        "E VERDICT: {} fold(s) in {total} single-row transactions -> one restart per ~{} writes; turso-observed checkpoint_seq after all of them = {}",
        folds.len(),
        if folds.is_empty() { "never".to_string() } else { (total / folds.len() as i64).to_string() },
        w.checkpoint_seq
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let want: Vec<String> = std::env::args().skip(1).map(|s| s.to_lowercase()).collect();
    let on = |c: &str| want.is_empty() || want.iter().any(|w| w == c);
    println!(
        "foreign writer: {} -> sqlite {}",
        sqlite3_bin(),
        sqlite3_clean(":memory:", "SELECT sqlite_version();")?
    );
    if on("a") {
        probe_a()?;
    }
    if on("b") {
        println!("\n=== PROBE B: fold, then a SHORTER new WAL (post-fold frames < watermark) ===");
        tail_across_fold("b", 6, 3, 3, 1, false).await?;
    }
    if on("f") {
        println!("\n=== PROBE F: fold, then a LONGER new WAL (post-fold frames > watermark) ===");
        tail_across_fold("f", 40, 20, 60, 30, true).await?;
    }
    if on("c") {
        probe_c().await?;
    }
    if on("d") {
        probe_d()?;
    }
    if on("g") {
        probe_g().await?;
    }
    if on("h") {
        probe_h().await?;
    }
    if on("s") {
        probe_s().await?;
    }
    if on("e") {
        probe_e()?;
    }
    Ok(())
}
