//! R850-F1 — **can an appliance's WAL tail run in a process separate from the
//! application that holds the database open?**
//!
//! This probe exists because the answer decides whether the backup half of
//! hydrate-on-place is buildable at all, and because the claim that it is *not*
//! reached this ticket as a read rather than a measurement.
//!
//! ## The question, and why it is not probe G
//!
//! `examples/foreign_checkpoint_probe.rs` measures turso against **upstream C
//! SQLite** — headscale's Go driver holding the file. That is a
//! two-engines-one-file question, and its verdict (G) is about a lock two
//! different lock protocols cannot share.
//!
//! The appliance case is the *other* pairing, and nothing had measured it:
//! **turso on both sides.** noisetable-account links `turso 0.7` and holds its
//! connections for the process lifetime; a `turso-backup-tail` sidecar would be
//! a second turso process on the same files. The reported blocker was that
//! `CoreWalSeam::open()` calls `wal_auto_actions_disable()` at construction and
//! turso takes a cross-process exclusive `fcntl` lock, so the sidecar could
//! never get in — which, if true, means BOTH arms of R850-F1's backup-side fork
//! (an `OwnershipSource` impl in tenant-streamer, or a kamaji sidecar) are dead
//! before either is built, because both put the tail outside the app process.
//!
//! ## What this measures
//!
//! A child process — this same binary, re-exec'd with `--hold` — opens the
//! database through the **high-level `turso` crate**, exactly as the application
//! does, and keeps that connection for its whole life. The parent then tries,
//! in order:
//!
//! - **A1** `CoreWalSeam::open` (writable). Expected to be refused: it is the
//!   constructor that takes the whole-file lock.
//! - **A2** `CoreWalSeam::open_reader` (`OpenFlags::ReadOnly`, R858-B18). The
//!   decisive one — same file, one variable changed.
//! - **A3** `wal_get_frame` through that reader. Opening is not tailing; a
//!   reader that cannot read a frame is no use to a streamer.
//! - **A4** whether a **held** reader sees frames the holder appends after it
//!   opened. Probe D found a held *writable* seam does not see a foreign
//!   writer's activity; if that also holds here, a tail loop must reopen its
//!   seam per round, which is a design constraint on the sidecar, not a
//!   blocker.
//! - **A5** a full separate-process backup: snapshot + `tail_frames` into a
//!   sink, then `restore_latest_stream` into a fresh path, row-counted against
//!   what the holder committed. This is the end-to-end the fork actually needs.
//! - **A6** the control the report named: two `CoreWalSeam::open()` calls in
//!   ONE process. This is a different question from A1 and is expected to
//!   succeed for a reason that has nothing to do with locking — turso's
//!   process-global `DATABASE_MANAGER` hands back the already-open `Database`
//!   (see `CoreWalSeam::open_reader`'s doc). Measured here so the two are not
//!   confused for each other.
//!
//! Run: `cargo run -p turso-backup --example appliance_tail_probe`

use anyhow::{Context, Result};
use object_store::memory::InMemory;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use turso_backup::backpressure::BackpressureConfig;
use turso_backup::snapshot::{upload_base_snapshot, BackupTarget};
use turso_backup::stream::{
    raw_consistent_copy_live, restore_latest_stream, tail_frames, CoreWalSeam, StreamConfig,
    StreamOutcome, WalSeam,
};

const PAGE_SIZE: usize = 4096;

/// Sentinels on the holder's stdout. The parent blocks on these rather than
/// sleeping, so a slow machine does not turn into a wrong verdict.
const READY: &str = "HOLDER-READY";
const WROTE: &str = "HOLDER-WROTE";

// ── child: the application, holding its database the way a real one does ─────

/// `--hold <db>`: open through the high-level `turso` crate and never let go.
/// Commands arrive on stdin (`write <n>` / `quit`) so the parent can order
/// writes against its own measurements instead of racing a free-running writer.
async fn hold(path: &str) -> Result<()> {
    let db = turso::Builder::new_local(path)
        .build()
        .await
        .with_context(|| format!("holder opening {path}"))?;
    let conn = db.connect().context("holder connecting")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)",
        (),
    )
    .await
    .context("holder creating table")?;

    let mut next_id: i64 = 0;
    println!("{READY}");
    std::io::stdout().flush()?;

    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        line.clear();
        if stdin.lock().read_line(&mut line)? == 0 {
            break;
        }
        let mut it = line.split_whitespace();
        match it.next() {
            Some("write") => {
                let n: i64 = it.next().unwrap_or("1").parse().unwrap_or(1);
                conn.execute("BEGIN", ()).await.context("holder BEGIN")?;
                for _ in 0..n {
                    conn.execute(
                        "INSERT INTO t (id, v) VALUES (?, ?)",
                        (next_id, format!("v{next_id}")),
                    )
                    .await
                    .context("holder INSERT")?;
                    next_id += 1;
                }
                conn.execute("COMMIT", ()).await.context("holder COMMIT")?;
                println!("{WROTE} {next_id}");
                std::io::stdout().flush()?;
            }
            Some("quit") | None => break,
            Some(other) => anyhow::bail!("holder got unknown command {other:?}"),
        }
    }
    // The connection is still open here; the parent SIGKILLs us so nothing
    // checkpoints on a clean close.
    Ok(())
}

// ── parent: the sidecar's posture ────────────────────────────────────────────

/// The live application process, plus the pipe used to order writes into it.
struct Holder {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl Holder {
    fn spawn(db: &str) -> Result<Self> {
        let exe = std::env::current_exe().context("locating current_exe")?;
        let mut child = Command::new(exe)
            .arg("--hold")
            .arg(db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("spawning the holder child")?;
        let stdin = child.stdin.take().expect("piped stdin");
        let mut stdout = BufReader::new(child.stdout.take().expect("piped stdout"));
        let mut line = String::new();
        anyhow::ensure!(
            stdout.read_line(&mut line)? > 0 && line.contains(READY),
            "holder never became ready (got {line:?})"
        );
        Ok(Self { child, stdin, stdout })
    }

    /// Commit `n` rows in the holder and return its running row total.
    fn write(&mut self, n: i64) -> Result<i64> {
        writeln!(self.stdin, "write {n}")?;
        self.stdin.flush()?;
        let mut line = String::new();
        anyhow::ensure!(
            self.stdout.read_line(&mut line)? > 0 && line.contains(WROTE),
            "holder did not confirm the write (got {line:?})"
        );
        Ok(line.split_whitespace().nth(1).unwrap_or("0").parse()?)
    }

    /// SIGKILL, so the holder cannot checkpoint on the way out.
    fn kill(mut self) {
        let _ = Command::new("kill")
            .arg("-9")
            .arg(self.child.id().to_string())
            .status();
        let _ = self.child.wait();
    }
}

fn scratch(tag: &str) -> Result<std::path::PathBuf> {
    let dir = std::env::temp_dir().join(format!(
        "turso-backup-appliance-tail-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn cfg(base_snapshot_key: &str) -> StreamConfig<'_> {
    StreamConfig {
        base_snapshot_key,
        page_size: PAGE_SIZE,
        backpressure: BackpressureConfig::default(),
        rpo_target: None,
        epoch: 0,
        owner: Some("R850-F1-appliance-tail-probe"),
        pointer_generation: 0,
    }
}

async fn count_rows(path: &str) -> Result<i64> {
    let db = turso::Builder::new_local(path).build().await?;
    let conn = db.connect()?;
    let mut r = conn.query("SELECT COUNT(*) FROM t", ()).await?;
    let row = r.next().await?.context("COUNT(*) returned no row")?;
    Ok(row.get::<i64>(0)?)
}

async fn probe() -> Result<()> {
    let dir = scratch("main")?;
    let db = dir.join("app.db").to_str().unwrap().to_string();

    println!("=== R850-F1: can a WAL tail run beside a turso application process? ===");
    let mut holder = Holder::spawn(&db)?;
    let committed = holder.write(200)?;
    println!("A0 holder is a separate turso process, connection open, {committed} rows committed");

    // A1 — the constructor the report assumed a sidecar would have to use.
    let a1_refused = match CoreWalSeam::open(&db) {
        Ok(s) => {
            println!("A1 CoreWalSeam::open (WRITABLE) SUCCEEDED -> {:?}", s.wal_state()?);
            false
        }
        Err(e) => {
            println!("A1 CoreWalSeam::open (WRITABLE) REFUSED: {e:#}");
            true
        }
    };

    // A2 — same file, one variable: OpenFlags::ReadOnly takes no whole-file lock.
    let reader = CoreWalSeam::open_reader(&db);
    let a2_ok = reader.is_ok();
    match &reader {
        Ok(s) => println!(
            "A2 CoreWalSeam::open_reader (ReadOnly) SUCCEEDED alongside it -> {:?}",
            s.wal_state()?
        ),
        Err(e) => println!("A2 CoreWalSeam::open_reader (ReadOnly) REFUSED: {e:#}"),
    }

    // A3 — opening is not tailing. Read an actual frame through the reader.
    let mut a3_ok = false;
    if let Ok(s) = &reader {
        let w = s.wal_state()?;
        let mut buf = vec![0u8; 24 + PAGE_SIZE];
        match s.wal_get_frame(w.last_frame.max(1), &mut buf) {
            Ok(info) => {
                a3_ok = true;
                println!(
                    "A3 wal_get_frame({}) through the ReadOnly seam SUCCEEDED -> page {} db_size {}",
                    w.last_frame, info.page_no, info.db_size
                );
            }
            Err(e) => println!("A3 wal_get_frame({}) REFUSED: {e:#}", w.last_frame),
        }
    }

    // A4 — does a HELD reader observe the holder's later commits, or is each
    // tail round obliged to reopen? Probe D says a held writable seam does not.
    //
    // The reopen has to happen AFTER the held seam is dropped, and that
    // ordering is load-bearing rather than tidiness: turso's process-global
    // `DATABASE_MANAGER` is keyed by file id, so a second `open_reader` while
    // the first handle is alive hands back *that same handle* (see
    // `CoreWalSeam::open_reader`'s doc, point 2). Measuring them side by side
    // therefore measures one seam twice and reports "reopening does not help"
    // no matter what is true.
    let mut a4_sees_new = false;
    let mut a4_reopen_sees_new = false;
    if let Ok(s) = &reader {
        let before = s.wal_state()?;
        let committed = holder.write(200)?;
        let after_held = s.wal_state()?;
        a4_sees_new = after_held.last_frame > before.last_frame;
        println!(
            "A4a holder committed up to {committed} rows; the HELD seam went {} -> {} (sees new: {a4_sees_new})",
            before.last_frame, after_held.last_frame
        );
        drop(reader);
        let after_fresh = CoreWalSeam::open_reader(&db)?.wal_state()?;
        a4_reopen_sees_new = after_fresh.last_frame > before.last_frame;
        println!(
            "A4b a seam reopened after dropping it reports {} (sees new: {a4_reopen_sees_new})",
            after_fresh.last_frame
        );
    } else {
        drop(reader);
    }

    // A5 — the end-to-end the fork needs: snapshot + tail from OUT HERE, then
    // restore into a fresh file and count what came back.
    let target = BackupTarget {
        store: Arc::new(InMemory::new()),
        prefix: "appliance".into(),
    };
    let image = raw_consistent_copy_live(&db, PAGE_SIZE)
        .await
        .context("A5 snapshot of the live, held database");
    let mut a5_rows: Option<(i64, i64)> = None;
    match image {
        Ok(img) => {
            let key = upload_base_snapshot(&target, &img).await?;
            println!("A5a base snapshot of the LIVE held db: {} bytes -> {key}", img.len());
            let committed = holder.write(200)?;
            let seam = CoreWalSeam::open_reader(&db)?;
            let outcome = tail_frames(&seam, &target, &cfg(&key)).await?;
            drop(seam);
            println!("A5b tail_frames from the sidecar: {}", describe(&outcome));
            let dest = dir.join("restored.db").to_str().unwrap().to_string();
            let restored = restore_latest_stream(&target, &dest).await?;
            println!(
                "A5c restore_latest_stream replayed {} frames from seq {}",
                restored.frames_replayed, restored.checkpoint_seq
            );
            let got = count_rows(&dest).await?;
            a5_rows = Some((got, committed));
            println!(
                "A5d restored copy has {got} rows; holder had committed {committed} at tail time"
            );
        }
        Err(e) => println!("A5a snapshot of the live held db REFUSED: {e:#}"),
    }

    holder.kill();

    // A6 — the in-process control, which is a DIFFERENT question: turso's
    // process-global registry, not the fcntl lock.
    let dir2 = scratch("same-process")?;
    let solo = dir2.join("solo.db").to_str().unwrap().to_string();
    {
        let d = turso::Builder::new_local(&solo).build().await?;
        let c = d.connect()?;
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", ()).await?;
        c.execute("INSERT INTO t (id) VALUES (1)", ()).await?;
    }
    let first = CoreWalSeam::open(&solo);
    let second = CoreWalSeam::open(&solo);
    println!(
        "A6 two CoreWalSeam::open calls in ONE process: first {}, second {}",
        if first.is_ok() { "Ok" } else { "Err" },
        match &second {
            Ok(_) => "Ok".to_string(),
            Err(e) => format!("Err({e:#})"),
        }
    );
    drop(first);
    drop(second);

    println!("\nVERDICT:");
    if a2_ok && a3_ok {
        println!(
            "  A SEPARATE-PROCESS TAIL IS POSSIBLE. The writable constructor is {} while the \
             application holds the file, but OpenFlags::ReadOnly opens and reads frames beside it. \
             The reported blocker is real ONLY for CoreWalSeam::open; it is not a property of turso \
             or of tailing.",
            if a1_refused { "refused, as reported" } else { "NOT refused (unexpected)" }
        );
    } else {
        println!(
            "  A SEPARATE-PROCESS TAIL IS NOT POSSIBLE with the current seam: open_reader {} and \
             wal_get_frame {}. The tail must run inside the application process.",
            if a2_ok { "succeeded" } else { "was refused" },
            if a3_ok { "succeeded" } else { "was refused" }
        );
    }
    match a5_rows {
        Some((got, committed)) if got == committed => println!(
            "  End to end: a snapshot+tail taken entirely from the sidecar restored to {got} rows, \
             matching the holder exactly."
        ),
        Some((got, committed)) => println!(
            "  End to end: restored {got} rows against {committed} committed. A SHORTFALL IS NOT \
             AUTOMATICALLY WRONG — a tail captures a point in time — but a surplus, or a shortfall \
             larger than the last transaction, is."
        ),
        None => println!("  End to end: NOT REACHED (the snapshot step failed above)."),
    }
    println!(
        "  A4 tail-loop constraint: a held reader {} the holder's later commits; reopening the \
         seam {}. So a streaming loop {}.",
        if a4_sees_new { "DOES see" } else { "does NOT see" },
        if a4_reopen_sees_new { "DOES surface them" } else { "does NOT surface them either" },
        match (a4_sees_new, a4_reopen_sees_new) {
            (true, _) => "may hold one seam across rounds",
            (false, true) => "MUST drop and reopen its seam every round \
                              (and cannot reopen while still holding the old one — the \
                              process-global registry would hand back the same handle)",
            (false, false) => "CANNOT follow a live writer at all through this seam — investigate \
                               before building on it",
        }
    );
    println!(
        "  A6 is not evidence about locking either way: turso's process-global DATABASE_MANAGER \
         returns the already-open Database for a second open of the same path in one process."
    );
    Ok(())
}

fn describe(o: &StreamOutcome) -> String {
    match o {
        StreamOutcome::Empty { watermark, .. } => format!("Empty at {watermark:?}"),
        StreamOutcome::Streamed { first_frame, last_frame, frame_count, .. } => {
            format!("Streamed frames {first_frame}..={last_frame} ({frame_count})")
        }
        StreamOutcome::Restarted { first_frame, last_frame, frame_count, .. } => {
            format!("Restarted, frames {first_frame}..={last_frame} ({frame_count})")
        }
        other => format!("{other:?}"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--hold") {
        let path = args.get(1).context("--hold needs a database path")?;
        return hold(path).await;
    }
    probe().await
}
