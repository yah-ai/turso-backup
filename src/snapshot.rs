//! Tier 1a — full snapshot sink.
//!
//! `VACUUM INTO` a temp file → upload the whole file to an object store
//! (S3 / R2 / MinIO) → restore = download + open with SQLite. The DB-header
//! `change_counter` gates skip-if-unchanged. Uses turso's public API only.
//!
//! @yah:relay(R003, "Tier 1a — full snapshot sink (VACUUM INTO → S3)")
//! @yah:at(2026-05-26T22:28:30Z)
//! @yah:status(open)
//! @yah:phase(P1)
//! @yah:parent(Q002)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//!
//! @yah:ticket(R003-F2, "Full-snapshot sink: VACUUM INTO temp + object_store upload (S3/R2/MinIO, path-style); change_counter skip-if-unchanged")
//! @yah:assignee(bundle-anthropic-ashguard)
//! @yah:at(2026-05-27T00:51:38Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R003)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R003-T1)
//! @yah:handoff("Implemented snapshot_and_upload with a TWO-GATE skip guard: gate 1 = source-file hash (main + -wal) skips vacuum+upload on the quiescent path; gate 2 = canonical VACUUM INTO output hash (byte-deterministic, verified) dedups the upload when a checkpoint reshuffled bytes without changing content. VACUUM INTO unique temp -> object_store.put + two sidecars (latest.source-fingerprint, latest.snapshot-fingerprint). Returns SnapshotOutcome::{Uploaded,Unchanged,Deduplicated}. Backend-agnostic (Arc<dyn ObjectStore>) -> path-style S3/R2/MinIO is the caller's builder config.")
//! @yah:handoff("DEVIATION from ticket title: change_counter is DEAD in turso — stays 1 across rollback/WAL/WAL+checkpoint (verified; VACUUM INTO doesn't copy it). Hashes replace it. Two-gate design follows user's steer (determinism worth the lift when not expensive); gate 1 keeps the idle path off the vacuum. Still in review for final sign-off. Working doc + Cargo.toml + rustdoc updated.")
//! @yah:verify("cargo test -p turso-backup: 2 tests green on InMemory store — (1) upload -> SQLite-magic + turso 100-row readback -> gate-1 Unchanged -> mutate -> re-upload; (2) gate-2 Deduplicated path + fingerprint refresh. cargo build/clippy clean.")
//! @yah:next("R003-F3: restore_latest (list snapshots/ by last-modified, get newest, write file). Layout: {prefix}/snapshots/snapshot-{unix_nanos:020}.db + {prefix}/latest.fingerprint.")
//! @yah:next("R003-T4: MinIO/path-style e2e in harness; thread experimental_multiprocess_wal through vacuum_into if the writer's WAL format requires it.")
//!
//! @yah:ticket(R003-F3, "Restore path: download latest snapshot, open with sqlite3, verify")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:04Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R003)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R003-F2)
//! @yah:verify("cargo test -p turso-backup: 3 green on InMemory store (new restore_latest_picks_newest_and_round_trips). cargo clippy --all-targets clean.")
//! @yah:handoff("Implemented restore_latest(target, dest_path) -> Result<String>: list_with_delimiter under {prefix}/snapshots/ (excludes the latest.* sidecars by construction), pick the lexically-greatest key (zero-padded nanos = newest; clock-skew-immune vs last_modified), get -> std::fs::write to dest. Returns the restored key. Errors if no snapshots. Moved the fn above mod tests (clippy items_after_test_module).")
//! @yah:next("R003-T4: wire writer -> snapshot_and_upload -> MinIO -> restore_latest -> verifier (5000 rows, sqlite3 integrity_check). The literal 'open with sqlite3' verify lives in the harness; F3 covers it at the lib level (SQLite-magic + turso readback).")
//!
//! @yah:ticket(R003-T4, "Green end-to-end in harness: writer -> snapshot sink -> MinIO -> restore -> verifier checks (5000 rows)")
//! @yah:assignee(bundle-anthropic-ashguard)
//! @yah:at(2026-05-27T08:29:26Z)
//! @yah:status(review)
//! @yah:phase(P1)
//! @yah:parent(R003)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R003-F3)
//! @yah:verify("bash ci/run.sh (staged: minio -> minio-init -> writer -> verifier); verifier exit 0 = green")
//! @yah:handoff("GREEN e2e at 5000 rows. Rewired the harness off Litestream onto turso-backup: new harness/ crate (writer + restore bins + shared backup_target lib, path-dep on turso-backup, object_store aws feature for MinIO). One multi-stage Dockerfile (shared builder, writer/verifier targets). Deleted dead Litestream writer/ + verifier/ dirs. Writer uploaded 446KB snapshot; verifier restored + passed sqlite3 integrity/rowcount/boundary/hash (matched byte-for-byte), exit 0.")

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use turso::Builder;

/// Configuration for an object-store backup target (S3 / R2 / MinIO, path-style).
///
/// Wraps a generic `object_store::ObjectStore` so the caller picks the backend
/// (e.g., `AmazonS3Builder::new().with_endpoint("http://minio:9000").build()`),
/// then hands this crate a `BackupTarget` to use for snapshot operations.
pub struct BackupTarget {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: String,
}

impl BackupTarget {
    /// Object key for a snapshot taken at `unix_nanos` (zero-padded so lexical
    /// order matches chronological order — restore can pick the newest by name
    /// or by last-modified).
    fn snapshot_key(&self, unix_nanos: u128) -> ObjPath {
        join_key(&self.prefix, &format!("snapshots/snapshot-{unix_nanos:020}.db"))
    }

    /// Sidecar recording the hash of the **source files** at the last run —
    /// the cheap gate-1 short-circuit (skip vacuum+upload when bytes match).
    fn source_fingerprint_key(&self) -> ObjPath {
        join_key(&self.prefix, "latest.source-fingerprint")
    }

    /// Sidecar recording the hash of the last uploaded **snapshot** (the
    /// canonical `VACUUM INTO` output) — the deterministic gate-2 dedup.
    fn snapshot_fingerprint_key(&self) -> ObjPath {
        join_key(&self.prefix, "latest.snapshot-fingerprint")
    }
}

/// Result of a [`snapshot_and_upload`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// A new snapshot was written to `key` (`bytes` long). `snapshot_hash` is
    /// the canonical hash of the uploaded file, now recorded in the sidecar.
    Uploaded {
        key: String,
        bytes: usize,
        snapshot_hash: String,
    },
    /// Gate 1: the source files were byte-identical to the last run, so nothing
    /// was vacuumed or uploaded. `source_hash` is the unchanged source hash.
    Unchanged { source_hash: String },
    /// Gate 2: the source bytes moved (e.g. an auto-checkpoint reshuffled
    /// pages) but the canonical `VACUUM INTO` output matched the last snapshot,
    /// so the upload was skipped. The cheap source fingerprint is refreshed so
    /// the next run short-circuits at gate 1.
    Deduplicated { snapshot_hash: String },
}

/// Take a full `VACUUM INTO` snapshot of the turso database at `db_path` and
/// upload the resulting file to the object store, behind a two-gate
/// skip-if-unchanged guard.
///
/// 1. **Gate 1 (cheap):** hash the source files (`db_path` + `-wal`) and compare
///    to the last run's sidecar. Identical bytes → return [`Unchanged`] without
///    touching the DB. This is the common quiescent path.
/// 2. Otherwise `VACUUM INTO` a unique temp file (point-in-time-consistent,
///    fsync'd; the output is valid vanilla SQLite).
/// 3. **Gate 2 (canonical):** `VACUUM INTO` output is byte-deterministic for
///    identical content, so if its hash matches the last uploaded snapshot the
///    source changed without the content changing → return [`Deduplicated`]
///    (upload skipped, source fingerprint refreshed).
/// 4. Otherwise upload the snapshot and record both fingerprints.
///
/// ## Why hashes, not the SQLite `change_counter`
///
/// The original plan used the DB-header `change_counter` (offset 24) as the
/// "did anything change?" signal. The turso rewrite **does not maintain it** —
/// it is left at the default `1` across every write in rollback, WAL, and
/// WAL+checkpoint mode (verified empirically; `VACUUM INTO` doesn't even copy it
/// through). Using it would skip every snapshot after the first. The two-gate
/// hash design is conservative: it only ever skips on a proven match, so the
/// failure direction is a redundant upload, never a missed backup.
///
/// [`Unchanged`]: SnapshotOutcome::Unchanged
/// [`Deduplicated`]: SnapshotOutcome::Deduplicated
pub async fn snapshot_and_upload(db_path: &str, target: &BackupTarget) -> Result<SnapshotOutcome> {
    // Gate 1 (cheap): source files unchanged since last run -> nothing to do.
    let source_hash = source_fingerprint(db_path)?;
    let prev_source = read_text(&target.store, &target.source_fingerprint_key()).await?;
    if prev_source.as_deref() == Some(source_hash.as_str()) {
        return Ok(SnapshotOutcome::Unchanged { source_hash });
    }

    // Source moved -> take the canonical snapshot. SQLite refuses an existing
    // VACUUM INTO target, so the nanosecond + pid name is unique and removed first.
    let nanos = unix_nanos();
    let temp_path = std::env::temp_dir().join(format!(
        "turso-snapshot-{}-{nanos}.db",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&temp_path);
    vacuum_into(db_path, &temp_path).await?;
    let bytes = std::fs::read(&temp_path)
        .with_context(|| format!("reading vacuum output {}", temp_path.display()))?;
    let _ = std::fs::remove_file(&temp_path);

    // Gate 2 (canonical): the vacuum output is byte-deterministic for identical
    // content, so a match means the bytes moved without the content changing
    // (e.g. an auto-checkpoint reshuffle). Skip the upload, but refresh the
    // cheap source fingerprint so the next run short-circuits at gate 1.
    let snapshot_hash = sha256_hex(&bytes);
    let prev_snapshot = read_text(&target.store, &target.snapshot_fingerprint_key()).await?;
    if prev_snapshot.as_deref() == Some(snapshot_hash.as_str()) {
        put_text(&target.store, &target.source_fingerprint_key(), &source_hash).await?;
        return Ok(SnapshotOutcome::Deduplicated { snapshot_hash });
    }

    // Genuinely new content -> upload, then record both fingerprints.
    let key = target.snapshot_key(nanos);
    let len = bytes.len();
    target
        .store
        .put(&key, bytes.into())
        .await
        .with_context(|| format!("uploading snapshot to {key}"))?;
    put_text(&target.store, &target.snapshot_fingerprint_key(), &snapshot_hash).await?;
    put_text(&target.store, &target.source_fingerprint_key(), &source_hash).await?;

    Ok(SnapshotOutcome::Uploaded {
        key: key.to_string(),
        bytes: len,
        snapshot_hash,
    })
}

/// SHA-256 of the source database files: the main `.db` plus its `-wal`
/// sidecar if present (WAL-mode commits live there until a checkpoint). A
/// quiescent database hashes identically on re-read; any committed write
/// changes the result. Returned as a lowercase hex string.
fn source_fingerprint(db_path: &str) -> Result<String> {
    let mut hasher = Sha256::new();
    let main = std::fs::read(db_path).with_context(|| format!("reading source db {db_path}"))?;
    hasher.update(&main);
    if let Ok(wal) = std::fs::read(format!("{db_path}-wal")) {
        hasher.update(b"\0-wal\0");
        hasher.update(&wal);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Lowercase-hex SHA-256 of an in-memory buffer (used for the canonical
/// snapshot fingerprint).
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Read a small text sidecar (a fingerprint), or `None` if it doesn't exist yet.
async fn read_text(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> Result<Option<String>> {
    match store.get(key).await {
        Ok(res) => {
            let bytes = res.bytes().await.context("reading fingerprint object")?;
            Ok(Some(String::from_utf8_lossy(&bytes).trim().to_string()))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).context("fetching fingerprint object"),
    }
}

/// Write a small text sidecar (a fingerprint).
async fn put_text(store: &Arc<dyn ObjectStore>, key: &ObjPath, text: &str) -> Result<()> {
    store
        .put(key, text.to_owned().into_bytes().into())
        .await
        .with_context(|| format!("recording fingerprint {key}"))?;
    Ok(())
}

/// `VACUUM INTO '<temp_path>'` against the turso DB at `db_path`. VACUUM INTO
/// returns no rows, but turso's `execute` rejects any row-bearing statement, so
/// we drive it through `query` and drain defensively.
async fn vacuum_into(db_path: &str, temp_path: &std::path::Path) -> Result<()> {
    let db = Builder::new_local(db_path)
        .build()
        .await
        .with_context(|| format!("opening turso db {db_path}"))?;
    let conn = db.connect().context("connecting to turso db")?;
    // Single-quote the path SQLite-style (double any embedded quote).
    let escaped = temp_path.to_string_lossy().replace('\'', "''");
    let sql = format!("VACUUM INTO '{escaped}'");
    let mut rows = conn.query(&sql, ()).await.context("VACUUM INTO failed")?;
    while rows.next().await.context("draining VACUUM INTO")?.is_some() {}
    Ok(())
}

/// Join an object-store prefix and a leaf into a normalized [`ObjPath`],
/// tolerating empty/slash-padded prefixes.
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

/// Download the most recent snapshot and write it to `dest_path`, returning the
/// object key it was restored from.
///
/// Lists the snapshot objects under `{prefix}/snapshots/` and picks the newest.
/// Snapshot keys are zero-padded nanosecond timestamps (see
/// [`BackupTarget::snapshot_key`]), so the lexically-greatest key is the most
/// recent. We order by the key's baked-in capture time rather than the store's
/// `last_modified` so the choice is deterministic and immune to clock skew
/// between the snapshotting host and the object store.
///
/// The `snapshots/` prefix naturally excludes the `latest.*-fingerprint`
/// sidecars (they live one level up at `{prefix}/`). The downloaded bytes are a
/// valid vanilla-SQLite file written verbatim to `dest_path`, ready to open with
/// `sqlite3` or re-open through turso.
///
/// Errors if no snapshots exist under the prefix yet.
pub async fn restore_latest(target: &BackupTarget, dest_path: &str) -> Result<String> {
    let snapshots_prefix = join_key(&target.prefix, "snapshots");
    let listing = target
        .store
        .list_with_delimiter(Some(&snapshots_prefix))
        .await
        .with_context(|| format!("listing snapshots under {snapshots_prefix}"))?;

    let newest = listing
        .objects
        .into_iter()
        .max_by(|a, b| a.location.cmp(&b.location))
        .with_context(|| format!("no snapshots found under {snapshots_prefix}"))?;

    let bytes = target
        .store
        .get(&newest.location)
        .await
        .with_context(|| format!("downloading snapshot {}", newest.location))?
        .bytes()
        .await
        .with_context(|| format!("reading snapshot body {}", newest.location))?;

    std::fs::write(dest_path, &bytes)
        .with_context(|| format!("writing restored snapshot to {dest_path}"))?;

    Ok(newest.location.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// A throwaway db path under the OS temp dir, cleaned up on drop.
    struct TempDb(std::path::PathBuf);
    impl TempDb {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!(
                "turso-backup-test-{}-{}-{}.db",
                tag,
                std::process::id(),
                unix_nanos()
            ));
            TempDb(p)
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }

    async fn seed_db(path: &str, rows: i64) {
        let db = Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)", ())
            .await
            .unwrap();
        conn.execute("BEGIN", ()).await.unwrap();
        for i in 0..rows {
            conn.execute("INSERT INTO t (id, v) VALUES (?, ?)", (i, format!("v{i}")))
                .await
                .unwrap();
        }
        conn.execute("COMMIT", ()).await.unwrap();
    }

    async fn count_rows(path: &str) -> i64 {
        let db = Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        let mut r = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
        let row = r.next().await.unwrap().unwrap();
        row.get::<i64>(0).unwrap()
    }

    #[tokio::test]
    async fn uploads_then_skips_unchanged_then_reuploads_on_change() {
        let src = TempDb::new("src");
        seed_db(src.path(), 100).await;

        let target = BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: "backups".into(),
        };

        // First snapshot: uploaded.
        let out = snapshot_and_upload(src.path(), &target).await.unwrap();
        let (key, snap1) = match out {
            SnapshotOutcome::Uploaded { key, bytes, snapshot_hash } => {
                assert!(bytes > 0, "snapshot should be non-empty");
                assert!(key.starts_with("backups/snapshots/snapshot-"), "key was {key}");
                (key, snapshot_hash)
            }
            other => panic!("expected Uploaded, got {other:?}"),
        };

        // The uploaded object is a real vanilla-SQLite file.
        let bytes = target
            .store
            .get(&ObjPath::from(key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            bytes.starts_with(b"SQLite format 3\0"),
            "uploaded object is not a SQLite database"
        );

        // ...and round-trips through turso with the right row count.
        let restored = TempDb::new("restored");
        std::fs::write(restored.path(), &bytes).unwrap();
        assert_eq!(count_rows(restored.path()).await, 100);

        // Gate 1: no source change -> skipped without vacuuming.
        match snapshot_and_upload(src.path(), &target).await.unwrap() {
            SnapshotOutcome::Unchanged { .. } => {}
            other => panic!("expected Unchanged, got {other:?}"),
        }

        // Mutate the source, then snapshot again: uploaded, new snapshot hash.
        {
            let db = Builder::new_local(src.path()).build().await.unwrap();
            let conn = db.connect().unwrap();
            conn.execute("INSERT INTO t (id, v) VALUES (?, ?)", (10_000, "new"))
                .await
                .unwrap();
        }
        match snapshot_and_upload(src.path(), &target).await.unwrap() {
            SnapshotOutcome::Uploaded { snapshot_hash, .. } => assert_ne!(snapshot_hash, snap1),
            other => panic!("expected Uploaded after change, got {other:?}"),
        }
    }

    /// Gate 2: when the source bytes look changed but the canonical VACUUM
    /// output matches the last snapshot, the upload is deduplicated. We simulate
    /// the "source moved without content change" case by deleting the cheap
    /// source-fingerprint sidecar so gate 1 misses and the vacuum runs.
    #[tokio::test]
    async fn deduplicates_when_vacuum_output_matches() {
        let src = TempDb::new("dedup-src");
        seed_db(src.path(), 50).await;

        let target = BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: String::new(),
        };

        let snap1 = match snapshot_and_upload(src.path(), &target).await.unwrap() {
            SnapshotOutcome::Uploaded { snapshot_hash, .. } => snapshot_hash,
            other => panic!("expected Uploaded, got {other:?}"),
        };

        // Force gate 1 to miss without changing content.
        target
            .store
            .delete(&target.source_fingerprint_key())
            .await
            .unwrap();

        match snapshot_and_upload(src.path(), &target).await.unwrap() {
            SnapshotOutcome::Deduplicated { snapshot_hash } => assert_eq!(snapshot_hash, snap1),
            other => panic!("expected Deduplicated, got {other:?}"),
        }

        // And the refreshed source fingerprint makes the next run short-circuit.
        match snapshot_and_upload(src.path(), &target).await.unwrap() {
            SnapshotOutcome::Unchanged { .. } => {}
            other => panic!("expected Unchanged after dedup refresh, got {other:?}"),
        }
    }

    /// restore_latest errors when empty, then downloads the newest of several
    /// snapshots and round-trips it back through turso with the latest row count.
    #[tokio::test]
    async fn restore_latest_picks_newest_and_round_trips() {
        let src = TempDb::new("restore-src");
        seed_db(src.path(), 100).await;

        let target = BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: "backups".into(),
        };

        // No snapshots yet -> error (and no file written to dest).
        let dest = TempDb::new("restore-dest");
        assert!(
            restore_latest(&target, dest.path()).await.is_err(),
            "restore should fail when no snapshots exist"
        );

        // First snapshot (100 rows).
        snapshot_and_upload(src.path(), &target).await.unwrap();

        // Mutate, then a second snapshot (101 rows) -> a lexically-newer key.
        {
            let db = Builder::new_local(src.path()).build().await.unwrap();
            let conn = db.connect().unwrap();
            conn.execute("INSERT INTO t (id, v) VALUES (?, ?)", (10_000, "new"))
                .await
                .unwrap();
        }
        let newest_key = match snapshot_and_upload(src.path(), &target).await.unwrap() {
            SnapshotOutcome::Uploaded { key, .. } => key,
            other => panic!("expected Uploaded, got {other:?}"),
        };

        // restore_latest reports it pulled the newest snapshot...
        let restored_from = restore_latest(&target, dest.path()).await.unwrap();
        assert_eq!(restored_from, newest_key, "should restore the newest snapshot");

        // ...the on-disk file is a valid vanilla-SQLite database...
        let on_disk = std::fs::read(dest.path()).unwrap();
        assert!(
            on_disk.starts_with(b"SQLite format 3\0"),
            "restored file is not a SQLite database"
        );

        // ...and re-opens through turso with the post-mutation row count.
        assert_eq!(count_rows(dest.path()).await, 101);
    }
}
