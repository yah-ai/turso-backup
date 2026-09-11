//! Tier 1b — incremental page-dedup snapshot.
//!
//! Raw point-in-time-consistent copy of the live `.db` (NOT `VACUUM INTO`,
//! which reshuffles pages and defeats dedup) → split into fixed 4 KB pages →
//! content-address (hash) each → upload only changed pages + a per-snapshot
//! manifest. Restore reassembles from the manifest. Engine-agnostic.
//!
//! @yah:relay(R004, "Tier 1b — incremental page-dedup snapshot")
//! @yah:at(2026-05-26T22:28:31Z)
//! @yah:status(open)
//! @yah:phase(P2)
//! @yah:parent(Q002)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//!
//! @yah:ticket(R004-T1, "Spike: raw point-in-time-consistent copy primitive (main+WAL); confirm pages keep fixed offsets across snapshots")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:06Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R004)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:handoff("Built dedup::raw_consistent_copy(db_path)->Vec<u8> + dedup::page_size(image). Primitive = PRAGMA wal_checkpoint(TRUNCATE) (turso supports it) to fold WAL->main, drop conn, fs::read main file. NOT vacuum (no repack). Confirmed point-in-time consistent main+WAL: writing only the main bytes to a fresh path reopens with the full row count.")
//! @yah:handoff("Page-offset stability CONFIRMED and it is structural: page N is always at (N-1)*page_size; existing pages never renumber on a raw write. Measured 1-row insert into a 5000-row DB = 1/18 pages changed. page_size read from header u16@16 = 4096 (turso default).")
//! @yah:handoff("CORRECTION to working doc: 'vacuum reshuffles pages and defeats dedup' is too strong. Append-only: raw 1/18 == vacuum 1/18 (equivalent). Vacuum only diverges under reclamation: delete-low-block = raw 10/18 vs vacuum 18/18 (vacuum renumbers all survivors). Reason to copy raw = its offset stability holds for every mutation shape; vacuum's is incidental. Doc updated with the spike findings section.")
//! @yah:verify("cargo test -p turso-backup dedup -- --nocapture: 3 spike tests green (raw_copy_keeps_page_offsets_stable, raw_copy_beats_vacuum_as_dedup_base, raw_copy_folds_wal_and_is_self_contained). Full suite 6/6, clippy --all-targets clean.")
//! @yah:next("R004-F2: chunk raw_consistent_copy output on page_size(), sha256 each page, diff against prior manifest, put only changed pages content-addressed + write manifest. Caller must release write conns first (snapshot/DR model) — a live writer can make the TRUNCATE checkpoint return busy; read-only WAL-replay copy is tier-2.")
//!
//! @yah:ticket(R004-F2, "Page chunking + content-addressed dedup upload (changed pages only) + manifest format")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:07Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R004)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R004-T1)
//! @yah:handoff("Built snapshot_dedup(db_path, &BackupTarget)->DedupOutcome on top of raw_consistent_copy: chunk on header page_size, sha256 each page, diff vs latest manifest, put only new pages content-addressed at pages/{hash}, write manifests/manifest-{nanos:020}.manifest last. Reuses tier-1a BackupTarget. DedupOutcome::{Snapshotted{total/uploaded/reused},Unchanged}. Manifest = dep-free text format (header + page_size + page_count + ordered hex hashes); to_bytes/from_bytes + image_len. load_latest_manifest is pub for F3.")
//! @yah:handoff("Measured: first snapshot of 5000 rows puts all 18 pages + 1 manifest; a 1-row insert then puts exactly 1 page + a 2nd manifest (pages/ grows by 1); both manifests reassemble to valid SQLite (5000/5001 rows). Unchanged path: re-snapshot with no change uploads nothing, writes no new manifest. Note: page-blob GC (unreferenced pages) is out of scope — flag for a later tier.")
//! @yah:verify("cargo test -p turso-backup: 9/9 green (3 dedup-F2: dedup_uploads_only_changed_pages_and_reassembles, dedup_unchanged_when_pages_identical, manifest_round_trips). clippy --all-targets clean. Run from turso-backup/ dir, NOT db_test root (root yah workspace has unrelated pre-existing compile errors in crates/yah/forms).")
//! @yah:next("R004-F3: implement restore_from_manifest(target, manifest, dest) — fetch each pages/{hash} in order, concat, write file. The test helper reassemble_latest already does this; F3 packages it + a restore_latest_dedup that calls load_latest_manifest first. Then R004-T4: harness e2e (two snapshots, assert only changed pages uploaded, restore both).")
//!
//! @yah:ticket(R004-F3, "Restore from manifest: fetch pages by hash, reassemble valid SQLite")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:07Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R004)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R004-F2)
//! @yah:handoff("Built restore_from_manifest(target, manifest, dest) + restore_latest_dedup(target, dest). Restore fetches each pages/{hash} in manifest order, VERIFIES content hash (corrupt/truncated blob -> loud error, not a silent-corrupt DB), concats, asserts SQLite magic, writes dest. No -wal needed (snapshot folded WAL in). restore_latest_dedup = load_latest_manifest (newest by nanos key) then restore_from_manifest; errors if no snapshot. Tier-1b counterpart to tier-1a snapshot::restore_latest.")
//! @yah:verify("cargo test -p turso-backup: 11/11 green (3 F3: restore_latest_dedup_picks_newest_and_round_trips, restore_rejects_corrupt_page; empty-prefix error covered). clippy --all-targets clean. Run from turso-backup/ dir.")
//! @yah:next("R004-T4: harness e2e against MinIO — wire writer to snapshot_dedup, take two snapshots with a mutation between, assert only changed pages uploaded (DedupOutcome.uploaded_pages), restore_latest_dedup both, sqlite3 verify. Lib round-trips use InMemory; T4 proves it on the object_store aws backend.")
//!
//! @yah:ticket(R004-T4, "Harness e2e incremental: two snapshots, assert only changed pages uploaded, restore both")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-26T22:30:08Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R004)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R004-F3)
//! @yah:handoff("GREEN e2e against MinIO. Added a tier-1b docker flow parallel to tier-1a (own bins + bucket prefix 'dedup'; tier-1a writer/verifier/run.sh untouched): harness bins dedup_writer (2 snapshots w/ a 5-row append between; ASSERTS uploaded2<uploaded1 && reused2>0 so a dedup regression fails the build) + dedup_restore (by MANIFEST_KEY or latest); shared row helpers in harness/src/lib.rs; verify_dedup.sh restores BOTH (snapshot1 by key, snapshot2 latest) + sqlite3 integrity/rows/boundary/hash; Dockerfile dedup-writer/dedup-verifier targets; compose services; ci/run_dedup.sh (staged). Lib gained dedup::restore_manifest_key (point-in-time restore by key).")
//! @yah:handoff("Result at 5000 rows + 5-row append: snapshot1 109/109 pages uploaded (446KB), snapshot2 1/109 uploaded + 108 reused (only the tail leaf changed; page 1 byte-identical since change_counter is dead + file didn't grow). Both restored to vanilla SQLite: by-key=5000 rows, latest=5005 rows, full-table hashes matched byte-for-byte, exit 0. Restoring snapshot1 by key proves its pages survived snapshot2's deduplicated upload.")
//! @yah:verify("bash ci/run_dedup.sh (staged: minio -> minio-init -> dedup-writer -> dedup-verifier; verifier exit 0 = green). Lib: cargo test -p turso-backup = 12/12, clippy --all-targets clean (run from turso-backup/ dir).")
//! @yah:next("R004 complete pending sign-off: all 4 children (T1/F2/F3/T4) in review. Followups filed: page-blob GC = relay R006; live-writer path = R005-F4. Tier 2 (R005, WAL-frame streaming) is the next major relay.")
//!
//! @yah:relay(R006, "Tier 1b — snapshot retention + page-blob GC")
//! @yah:at(2026-05-27T03:10:00Z)
//! @yah:status(open)
//! @yah:phase(P2)
//! @yah:parent(Q002)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//!
//! @yah:ticket(R006-F1, "Retention + mark-and-sweep GC: keep the most recent N manifests, delete pages/{hash} blobs no retained manifest references, prune the pruned manifests")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-27T03:10:01Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R006)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:gotcha("R004's snapshot_dedup never deletes pages — pages/{hash} blobs accumulate forever as content changes. GC = compute the union of page hashes across the retained manifests, delete every pages/* blob outside that union, then delete the non-retained manifests. Design the retained-set so a concurrent restore can't lose a page mid-sweep (delete manifests last / GC infrequently).")
//! @yah:handoff("Implemented dedup::gc_dedup(target, keep_n) -> GcOutcome. Mark-and-sweep: list manifests/, the lexically-greatest keep_n are retained; download each, union their page_hashes into the retain set; list pages/, delete every blob whose leaf isn't in the retain union; finally delete the pruned manifests. Order is pages-first / manifests-last so a retained manifest never references a missing page at any moment of a partial GC, and restore_latest_dedup (which targets the newest = retained) is safe through the whole sweep.")
//! @yah:handoff("GcOutcome reports {retained_manifests, pruned_manifests, deleted_pages, retained_pages}. keep_n >= total is a no-op (saturating_sub). restore_manifest_key against a soon-to-be-pruned manifest is documented as inherently racy — schedule GC infrequently, in windows where no point-in-time restores are in flight.")
//! @yah:next("R006-T2: the F1 lib tests already cover keep-N retains restorable + orphan pages deleted + newest still restores byte-for-byte. The piece T2 still owns: prove an *in-window older* snapshot (e.g. GC keep_n=2 with 3 snapshots → assert older retained manifest still restores byte-for-byte via restore_manifest_key). Optionally extend the dedup-writer/verifier harness to call gc_dedup between snapshots and re-verify both restores against MinIO, parallel to R004-T4's tier-1b e2e shape.")
//! @yah:verify("cargo test -p turso-backup: 41/41 lib green (3 new GC tests: gc_prunes_older_manifests_and_sweeps_orphan_pages, gc_keep_n_at_or_above_total_is_noop, gc_keeps_pages_shared_with_retained_manifests). cargo clippy --all-targets clean. Run from turso-backup/ dir.")
//!
//! @yah:ticket(R006-T2, "Test GC: keep-N retains restorable snapshots, orphaned pages deleted, latest + an in-window older snapshot still restore byte-for-byte")
//! @yah:assignee(agent:claude)
//! @yah:at(2026-05-27T03:10:02Z)
//! @yah:status(review)
//! @yah:phase(P2)
//! @yah:parent(R006)
//! @arch:see(.yah/docs/working/turso-s3-backup.md)
//! @yah:depends_on(R006-F1)
//! @yah:handoff("Added gc_keep_two_lets_in_window_older_snapshot_restore_byte_for_byte: takes 3 snapshots with mutations between, restores the middle snapshot via restore_manifest_key BEFORE GC as ground-truth bytes, runs gc_dedup(target, 2), then asserts (a) snapshot 1's pruned key errors on restore, (b) the in-window older snapshot 2 restores BYTE-FOR-BYTE equal to the pre-GC bytes, (c) restore_latest_dedup picks snapshot 3 with the post-second-mutation row count. Locks in the property that GC's mark set covers every retained manifest, not just the one restore_latest_dedup picks.")
//! @yah:handoff("F1's three tests already covered keep-N retain + orphan delete + newest-restores-byte-for-byte + shared-page survival; T2 added the missing piece (in-window older snapshot byte-equality). 42/42 lib tests green, clippy clean.")
//! @yah:handoff("NB: the turso-backup crate was relocated from external/db_test/turso-backup to external/turso-backup during this session — board tools now need path=/Users/user/ss/yah to resolve R006 tickets.")
//! @yah:next("R006 complete pending sign-off: both children (F1, T2) in review. Optional follow-up — extend dedup-writer/verifier docker harness to call gc_dedup between snapshots and re-verify both restores against MinIO, parallel to R004-T4's tier-1b e2e shape. Not blocking R006 archive.")
//! @yah:verify("cd /Users/user/ss/yah/external/turso-backup && cargo test --lib: 42/42 green (4 GC tests under dedup::tests::gc*). cargo clippy --all-targets clean.")

use crate::snapshot::BackupTarget;
use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use turso::Builder;

/// SQLite stores the database page size as a big-endian `u16` at byte offset 16
/// of the file header. The encoded value `1` means the maximum 65 536-byte page.
const PAGE_SIZE_OFFSET: usize = 16;

/// The SQLite file-format magic, present at the start of every valid database
/// (and every `VACUUM INTO` / raw-copy image we produce).
const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";

/// Read the page size (in bytes) from a SQLite file image's header.
///
/// Tier 1b chunks on **real page boundaries**, so the dedup unit is whatever the
/// engine actually uses — read it from the header rather than assuming 4 KB.
pub fn page_size(image: &[u8]) -> Result<usize> {
    anyhow::ensure!(image.starts_with(SQLITE_MAGIC), "not a SQLite database (bad magic)");
    anyhow::ensure!(image.len() >= PAGE_SIZE_OFFSET + 2, "image too short for a header");
    let raw = u16::from_be_bytes([image[PAGE_SIZE_OFFSET], image[PAGE_SIZE_OFFSET + 1]]);
    Ok(if raw == 1 { 65_536 } else { raw as usize })
}

/// Take a raw, point-in-time-consistent byte image of the database at `db_path`,
/// preserving its native page layout. This is the tier-1b foundation and is
/// deliberately **not** `VACUUM INTO`: vacuum repacks the b-tree into a fresh
/// file, relocating pages and defeating content-addressed dedup.
///
/// We fold any pending WAL frames into the main file with a `TRUNCATE`
/// checkpoint, then read the main file. After the checkpoint the main file is
/// the complete, self-consistent image — page `N` lives at byte offset
/// `(N - 1) * page_size`, so a logically-unchanged page hashes identically
/// across snapshots. That fixed-offset stability is the precondition that makes
/// "upload only changed pages" actually save bytes (confirmed by the spike tests
/// below: a one-row insert into a 5 000-row DB rewrites only a handful of pages
/// via this path, but a large fraction of them via `VACUUM INTO`).
///
/// Consistency comes from folding **main + WAL** into one image: any committed
/// frame still sitting in `-wal` is written back to its page slot, so the
/// returned bytes open in vanilla SQLite with no sidecar.
///
/// Assumes no live writer holds the WAL — the snapshot/DR model, where the
/// caller has released its write connections (as the harness writer does before
/// backing up). A concurrent writer can make a `TRUNCATE` checkpoint return
/// `busy`; the read-only "copy main + replay WAL frames ourselves" path that
/// would cover live writers is tier-2 territory.
pub async fn raw_consistent_copy(db_path: &str) -> Result<Vec<u8>> {
    // Fold WAL -> main and reset the WAL so the main file alone is complete.
    {
        let db = Builder::new_local(db_path)
            .build()
            .await
            .with_context(|| format!("opening turso db {db_path}"))?;
        let conn = db.connect().context("connecting to turso db")?;
        let mut rows = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
            .context("PRAGMA wal_checkpoint(TRUNCATE)")?;
        while rows.next().await.context("draining wal_checkpoint")?.is_some() {}
    } // drop the connection before reading the file off disk

    let bytes = std::fs::read(db_path).with_context(|| format!("reading db image {db_path}"))?;
    anyhow::ensure!(
        bytes.starts_with(SQLITE_MAGIC),
        "raw copy of {db_path} is not a SQLite database"
    );
    Ok(bytes)
}

/// The first line of every serialized [`Manifest`] — magic + format version.
const MANIFEST_HEADER: &str = "TURSO-BACKUP MANIFEST v1";

/// A per-snapshot manifest: the page size plus the ordered content hashes of the
/// database's pages. Page `i` of the restored file is the blob stored at
/// `pages/{page_hashes[i]}`, so the manifest alone is enough to reassemble the
/// whole image (see [`restore_from_manifest`]).
///
/// Serialized as a small, dependency-free text format (no serde):
///
/// ```text
/// TURSO-BACKUP MANIFEST v1
/// page_size 4096
/// page_count 18
/// <hex sha256 of page 0>
/// <hex sha256 of page 1>
/// ...
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub page_size: usize,
    /// Lowercase-hex SHA-256 of each page, in file order.
    pub page_hashes: Vec<String>,
}

impl Manifest {
    /// Serialize to the text wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut s = String::with_capacity(64 + self.page_hashes.len() * 65);
        s.push_str(MANIFEST_HEADER);
        s.push('\n');
        s.push_str(&format!("page_size {}\n", self.page_size));
        s.push_str(&format!("page_count {}\n", self.page_hashes.len()));
        for h in &self.page_hashes {
            s.push_str(h);
            s.push('\n');
        }
        s.into_bytes()
    }

    /// Parse the text wire format, validating the header and page count.
    pub fn from_bytes(bytes: &[u8]) -> Result<Manifest> {
        let text = std::str::from_utf8(bytes).context("manifest is not valid UTF-8")?;
        let mut lines = text.lines();
        let header = lines.next().context("empty manifest")?;
        anyhow::ensure!(header == MANIFEST_HEADER, "unexpected manifest header: {header:?}");
        let page_size = parse_header_field(lines.next(), "page_size")?;
        let page_count = parse_header_field(lines.next(), "page_count")?;
        let page_hashes: Vec<String> = lines.map(|l| l.trim().to_string()).collect();
        anyhow::ensure!(
            page_hashes.len() == page_count,
            "manifest page_count {page_count} != {} hash lines",
            page_hashes.len()
        );
        Ok(Manifest { page_size, page_hashes })
    }

    /// Total reassembled image size in bytes.
    pub fn image_len(&self) -> usize {
        self.page_size * self.page_hashes.len()
    }
}

/// Parse a `"<name> <usize>"` header line.
fn parse_header_field(line: Option<&str>, name: &str) -> Result<usize> {
    let line = line.with_context(|| format!("manifest missing {name}"))?;
    let value = line
        .strip_prefix(name)
        .and_then(|rest| rest.trim().parse::<usize>().ok())
        .with_context(|| format!("malformed manifest {name} line: {line:?}"))?;
    Ok(value)
}

/// Outcome of a [`snapshot_dedup`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupOutcome {
    /// A new manifest was written. `uploaded_pages` is the number of distinct
    /// pages put this run (changed or new); `reused_pages` were already present
    /// from an earlier snapshot and so were *not* re-uploaded.
    Snapshotted {
        manifest_key: String,
        total_pages: usize,
        uploaded_pages: usize,
        reused_pages: usize,
    },
    /// The page set was byte-identical to the latest manifest, so nothing was
    /// uploaded and no new manifest was written — the prior one still stands.
    Unchanged { manifest_key: String },
}

/// Take a raw consistent snapshot of the database at `db_path` and upload only
/// the pages that changed since the last snapshot, plus a per-snapshot manifest.
/// Tier 1b.
///
/// 1. [`raw_consistent_copy`] the database (page layout preserved — the
///    precondition for dedup, see that fn).
/// 2. Split on the header [`page_size`] and SHA-256 each page.
/// 3. Load the latest manifest (if any). If this snapshot's ordered page hashes
///    are identical, return [`Unchanged`] without uploading.
/// 4. Otherwise upload every page whose hash isn't already referenced by the
///    prior manifest — content-addressed at `{prefix}/pages/{hash}`, so
///    identical pages (within or across snapshots) are stored once — then write
///    the manifest at `{prefix}/manifests/manifest-{unix_nanos:020}.manifest`.
///
/// The prior-manifest diff is conservative: a page present in the store but not
/// in the prior manifest is re-uploaded (idempotent — same content-addressed
/// key), so the failure direction is a redundant put, never a missing page.
///
/// [`Unchanged`]: DedupOutcome::Unchanged
pub async fn snapshot_dedup(db_path: &str, target: &BackupTarget) -> Result<DedupOutcome> {
    let image = raw_consistent_copy(db_path).await?;
    snapshot_dedup_image(&image, target).await
}

/// Steps 2–4 of [`snapshot_dedup`] against an image the caller already holds.
///
/// R850-F1 split this out for the caller [`snapshot_dedup`] cannot serve: a tail
/// running **beside a live application** that holds the database open. Step 1's
/// [`raw_consistent_copy`] takes a `PRAGMA wal_checkpoint(TRUNCATE)` through an
/// ordinary writable connection, and turso locks the whole file at open — so
/// against a held database that step does not merely block the checkpoint, it
/// fails to open at all (measured: `examples/appliance_tail_probe.rs`, A1).
/// Such a caller produces its image with
/// [`stream::raw_consistent_copy_live`](crate::stream::raw_consistent_copy_live)
/// instead, which opens read-only and validates optimistically, and hands it
/// here.
///
/// The page-layout precondition [`snapshot_dedup`] documents is unchanged and is
/// now the caller's to keep: the image must be a raw page-preserving copy, not
/// `VACUUM INTO` output, or every page hash moves on every run and the dedup
/// stores a fresh full copy each time.
pub async fn snapshot_dedup_image(image: &[u8], target: &BackupTarget) -> Result<DedupOutcome> {
    let ps = page_size(image)?;

    let page_hashes: Vec<String> = image.chunks(ps).map(sha256_hex).collect();
    let total_pages = page_hashes.len();

    // Latest manifest = the prior snapshot's page set (empty on the first run).
    let prior = load_latest_manifest(target).await?;
    if let Some((key, manifest)) = &prior {
        if manifest.page_hashes == page_hashes {
            return Ok(DedupOutcome::Unchanged { manifest_key: key.clone() });
        }
    }
    let already: HashSet<&str> = prior
        .as_ref()
        .map(|(_, m)| m.page_hashes.iter().map(String::as_str).collect())
        .unwrap_or_default();

    // Distinct pages new to this snapshot (dedups identical pages within it too).
    let mut to_upload: HashMap<&str, &[u8]> = HashMap::new();
    for (hash, page) in page_hashes.iter().zip(image.chunks(ps)) {
        if !already.contains(hash.as_str()) {
            to_upload.insert(hash.as_str(), page);
        }
    }
    let uploaded_pages = to_upload.len();
    for (hash, page) in &to_upload {
        let key = page_key(target, hash);
        target
            .store
            .put(&key, page.to_vec().into())
            .await
            .with_context(|| format!("uploading page {key}"))?;
    }

    // Write the manifest last, so a manifest never references a missing page.
    let manifest = Manifest { page_size: ps, page_hashes };
    let key = manifest_key(target, unix_nanos());
    target
        .store
        .put(&key, manifest.to_bytes().into())
        .await
        .with_context(|| format!("uploading manifest {key}"))?;

    Ok(DedupOutcome::Snapshotted {
        manifest_key: key.to_string(),
        total_pages,
        uploaded_pages,
        reused_pages: total_pages - uploaded_pages,
    })
}

/// Load the most recent manifest under `{prefix}/manifests/`, returning its key
/// and parsed contents, or `None` if no snapshot has been taken yet. Manifest
/// keys are zero-padded nanosecond timestamps, so the lexically-greatest key is
/// the newest (clock-skew-immune, matching tier 1a's `restore_latest`). Shared
/// with restore (R004-F3).
pub async fn load_latest_manifest(target: &BackupTarget) -> Result<Option<(String, Manifest)>> {
    let prefix = join_key(&target.prefix, "manifests");
    let listing = target
        .store
        .list_with_delimiter(Some(&prefix))
        .await
        .with_context(|| format!("listing manifests under {prefix}"))?;
    let Some(newest) = listing.objects.into_iter().max_by(|a, b| a.location.cmp(&b.location)) else {
        return Ok(None);
    };
    let bytes = target
        .store
        .get(&newest.location)
        .await
        .with_context(|| format!("downloading manifest {}", newest.location))?
        .bytes()
        .await
        .with_context(|| format!("reading manifest body {}", newest.location))?;
    let manifest = Manifest::from_bytes(&bytes)
        .with_context(|| format!("parsing manifest {}", newest.location))?;
    Ok(Some((newest.location.to_string(), manifest)))
}

/// Reassemble a database file from a manifest + content-addressed pages, writing
/// it to `dest`. Tier 1b restore.
///
/// Fetches each `pages/{hash}` in manifest order, **verifies its content hash**
/// (the page is content-addressed, so a hash mismatch means a corrupt or
/// truncated blob — fail loudly rather than write a silently-broken database),
/// concatenates, and writes the image. The result is a valid vanilla-SQLite file
/// needing no `-wal` sidecar — the snapshot folded WAL into the image.
pub async fn restore_from_manifest(
    target: &BackupTarget,
    manifest: &Manifest,
    dest: &str,
) -> Result<()> {
    let mut image = Vec::with_capacity(manifest.image_len());
    for (i, hash) in manifest.page_hashes.iter().enumerate() {
        let key = page_key(target, hash);
        let bytes = target
            .store
            .get(&key)
            .await
            .with_context(|| format!("fetching page {i} ({key})"))?
            .bytes()
            .await
            .with_context(|| format!("reading page {i} body ({key})"))?;
        let got = sha256_hex(&bytes);
        anyhow::ensure!(
            &got == hash,
            "page {i} ({key}) content hash {got} != manifest {hash} — corrupt or truncated blob"
        );
        image.extend_from_slice(&bytes);
    }
    anyhow::ensure!(
        image.starts_with(SQLITE_MAGIC),
        "reassembled image is not a SQLite database"
    );
    std::fs::write(dest, &image).with_context(|| format!("writing restored db to {dest}"))?;
    Ok(())
}

/// Restore the most recent snapshot for `target` to `dest`, returning the
/// manifest key it restored from. Errors if no snapshot exists under the prefix.
/// This is the tier-1b counterpart to tier 1a's `snapshot::restore_latest`.
pub async fn restore_latest_dedup(target: &BackupTarget, dest: &str) -> Result<String> {
    let (key, manifest) = load_latest_manifest(target)
        .await?
        .context("no snapshot manifest found under prefix")?;
    restore_from_manifest(target, &manifest, dest).await?;
    Ok(key)
}

/// Restore a *specific* snapshot, named by its manifest object key (as returned
/// in [`DedupOutcome::Snapshotted::manifest_key`]), to `dest`. Use this for a
/// point-in-time restore of an older snapshot; [`restore_latest_dedup`] covers
/// the common "newest" case. Fetches and parses the manifest object, then defers
/// to [`restore_from_manifest`] (so the same per-page content-hash verification
/// applies).
pub async fn restore_manifest_key(target: &BackupTarget, manifest_key: &str, dest: &str) -> Result<()> {
    let key = ObjPath::from(manifest_key);
    let bytes = target
        .store
        .get(&key)
        .await
        .with_context(|| format!("fetching manifest {key}"))?
        .bytes()
        .await
        .with_context(|| format!("reading manifest body {key}"))?;
    let manifest =
        Manifest::from_bytes(&bytes).with_context(|| format!("parsing manifest {key}"))?;
    restore_from_manifest(target, &manifest, dest).await
}

/// Outcome of a [`gc_dedup`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcOutcome {
    /// Number of manifests retained (the `keep_n` newest, or fewer if the store
    /// holds less).
    pub retained_manifests: usize,
    /// Number of older manifests deleted from `manifests/`.
    pub pruned_manifests: usize,
    /// Number of `pages/{hash}` blobs deleted (referenced by no retained
    /// manifest).
    pub deleted_pages: usize,
    /// Number of distinct page hashes referenced by the retained manifests.
    /// Every blob with one of these hashes survives the sweep.
    pub retained_pages: usize,
}

/// Mark-and-sweep GC: keep the `keep_n` newest manifests, delete every
/// `pages/{hash}` blob no retained manifest references, then prune the older
/// manifests.
///
/// Manifest keys are zero-padded nanosecond timestamps, so lexical order is
/// chronological — the `keep_n` lexically-greatest keys are the newest
/// snapshots. If the store holds `<= keep_n` manifests, nothing is pruned.
///
/// ## Sweep order
///
/// 1. List `manifests/`, split into retained (newest `keep_n`) vs pruned.
/// 2. Download + parse the retained manifests; union their `page_hashes`.
/// 3. List `pages/`, delete every blob whose leaf isn't in the retained union.
/// 4. **Delete the pruned manifests last.**
///
/// Concurrent-restore safety: [`restore_latest_dedup`] always targets the
/// lexically-greatest manifest, which is in the retain set, and *all* of its
/// pages are in the retain union — so it survives a concurrent GC. The
/// `manifests-last` ordering means that even if we crash mid-sweep, every
/// surviving manifest still resolves: orphan pages may briefly outlive their
/// (already-pruned) referrers, but no retained manifest ever points at a
/// missing page. The reverse order would risk a half-deleted retain set.
///
/// `restore_manifest_key` against a manifest in the prune set is inherently a
/// race — the caller is asking to restore something we've decided to discard.
/// Schedule GC to run when no point-in-time restores are in flight.
pub async fn gc_dedup(target: &BackupTarget, keep_n: usize) -> Result<GcOutcome> {
    // 1. List manifests, sort lexically (== chronologically).
    let manifests_prefix = join_key(&target.prefix, "manifests");
    let manifests_listing = target
        .store
        .list_with_delimiter(Some(&manifests_prefix))
        .await
        .with_context(|| format!("listing manifests under {manifests_prefix}"))?;
    let mut all_manifests: Vec<ObjPath> =
        manifests_listing.objects.into_iter().map(|o| o.location).collect();
    all_manifests.sort();

    // Split: the newest `keep_n` retained, the rest pruned. If keep_n exceeds
    // the count, every manifest is retained and there's nothing to prune.
    let total = all_manifests.len();
    let split = total.saturating_sub(keep_n);
    let pruned: Vec<ObjPath> = all_manifests.iter().take(split).cloned().collect();
    let retained: Vec<ObjPath> = all_manifests.into_iter().skip(split).collect();

    // 2. Compute the union of page hashes referenced by the retained manifests.
    let mut retained_hashes: HashSet<String> = HashSet::new();
    for key in &retained {
        let bytes = target
            .store
            .get(key)
            .await
            .with_context(|| format!("fetching retained manifest {key}"))?
            .bytes()
            .await
            .with_context(|| format!("reading retained manifest body {key}"))?;
        let manifest = Manifest::from_bytes(&bytes)
            .with_context(|| format!("parsing retained manifest {key}"))?;
        retained_hashes.extend(manifest.page_hashes);
    }

    // 3. List pages, delete every blob whose hash isn't in the retain union.
    // Pages are stored at `{prefix}/pages/{hash}` with no further slashes, so
    // list_with_delimiter returns each one directly under `objects`.
    let pages_prefix = join_key(&target.prefix, "pages");
    let pages_listing = target
        .store
        .list_with_delimiter(Some(&pages_prefix))
        .await
        .with_context(|| format!("listing pages under {pages_prefix}"))?;
    let mut deleted_pages = 0usize;
    for obj in pages_listing.objects {
        let leaf = obj
            .location
            .filename()
            .with_context(|| format!("page key has no leaf: {}", obj.location))?
            .to_string();
        if !retained_hashes.contains(&leaf) {
            target
                .store
                .delete(&obj.location)
                .await
                .with_context(|| format!("deleting orphan page {}", obj.location))?;
            deleted_pages += 1;
        }
    }

    // 4. Delete the pruned manifests LAST (see fn doc — keeps retained
    // manifests fully resolvable through the entire sweep).
    for key in &pruned {
        target
            .store
            .delete(key)
            .await
            .with_context(|| format!("deleting pruned manifest {key}"))?;
    }

    Ok(GcOutcome {
        retained_manifests: retained.len(),
        pruned_manifests: pruned.len(),
        deleted_pages,
        retained_pages: retained_hashes.len(),
    })
}

/// Object key for a content-addressed page blob.
fn page_key(target: &BackupTarget, hash: &str) -> ObjPath {
    join_key(&target.prefix, &format!("pages/{hash}"))
}

/// Object key for a snapshot's manifest, taken at `unix_nanos` (zero-padded so
/// lexical order matches chronological order).
fn manifest_key(target: &BackupTarget, unix_nanos: u128) -> ObjPath {
    join_key(&target.prefix, &format!("manifests/manifest-{unix_nanos:020}.manifest"))
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

/// Lowercase-hex SHA-256 of a byte buffer.
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
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
    use std::sync::Arc;

    /// A throwaway db path under the OS temp dir, cleaned up (with sidecars) on drop.
    struct TempDb(std::path::PathBuf);
    impl TempDb {
        fn new(tag: &str) -> Self {
            TempDb(std::env::temp_dir().join(format!(
                "turso-dedup-test-{tag}-{}-{}.db",
                std::process::id(),
                unix_nanos()
            )))
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

    async fn insert_rows(path: &str, start: i64, n: i64) {
        let db = Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        for i in 0..n {
            conn.execute("INSERT INTO t (id, v) VALUES (?, ?)", (start + i, format!("v{}", start + i)))
                .await
                .unwrap();
        }
    }

    async fn count_rows(path: &str) -> i64 {
        let db = Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        let mut r = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
        let row = r.next().await.unwrap().unwrap();
        row.get::<i64>(0).unwrap()
    }

    /// `VACUUM INTO` a fresh file and return its bytes — the contrast case that
    /// shows why tier 1b copies raw instead of vacuuming.
    async fn vacuum_image(db_path: &str) -> Vec<u8> {
        let out = std::env::temp_dir().join(format!(
            "turso-dedup-vac-{}-{}.db",
            std::process::id(),
            unix_nanos()
        ));
        let _ = std::fs::remove_file(&out);
        let db = Builder::new_local(db_path).build().await.unwrap();
        let conn = db.connect().unwrap();
        let escaped = out.to_string_lossy().replace('\'', "''");
        let mut rows = conn.query(&format!("VACUUM INTO '{escaped}'"), ()).await.unwrap();
        while rows.next().await.unwrap().is_some() {}
        drop(conn);
        let bytes = std::fs::read(&out).unwrap();
        let _ = std::fs::remove_file(&out);
        bytes
    }

    /// Count pages that differ at the same offset between two images, padding the
    /// shorter image's tail to `total` (a grown file = appended pages = changed).
    fn changed_pages(a: &[u8], b: &[u8], ps: usize) -> (usize, usize) {
        let pa: Vec<&[u8]> = a.chunks(ps).collect();
        let pb: Vec<&[u8]> = b.chunks(ps).collect();
        let total = pa.len().max(pb.len());
        let mut changed = 0;
        for i in 0..total {
            if pa.get(i) != pb.get(i) {
                changed += 1;
            }
        }
        (changed, total)
    }

    /// SPIKE R004-T1 (headline): a raw consistent copy keeps unchanged pages at
    /// fixed offsets, so a one-row mutation rewrites only a few pages — the
    /// precondition for content-addressed dedup.
    #[tokio::test]
    async fn raw_copy_keeps_page_offsets_stable() {
        let src = TempDb::new("offsets");
        seed_db(src.path(), 5000).await;

        let a = raw_consistent_copy(src.path()).await.unwrap();
        let ps = page_size(&a).unwrap();
        assert_eq!(ps, 4096, "expected turso's default 4 KB page, got {ps}");

        // A single-row insert: the smallest meaningful mutation.
        insert_rows(src.path(), 1_000_000, 1).await;
        let b = raw_consistent_copy(src.path()).await.unwrap();
        assert_eq!(page_size(&b).unwrap(), ps, "page size must not change");

        let (changed, total) = changed_pages(&a, &b, ps);
        eprintln!(
            "[R004-T1] raw copy: {changed}/{total} pages changed by a 1-row insert (page_size={ps})"
        );
        assert!(changed >= 1, "the mutation must change at least one page");
        assert!(
            changed * 4 < total,
            "raw copy not dedup-friendly: {changed}/{total} pages changed by one row"
        );

        // The second image is itself complete and consistent (opens in turso).
        let restored = TempDb::new("offsets-restored");
        std::fs::write(restored.path(), &b).unwrap();
        assert_eq!(count_rows(restored.path()).await, 5001);
    }

    /// SPIKE R004-T1 (contrast): the honest comparison of raw copy vs
    /// `VACUUM INTO` as the dedup base, across two mutation shapes.
    ///
    /// - **Append** (1-row insert): the two are *equivalent* — both touch a
    ///   single page. The working doc's blanket "vacuum reshuffles and defeats
    ///   dedup" does NOT hold for append-only growth.
    /// - **Reclamation** (delete a low-id block): vacuum repacks the surviving
    ///   rows from page 1, renumbering them, so a large fraction of pages
    ///   change. A raw copy leaves every surviving page at its original offset,
    ///   so only the freed region changes.
    ///
    /// The takeaway for F2: raw copy's offset stability is a *structural
    /// guarantee* (page N is always at `(N-1)*ps`, existing pages never
    /// renumber), independent of workload. Vacuum's dedup-friendliness is
    /// incidental and collapses under reclamation — so tier 1b copies raw.
    #[tokio::test]
    async fn raw_copy_beats_vacuum_as_dedup_base() {
        let src = TempDb::new("contrast");
        seed_db(src.path(), 5000).await;

        let raw_a = raw_consistent_copy(src.path()).await.unwrap();
        let vac_a = vacuum_image(src.path()).await;
        let ps = page_size(&raw_a).unwrap();

        // Phase 1 — append one row: raw and vacuum should be ~equivalent.
        insert_rows(src.path(), 1_000_000, 1).await;
        let raw_b = raw_consistent_copy(src.path()).await.unwrap();
        let vac_b = vacuum_image(src.path()).await;
        let (raw_app, raw_app_total) = changed_pages(&raw_a, &raw_b, ps);
        let (vac_app, vac_app_total) = changed_pages(&vac_a, &vac_b, ps);
        eprintln!(
            "[R004-T1] append 1 row -> raw: {raw_app}/{raw_app_total} | vacuum: {vac_app}/{vac_app_total} (expected ~equal)"
        );

        // Phase 2 — free a low-id block: vacuum renumbers survivors, raw doesn't.
        let src2 = TempDb::new("contrast-reclaim");
        seed_db(src2.path(), 5000).await;
        let raw_c = raw_consistent_copy(src2.path()).await.unwrap();
        let vac_c = vacuum_image(src2.path()).await;
        {
            let db = Builder::new_local(src2.path()).build().await.unwrap();
            let conn = db.connect().unwrap();
            conn.execute("DELETE FROM t WHERE id < 2000", ()).await.unwrap();
        }
        let raw_d = raw_consistent_copy(src2.path()).await.unwrap();
        let vac_d = vacuum_image(src2.path()).await;
        let (raw_rec, raw_rec_total) = changed_pages(&raw_c, &raw_d, ps);
        let (vac_rec, vac_rec_total) = changed_pages(&vac_c, &vac_d, ps);
        eprintln!(
            "[R004-T1] delete low block -> raw: {raw_rec}/{raw_rec_total} | vacuum: {vac_rec}/{vac_rec_total} (vacuum renumbers)"
        );

        // The defensible, workload-independent claim: under reclamation the raw
        // copy preserves more pages than vacuum does.
        assert!(
            vac_rec > raw_rec,
            "under reclamation expected vacuum to perturb more pages than raw copy \
             (raw {raw_rec}, vacuum {vac_rec})"
        );
    }

    /// SPIKE R004-T1: `raw_consistent_copy` folds main + WAL into one image, so
    /// the returned bytes are complete with no `-wal` sidecar.
    #[tokio::test]
    async fn raw_copy_folds_wal_and_is_self_contained() {
        let src = TempDb::new("walfold");
        seed_db(src.path(), 100).await;
        insert_rows(src.path(), 200, 50).await;

        let img = raw_consistent_copy(src.path()).await.unwrap();
        assert!(img.starts_with(SQLITE_MAGIC));

        // Write ONLY the main image to a fresh path (no -wal/-shm): a complete
        // image means the WAL frames were folded in.
        let restored = TempDb::new("walfold-restored");
        std::fs::write(restored.path(), &img).unwrap();
        assert_eq!(count_rows(restored.path()).await, 150);
    }

    // ---- R004-F2: page chunking + content-addressed dedup upload + manifest ----

    /// Number of objects stored under `{prefix}/{leaf}/`.
    async fn count_under(target: &BackupTarget, leaf: &str) -> usize {
        target
            .store
            .list_with_delimiter(Some(&join_key(&target.prefix, leaf)))
            .await
            .unwrap()
            .objects
            .len()
    }

    /// Reassemble the image described by the latest manifest, fetching each page
    /// blob by its content hash (what R004-F3 will package as `restore`).
    async fn reassemble_latest(target: &BackupTarget) -> Vec<u8> {
        let (_, m) = load_latest_manifest(target).await.unwrap().unwrap();
        let mut out = Vec::with_capacity(m.image_len());
        for h in &m.page_hashes {
            let bytes = target.store.get(&page_key(target, h)).await.unwrap().bytes().await.unwrap();
            out.extend_from_slice(&bytes);
        }
        out
    }

    #[test]
    fn manifest_round_trips() {
        let m = Manifest {
            page_size: 4096,
            page_hashes: vec!["aa".repeat(32), "bb".repeat(32)],
        };
        assert_eq!(Manifest::from_bytes(&m.to_bytes()).unwrap(), m);
        assert_eq!(m.image_len(), 8192);
        assert!(Manifest::from_bytes(b"not a manifest\n").is_err());
        assert!(Manifest::from_bytes(&[0xff, 0xfe]).is_err());
    }

    /// First snapshot uploads every distinct page + a manifest; a one-row change
    /// then uploads only the few changed pages and writes a second manifest. Both
    /// manifests reassemble to a valid SQLite with the expected row count.
    #[tokio::test]
    async fn dedup_uploads_only_changed_pages_and_reassembles() {
        let src = TempDb::new("dedup-src");
        seed_db(src.path(), 5000).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: "backups".into() };

        // First snapshot: nothing prior -> every distinct page is new.
        let total = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { total_pages, uploaded_pages, reused_pages, .. } => {
                assert_eq!(reused_pages, 0, "no prior snapshot -> nothing reused");
                assert_eq!(uploaded_pages, count_under(&target, "pages").await, "every put is a distinct page");
                total_pages
            }
            other => panic!("expected Snapshotted, got {other:?}"),
        };
        assert_eq!(count_under(&target, "manifests").await, 1);
        let pages_after_first = count_under(&target, "pages").await;

        // Reassembles to a valid SQLite with all 5000 rows.
        let img = reassemble_latest(&target).await;
        assert!(img.starts_with(SQLITE_MAGIC));
        let r1 = TempDb::new("dedup-r1");
        std::fs::write(r1.path(), &img).unwrap();
        assert_eq!(count_rows(r1.path()).await, 5000);

        // One-row mutation -> only a handful of changed pages uploaded.
        insert_rows(src.path(), 1_000_000, 1).await;
        match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { uploaded_pages, total_pages, .. } => {
                assert!(
                    uploaded_pages >= 1 && uploaded_pages * 4 < total_pages,
                    "expected few changed pages, got {uploaded_pages}/{total_pages}"
                );
                // Content-addressed store grew by exactly the new pages.
                assert_eq!(
                    count_under(&target, "pages").await - pages_after_first,
                    uploaded_pages
                );
            }
            other => panic!("expected Snapshotted, got {other:?}"),
        }
        assert_eq!(count_under(&target, "manifests").await, 2, "second snapshot -> second manifest");
        assert_eq!(total, 18, "sanity: 5000 short rows -> 18 pages");

        // The newest manifest reassembles to the post-mutation database.
        let img2 = reassemble_latest(&target).await;
        let r2 = TempDb::new("dedup-r2");
        std::fs::write(r2.path(), &img2).unwrap();
        assert_eq!(count_rows(r2.path()).await, 5001);
    }

    /// Re-snapshotting an unchanged database uploads nothing and writes no new
    /// manifest — the prior one still stands.
    #[tokio::test]
    async fn dedup_unchanged_when_pages_identical() {
        let src = TempDb::new("dedup-unch");
        seed_db(src.path(), 50).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: String::new() };

        let first_key = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };
        let pages = count_under(&target, "pages").await;

        match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Unchanged { manifest_key } => assert_eq!(manifest_key, first_key),
            other => panic!("expected Unchanged, got {other:?}"),
        }
        assert_eq!(count_under(&target, "manifests").await, 1, "no new manifest on unchanged");
        assert_eq!(count_under(&target, "pages").await, pages, "no new pages on unchanged");
    }

    // ---- R004-F3: restore from manifest ----

    /// `restore_latest_dedup` errors when empty, then restores the newest of two
    /// snapshots (post-mutation row count) as a valid vanilla-SQLite file.
    #[tokio::test]
    async fn restore_latest_dedup_picks_newest_and_round_trips() {
        let src = TempDb::new("f3-src");
        seed_db(src.path(), 200).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: "backups".into() };

        // No snapshot yet -> error, no file written.
        let dest = TempDb::new("f3-dest");
        assert!(restore_latest_dedup(&target, dest.path()).await.is_err());

        snapshot_dedup(src.path(), &target).await.unwrap();
        insert_rows(src.path(), 1_000_000, 5).await;
        let newest = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };

        let restored_from = restore_latest_dedup(&target, dest.path()).await.unwrap();
        assert_eq!(restored_from, newest, "should restore the newest manifest");

        let on_disk = std::fs::read(dest.path()).unwrap();
        assert!(on_disk.starts_with(SQLITE_MAGIC), "restored file is not SQLite");
        assert_eq!(count_rows(dest.path()).await, 205);
    }

    /// A corrupt page blob is caught by the content-hash check rather than
    /// producing a silently-broken database.
    #[tokio::test]
    async fn restore_rejects_corrupt_page() {
        let src = TempDb::new("f3-corrupt");
        seed_db(src.path(), 100).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: String::new() };
        snapshot_dedup(src.path(), &target).await.unwrap();

        let (_, manifest) = load_latest_manifest(&target).await.unwrap().unwrap();
        // Overwrite one page blob with garbage of the right length (so only the
        // hash check, not a size check, can catch it).
        let victim = &manifest.page_hashes[0];
        target
            .store
            .put(&page_key(&target, victim), vec![0xab; manifest.page_size].into())
            .await
            .unwrap();

        let dest = TempDb::new("f3-corrupt-dest");
        let err = restore_from_manifest(&target, &manifest, dest.path()).await.unwrap_err();
        assert!(
            err.to_string().contains("content hash"),
            "expected a hash-mismatch error, got: {err}"
        );
    }

    /// `restore_manifest_key` targets an *older* snapshot by key while the latest
    /// reflects the post-mutation state — both reassemble to their own row counts.
    #[tokio::test]
    async fn restore_manifest_key_targets_older_snapshot() {
        let src = TempDb::new("f3-key-src");
        seed_db(src.path(), 100).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: "backups".into() };

        let key1 = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };
        insert_rows(src.path(), 1_000_000, 10).await;
        snapshot_dedup(src.path(), &target).await.unwrap();

        // Older snapshot by key -> 100 rows.
        let d1 = TempDb::new("f3-key-d1");
        restore_manifest_key(&target, &key1, d1.path()).await.unwrap();
        assert_eq!(count_rows(d1.path()).await, 100);

        // Latest -> 110 rows.
        let d2 = TempDb::new("f3-key-d2");
        restore_latest_dedup(&target, d2.path()).await.unwrap();
        assert_eq!(count_rows(d2.path()).await, 110);
    }

    // ---- R006-F1: retention + page-blob GC ----

    /// Take three snapshots, GC down to the newest one. Older manifests are
    /// pruned, every page hash referenced *only* by them is swept, the pages
    /// the surviving manifest still references stay put, and the surviving
    /// snapshot restores byte-for-byte against the un-GC'd image.
    #[tokio::test]
    async fn gc_prunes_older_manifests_and_sweeps_orphan_pages() {
        let src = TempDb::new("gc-src");
        seed_db(src.path(), 5000).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: "backups".into() };

        // Three snapshots with a row appended between each — every snapshot
        // produces at least one fresh page that the next one doesn't reference.
        snapshot_dedup(src.path(), &target).await.unwrap();
        insert_rows(src.path(), 1_000_000, 1).await;
        snapshot_dedup(src.path(), &target).await.unwrap();
        insert_rows(src.path(), 2_000_000, 1).await;
        let newest_key = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };
        assert_eq!(count_under(&target, "manifests").await, 3);
        let pages_before_gc = count_under(&target, "pages").await;

        // Snapshot the bytes-on-disk of every page the newest manifest needs,
        // so we can prove the GC leaves them byte-identical.
        let (_, newest_manifest) = load_latest_manifest(&target).await.unwrap().unwrap();
        let mut expected_pages: HashMap<String, Vec<u8>> = HashMap::new();
        for h in &newest_manifest.page_hashes {
            let bytes = target.store.get(&page_key(&target, h)).await.unwrap().bytes().await.unwrap();
            expected_pages.insert(h.clone(), bytes.to_vec());
        }

        // GC down to keep=1 (just the newest manifest).
        let outcome = gc_dedup(&target, 1).await.unwrap();
        assert_eq!(outcome.retained_manifests, 1);
        assert_eq!(outcome.pruned_manifests, 2);
        assert_eq!(outcome.retained_pages, newest_manifest.page_hashes.len());
        assert!(
            outcome.deleted_pages > 0,
            "earlier snapshots wrote at least one page the newest manifest no longer references"
        );

        // Store state matches the outcome counts.
        assert_eq!(count_under(&target, "manifests").await, 1, "only the newest manifest survives");
        assert_eq!(
            count_under(&target, "pages").await,
            pages_before_gc - outcome.deleted_pages,
            "pages/ shrank by exactly the orphan count"
        );

        // Every page the newest manifest references is still there, byte-identical.
        for (h, want) in &expected_pages {
            let got = target.store.get(&page_key(&target, h)).await.unwrap().bytes().await.unwrap();
            assert_eq!(&got[..], &want[..], "retained page {h} was mutated by GC");
        }

        // And the newest manifest still restores to a valid SQLite with the
        // post-second-mutation row count (5000 + 1 + 1 = 5002).
        let dest = TempDb::new("gc-restored");
        let restored_from = restore_latest_dedup(&target, dest.path()).await.unwrap();
        assert_eq!(restored_from, newest_key);
        assert_eq!(count_rows(dest.path()).await, 5002);
    }

    /// keep_n >= total manifests is a no-op: nothing pruned, no pages deleted.
    #[tokio::test]
    async fn gc_keep_n_at_or_above_total_is_noop() {
        let src = TempDb::new("gc-noop");
        seed_db(src.path(), 100).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: String::new() };

        snapshot_dedup(src.path(), &target).await.unwrap();
        insert_rows(src.path(), 1_000_000, 1).await;
        snapshot_dedup(src.path(), &target).await.unwrap();
        let manifests_before = count_under(&target, "manifests").await;
        let pages_before = count_under(&target, "pages").await;

        // keep_n == total
        let outcome = gc_dedup(&target, 2).await.unwrap();
        assert_eq!(outcome.pruned_manifests, 0);
        assert_eq!(outcome.deleted_pages, 0);
        assert_eq!(outcome.retained_manifests, manifests_before);

        // keep_n > total
        let outcome = gc_dedup(&target, 999).await.unwrap();
        assert_eq!(outcome.pruned_manifests, 0);
        assert_eq!(outcome.deleted_pages, 0);

        assert_eq!(count_under(&target, "manifests").await, manifests_before);
        assert_eq!(count_under(&target, "pages").await, pages_before);
    }

    /// Pages shared between a pruned and a retained manifest survive — content
    /// addressing means they're one blob, and the retain union still needs it.
    /// This is the load-bearing safety property: incremental snapshots reuse
    /// most pages, so a naive "delete every page from pruned manifests" would
    /// destroy the retained snapshot.
    #[tokio::test]
    async fn gc_keeps_pages_shared_with_retained_manifests() {
        let src = TempDb::new("gc-shared");
        seed_db(src.path(), 5000).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: "backups".into() };

        // Snapshot 1 — all pages new.
        snapshot_dedup(src.path(), &target).await.unwrap();
        let (_, m1) = load_latest_manifest(&target).await.unwrap().unwrap();
        let s1_hashes: HashSet<String> = m1.page_hashes.iter().cloned().collect();

        // Small mutation -> snapshot 2 shares almost every page with snapshot 1.
        insert_rows(src.path(), 1_000_000, 1).await;
        snapshot_dedup(src.path(), &target).await.unwrap();
        let (_, m2) = load_latest_manifest(&target).await.unwrap().unwrap();
        let s2_hashes: HashSet<String> = m2.page_hashes.iter().cloned().collect();
        let shared: HashSet<&String> = s1_hashes.intersection(&s2_hashes).collect();
        assert!(
            shared.len() > s1_hashes.len() / 2,
            "incremental snapshot should share most pages with the prior \
             (shared {}/{})",
            shared.len(),
            s1_hashes.len()
        );

        // GC keep=1: snapshot 1 is pruned, but every page snapshot 2 still
        // references must survive — including the ones it shares with snapshot 1.
        gc_dedup(&target, 1).await.unwrap();
        for h in &s2_hashes {
            assert!(
                target.store.get(&page_key(&target, h)).await.is_ok(),
                "shared page {h} (in retained snapshot 2) was deleted"
            );
        }
        // Pages exclusive to snapshot 1 are gone.
        for h in s1_hashes.difference(&s2_hashes) {
            assert!(
                target.store.get(&page_key(&target, h)).await.is_err(),
                "page {h} exclusive to pruned snapshot 1 still exists"
            );
        }
    }

    // ---- R006-T2: GC preserves byte-for-byte restorability of every
    // retained snapshot, not just the latest. Three snapshots, keep_n=2 ----
    //
    // The headline contract: an in-window *older* snapshot (the middle one
    // here) must restore byte-for-byte after GC, proving that GC's mark set
    // covers every retained manifest — not just the lexically-newest one
    // restore_latest_dedup uses. Ground truth is the pre-GC restore of the
    // same key; post-GC bytes are asserted equal.
    #[tokio::test]
    async fn gc_keep_two_lets_in_window_older_snapshot_restore_byte_for_byte() {
        let src = TempDb::new("gc-window");
        seed_db(src.path(), 5000).await;
        let target = BackupTarget { store: Arc::new(InMemory::new()), prefix: "backups".into() };

        // Three snapshots with a mutation between each.
        let key1 = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };
        insert_rows(src.path(), 1_000_000, 1).await;
        let key2 = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };
        insert_rows(src.path(), 2_000_000, 1).await;
        let key3 = match snapshot_dedup(src.path(), &target).await.unwrap() {
            DedupOutcome::Snapshotted { manifest_key, .. } => manifest_key,
            other => panic!("expected Snapshotted, got {other:?}"),
        };

        // Ground truth: restore the middle snapshot BEFORE GC.
        let pre = TempDb::new("gc-window-pre");
        restore_manifest_key(&target, &key2, pre.path()).await.unwrap();
        let pre_bytes = std::fs::read(pre.path()).unwrap();
        assert_eq!(count_rows(pre.path()).await, 5001);

        // GC keep_n=2 -> snapshot 1 pruned, 2 and 3 retained.
        let outcome = gc_dedup(&target, 2).await.unwrap();
        assert_eq!(outcome.pruned_manifests, 1);
        assert_eq!(outcome.retained_manifests, 2);
        assert!(
            outcome.deleted_pages > 0,
            "snapshot 1 had at least one page neither snapshot 2 nor 3 references"
        );

        // Snapshot 1 is gone — its manifest object no longer exists.
        let gone = TempDb::new("gc-window-gone");
        assert!(
            restore_manifest_key(&target, &key1, gone.path()).await.is_err(),
            "pruned snapshot 1 should not be restorable"
        );

        // Snapshot 2 (in-window older) restores BYTE-FOR-BYTE to the pre-GC bytes.
        let post = TempDb::new("gc-window-post");
        restore_manifest_key(&target, &key2, post.path()).await.unwrap();
        let post_bytes = std::fs::read(post.path()).unwrap();
        assert_eq!(
            pre_bytes, post_bytes,
            "in-window older snapshot must restore byte-for-byte after GC"
        );

        // And the newest (snapshot 3) restores to its own row count via the
        // latest path — proving keep_n=2 left both retained snapshots whole.
        let latest = TempDb::new("gc-window-latest");
        let restored_from = restore_latest_dedup(&target, latest.path()).await.unwrap();
        assert_eq!(restored_from, key3);
        assert_eq!(count_rows(latest.path()).await, 5002);
    }
}
