//! One-puller-per-box fan-out + warm applier (R574-F3).
//!
//! `stream::tail_frames`'s R2 *write* side has no fan-out concept — it's a
//! 1:1 DB↔sink primitive by design (R005-T1). This module is the *read*
//! side's answer to the doc's §5 fan-in topology: a [`WalPuller`] tails one
//! tenant's R2 frame stream **once per box** and fans each newly-pulled
//! frame to every locally-attached warm-standby applier over an in-process
//! loop, instead of every replica running its own R2 client. R2 read ops are
//! O(boxes), not O(replicas).
//!
//! Depends on R574-T1's §8 RSS curve (this crate's own
//! `rss_harness` example): the untrimmed measured slope was ~86
//! KB/replica, which is what justifies keeping hundreds of `CoreWalSeam`
//! appliers resident per box in the first place — [`WalPullerConfig`]'s FD
//! budget defaults straight off that number. Trimming (below) is this
//! ticket's other half of the sizing story: a warm applier never serves
//! reads, so every cached page is pure standing RSS with no benefit.
//!
//! Implements R574-F3 (relay R574, "WAL→R2 streamer hardening"); the ticket
//! itself — status, assignee, handoff — is tracked in the W248 working doc,
//! not duplicated as an in-source annotation here (same convention as
//! `backpressure.rs`).
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)
//!
//! ## Design notes (for the reviewer, not the board)
//!
//! - **Warm applier = [`WarmApplier`]**, a small supertrait of
//!   `stream::WalInsertSeam` adding one call: `trim_page_cache`. Kept
//!   separate from `WalInsertSeam` itself (rather than adding a method to
//!   it) so R574-T4/F2's already-reviewed trait and its existing
//!   implementors (`CoreWalSeam`, `stream`'s test `MockInsertSeam`) stay
//!   untouched — this ticket is purely additive over stream.rs. The one
//!   `CoreWalSeam` change is a single new inherent method,
//!   [`crate::stream::CoreWalSeam::trim_page_cache_kb`], which drives the
//!   trim through `PRAGMA cache_size` (turso_core's own public, documented
//!   SQL surface) rather than reaching into `turso_core::Pager` directly —
//!   `Pager` is `pub use`d at turso_core's crate root but its
//!   `CacheResizeResult` return type is not, so the pragma path is the
//!   clean way in.
//! - **v1 scope cut — attach is cold-start-only.** `WalPuller::attach`
//!   refuses once `pull_once()` has succeeded at least once. Fanning
//!   *future* frames to a late-joining applier is easy (this module does
//!   exactly that for every already-attached applier); replaying the
//!   *backlog* the applier missed is not something this module reinvents —
//!   that's `stream::restore_latest_stream`'s job, already implemented and
//!   reviewed (R005-F3). The intended flow for adding a replica mid-stream:
//!   materialize it cold via `restore_latest_stream`, then hand it to a
//!   **new** `WalPuller` (or restart the box's puller) rather than grafting
//!   a partially-caught-up seam onto a puller that's already mid-fan-out.
//!   Flagged in the ticket handoff as the main thing worth reviewer
//!   pushback: a longer-lived puller that supports hot-attach would need to
//!   either replay the backlog itself (duplicating restore's logic) or
//!   retain every pulled frame in memory to serve late joiners (defeats the
//!   whole point of a bounded footprint) — neither seemed like the right
//!   default to ship without a concrete caller shaped by real fan-out
//!   traffic.
//! - **A WAL restart mid-fan-out is refused, not patched over** — same
//!   posture as `restore_latest_stream`'s `validate_generation_chain`
//!   check. If `pull_once()` observes the source's `checkpoint_seq` change
//!   from what it already fanned out, every attached applier's WAL already
//!   contains frames from the *old* sequence that don't compose with the
//!   new one; the fix is a fresh `restore_latest_stream` + a new
//!   `WalPuller`, not heroics here.
//! - **FD/disk budget** ([`WalPullerConfig`]) is enforced at `attach()`
//!   time: `max_appliers` bounds concurrently-open applier connections (each
//!   is several FDs — raise the box's ulimit accordingly, per §5), and
//!   `max_disk_bytes` bounds the sum of caller-supplied `dest_size_bytes`
//!   hints (a warm standby is a full local copy, so disk is Σ tenant DB
//!   sizes — this module has no way to `stat()` a size that's meaningful
//!   before the applier exists, hence the caller-supplied hint rather than
//!   inspecting the filesystem itself).
//! - The doc left every one of these numbers open, same as F2's backpressure
//!   knobs. Chosen `WalPullerConfig::default()`: `max_appliers = 1000`
//!   (directly off R574-T1's measured curve — 1000 replicas/box read as
//!   "strongly supportive of warm-for-everyone" at ~86 KB/replica
//!   *untrimmed*; trimming should only improve on that), `max_disk_bytes =
//!   500 GiB` (an unmeasured, round placeholder for a modern box's local
//!   NVMe — there is no §8-shaped disk measurement yet, unlike RSS),
//!   `applier_cache_kb = 64` (SQLite's own historical default is 2 MiB;
//!   64 KiB is a deliberately hard trim per §5's "trimmed cache" ask for a
//!   connection that never serves reads — plain struct fields, so a caller
//!   overrides per-tenant SLA without a code change, same pattern as
//!   `BackpressureConfig`).

use std::collections::HashMap;

use anyhow::{Context, Result};
use crate::snapshot::BackupTarget;
use crate::stream::{
    for_each_frame_in_generation, list_and_parse_generation_manifests, validate_generation_chain,
    CoreWalSeam, WalInsertSeam, Watermark,
};

/// A [`WalInsertSeam`] plus the one extra call a warm standby needs: trim
/// its page cache hard immediately on attach, since it never serves reads
/// (§5: "page cache trimmed hard"). See the module doc for why this is a
/// separate trait rather than a new `WalInsertSeam` method.
pub trait WarmApplier: WalInsertSeam {
    /// Resize the underlying page cache toward `target_kb` kilobytes.
    /// Best-effort in the sense that `turso_core`'s page cache only evicts
    /// pages that aren't pinned/dirty — harmless here, since an applier
    /// only ever holds pages it just wrote and is about to hand off to the
    /// engine, not mid-query state.
    fn trim_page_cache(&self, target_kb: i64) -> Result<()>;
}

impl WarmApplier for CoreWalSeam {
    fn trim_page_cache(&self, target_kb: i64) -> Result<()> {
        self.trim_page_cache_kb(target_kb)
    }
}

/// FD/disk budget + cache-trim knob for a [`WalPuller`]'s attached
/// appliers. See the module doc for the chosen `Default` numbers and their
/// (unmeasured) provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalPullerConfig {
    /// Max concurrently-attached appliers. Each is a live `CoreWalSeam` —
    /// several open FDs — so this is the box's FD budget, not just a
    /// counter; raise the box's ulimit to match before raising this.
    pub max_appliers: usize,
    /// Max sum of attached appliers' `dest_size_bytes` hints. A warm
    /// standby is a full local copy of the tenant DB, so this is the box's
    /// local-disk budget (Σ tenant DB sizes), per §5.
    pub max_disk_bytes: u64,
    /// Page cache size (KB) an applier is trimmed to immediately on attach.
    pub applier_cache_kb: i64,
}

impl Default for WalPullerConfig {
    fn default() -> Self {
        Self {
            max_appliers: 1_000,
            max_disk_bytes: 500 * 1024 * 1024 * 1024,
            applier_cache_kb: 64,
        }
    }
}

/// What a [`WalPuller::pull_once`] call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PullReport {
    /// Frames downloaded from the object store and fanned out this call.
    /// Each was downloaded exactly once regardless of `appliers_fanned`.
    pub frames_pulled: u64,
    /// The source's `checkpoint_seq` as of this call (0 if nothing has ever
    /// been pulled and there are still no generation manifests to read).
    pub checkpoint_seq: u32,
    /// Attached-applier count at the time of this call.
    pub appliers_fanned: usize,
}

struct AttachedApplier {
    seam: Box<dyn WarmApplier>,
    dest_size_bytes: u64,
}

/// Tails one tenant's R2 frame stream once per box and fans each newly
/// pulled frame to every attached [`WarmApplier`]. See the module doc for
/// the full design, the v1 cold-start-only `attach` scope cut, and the
/// restart-refusal posture.
pub struct WalPuller<'a> {
    target: &'a BackupTarget,
    page_size: usize,
    cfg: WalPullerConfig,
    appliers: HashMap<String, AttachedApplier>,
    disk_used_bytes: u64,
    /// The last `(checkpoint_seq, last_frame)` this puller has already
    /// fanned out to every currently-attached applier. `None` before the
    /// first successful `pull_once()`.
    pulled: Option<Watermark>,
}

impl<'a> WalPuller<'a> {
    pub fn new(target: &'a BackupTarget, page_size: usize, cfg: WalPullerConfig) -> Self {
        Self {
            target,
            page_size,
            cfg,
            appliers: HashMap::new(),
            disk_used_bytes: 0,
            pulled: None,
        }
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn attached_count(&self) -> usize {
        self.appliers.len()
    }

    pub fn disk_used_bytes(&self) -> u64 {
        self.disk_used_bytes
    }

    /// `true` once `pull_once()` has succeeded at least once — the point
    /// past which `attach()` refuses (see module doc: v1 does not replay
    /// backlog to a late-joining applier).
    pub fn has_pulled(&self) -> bool {
        self.pulled.is_some()
    }

    /// Attach a warm applier: begins its `wal_insert` session and trims its
    /// page cache to `cfg.applier_cache_kb`. From this call on it receives
    /// every frame this puller fans out.
    ///
    /// `dest_size_bytes` is the caller's own estimate of the applier's
    /// on-disk footprint, counted against `cfg.max_disk_bytes` — see the
    /// module doc for why this crate takes a hint instead of `stat`-ing a
    /// file.
    ///
    /// Errors if: an applier is already attached under `name`; the FD
    /// budget (`max_appliers`) or disk budget (`max_disk_bytes`) would be
    /// exceeded; or this puller has already completed a `pull_once()` (v1
    /// scope cut — see module doc).
    pub fn attach(
        &mut self,
        name: impl Into<String>,
        seam: Box<dyn WarmApplier>,
        dest_size_bytes: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            self.pulled.is_none(),
            "WalPuller v1 does not support attaching an applier mid-stream — materialize it via \
             stream::restore_latest_stream and hand it to a fresh WalPuller, or attach before the \
             first pull_once()"
        );
        let name = name.into();
        anyhow::ensure!(
            !self.appliers.contains_key(&name),
            "applier {name:?} is already attached"
        );
        anyhow::ensure!(
            self.appliers.len() < self.cfg.max_appliers,
            "FD budget exhausted: {} appliers already attached (max_appliers={})",
            self.appliers.len(),
            self.cfg.max_appliers,
        );
        let projected = self.disk_used_bytes.saturating_add(dest_size_bytes);
        anyhow::ensure!(
            projected <= self.cfg.max_disk_bytes,
            "disk budget exhausted: attaching {name:?} ({dest_size_bytes} bytes) would use \
             {projected} bytes total, max_disk_bytes={}",
            self.cfg.max_disk_bytes,
        );

        seam.wal_insert_begin()
            .with_context(|| format!("wal_insert_begin for applier {name:?}"))?;
        seam.trim_page_cache(self.cfg.applier_cache_kb)
            .with_context(|| format!("trimming page cache for applier {name:?}"))?;

        self.disk_used_bytes = projected;
        self.appliers
            .insert(name, AttachedApplier { seam, dest_size_bytes });
        Ok(())
    }

    /// Detach an applier without promoting it: closes its `wal_insert`
    /// session (`force_commit=false`, the same crash-consistent default
    /// `restore_latest_stream` uses) and returns ownership, freeing its
    /// share of the FD/disk budget.
    pub fn detach(&mut self, name: &str) -> Result<Box<dyn WarmApplier>> {
        let applier = self
            .appliers
            .remove(name)
            .with_context(|| format!("no applier attached under {name:?}"))?;
        self.disk_used_bytes = self.disk_used_bytes.saturating_sub(applier.dest_size_bytes);
        applier
            .seam
            .wal_insert_end(false)
            .with_context(|| format!("wal_insert_end for applier {name:?}"))?;
        Ok(applier.seam)
    }

    /// Promote a warm applier to serve live traffic. Mechanically identical
    /// to [`Self::detach`] today (close the session cleanly, hand back the
    /// seam — it is already caught up as of the last `pull_once()`); kept as
    /// a separate name so call sites read intent, matching §5's
    /// "Promotable fast → SLA-tier RTO".
    pub fn promote(&mut self, name: &str) -> Result<Box<dyn WarmApplier>> {
        self.detach(name)
    }

    /// Pull every frame newly written since the last call (or since this
    /// puller was created) and fan each one out to every attached applier.
    /// Each frame is downloaded from the object store **once** regardless
    /// of how many appliers are attached — R2 reads are O(boxes), not
    /// O(replicas).
    ///
    /// Errors if: the chain's `page_size` doesn't match this puller's; or
    /// the source's `checkpoint_seq` has changed since the last successful
    /// pull (a WAL restart — see module doc, refused rather than patched
    /// over). A missing or wrong-length frame object errors the same way
    /// `restore_latest_stream`'s replay does.
    pub async fn pull_once(&mut self) -> Result<PullReport> {
        let manifests = list_and_parse_generation_manifests(self.target).await?;
        if manifests.is_empty() {
            return Ok(PullReport {
                frames_pulled: 0,
                checkpoint_seq: self.pulled.map(|p| p.checkpoint_seq).unwrap_or(0),
                appliers_fanned: self.appliers.len(),
            });
        }
        let chain = validate_generation_chain(&manifests)?;
        anyhow::ensure!(
            chain.page_size == self.page_size,
            "generation chain page_size {} does not match this WalPuller's page_size {}",
            chain.page_size,
            self.page_size,
        );
        if let Some(prior) = self.pulled {
            anyhow::ensure!(
                chain.checkpoint_seq == prior.checkpoint_seq,
                "WAL restart since the last pull (checkpoint_seq {} -> {}) — attached appliers \
                 hold frames from the old sequence and can't be trusted to compose with the new \
                 one; re-materialize them via a fresh restore_latest_stream and start a new \
                 WalPuller",
                prior.checkpoint_seq,
                chain.checkpoint_seq,
            );
        }
        let start_frame = self.pulled.map(|p| p.last_frame + 1).unwrap_or(1);
        if chain.total_frames < start_frame {
            return Ok(PullReport {
                frames_pulled: 0,
                checkpoint_seq: chain.checkpoint_seq,
                appliers_fanned: self.appliers.len(),
            });
        }

        let mut frames_pulled = 0u64;
        let mut last_frame_no = start_frame - 1;
        let appliers = &self.appliers;
        for m in &manifests {
            if m.last_frame < start_frame {
                continue; // fully covered by a prior pull_once() call
            }
            // R732-F2: the epoch comes from each manifest, not from the chain,
            // so a pull spanning an ownership transfer still finds both
            // owners' frames under their own key prefixes. R761-F2: and the
            // manifest also says whether they are batched, so a pull spanning
            // the layout change finds both shapes — all of that is
            // `for_each_frame_in_generation`'s business, not this loop's.
            frames_pulled += for_each_frame_in_generation(
                self.target,
                m,
                start_frame,
                |frame_no, bytes| {
                    for (applier_name, applier) in appliers.iter() {
                        applier.seam.wal_insert_frame(frame_no, bytes).with_context(|| {
                            format!("fanning frame {frame_no} to applier {applier_name:?}")
                        })?;
                    }
                    last_frame_no = frame_no;
                    Ok(())
                },
            )
            .await?;
        }
        self.pulled = Some(Watermark {
            checkpoint_seq: chain.checkpoint_seq,
            last_frame: last_frame_no,
        });
        Ok(PullReport {
            frames_pulled,
            checkpoint_seq: chain.checkpoint_seq,
            appliers_fanned: self.appliers.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::BackpressureConfig;
    use crate::snapshot::BackupTarget;
    use crate::stream::{tail_frames, FrameInfo, StreamConfig, WalSeam, WAL_FRAME_HEADER_SIZE};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
    };
    use std::cell::RefCell;
    use std::fmt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    // --- fixtures shared with stream.rs's own conventions ------------------

    fn fresh_target() -> BackupTarget {
        BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: "backups".into(),
        }
    }

    fn stream_cfg() -> StreamConfig<'static> {
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

    fn puller_cfg(max_appliers: usize, max_disk_bytes: u64) -> WalPullerConfig {
        WalPullerConfig {
            max_appliers,
            max_disk_bytes,
            applier_cache_kb: 64,
        }
    }

    /// In-memory WAL seam mirroring `stream.rs`'s `MockWal` (duplicated
    /// rather than exposed from `stream`'s private test module — same
    /// per-module self-contained fixture convention `dedup.rs`/`snapshot.rs`
    /// already follow).
    struct MockWal {
        state: RefCell<MockState>,
    }
    struct MockState {
        checkpoint_seq: u32,
        frames: Vec<FrameInfo>,
    }
    impl MockWal {
        fn new() -> Self {
            Self { state: RefCell::new(MockState { checkpoint_seq: 0, frames: Vec::new() }) }
        }
        fn append(&self, page_no: u32, db_size: u32) {
            self.state.borrow_mut().frames.push(FrameInfo { page_no, db_size });
        }
        fn restart(&self) {
            let mut s = self.state.borrow_mut();
            s.checkpoint_seq += 1;
            s.frames.clear();
        }
    }
    impl WalSeam for MockWal {
        fn wal_state(&self) -> Result<Watermark> {
            let s = self.state.borrow();
            Ok(Watermark { checkpoint_seq: s.checkpoint_seq, last_frame: s.frames.len() as u64 })
        }
        fn wal_get_frame(&self, frame_no: u64, buf: &mut [u8]) -> Result<FrameInfo> {
            let s = self.state.borrow();
            let f = s.frames[frame_no as usize - 1];
            buf[0..4].copy_from_slice(&f.page_no.to_be_bytes());
            buf[4..8].copy_from_slice(&f.db_size.to_be_bytes());
            buf[8..WAL_FRAME_HEADER_SIZE].fill(0);
            buf[WAL_FRAME_HEADER_SIZE..].fill(frame_no as u8);
            Ok(f)
        }
        fn wal_auto_actions_disable(&self) {}
    }

    /// Captured applier: records every call (including `trim_page_cache`)
    /// so tests can assert ordering and content without a real turso
    /// connection. The event log is `Arc`-shared so a test can keep
    /// inspecting it after handing `Box<dyn WarmApplier>` ownership to a
    /// `WalPuller` — mirrors `stream.rs`'s `MockInsertSeam` convention,
    /// extended with `WarmApplier::trim_page_cache`.
    #[derive(Clone)]
    struct MockApplier(std::rc::Rc<RefCell<Vec<MockEvent>>>);
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MockEvent {
        Begin,
        TrimCache { target_kb: i64 },
        Frame { frame_no: u64, fill: u8 },
        End { force_commit: bool },
    }
    impl MockApplier {
        fn new() -> Self {
            Self(std::rc::Rc::new(RefCell::new(Vec::new())))
        }
        fn events(&self) -> Vec<MockEvent> {
            self.0.borrow().clone()
        }
    }
    impl WalInsertSeam for MockApplier {
        fn wal_insert_begin(&self) -> Result<()> {
            self.0.borrow_mut().push(MockEvent::Begin);
            Ok(())
        }
        fn wal_insert_frame(&self, frame_no: u64, frame: &[u8]) -> Result<()> {
            self.0
                .borrow_mut()
                .push(MockEvent::Frame { frame_no, fill: frame[WAL_FRAME_HEADER_SIZE] });
            Ok(())
        }
        fn wal_insert_end(&self, force_commit: bool) -> Result<()> {
            self.0.borrow_mut().push(MockEvent::End { force_commit });
            Ok(())
        }
    }
    impl WarmApplier for MockApplier {
        fn trim_page_cache(&self, target_kb: i64) -> Result<()> {
            self.0.borrow_mut().push(MockEvent::TrimCache { target_kb });
            Ok(())
        }
    }

    /// A [`WarmApplier`] whose `trim_page_cache` always fails — for
    /// exercising `attach`'s error path without touching a real connection.
    struct FailingTrimApplier;
    impl WalInsertSeam for FailingTrimApplier {
        fn wal_insert_begin(&self) -> Result<()> {
            Ok(())
        }
        fn wal_insert_frame(&self, _: u64, _: &[u8]) -> Result<()> {
            Ok(())
        }
        fn wal_insert_end(&self, _: bool) -> Result<()> {
            Ok(())
        }
    }
    impl WarmApplier for FailingTrimApplier {
        fn trim_page_cache(&self, _: i64) -> Result<()> {
            anyhow::bail!("simulated trim failure")
        }
    }

    /// Wraps an inner store and counts `get_opts` calls — used to prove
    /// `pull_once` downloads each frame exactly once regardless of how many
    /// appliers are attached. Mirrors `backpressure.rs`'s `FaultyStore`
    /// boilerplate (delegate everything, instrument one method).
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        gets: AtomicU64,
    }
    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self { inner, gets: AtomicU64::new(0) }
        }
        fn get_count(&self) -> u64 {
            self.gets.load(Ordering::SeqCst)
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
    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
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
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, OsResult<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'static, OsResult<ObjPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    // --- attach: FD/disk budget + cold-start-only scope --------------------

    #[test]
    fn attach_enforces_fd_budget() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(1, u64::MAX));
        puller.attach("a", Box::new(MockApplier::new()), 0).unwrap();
        let err = puller.attach("b", Box::new(MockApplier::new()), 0).unwrap_err();
        assert!(format!("{err}").contains("FD budget"), "err was {err}");
        assert_eq!(puller.attached_count(), 1);
    }

    #[test]
    fn attach_enforces_disk_budget() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, 100));
        puller.attach("a", Box::new(MockApplier::new()), 60).unwrap();
        let err = puller.attach("b", Box::new(MockApplier::new()), 60).unwrap_err();
        assert!(format!("{err}").contains("disk budget"), "err was {err}");
        assert_eq!(puller.disk_used_bytes(), 60);
    }

    #[test]
    fn attach_rejects_duplicate_name() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        puller.attach("a", Box::new(MockApplier::new()), 0).unwrap();
        let err = puller.attach("a", Box::new(MockApplier::new()), 0).unwrap_err();
        assert!(format!("{err}").contains("already attached"), "err was {err}");
    }

    /// `attach` opens the wal_insert session and trims the cache, in that
    /// order, before the applier is otherwise touched.
    #[test]
    fn attach_begins_session_and_trims_cache() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        let a = MockApplier::new();
        puller.attach("a", Box::new(a.clone()), 0).unwrap();
        assert_eq!(a.events(), vec![MockEvent::Begin, MockEvent::TrimCache { target_kb: 64 }]);
    }

    #[test]
    fn attach_propagates_trim_failure_without_registering_applier() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        let err = puller.attach("a", Box::new(FailingTrimApplier), 0).unwrap_err();
        assert!(format!("{err}").contains("trimming page cache"), "err was {err}");
        assert_eq!(puller.attached_count(), 0, "a failed attach must not register");
    }

    #[tokio::test]
    async fn attach_refuses_after_first_pull() {
        let seam = MockWal::new();
        seam.append(1, 1); // commit
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();

        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        assert!(!puller.has_pulled());
        let report = puller.pull_once().await.unwrap();
        assert_eq!(report.frames_pulled, 1, "a real frame must actually be fanned");
        assert!(puller.has_pulled());
        let err = puller
            .attach("late", Box::new(MockApplier::new()), 0)
            .unwrap_err();
        assert!(format!("{err}").contains("mid-stream"), "err was {err}");
    }

    /// An empty `pull_once()` (no generation manifests exist yet) leaves
    /// `has_pulled()` false — nothing was fanned out, so there is no
    /// backlog a late-joining applier could miss, and attach must still be
    /// allowed.
    #[tokio::test]
    async fn attach_still_allowed_after_an_empty_pull() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        let report = puller.pull_once().await.unwrap();
        assert_eq!(report.frames_pulled, 0);
        assert!(!puller.has_pulled());
        puller.attach("a", Box::new(MockApplier::new()), 0).unwrap();
        assert_eq!(puller.attached_count(), 1);
    }

    // --- pull_once: single download, fan-out, resumability -----------------

    /// Core fan-in property: N frames staged, M appliers attached — the
    /// object store sees a fixed number of `get` calls that does not scale
    /// with M. Since R761-F2 it does not scale with N either: the whole tail
    /// call is one batch object, so three frames cost one GET, not three.
    #[tokio::test]
    async fn pull_once_downloads_each_frame_once_regardless_of_applier_count() {
        let seam = MockWal::new();
        seam.append(1, 0);
        seam.append(2, 0);
        seam.append(3, 3); // commit
        let raw_target = fresh_target();
        let _ = tail_frames(&seam, &raw_target, &stream_cfg()).await.unwrap();

        let counting = Arc::new(CountingStore::new(raw_target.store.clone()));
        let counted_target = BackupTarget { store: counting.clone(), prefix: "backups".into() };

        let mut puller = WalPuller::new(&counted_target, 4096, puller_cfg(10, u64::MAX));
        for name in ["a", "b", "c"] {
            puller.attach(name, Box::new(MockApplier::new()), 0).unwrap();
        }
        let report = puller.pull_once().await.unwrap();
        assert_eq!(report.frames_pulled, 3);
        assert_eq!(report.appliers_fanned, 3);
        assert_eq!(
            counting.get_count(),
            2,
            "1 batch object holding all 3 frames + 1 generation manifest, once each"
        );
    }

    /// Every attached applier receives the identical frame content and
    /// ordering; `wal_insert_begin`/cache-trim precede every frame, and
    /// `pull_once` itself never calls `wal_insert_end` (that's
    /// `detach`/`promote`'s job).
    #[tokio::test]
    async fn pull_once_fans_identical_frames_to_every_applier() {
        let seam = MockWal::new();
        seam.append(1, 0);
        seam.append(2, 2); // commit
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();

        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        let a = MockApplier::new();
        let b = MockApplier::new();
        puller.attach("a", Box::new(a.clone()), 0).unwrap();
        puller.attach("b", Box::new(b.clone()), 0).unwrap();
        puller.pull_once().await.unwrap();

        let want = vec![
            MockEvent::Begin,
            MockEvent::TrimCache { target_kb: 64 },
            MockEvent::Frame { frame_no: 1, fill: 1 },
            MockEvent::Frame { frame_no: 2, fill: 2 },
        ];
        assert_eq!(a.events(), want);
        assert_eq!(b.events(), want);
    }

    /// A second `pull_once()` after new frames land only fans the delta —
    /// resumable, same posture as `tail_frames`.
    #[tokio::test]
    async fn pull_once_is_incremental_across_calls() {
        let seam = MockWal::new();
        seam.append(1, 1); // commit
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();

        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        puller.attach("a", Box::new(MockApplier::new()), 0).unwrap();
        let first = puller.pull_once().await.unwrap();
        assert_eq!(first.frames_pulled, 1);

        // Nothing new yet.
        let second = puller.pull_once().await.unwrap();
        assert_eq!(second.frames_pulled, 0);

        seam.append(2, 2); // commit
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();
        let third = puller.pull_once().await.unwrap();
        assert_eq!(third.frames_pulled, 1, "only the new frame, not a re-fan of frame 1");
    }

    /// No manifests under the prefix yet: a clean no-op, not an error.
    #[tokio::test]
    async fn pull_once_with_no_manifests_is_a_noop() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        let report = puller.pull_once().await.unwrap();
        assert_eq!(report, PullReport { frames_pulled: 0, checkpoint_seq: 0, appliers_fanned: 0 });
    }

    /// A page_size mismatch between the puller's config and the chain's
    /// manifests is refused loudly rather than silently misreading frames.
    #[tokio::test]
    async fn pull_once_rejects_page_size_mismatch() {
        let seam = MockWal::new();
        seam.append(1, 1);
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();

        let mut puller = WalPuller::new(&target, 8192, puller_cfg(10, u64::MAX));
        let err = puller.pull_once().await.unwrap_err();
        assert!(format!("{err}").contains("page_size"), "err was {err}");
    }

    /// A WAL restart observed between two `pull_once()` calls is refused —
    /// attached appliers hold frames from the stale sequence.
    #[tokio::test]
    async fn pull_once_refuses_across_a_restart() {
        let seam = MockWal::new();
        seam.append(1, 1); // commit under seq 0
        let target = fresh_target();
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();

        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        puller.attach("a", Box::new(MockApplier::new()), 0).unwrap();
        puller.pull_once().await.unwrap();

        seam.restart();
        seam.append(1, 1);
        let _ = tail_frames(&seam, &target, &stream_cfg()).await.unwrap();

        let err = puller.pull_once().await.unwrap_err();
        assert!(format!("{err}").contains("WAL restart"), "err was {err}");
    }

    // --- detach / promote ---------------------------------------------------

    #[test]
    fn detach_frees_disk_budget_and_ends_session() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, 100));
        let a = MockApplier::new();
        puller.attach("a", Box::new(a.clone()), 60).unwrap();
        assert_eq!(puller.disk_used_bytes(), 60);

        puller.detach("a").unwrap();
        assert_eq!(puller.disk_used_bytes(), 0);
        assert_eq!(puller.attached_count(), 0);
        assert_eq!(a.events().last(), Some(&MockEvent::End { force_commit: false }));

        // A fresh attach can now reuse the freed budget.
        puller.attach("b", Box::new(MockApplier::new()), 60).unwrap();
        assert_eq!(puller.disk_used_bytes(), 60);
    }

    #[test]
    fn detach_unknown_name_errors() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        assert!(puller.detach("ghost").is_err());
    }

    #[test]
    fn promote_ends_session_with_no_forced_commit() {
        let target = fresh_target();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        let a = MockApplier::new();
        puller.attach("a", Box::new(a.clone()), 0).unwrap();
        puller.promote("a").unwrap();
        assert_eq!(puller.attached_count(), 0);
        assert_eq!(a.events().last(), Some(&MockEvent::End { force_commit: false }));
    }

    // --- live end-to-end: real turso + CoreWalSeam --------------------------

    struct TempDb(std::path::PathBuf);
    impl TempDb {
        fn new(tag: &str) -> Self {
            TempDb(std::env::temp_dir().join(format!(
                "turso-backup-puller-{tag}-{}-{}.db",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
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

    async fn count_rows(path: &str) -> i64 {
        let db = turso::Builder::new_local(path).build().await.unwrap();
        let conn = db.connect().unwrap();
        let mut r = conn.query("SELECT COUNT(*) FROM t", ()).await.unwrap();
        let row = r.next().await.unwrap().unwrap();
        row.get::<i64>(0).unwrap()
    }

    /// End-to-end against a real turso DB and `CoreWalSeam`: seed, snapshot,
    /// stream WAL frames to the sink, then attach a real warm-applier seam
    /// (a bare copy of the base snapshot, no WAL yet) to a `WalPuller` and
    /// `pull_once()` — the applier's row count must match the source after
    /// the fan-out, and the cache-trim pragma must not error.
    #[tokio::test]
    async fn live_pull_once_replays_onto_a_real_applier() {
        let src = TempDb::new("live-src");
        let dest = TempDb::new("live-dest");
        seed_rows(src.path(), 0, 20).await;

        let target = fresh_target();
        let base_key = match crate::snapshot::snapshot_and_upload(src.path(), &target)
            .await
            .unwrap()
        {
            crate::snapshot::SnapshotOutcome::Uploaded { key, .. } => key,
            other => panic!("expected a fresh Uploaded snapshot, got {other:?}"),
        };

        // Fresh WAL frames past the base snapshot.
        seed_rows(src.path(), 1000, 5).await;
        {
            let seam = crate::stream::CoreWalSeam::open(src.path()).unwrap();
            let cfg = StreamConfig {
                base_snapshot_key: &base_key,
                page_size: 4096,
                backpressure: BackpressureConfig::default(),
                rpo_target: None,
                epoch: 0,
                owner: None,
                pointer_generation: 0,
            };
            tail_frames(&seam, &target, &cfg).await.unwrap();
        }

        // Materialize the applier's destination at the base snapshot only —
        // no WAL replay yet. `WalPuller::pull_once` supplies the frames.
        let base_bytes = target
            .store
            .get(&ObjPath::from(base_key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        std::fs::write(dest.path(), &base_bytes).unwrap();

        let applier = crate::stream::CoreWalSeam::open(dest.path()).unwrap();
        let mut puller = WalPuller::new(&target, 4096, puller_cfg(10, u64::MAX));
        puller.attach("dest", Box::new(applier), 0).unwrap();
        let report = puller.pull_once().await.unwrap();
        assert!(report.frames_pulled > 0);

        puller.promote("dest").unwrap();
        assert_eq!(count_rows(dest.path()).await, 25, "20 base + 5 streamed via the puller");
    }
}
