//! R850-F1 — the **backup half** of hydrate-on-place: keep a running
//! appliance's declared databases shipped to the object store, under the same
//! [`claim`] fence [`hydrate`](crate::hydrate) reads.
//!
//! [`hydrate`](crate::hydrate) fills an empty volume from the store before a
//! workload starts. Until something puts bytes *into* the store, that is a
//! restore path with nothing to restore from — every hydrate returns
//! `nothing_in_the_store` forever. This module is that something.
//!
//! # The constraint that shapes everything here: the application holds the file
//!
//! A tenant's local database is a replica this fleet owns outright, so
//! `yubaba/crates/tenant-streamer` opens one writable [`CoreWalSeam`] per tenant
//! and holds it for the process lifetime. An appliance's database is the
//! opposite: the *application* opened it, and turso takes a whole-file exclusive
//! `fcntl` lock at a writable open. Three consequences, all measured by
//! `examples/appliance_tail_probe.rs` rather than reasoned about:
//!
//! 1. **`CoreWalSeam::open` is refused outright** (probe A1) while the app runs.
//!    Everything here uses [`CoreWalSeam::open_reader`], which takes no lock.
//! 2. **A held reader never sees the application's later commits** (A4a); a
//!    reader opened fresh does (A4b). So each round opens its own seam and drops
//!    it — and it must drop the old one *first*, because turso's process-global
//!    `DATABASE_MANAGER` hands a second open of the same path the first handle
//!    back.
//! 3. **`VACUUM INTO` and `PRAGMA wal_checkpoint(TRUNCATE)` are unavailable**,
//!    because both need an ordinary writable connection. That rules out
//!    [`snapshot::snapshot_and_upload`] and [`dedup::snapshot_dedup`] as written.
//!    All three tiers here take their image from
//!    [`stream::raw_consistent_copy_live`] instead, and hand it to the
//!    image-shaped entry points ([`snapshot::upload_snapshot_image`],
//!    [`dedup::snapshot_dedup_image`]) that exist for this caller.
//!
//! That measurement is also why this is a separate process at all. It was
//! reported — from the first consumer, and read rather than measured — that a
//! turso database cannot be tailed from outside the application that holds it,
//! which would have killed every out-of-process design. It is true of
//! `CoreWalSeam::open` and false of the crate: probe A5 takes a full
//! snapshot + tail from a second process and restores it byte-exact.
//!
//! # Ownership: acquire once, assert every round
//!
//! [`start`] takes the claim; [`round`] re-verifies it. The claim is not a
//! lease and has no TTL — as [`claim`]'s module doc says, whether a takeover is
//! *allowed* is placement's call, and what the store guarantees is only that
//! takeovers are totally ordered and the loser finds out.
//!
//! Here the placement call is delegated to the supervisor, and the delegation is
//! the whole safety argument: **a tail is started only by the node that is
//! actually running the workload.** If two nodes run it — the split brain the
//! fence exists for — both tails acquire, the later acquire wins, and the
//! earlier one's next [`round`] returns [`RoundOutcome::Fenced`]. That is
//! decisive rather than a coin flip only because the supervisor is obliged to
//! act on it: `Fenced` means **stop the workload**, not "log and carry on".
//! Acquiring happens once, at start, so the two nodes converge instead of
//! ping-ponging.
//!
//! [`claim`]: crate::claim
//! [`CoreWalSeam`]: crate::stream::CoreWalSeam
//! [`CoreWalSeam::open_reader`]: crate::stream::CoreWalSeam::open_reader

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::claim::{self, ClaimLost, ClaimOutcome, ClaimRecord};
use crate::dedup::{self, DedupOutcome};
use crate::hydrate::{subject_path, Tier};
use crate::snapshot::{self, BackupTarget, SnapshotOutcome};
use crate::stream::{
    self, CoreWalSeam, PreconditionSupport, PreflightStage, SourceFingerprint, StreamConfig,
    StreamOutcome,
};

/// How many times [`round`] will re-take a tier-2 base whose WAL generation
/// moved before the first tail could anchor to it.
///
/// Three, and the bound matters more than the number: each attempt is a full
/// copy of the database, so an application checkpointing faster than we can copy
/// must give up and report rather than spin. See [`SubjectOutcome::Stream`].
const MAX_BASE_ATTEMPTS: u32 = 3;

/// Everything [`start`] needs. Mirrors
/// [`HydrateRequest`](crate::hydrate::HydrateRequest) field for field where the
/// meaning is the same, because they are read from the same declaration.
pub struct TailRequest<'a> {
    /// The object store both the claim and the data live in.
    pub store: Arc<dyn ObjectStore>,
    /// Key prefix for this *workload*. The claim sits here; each subject hangs
    /// one level below — the identical layout [`crate::hydrate`] restores from,
    /// and not a parallel one, so a tail and a hydrate of the same workload
    /// cannot disagree about where the bytes are.
    pub store_prefix: &'a str,
    /// Host directory the named volume is bound from.
    pub volume_root: &'a Path,
    /// Volume-relative database paths, in declaration order.
    pub subjects: &'a [String],
    pub tier: Tier,
    /// Label recorded in the claim. Diagnostic; see [`ClaimRecord::owner`].
    pub owner: &'a str,
    /// Page size of the source databases. Required rather than sniffed for the
    /// reason [`StreamConfig::page_size`] is: a wrong guess silently produces
    /// frames nothing can replay.
    pub page_size: usize,
    /// RPO the caller's scheduler promises, carried into the watermark so a
    /// missed round is visible as a breach rather than as silence.
    pub rpo_target: Option<Duration>,
}

/// Why a tail could not start. Every variant means **do not stream**, and the
/// caller must decide separately whether the workload may still run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailRefusal {
    /// The sink does not honour conditional puts, so the fence is not real.
    /// Same refusal [`crate::hydrate`] makes, for the same reason: pointed at a
    /// store that ignores `If-Match`, two nodes' claims both succeed and every
    /// test still passes.
    SinkNotFenced { stage: PreflightStage },
    /// Somebody else's acquire landed between our read and our compare-and-swap.
    /// Not "somebody else owns this" — a sequentially-later acquire always wins;
    /// this is the genuinely concurrent case, and retrying it is the caller's
    /// call, not ours.
    ClaimLost { current: ClaimRecord },
}

impl TailRefusal {
    /// One line an operator can act on.
    pub fn headline(&self) -> String {
        match self {
            TailRefusal::SinkNotFenced { stage } => format!(
                "the sink does not enforce conditional puts ({}), so an ownership claim on it \
                 cannot fence anybody; refusing to stream rather than ship bytes behind a fence \
                 that is not there",
                stage.as_str()
            ),
            TailRefusal::ClaimLost { current } => format!(
                "lost the ownership claim race to epoch {} (owner {})",
                current.epoch, current.owner
            ),
        }
    }
}

/// A running tail. Holds the epoch it acquired and one target per subject.
pub struct TailSession {
    /// The fencing token every write in this session is stamped with.
    epoch: u64,
    /// Whoever we displaced at [`start`], if anybody. Diagnostic, and the only
    /// record that a takeover happened at all.
    displaced: Option<ClaimRecord>,
    workload_target: BackupTarget,
    subjects: Vec<SubjectState>,
    tier: Tier,
    owner: String,
    page_size: usize,
    rpo_target: Option<Duration>,
}

/// Per-subject state carried across rounds.
struct SubjectState {
    /// Volume-relative name, as declared.
    name: String,
    /// Absolute path on this node.
    path: PathBuf,
    target: BackupTarget,
    /// Tier 2 only: the base snapshot the frames are anchored to. `None` until
    /// one is published (or discovered) — see [`SubjectOutcome::Stream`].
    base_snapshot_key: Option<String>,
}

/// What one subject's turn in a round did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectOutcome {
    /// The declared database does not exist on the volume. Not an error: a
    /// workload may declare a subject it creates lazily, and the first rounds
    /// after a first placement legitimately find nothing. Reported rather than
    /// skipped silently so "why is this subject not backed up" has an answer.
    Absent,
    /// The source database was moving under every attempt to copy it, so
    /// nothing was published this round.
    ///
    /// R858-B18 made [`stream::raw_consistent_copy_live`] **refuse** rather than
    /// return a torn image when a checkpoint lands inside the read window, after
    /// [`stream::COPY_VALIDATION_ATTEMPTS`] tries. That refusal is a transient
    /// property of the writer, not a failure of this tail — @Ashguard:polaris
    /// measured 9 of 12 copies refused under a hammering writer with a 16-page
    /// autocheckpoint, and 12 of 12 accepted at a realistic rate. Treating it as
    /// fatal would take a busy appliance's backup down and leave it down.
    ///
    /// It is a *reported* non-event rather than a silent one because a subject
    /// that reports this every round forever is not backed up, and the operator
    /// needs to see that as a longer `wal_autocheckpoint`, not as silence.
    SourceTooHot { detail: String },
    /// Tier 1a.
    Snapshot(SnapshotOutcome),
    /// Tier 1b.
    Dedup(DedupOutcome),
    /// Tier 2. `base_published` is set on the round that established the base
    /// this stream is anchored to; `base_attempts` is how many copies that took
    /// (see [`MAX_BASE_ATTEMPTS`]).
    Stream {
        base_snapshot_key: String,
        base_published: bool,
        base_attempts: u32,
        outcome: StreamOutcome,
    },
}

/// One subject's turn, with what it cost.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectReport {
    pub subject: String,
    pub outcome: SubjectOutcome,
    pub seconds: f64,
}

/// What one [`round`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum RoundOutcome {
    /// Every subject got its turn.
    Backed(Vec<SubjectReport>),
    /// **This node is no longer the owner and wrote nothing.** The supervisor
    /// must stop the workload: a fenced node cannot ship a byte, so every write
    /// its application accepts from here on is a write nobody will ever be able
    /// to recover.
    ///
    /// Reached two ways, deliberately collapsed into one outcome because they
    /// oblige the caller identically: the pre-round [`claim::assert_holds`] said
    /// somebody minted a higher epoch, or a tier-2 sink bounced the write on the
    /// same evidence.
    Fenced { detail: String },
}

/// Acquire the claim and prepare a session, or say why not.
///
/// The order is [`crate::hydrate`]'s: prove the sink can fence *before* leaning
/// on the fence, then take the claim. Unlike hydrate there is no readiness check
/// to run first — a tail is only ever started for a workload the supervisor is
/// running, so it always intends to take ownership.
pub async fn start(req: TailRequest<'_>) -> Result<std::result::Result<TailSession, TailRefusal>> {
    let workload_target = BackupTarget {
        store: req.store.clone(),
        prefix: req.store_prefix.to_string(),
    };

    if let PreconditionSupport::Degraded { stage } =
        stream::probe_conditional_puts(&workload_target).await?
    {
        return Ok(Err(TailRefusal::SinkNotFenced { stage }));
    }

    let (epoch, displaced) = match claim::acquire(&workload_target, req.owner).await? {
        ClaimOutcome::Granted { claim, previous } => (claim.epoch, previous),
        ClaimOutcome::Lost { current } => return Ok(Err(TailRefusal::ClaimLost { current })),
    };

    let prefix = req.store_prefix.trim_matches('/');
    let mut subjects = Vec::with_capacity(req.subjects.len());
    for name in req.subjects {
        subjects.push(SubjectState {
            path: subject_path(req.volume_root, name)?,
            target: BackupTarget {
                store: req.store.clone(),
                prefix: if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                },
            },
            name: name.clone(),
            base_snapshot_key: None,
        });
    }

    Ok(Ok(TailSession {
        epoch,
        displaced,
        workload_target,
        subjects,
        tier: req.tier,
        owner: req.owner.to_string(),
        page_size: req.page_size,
        rpo_target: req.rpo_target,
    }))
}

impl TailSession {
    /// The fencing token this session streams under.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whoever this session's [`start`] displaced, if anybody.
    pub fn displaced(&self) -> Option<&ClaimRecord> {
        self.displaced.as_ref()
    }

    pub fn subjects(&self) -> impl Iterator<Item = &str> {
        self.subjects.iter().map(|s| s.name.as_str())
    }
}

/// One pass over every subject.
///
/// The claim is re-verified first, and that check is not redundant with the
/// sink-side fence: only tier 2 stamps its writes with an epoch the sink can
/// bounce. A tier-1a or tier-1b tail writes snapshots and manifests
/// unconditionally, so without this read a fenced node would keep overwriting
/// the real owner's backups with its own stale ones — the exact corruption the
/// fence exists to prevent, arriving through the door tier 2 happens to have
/// closed and the other two do not.
///
/// A subject that fails is propagated, not swallowed: unlike
/// `tenant-streamer`'s per-tenant isolation (one sick tenant must not stop a box
/// full of healthy ones), every subject here belongs to the same workload, and a
/// workload whose accounts database is backed up but whose sessions database is
/// not is not backed up.
pub async fn round(session: &mut TailSession) -> Result<RoundOutcome> {
    if let Err(lost) = claim::assert_holds(&session.workload_target, session.epoch).await? {
        return Ok(RoundOutcome::Fenced {
            detail: fenced_detail(&lost),
        });
    }

    let tier = session.tier;
    let page_size = session.page_size;
    let owner = session.owner.clone();
    let epoch = session.epoch;
    let rpo_target = session.rpo_target;

    // Cloned rather than borrowed: the loop below takes `session.subjects`
    // mutably, and `rebase` needs the WORKLOAD prefix — the claim lives there,
    // one level above each subject's. Getting that wrong is not a compile error
    // and not a wrong number; it is a rebase that asserts a claim nobody ever
    // wrote and refuses forever, which is how the test that found it failed.
    let workload_target = BackupTarget {
        store: session.workload_target.store.clone(),
        prefix: session.workload_target.prefix.clone(),
    };

    let mut reports = Vec::with_capacity(session.subjects.len());
    for subject in &mut session.subjects {
        let started = Instant::now();
        if !subject.path.exists() {
            reports.push(SubjectReport {
                subject: subject.name.clone(),
                outcome: SubjectOutcome::Absent,
                seconds: started.elapsed().as_secs_f64(),
            });
            continue;
        }
        let outcome = back_up_subject(
            subject,
            &workload_target,
            tier,
            page_size,
            &owner,
            epoch,
            rpo_target,
        )
            .await
            .with_context(|| format!("backing up subject {}", subject.name))?;
        if let SubjectOutcome::Stream {
            outcome: StreamOutcome::Fenced { current_epoch, our_epoch, .. },
            ..
        } = &outcome
        {
            return Ok(RoundOutcome::Fenced {
                detail: format!(
                    "the sink bounced subject {}: it is stamped with epoch {current_epoch} and we \
                     hold {our_epoch}",
                    subject.name
                ),
            });
        }
        reports.push(SubjectReport {
            subject: subject.name.clone(),
            outcome,
            seconds: started.elapsed().as_secs_f64(),
        });
    }
    Ok(RoundOutcome::Backed(reports))
}

fn fenced_detail(lost: &ClaimLost) -> String {
    match lost {
        ClaimLost::Superseded { .. } => format!("the ownership claim moved: {lost}"),
        // Not a state this crate produces. Treated as fenced anyway rather than
        // as "carry on": a claim we cannot verify is a claim we do not hold,
        // which is the same rule `claim::assert_holds`'s own doc states for an
        // unreachable store.
        ClaimLost::Vanished { .. } => format!("{lost}; treating that as fenced"),
    }
}

async fn back_up_subject(
    subject: &mut SubjectState,
    workload_target: &BackupTarget,
    tier: Tier,
    page_size: usize,
    owner: &str,
    epoch: u64,
    rpo_target: Option<Duration>,
) -> Result<SubjectOutcome> {
    // Owned, so the `Tier::Stream` arm can take `subject` mutably below.
    let path = subject
        .path
        .to_str()
        .with_context(|| format!("subject path {} is not UTF-8", subject.path.display()))?
        .to_string();
    let path = path.as_str();

    match tier {
        Tier::Snapshot => {
            let image = match live_image(path, page_size).await? {
                Ok(image) => image,
                Err(too_hot) => return Ok(too_hot),
            };
            Ok(SubjectOutcome::Snapshot(
                snapshot::upload_snapshot_image(&subject.target, &image).await?,
            ))
        }
        Tier::Dedup => {
            let image = match live_image(path, page_size).await? {
                Ok(image) => image,
                Err(too_hot) => return Ok(too_hot),
            };
            Ok(SubjectOutcome::Dedup(
                dedup::snapshot_dedup_image(&image, &subject.target).await?,
            ))
        }
        Tier::Stream => {
            stream_subject(subject, workload_target, path, page_size, owner, epoch, rpo_target)
                .await
        }
    }
}

/// [`stream::raw_consistent_copy_live`] with its one *expected* failure split
/// out of the error channel.
///
/// `Ok(Ok(image))` is a validated point-in-time copy; `Ok(Err(SourceTooHot))` is
/// "the writer never held still, try again next round"; `Err` is a real failure.
///
/// The discrimination is a string match on the refusal's own sentence, which is
/// ugly and is the honest option available: that path returns `anyhow::Error`
/// with no typed variant, and adding one means editing `stream.rs`, which
/// @Ashguard:polaris holds live under R858-B18. The phrase is pinned by
/// [`tests::a_source_that_never_holds_still_is_too_hot_not_a_failure`], which
/// provokes the real error rather than asserting on a copy of the string — so a
/// reword on their side fails a test here instead of silently turning a busy
/// database into a dead backup.
async fn live_image(
    path: &str,
    page_size: usize,
) -> Result<std::result::Result<Vec<u8>, SubjectOutcome>> {
    match stream::raw_consistent_copy_live(path, page_size).await {
        Ok(image) => Ok(Ok(image)),
        Err(e) if is_source_too_hot(&e) => Ok(Err(SubjectOutcome::SourceTooHot {
            detail: format!("{e:#}"),
        })),
        Err(e) => Err(e),
    }
}

/// Whether an error is R858-B18's "the source moved under every attempt"
/// refusal. See [`live_image`].
fn is_source_too_hot(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("the source moved under every one of")
}

/// Tier 2: anchor to a base, then tail frames onto it.
///
/// ## Why the base and the first tail are one operation
///
/// A tier-2 restore is *base image + replayed frames*. The base this module
/// publishes is a [`stream::raw_consistent_copy_live`] image, which already has
/// every frame committed up to the copy point folded in; the first tail then
/// uploads frames `1..=max` of the live WAL, and replaying those over the base
/// is idempotent because it rewrites the same pages with the same content.
///
/// What breaks that is a **checkpoint landing between the copy and the tail**.
/// The fold moves those frames into the main file and resets the WAL, so the
/// tail uploads frames `1..=max` of a *new* generation — and anything the
/// application committed after our copy point but before the fold is in neither
/// the base nor the uploaded frames. Nothing downstream can detect this: the
/// chain validates, the restore succeeds, and the result is a database missing a
/// window of writes.
///
/// So the generation is sampled around the whole base-plus-first-tail and the
/// pair is redone if it moved, up to [`MAX_BASE_ATTEMPTS`]. This costs nothing
/// on every subsequent round, which is the overwhelming majority: once a base
/// exists, a fold is just [`StreamOutcome::Restarted`], which re-uploads from
/// frame 1 against a base that is already correct.
async fn stream_subject(
    subject: &mut SubjectState,
    workload_target: &BackupTarget,
    path: &str,
    page_size: usize,
    owner: &str,
    epoch: u64,
    rpo_target: Option<Duration>,
) -> Result<SubjectOutcome> {
    // A base may already exist from an earlier incarnation of this workload —
    // including the one whose bytes `hydrate` just restored onto this volume.
    // Re-publishing then would be a second full copy for nothing.
    if subject.base_snapshot_key.is_none() {
        subject.base_snapshot_key = snapshot::latest_snapshot_key(&subject.target).await?;
    }

    if let Some(base) = subject.base_snapshot_key.clone() {
        let outcome = tail_once(subject, path, &base, page_size, owner, epoch, rpo_target).await?;
        // R858-B19: a WAL recreate is not an ordinary round. `tail_frames` has
        // re-uploaded frames 1..N under a NEW generation, and the prefix now
        // holds manifests from two — which `validate_generation_chain` refuses
        // ("WAL restart between generations, restore needs a fresh tier-1a
        // snapshot"). Folding this into the Streamed arm and logging "streamed"
        // leaves a prefix that looks healthy and cannot be restored, which is
        // the worst of the available outcomes.
        if matches!(outcome, StreamOutcome::Restarted { .. }) {
            return rebase(
                subject,
                workload_target,
                path,
                page_size,
                owner,
                epoch,
                rpo_target,
            )
            .await;
        }
        return Ok(SubjectOutcome::Stream {
            base_snapshot_key: base,
            base_published: false,
            base_attempts: 0,
            outcome,
        });
    }

    let mut last_generation = None;
    for attempt in 1..=MAX_BASE_ATTEMPTS {
        let before = SourceFingerprint::read(path)?;
        let image = match live_image(path, page_size).await? {
            Ok(image) => image,
            Err(too_hot) => return Ok(too_hot),
        };
        let base = snapshot::upload_base_snapshot(&subject.target, &image).await?;
        let outcome = tail_once(subject, path, &base, page_size, owner, epoch, rpo_target).await?;
        let after = SourceFingerprint::read(path)?;
        if before.stable_across(&after) {
            subject.base_snapshot_key = Some(base.clone());
            return Ok(SubjectOutcome::Stream {
                base_snapshot_key: base,
                base_published: true,
                base_attempts: attempt,
                outcome,
            });
        }
        last_generation = Some((before, after));
    }

    let (before, after) = last_generation.expect("MAX_BASE_ATTEMPTS is non-zero");
    anyhow::bail!(
        "could not anchor a tier-2 base for {path}: the source WAL was folded during each of \
         {MAX_BASE_ATTEMPTS} attempts (last: {before:?} -> {after:?}). The base and the frames \
         would describe different points in time, so nothing was anchored; the next round retries. \
         An application checkpointing faster than its database can be copied needs a longer \
         wal_autocheckpoint, not a longer retry loop."
    );
}

/// Re-anchor a subject whose WAL was recreated: publish a fresh tier-1a base,
/// drop the chain that can no longer validate, and tail onto the new base.
///
/// R858-B19 made a generation's identity `(checkpoint_seq, salt)` and taught
/// restore to REFUSE a chain that spans a recreate rather than splice it. That
/// turned a silent wrong restore into a loud one, and left somebody owing the
/// repair. This is it.
///
/// ## The order is chosen for what a crash in the middle leaves behind
///
/// 1. **Publish the new base.** A crash here leaves two bases and the old
///    manifests, so a restore *refuses* — loud, and the next round rebases
///    again and clears it.
/// 2. **Delete every generation manifest.** A crash here leaves the new base
///    and no chain, which `hydrate` restores as a bare tier-1a snapshot at the
///    point the base was taken. Correct, just behind.
/// 3. **Delete the watermark sidecar**, so the next tail starts a chain at
///    frame 1 — `validate_generation_chain` requires a chain to start there,
///    so resuming mid-range would produce another unrestorable prefix.
/// 4. **Tail onto the new base.**
///
/// Deleting them the other way round — chain first — would leave the *old* base
/// as the newest snapshot with no manifests, and a restore would silently
/// succeed at a stale point in time. Loud beats silent, so the base goes first.
///
/// ## What this leaves behind, deliberately
///
/// The old generation's frame objects are orphaned under
/// `frames/{old_checkpoint_seq}/`. They are keyed by sequence so they collide
/// with nothing and are invisible to restore once their manifests are gone;
/// reclaiming them is GC's job, not a rebase's, and deleting data as part of a
/// recovery path is how a recovery path becomes the outage.
///
/// ## The fence during the gap
///
/// Step 3 removes the sidecar that carries the epoch `tail_frames` bounces a
/// stale writer on, so between it and step 4 the sink is briefly unfenced. The
/// claim is re-asserted immediately before step 1 to narrow that window, and the
/// claim — not the watermark — is this design's real fence: `round` verifies it
/// every pass, and a node that lost it stops its workload.
async fn rebase(
    subject: &mut SubjectState,
    workload_target: &BackupTarget,
    path: &str,
    page_size: usize,
    owner: &str,
    epoch: u64,
    rpo_target: Option<Duration>,
) -> Result<SubjectOutcome> {
    if let Err(lost) = claim::assert_holds(workload_target, epoch).await? {
        anyhow::bail!(
            "refusing to re-anchor {path} after a WAL restart: {lost}. Rebasing rewrites the \
             chain, and doing that without the claim would destroy the real owner's"
        );
    }

    let image = match live_image(path, page_size).await? {
        Ok(image) => image,
        Err(too_hot) => return Ok(too_hot),
    };
    let base = snapshot::upload_base_snapshot(&subject.target, &image).await?;
    delete_generation_manifests(&subject.target).await?;
    subject
        .target
        .store
        .delete(&subject.target.watermark_key())
        .await
        .or_else(ignore_absent)
        .with_context(|| format!("clearing the stream watermark for {path}"))?;

    let outcome = tail_once(subject, path, &base, page_size, owner, epoch, rpo_target).await?;
    subject.base_snapshot_key = Some(base.clone());
    Ok(SubjectOutcome::Stream {
        base_snapshot_key: base,
        base_published: true,
        base_attempts: 1,
        outcome,
    })
}

async fn delete_generation_manifests(target: &BackupTarget) -> Result<()> {
    let prefix = target.prefix.trim_matches('/');
    let dir = object_store::path::Path::from(if prefix.is_empty() {
        "generations".to_string()
    } else {
        format!("{prefix}/generations")
    });
    let listing = target
        .store
        .list_with_delimiter(Some(&dir))
        .await
        .with_context(|| format!("listing generation manifests under {dir}"))?;
    for object in listing.objects {
        target
            .store
            .delete(&object.location)
            .await
            .or_else(ignore_absent)
            .with_context(|| format!("deleting stale generation manifest {}", object.location))?;
    }
    Ok(())
}

/// A delete of something already gone is the outcome we wanted, not a failure —
/// two rebases racing, or a retry after a partial one, both land here.
fn ignore_absent(e: object_store::Error) -> std::result::Result<(), object_store::Error> {
    match e {
        object_store::Error::NotFound { .. } => Ok(()),
        other => Err(other),
    }
}

/// One `tail_frames` through a seam opened for this call only.
///
/// The seam's lifetime is exactly this function, and that is load-bearing rather
/// than tidy — see this module's doc, point 2: a held reader never observes the
/// application's later commits, and reopening while still holding one returns
/// the same handle from turso's process-global registry.
async fn tail_once(
    subject: &SubjectState,
    path: &str,
    base_snapshot_key: &str,
    page_size: usize,
    owner: &str,
    epoch: u64,
    rpo_target: Option<Duration>,
) -> Result<StreamOutcome> {
    let cfg = StreamConfig {
        base_snapshot_key,
        page_size,
        backpressure: Default::default(),
        rpo_target,
        epoch,
        owner: Some(owner),
        // R736-T2: an appliance is not in a cell-move protocol, so there is no
        // pointer generation to track. `0` is the documented unfenced default
        // for that level; the epoch above is the level that is real here.
        pointer_generation: 0,
    };
    let seam = CoreWalSeam::open_reader(path)
        .with_context(|| format!("opening a read-only WAL seam on {path}"))?;
    stream::tail_frames(&seam, &subject.target, &cfg)
        .await
        .with_context(|| format!("tailing {path} at epoch {epoch}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// A scratch volume that cleans up after itself.
    struct Volume(PathBuf);

    impl Volume {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "turso-backup-tail-{tag}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn path(&self) -> &Path {
            &self.0
        }
        fn subject(&self, name: &str) -> String {
            self.0.join(name).to_str().unwrap().to_string()
        }
    }

    impl Drop for Volume {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn seed(path: &str, start: i64, count: i64) {
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
        r.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn request<'a>(
        store: Arc<dyn ObjectStore>,
        vol: &'a Volume,
        subjects: &'a [String],
        tier: Tier,
        owner: &'a str,
    ) -> TailRequest<'a> {
        TailRequest {
            store,
            store_prefix: "workloads/acct",
            volume_root: vol.path(),
            subjects,
            tier,
            owner,
            page_size: 4096,
            rpo_target: None,
        }
    }

    /// The end-to-end this ticket's remaining half is for: a tail writes to the
    /// store and a hydrate reads it back. Before this module a hydrate against a
    /// real camp returned `nothing_in_the_store` forever, because nothing wrote.
    #[tokio::test]
    async fn a_tail_then_a_hydrate_round_trips_the_declared_state() {
        let vol = Volume::new("roundtrip");
        let subjects = vec!["accounts.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 120).await;

        let store = store();
        let mut session =
            match start(request(store.clone(), &vol, &subjects, Tier::Stream, "node-a"))
                .await
                .unwrap()
            {
                Ok(s) => s,
                Err(r) => panic!("start refused: {}", r.headline()),
            };
        assert_eq!(session.epoch(), claim::FIRST_EPOCH);
        assert!(session.displaced().is_none(), "a virgin prefix displaces nobody");

        let RoundOutcome::Backed(reports) = round(&mut session).await.unwrap() else {
            panic!("the first round should not be fenced");
        };
        assert_eq!(reports.len(), 1);
        assert!(
            matches!(&reports[0].outcome, SubjectOutcome::Stream { base_published: true, .. }),
            "the first round publishes the base: {:?}",
            reports[0].outcome
        );

        // More writes, then a second round: the base is reused, frames advance.
        seed(&vol.subject("accounts.db"), 120, 80).await;
        let RoundOutcome::Backed(reports) = round(&mut session).await.unwrap() else {
            panic!("the second round should not be fenced");
        };
        assert!(
            matches!(&reports[0].outcome, SubjectOutcome::Stream { base_published: false, .. }),
            "the base is published once, not per round: {:?}",
            reports[0].outcome
        );

        // Now the consumer: hydrate an empty volume from what the tail wrote.
        let dest = Volume::new("roundtrip-dest");
        let outcome = crate::hydrate::hydrate(crate::hydrate::HydrateRequest {
            store,
            store_prefix: "workloads/acct",
            volume_root: dest.path(),
            subjects: &subjects,
            tier: Tier::Stream,
            owner: "node-b",
        })
        .await
        .unwrap();
        assert!(
            matches!(outcome, crate::hydrate::HydrateOutcome::Hydrated { .. }),
            "the tail's output must be hydratable: {outcome:?}"
        );
        assert_eq!(
            count_rows(&dest.subject("accounts.db")).await,
            200,
            "every row the application committed before the last round must come back"
        );
    }

    /// The driving shape: three databases in one volume, all of them declared.
    /// A workload backed up on two of three is not backed up.
    #[tokio::test]
    async fn every_declared_subject_is_backed_up_not_just_the_first() {
        let vol = Volume::new("three");
        let subjects = vec![
            "accounts.db".to_string(),
            "passkeys.db".to_string(),
            "sessions.db".to_string(),
        ];
        for (i, s) in subjects.iter().enumerate() {
            seed(&vol.subject(s), 0, 10 * (i as i64 + 1)).await;
        }

        let store = store();
        let mut session = start(request(store.clone(), &vol, &subjects, Tier::Stream, "node-a"))
            .await
            .unwrap()
            .expect("start");
        let RoundOutcome::Backed(reports) = round(&mut session).await.unwrap() else {
            panic!("not fenced");
        };
        assert_eq!(reports.len(), 3);
        for r in &reports {
            assert!(
                matches!(r.outcome, SubjectOutcome::Stream { .. }),
                "{} was not streamed: {:?}",
                r.subject,
                r.outcome
            );
        }

        let dest = Volume::new("three-dest");
        crate::hydrate::hydrate(crate::hydrate::HydrateRequest {
            store,
            store_prefix: "workloads/acct",
            volume_root: dest.path(),
            subjects: &subjects,
            tier: Tier::Stream,
            owner: "node-b",
        })
        .await
        .unwrap();
        for (i, s) in subjects.iter().enumerate() {
            assert_eq!(
                count_rows(&dest.subject(s)).await,
                10 * (i as i64 + 1),
                "subject {s} did not come back"
            );
        }
    }

    /// R858-B18's refusal must reach [`round`] as a reported non-event, not as a
    /// dead backup. Provokes the REAL error rather than asserting on a copy of
    /// its text, so a reword in `stream.rs` fails here instead of silently
    /// reclassifying a busy database as a failure.
    #[tokio::test]
    async fn a_source_that_never_holds_still_is_too_hot_not_a_failure() {
        let vol = Volume::new("hot");
        let path = vol.subject("accounts.db");
        seed(&path, 0, 10).await;

        // A `take` that grows the source between the two fingerprint samples —
        // every attempt sees movement, which is exactly the hammering-writer
        // shape, without needing a second process to hammer.
        let err = stream::validated_against_source(&path, "live copy", || async {
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            std::io::Write::write_all(&mut f, &vec![0u8; 4096]).unwrap();
            Ok(Vec::<u8>::new())
        })
        .await
        .expect_err("a source that moves under every attempt must be refused");
        assert!(
            is_source_too_hot(&err),
            "the too-hot refusal was not recognised, so a busy appliance's tail would die \
             instead of retrying next round: {err:#}"
        );

        // The negative half: an ordinary failure must NOT be classified as
        // transient, or a genuinely broken subject retries forever in silence.
        let missing = stream::raw_consistent_copy_live(&vol.subject("nope.db"), 4096)
            .await
            .expect_err("a missing database is a failure");
        assert!(!is_source_too_hot(&missing), "{missing:#}");
    }

    /// Fold the WAL under a running tail and the restore must still work.
    ///
    /// This is the failure @Ashguard:hydra caught in review, and it is silent
    /// without the rebase: `tail_frames` reports `Restarted`, the prefix ends up
    /// holding manifests from two WAL generations, and
    /// `validate_generation_chain` refuses the whole chain — so a hydrate of a
    /// prefix that has been happily "streaming" for weeks fails, or (if the
    /// refusal is ever relaxed) restores spliced frames. Both halves are
    /// asserted: the chain restores AND it carries the post-fold rows.
    #[tokio::test]
    async fn a_wal_restart_re_anchors_the_chain_instead_of_breaking_the_restore() {
        let vol = Volume::new("refold");
        let subjects = vec!["accounts.db".to_string()];
        let db = vol.subject("accounts.db");
        seed(&db, 0, 50).await;

        let store = store();
        let mut session = start(request(store.clone(), &vol, &subjects, Tier::Stream, "node-a"))
            .await
            .unwrap()
            .expect("start");
        assert!(matches!(round(&mut session).await.unwrap(), RoundOutcome::Backed(_)));

        // Fold the WAL into the main file and reset it — a checkpoint, which is
        // what SQLite does on its own every `wal_autocheckpoint` pages. The test
        // owns this database, so it can drive one directly; a real appliance's
        // does it unprompted, which is why this cannot be designed away.
        {
            let d = turso::Builder::new_local(&db).build().await.unwrap();
            let c = d.connect().unwrap();
            let mut rows = c.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await.unwrap();
            while rows.next().await.unwrap().is_some() {}
        }
        seed(&db, 50, 25).await;

        let RoundOutcome::Backed(reports) = round(&mut session).await.unwrap() else {
            panic!("not fenced")
        };
        match &reports[0].outcome {
            SubjectOutcome::Stream { base_published, .. } => assert!(
                base_published,
                "a WAL restart must re-anchor onto a fresh base, not keep streaming onto the \
                 old one: {:?}",
                reports[0].outcome
            ),
            other => panic!("{other:?}"),
        }

        let dest = Volume::new("refold-dest");
        crate::hydrate::hydrate(crate::hydrate::HydrateRequest {
            store,
            store_prefix: "workloads/acct",
            volume_root: dest.path(),
            subjects: &subjects,
            tier: Tier::Stream,
            owner: "node-b",
        })
        .await
        .expect("a prefix that survived a WAL restart must still be restorable");
        assert_eq!(
            count_rows(&dest.subject("accounts.db")).await,
            75,
            "the restore came back at the wrong point in time"
        );
    }

    /// A subject the application has not created yet is reported, not fatal.
    #[tokio::test]
    async fn an_absent_subject_is_reported_rather_than_failing_the_round() {
        let vol = Volume::new("absent");
        let subjects = vec!["accounts.db".to_string(), "not-yet.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 5).await;

        let store = store();
        let mut session = start(request(store, &vol, &subjects, Tier::Stream, "node-a"))
            .await
            .unwrap()
            .expect("start");
        let RoundOutcome::Backed(reports) = round(&mut session).await.unwrap() else {
            panic!("not fenced");
        };
        assert!(matches!(reports[0].outcome, SubjectOutcome::Stream { .. }));
        assert_eq!(reports[1].outcome, SubjectOutcome::Absent);
    }

    /// The fence, from the losing side. A second node's `start` displaces the
    /// first, and the first finds out on its next round rather than continuing
    /// to write into somebody else's prefix.
    #[tokio::test]
    async fn a_displaced_tail_is_fenced_on_its_next_round() {
        let vol = Volume::new("fence");
        let subjects = vec!["accounts.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 20).await;

        let store = store();
        let mut first = start(request(store.clone(), &vol, &subjects, Tier::Stream, "node-a"))
            .await
            .unwrap()
            .expect("start a");
        assert!(matches!(round(&mut first).await.unwrap(), RoundOutcome::Backed(_)));

        let second = start(request(store, &vol, &subjects, Tier::Stream, "node-b"))
            .await
            .unwrap()
            .expect("start b");
        assert_eq!(second.epoch(), first.epoch() + 1, "a takeover is a monotonic bump");
        assert_eq!(
            second.displaced().map(|c| c.owner.as_str()),
            Some("node-a"),
            "the takeover records who it displaced"
        );

        match round(&mut first).await.unwrap() {
            RoundOutcome::Fenced { detail } => {
                assert!(detail.contains("claim"), "unhelpful fence detail: {detail}")
            }
            other => panic!("the displaced tail kept writing: {other:?}"),
        }
    }

    /// The reason [`round`] re-reads the claim rather than relying on the
    /// sink-side fence: tiers 1a and 1b stamp nothing, so *only* this check
    /// stops a fenced node from overwriting the real owner's backups.
    #[tokio::test]
    async fn a_tier_1_tail_is_fenced_too_even_though_its_writes_carry_no_epoch() {
        let vol = Volume::new("fence-t1");
        let subjects = vec!["accounts.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 20).await;

        let store = store();
        let mut first = start(request(store.clone(), &vol, &subjects, Tier::Snapshot, "node-a"))
            .await
            .unwrap()
            .expect("start a");
        assert!(matches!(round(&mut first).await.unwrap(), RoundOutcome::Backed(_)));
        let _second = start(request(store, &vol, &subjects, Tier::Snapshot, "node-b"))
            .await
            .unwrap()
            .expect("start b");
        assert!(
            matches!(round(&mut first).await.unwrap(), RoundOutcome::Fenced { .. }),
            "a tier-1a tail that lost the claim must stop writing"
        );
    }

    /// Tier 1a on an idle database must not re-upload the whole file every
    /// round — that is what [`snapshot::upload_snapshot_image`]'s gate is for.
    #[tokio::test]
    async fn an_idle_tier_1a_subject_stops_uploading() {
        let vol = Volume::new("idle");
        let subjects = vec!["accounts.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 30).await;

        let store = store();
        let mut session = start(request(store, &vol, &subjects, Tier::Snapshot, "node-a"))
            .await
            .unwrap()
            .expect("start");
        let RoundOutcome::Backed(first) = round(&mut session).await.unwrap() else {
            panic!("not fenced")
        };
        assert!(
            matches!(first[0].outcome, SubjectOutcome::Snapshot(SnapshotOutcome::Uploaded { .. })),
            "{:?}",
            first[0].outcome
        );
        let RoundOutcome::Backed(second) = round(&mut session).await.unwrap() else {
            panic!("not fenced")
        };
        assert!(
            matches!(
                second[0].outcome,
                SubjectOutcome::Snapshot(SnapshotOutcome::Deduplicated { .. })
            ),
            "an untouched database was re-uploaded: {:?}",
            second[0].outcome
        );
    }

    /// Tier 1b round-trips through the image-shaped entry point this ticket
    /// split out of [`dedup::snapshot_dedup`].
    #[tokio::test]
    async fn tier_1b_backs_up_a_live_database_and_hydrate_reads_it() {
        let vol = Volume::new("dedup");
        let subjects = vec!["accounts.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 60).await;

        let store = store();
        let mut session = start(request(store.clone(), &vol, &subjects, Tier::Dedup, "node-a"))
            .await
            .unwrap()
            .expect("start");
        let RoundOutcome::Backed(reports) = round(&mut session).await.unwrap() else {
            panic!("not fenced")
        };
        assert!(
            matches!(reports[0].outcome, SubjectOutcome::Dedup(DedupOutcome::Snapshotted { .. })),
            "{:?}",
            reports[0].outcome
        );

        let dest = Volume::new("dedup-dest");
        crate::hydrate::hydrate(crate::hydrate::HydrateRequest {
            store,
            store_prefix: "workloads/acct",
            volume_root: dest.path(),
            subjects: &subjects,
            tier: Tier::Dedup,
            owner: "node-b",
        })
        .await
        .unwrap();
        assert_eq!(count_rows(&dest.subject("accounts.db")).await, 60);
    }

    /// A second incarnation on the same node — the ordinary restart — must not
    /// re-upload a base it already has. The hydrate that put the bytes there
    /// left a base in the prefix; a new session discovers it.
    #[tokio::test]
    async fn a_restarted_tail_adopts_the_existing_base_instead_of_republishing() {
        let vol = Volume::new("restart");
        let subjects = vec!["accounts.db".to_string()];
        seed(&vol.subject("accounts.db"), 0, 40).await;

        let store = store();
        let mut first = start(request(store.clone(), &vol, &subjects, Tier::Stream, "node-a"))
            .await
            .unwrap()
            .expect("start a");
        assert!(matches!(round(&mut first).await.unwrap(), RoundOutcome::Backed(_)));
        drop(first);

        let mut second = start(request(store, &vol, &subjects, Tier::Stream, "node-a"))
            .await
            .unwrap()
            .expect("restart a");
        let RoundOutcome::Backed(reports) = round(&mut second).await.unwrap() else {
            panic!("not fenced")
        };
        match &reports[0].outcome {
            SubjectOutcome::Stream { base_published, base_snapshot_key, .. } => {
                assert!(!base_published, "a restart re-copied the whole database for nothing");
                assert!(base_snapshot_key.contains("snapshots/"), "{base_snapshot_key}");
            }
            other => panic!("{other:?}"),
        }
    }
}
