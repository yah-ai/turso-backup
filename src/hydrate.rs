//! R850-F1 — **hydrate-on-place**: materialise a workload's declared databases
//! into an empty volume before the workload starts, under a fence.
//!
//! The three restore entry points this crate already has
//! ([`snapshot::restore_latest`], [`dedup::restore_latest_dedup`],
//! [`stream::restore_latest_stream`]) each take one database path and assume
//! the caller decided that a restore should happen. This module is that
//! decision, for the shape R850 is actually about: **one volume, several
//! databases, one node that may or may not be allowed to bring them up.**
//!
//! # The trigger is emptiness, and emptiness has three answers not two
//!
//! The rule R850 filed was "restore when the named volume is EMPTY, so a normal
//! restart never re-hydrates". That is right and it is incomplete, because a
//! workload with three databases has a third state. [`assess`] names all three:
//!
//! - **every subject absent** → [`Readiness::Hydrate`]. A fresh node, or a
//!   node taking over after the old one died.
//! - **every subject present** → [`Readiness::AlreadyPopulated`]. An ordinary
//!   restart. Re-hydrating here would roll the databases back to the last
//!   watermark and silently destroy everything written since — the single worst
//!   thing this module could do, so it is the case with the loudest test.
//! - **some of each** → [`Readiness::TornVolume`], and it is **refused**.
//!   Restoring only the absent ones would reconstruct a set of databases at
//!   different points in time; for the driving case (accounts, passkeys and
//!   sessions that reference each other) that is a live application whose data
//!   silently disagrees with itself. There is no safe automatic answer, so the
//!   operator gets a refusal that names both lists instead of a repair that
//!   looks like it worked.
//!
//! # The fence, and why it is checked twice
//!
//! Ownership comes from [`claim`], not from this module — see that module for
//! why the object store is the authority an appliance has. Here it is used at
//! two points, and the second is the one that is easy to leave out:
//!
//! 1. Before reading a byte, [`claim::acquire`]. Losing means another node is
//!    placing the same appliance *right now*, and the loser must not hydrate.
//! 2. After the last subject lands and before the caller starts anything,
//!    [`claim::assert_holds`]. A restore is seconds to minutes long and is not
//!    atomic; a takeover that lands in the middle leaves this node holding a
//!    complete, plausible, *stale* copy of somebody else's database.
//!
//! On (2) the restored bytes are deliberately **left on disk**. Deleting a
//! database because a fence moved is a worse failure than refusing to start
//! with it, and the caller has the outcome it needs to refuse.
//!
//! What this module cannot do is stop the *losing* node's workload: it is not a
//! supervisor and has no handle on the process. Whoever calls this must treat
//! [`HydrateOutcome::Refused`] as "do not start", and must treat a later
//! [`stream::StreamOutcome::Fenced`] as "stop what you started". That
//! obligation is the whole reason it is stated here rather than assumed.
//!
//! # Object layout
//!
//! Each subject hangs at `<store_prefix>/<subject>`, so a volume containing
//! `db/accounts.db` backs up under `.../db/accounts.db/{snapshots,frames,…}`
//! and an operator listing the bucket sees the layout they see on the volume.
//! The **claim is one level up**, on `<store_prefix>` itself: at-most-one-live
//! is a property of the workload, not of each file, and per-file claims would
//! let a node win two of three and hydrate a torn set legitimately.
//!
//! [`snapshot::restore_latest`]: crate::snapshot::restore_latest
//! [`dedup::restore_latest_dedup`]: crate::dedup::restore_latest_dedup
//! [`stream::restore_latest_stream`]: crate::stream::restore_latest_stream
//! [`claim`]: crate::claim
//! [`claim::acquire`]: crate::claim::acquire
//! [`claim::assert_holds`]: crate::claim::assert_holds
//! [`stream::StreamOutcome::Fenced`]: crate::stream::StreamOutcome::Fenced

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use object_store::ObjectStore;

use store_claim::claim::{self, ClaimLost, ClaimOutcome, ClaimRecord};
use store_claim::{BackupTarget, PreconditionSupport, PreflightStage};

/// The bytes-shipping half of a durability tier.
///
/// Deliberately not `workload_spec::DurabilityTier`: that enum has a `None`
/// arm, and a tier that ships nothing has no hydrate path — carrying it here
/// would mean an unreachable arm in every match. turso-backup also takes no
/// dependency on yah types (the same stance `stream::StreamConfig::epoch`
/// takes toward `yah_tenant_pointer`), so the caller translates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Tier 1a — newest full `VACUUM INTO` snapshot.
    Snapshot,
    /// Tier 1b — newest page-dedup manifest.
    Dedup,
    /// Tier 2 — base snapshot plus WAL-frame replay. Falls back to the base
    /// alone when no generations have been written yet, which is the state a
    /// stream-tier prefix is in between its first snapshot and its first tail.
    Stream,
}

impl Tier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Tier::Snapshot => "snapshot",
            Tier::Dedup => "dedup",
            Tier::Stream => "stream",
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether one subject already exists on the volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectState {
    /// No file, or a zero-length one.
    ///
    /// Zero counts as absent on purpose: a zero-byte file is not a database,
    /// and treating one as populated would wedge a node into
    /// [`Readiness::AlreadyPopulated`] forever after a truncated write or a
    /// bind-mount that touched the path into existence.
    Absent,
    /// A file with content. Its size is carried only for the diagnostic in
    /// [`Readiness::TornVolume`] — nothing decides on it.
    Populated { bytes: u64 },
}

impl SubjectState {
    fn is_absent(&self) -> bool {
        matches!(self, SubjectState::Absent)
    }
}

/// What the state of the volume says should happen. See the module doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    /// Every subject is absent.
    Hydrate,
    /// Every subject is present. An ordinary restart.
    AlreadyPopulated,
    /// Some of each. Refused — see the module doc.
    TornVolume {
        populated: Vec<String>,
        absent: Vec<String>,
    },
}

/// Decide from the observed volume state alone. Pure — the I/O is
/// [`inspect_volume`], split out so this half is testable without a filesystem
/// and so a caller that already knows the state need not stat twice.
///
/// An empty `states` is an error, not a `Hydrate`: a bytes-shipping tier with
/// no subjects is a declaration that names nothing, and treating it as "restore
/// everything" (or as "nothing to do") both read a mistake as an intention.
pub fn assess(states: &[(String, SubjectState)]) -> Result<Readiness> {
    if states.is_empty() {
        bail!("no subjects to hydrate — a tier that ships bytes must name at least one database");
    }
    let (absent, populated): (Vec<_>, Vec<_>) =
        states.iter().partition(|(_, s)| s.is_absent());
    match (absent.is_empty(), populated.is_empty()) {
        (true, false) => Ok(Readiness::AlreadyPopulated),
        (false, true) => Ok(Readiness::Hydrate),
        (false, false) => Ok(Readiness::TornVolume {
            populated: populated.iter().map(|(n, _)| n.clone()).collect(),
            absent: absent.iter().map(|(n, _)| n.clone()).collect(),
        }),
        // Both empty is unreachable given the guard above.
        (true, true) => unreachable!("states was checked non-empty"),
    }
}

/// Stat each subject under `volume_root`.
///
/// `subjects` are volume-relative and are joined verbatim, so a caller passing
/// an absolute or `..`-bearing path would escape the volume. Validating that is
/// the declaration's job
/// (`workload_spec::WorkloadSpec::durability` refuses both at parse time), and
/// it is re-checked here rather than trusted, because this function's argument
/// can come from anywhere and its result decides where bytes get written.
pub fn inspect_volume(
    volume_root: &Path,
    subjects: &[String],
) -> Result<Vec<(String, SubjectState)>> {
    subjects
        .iter()
        .map(|subject| {
            let path = subject_path(volume_root, subject)?;
            let state = match std::fs::metadata(&path) {
                Ok(m) if m.len() > 0 => SubjectState::Populated { bytes: m.len() },
                Ok(_) => SubjectState::Absent,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => SubjectState::Absent,
                Err(e) => {
                    return Err(e).with_context(|| format!("stat-ing subject {}", path.display()))
                }
            };
            Ok((subject.clone(), state))
        })
        .collect()
}

/// Join a volume-relative subject onto the volume root, refusing anything that
/// would land outside it.
///
/// Public since R850-F1's tail half: the backup side must resolve exactly the
/// same subject to exactly the same file the restore side writes, and a second
/// implementation of that join is a second traversal check to get wrong.
pub fn subject_path(volume_root: &Path, subject: &str) -> Result<PathBuf> {
    if subject.is_empty() {
        bail!("empty subject path");
    }
    let p = Path::new(subject);
    if p.is_absolute() {
        bail!("subject {subject:?} is absolute; subjects are relative to the volume root");
    }
    if p.components()
        .any(|c| matches!(c, std::path::Component::ParentDir | std::path::Component::CurDir))
    {
        bail!("subject {subject:?} contains a \".\" or \"..\" component");
    }
    Ok(volume_root.join(p))
}

/// One subject's restore, with what it cost.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectRestore {
    pub subject: String,
    /// The object key the bytes came from — a snapshot key, a manifest key, or
    /// the base snapshot key of a replayed chain.
    pub source: String,
    /// Bytes on disk after the restore.
    pub bytes: u64,
    /// Wall-clock seconds this subject took. **Measured, not modelled** — the
    /// distinction `topology::RecoveryEstimate` has to make in the other
    /// direction, where a declared `state-mb` is multiplied by a constant
    /// measured once on one host.
    pub seconds: f64,
    /// Frames replayed, for [`Tier::Stream`] only. `None` for the snapshot
    /// tiers, and `Some(0)` for a stream prefix that had a base but no
    /// generations yet — those are different facts and the report keeps them
    /// different.
    pub frames_replayed: Option<u64>,
}

/// What [`hydrate`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum HydrateOutcome {
    /// Bytes landed. `epoch` is the fencing token to stream under, and it was
    /// re-verified after the last subject.
    Hydrated {
        epoch: u64,
        subjects: Vec<SubjectRestore>,
        /// Wall-clock seconds for the whole set, including the claim round
        /// trips. This is the number a recovery-time estimate should be
        /// replaced by once it exists.
        seconds: f64,
        /// The volume was populated but stale, and was moved aside before the
        /// restore. See [`Displaced`].
        displaced: Option<Displaced>,
    },
    /// The volume already holds every subject. Nothing was read and **no claim
    /// was taken** — an ordinary restart must not mint an epoch, because doing
    /// so would fence out a streamer that is legitimately still running against
    /// the same volume from a previous incarnation the supervisor has not
    /// reaped yet.
    AlreadyPopulated,
    /// The claim was taken and the store holds nothing for these subjects — a
    /// first placement. The caller starts the workload empty and the backup
    /// side creates the first snapshot.
    NothingInTheStore { epoch: u64, subjects: Vec<String> },
    /// Do not start the workload.
    Refused(HydrateRefusal),
}

/// A populated volume that was moved aside because the store's claim names a
/// different owner (R936-B2).
///
/// The claim is written by whoever last streamed this workload's state, so a
/// claim held by someone else means that someone wrote after this volume's
/// copy. Serving the local copy would hand out a stale history. On 2026-09-22
/// that was a Sep-6 headscale.db with a different address plan, served as the
/// mesh coordinator's database.
#[derive(Debug, Clone, PartialEq)]
pub struct Displaced {
    /// The claim holder the local copy lost to.
    pub owner: String,
    pub epoch: u64,
    /// Suffix appended to every moved file (`<file>.<suffix>`).
    pub suffix: String,
}

/// Every way a hydrate says "do not start this workload".
#[derive(Debug, Clone, PartialEq)]
pub enum HydrateRefusal {
    /// Some subjects present, some absent. See [`Readiness::TornVolume`].
    TornVolume {
        populated: Vec<String>,
        absent: Vec<String>,
    },
    /// Lost the claim race at step 1 — another node is placing this workload
    /// concurrently.
    ClaimLost { current: ClaimRecord },
    /// Held the claim, restored, and lost it before the check at step 3. The
    /// restored bytes are still on disk (see the module doc) and belong to the
    /// node named in `lost`.
    FencedMidRestore {
        lost: ClaimLost,
        restored: Vec<SubjectRestore>,
    },
    /// The sink does not enforce conditional puts, so the claim's
    /// compare-and-swap would silently degrade to last-write-wins and *both*
    /// nodes would believe they won.
    ///
    /// This is a property of the deployed bucket, not of this code, so the only
    /// guard that can exist is a runtime probe — the same one
    /// `tenant-streamer::verify_sink` refuses to start on, made a refusal here
    /// rather than a caller's responsibility because a caller who forgets gets
    /// a fence that isn't there and no way to tell.
    SinkNotFenced { stage: PreflightStage },
    /// The volume was stale (the claim names `owner`), but the store held
    /// nothing to restore. The local files were put back and nothing starts:
    /// a stale history and an empty one are both wrong, and choosing between
    /// them is an operator's call.
    StaleVolumeNothingToRestore { owner: String, epoch: u64 },
    /// R977-B19: the pre-claim [`verify`] says the store's chain does not
    /// restore (corrupt or torn). No claim was taken. Retrying changes nothing;
    /// the chain needs `turso-backup-hydrate reseed`.
    ChainUnrestorable { reason: String },
}

impl HydrateRefusal {
    /// One line an operator can act on.
    pub fn headline(&self) -> String {
        match self {
            HydrateRefusal::TornVolume { populated, absent } => format!(
                "volume is torn: {} present ({}), {} missing ({}). Restoring only the missing \
                 ones would rebuild them at a different point in time from the ones already \
                 here; decide which copy is authoritative and clear or complete the volume by \
                 hand",
                populated.len(),
                populated.join(", "),
                absent.len(),
                absent.join(", ")
            ),
            HydrateRefusal::ClaimLost { current } => format!(
                "another node holds this workload's state: epoch {} (owner {}). It is being \
                 placed somewhere else right now",
                current.epoch, current.owner
            ),
            HydrateRefusal::FencedMidRestore { lost, restored } => format!(
                "{lost} while restoring {} subject(s); the bytes are on disk but this node no \
                 longer owns them — do not start the workload",
                restored.len()
            ),
            HydrateRefusal::SinkNotFenced { stage } => format!(
                "the object store does not enforce {} — the ownership claim would degrade to \
                 last-write-wins and two nodes could both believe they own this workload's \
                 state. Check that the bucket supports conditional writes (R2 and MinIO do; \
                 some S3-compatible backends do not)",
                stage.as_str()
            ),
            HydrateRefusal::StaleVolumeNothingToRestore { owner, epoch } => format!(
                "this volume's copy is stale (the claim is held by {owner} at epoch {epoch}), \
                 but the store holds nothing to restore. The local files were left in place; \
                 decide by hand whether they or an empty start is authoritative"
            ),
            HydrateRefusal::ChainUnrestorable { reason } => format!(
                "the store's durability chain does not restore ({reason}); no claim was taken. \
                 Retrying will not help: recover it with `turso-backup-hydrate reseed`"
            ),
        }
    }
}

/// Everything [`hydrate`] needs.
pub struct HydrateRequest<'a> {
    /// The object store both the claim and the data live in.
    pub store: Arc<dyn ObjectStore>,
    /// Key prefix for this *workload*. The claim sits here; each subject hangs
    /// one level below.
    pub store_prefix: &'a str,
    /// Host directory the named volume is bound from, e.g.
    /// `/var/lib/yah/kamaji/volumes/<name>`.
    pub volume_root: &'a Path,
    /// Volume-relative database paths, in declaration order.
    pub subjects: &'a [String],
    pub tier: Tier,
    /// Label recorded in the claim. Diagnostic; see [`ClaimRecord::owner`].
    pub owner: &'a str,
}

impl HydrateRequest<'_> {
    fn workload_target(&self) -> BackupTarget {
        BackupTarget {
            store: self.store.clone(),
            prefix: self.store_prefix.to_string(),
        }
    }

    /// The claim-free half of this request: everything [`restore_all`] reads.
    fn chain(&self) -> Chain<'_> {
        Chain {
            store: self.store.clone(),
            store_prefix: self.store_prefix,
            subjects: self.subjects,
            tier: self.tier,
        }
    }
}

/// A workload's durability chain in the store: where it is and how to read it.
///
/// R977-T17. This is what a restore needs and *all* it needs — there is no
/// owner here on purpose. [`hydrate`] is claim + restore of a `Chain`;
/// [`verify`] is restore of a `Chain` only, so the type itself is the proof
/// that verify cannot read or write a claim or fence.
pub struct Chain<'a> {
    pub store: Arc<dyn ObjectStore>,
    /// Key prefix for the workload; each subject hangs one level below.
    pub store_prefix: &'a str,
    pub subjects: &'a [String],
    pub tier: Tier,
}

impl Chain<'_> {
    fn subject_target(&self, subject: &str) -> BackupTarget {
        let prefix = self.store_prefix.trim_matches('/');
        BackupTarget {
            store: self.store.clone(),
            prefix: if prefix.is_empty() {
                subject.to_string()
            } else {
                format!("{prefix}/{subject}")
            },
        }
    }
}

/// What [`verify`] found.
#[derive(Debug, Clone, PartialEq)]
pub enum VerifyOutcome {
    /// Every subject restored, opened and returned `ok` from
    /// `PRAGMA integrity_check`.
    Valid {
        subjects: Vec<SubjectRestore>,
        seconds: f64,
    },
    /// A subject restored but did not open, or `integrity_check` reported
    /// problems. `problems` is the check's own rows (or the open error).
    Corrupt {
        subject: String,
        problems: Vec<String>,
    },
    /// The store holds some subjects and not others.
    Torn {
        present: Vec<String>,
        absent: Vec<String>,
    },
    /// The store holds nothing for any subject.
    NothingInTheStore,
}

/// R977-T17 — restore `chain` into `scratch_root` and check that every subject
/// opens and passes `PRAGMA integrity_check`, **without reading or writing the
/// ownership claim or fence**.
///
/// [`hydrate`] is the only other way to prove a chain restores, and it takes
/// the claim — run on a third node, that fences the live owner's tail
/// (measured in R977-B15 phase A). `scratch_root` must hold none of the
/// subjects already; a populated scratch dir would be verified as-is and
/// prove nothing about the store.
pub async fn verify(chain: &Chain<'_>, scratch_root: &Path) -> Result<VerifyOutcome> {
    let states = inspect_volume(scratch_root, chain.subjects)?;
    if let Some((s, _)) = states.iter().find(|(_, st)| *st != SubjectState::Absent) {
        bail!(
            "scratch dir {} already holds {s}; verify restores into an empty dir",
            scratch_root.display()
        );
    }
    let started = Instant::now();
    let mut restored = Vec::new();
    let mut missing = Vec::new();
    restore_all(chain, scratch_root, &mut restored, &mut missing).await?;
    if restored.is_empty() {
        return Ok(VerifyOutcome::NothingInTheStore);
    }
    if !missing.is_empty() {
        return Ok(VerifyOutcome::Torn {
            present: restored.into_iter().map(|r| r.subject).collect(),
            absent: missing,
        });
    }
    for r in &restored {
        let path = subject_path(scratch_root, &r.subject)?;
        let problems = integrity_problems(&path).await;
        if !problems.is_empty() {
            return Ok(VerifyOutcome::Corrupt {
                subject: r.subject.clone(),
                problems,
            });
        }
    }
    Ok(VerifyOutcome::Valid {
        subjects: restored,
        seconds: started.elapsed().as_secs_f64(),
    })
}

/// Open `path` and run `PRAGMA integrity_check`; empty means `ok`.
async fn integrity_problems(path: &Path) -> Vec<String> {
    let run = async {
        let p = path
            .to_str()
            .with_context(|| format!("path {} is not UTF-8", path.display()))?;
        let db = turso::Builder::new_local(p).build().await?;
        let conn = db.connect()?;
        let mut rows = conn.query("PRAGMA integrity_check", ()).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row.get::<String>(0)?);
        }
        anyhow::Ok(out)
    };
    match run.await {
        Ok(rows) if rows == ["ok"] => Vec::new(),
        Ok(rows) if rows.is_empty() => vec!["integrity_check returned no rows".into()],
        Ok(rows) => rows,
        Err(e) => vec![format!("opening {}: {e:#}", path.display())],
    }
}

/// Bring a workload's declared state onto this node, or say why not.
///
/// The order is the protocol from [`claim`]'s module doc, and it is the order
/// for a reason: emptiness is checked *before* the claim so an ordinary restart
/// costs no epoch, and the claim is re-verified *after* the last byte so a
/// takeover during a long restore cannot go unnoticed.
///
/// An `Err` means the hydrate could not reach a verdict — an unreachable store,
/// an unreadable volume. Callers must treat that as "do not start" too, for the
/// same reason [`claim::assert_holds`] does: an unreachable store is
/// indistinguishable from the partition the fence exists for.
pub async fn hydrate(req: HydrateRequest<'_>) -> Result<HydrateOutcome> {
    let states = inspect_volume(req.volume_root, req.subjects)?;
    let mut stale_against = None;
    match assess(&states)? {
        // R936-B2: "populated" is not "current". The one node whose local copy
        // is known current is the node that last held the claim, because the
        // claim holder is the only writer. Any other populated volume is a
        // leftover from an earlier ownership and must not serve. Reading the
        // claim is a single GET and takes no epoch, so an ordinary restart of
        // the holder still mints nothing. An unreachable store is an `Err`
        // (do not start), the same posture as every other store failure here.
        Readiness::AlreadyPopulated => match claim::read_claim(&req.workload_target()).await? {
            Some(held) => match volume_currency(req.volume_root, &held, req.owner)? {
                VolumeCurrency::Current => return Ok(HydrateOutcome::AlreadyPopulated),
                // The first start after this marker shipped: the claim holder is
                // the only writer, so its own unmarked volume is current. Stamp
                // it so every later start is checked against the store.
                VolumeCurrency::AdoptUnmarked => {
                    write_volume_marker(req.volume_root, &held.owner, held.epoch)?;
                    return Ok(HydrateOutcome::AlreadyPopulated);
                }
                VolumeCurrency::Stale => stale_against = Some(held),
            },
            None => return Ok(HydrateOutcome::AlreadyPopulated),
        },
        Readiness::TornVolume { populated, absent } => {
            return Ok(HydrateOutcome::Refused(HydrateRefusal::TornVolume {
                populated,
                absent,
            }))
        }
        Readiness::Hydrate => {}
    }

    let started = Instant::now();
    let workload_target = req.workload_target();

    // R977-B19: prove the chain restores BEFORE taking the claim. A chain that
    // cannot restore used to cost an epoch on every attempt (the floater retries
    // every 30 s; noisetable epochs reached 6208). The real restore still runs
    // after the claim below, because claiming first is what fences the old
    // owner's tail; this only gates it. Runs before displacement so a refusal
    // leaves a stale volume exactly where it was.
    if let Some(refusal) = preflight_chain(&req).await? {
        return Ok(HydrateOutcome::Refused(refusal));
    }
    let displaced = match &stale_against {
        Some(held) => Some(displace_volume(req.volume_root, req.subjects, held)?),
        None => None,
    };

    // Prove the fence is real before leaning on it. Deliberately *after* the
    // readiness check: an ordinary restart takes no claim, so it should not pay
    // for a probe or be blocked by a degraded sink it never writes to.
    if let PreconditionSupport::Degraded { stage } =
        store_claim::probe_conditional_puts(&workload_target).await?
    {
        return Ok(HydrateOutcome::Refused(HydrateRefusal::SinkNotFenced {
            stage,
        }));
    }

    let epoch = match claim::acquire(&workload_target, req.owner).await? {
        ClaimOutcome::Granted { claim, .. } => claim.epoch,
        ClaimOutcome::Lost { current } => {
            return Ok(HydrateOutcome::Refused(HydrateRefusal::ClaimLost {
                current,
            }))
        }
    };

    let mut restored = Vec::new();
    let mut missing = Vec::new();
    // R936-B2, measured on prod 2026-09-23 00:02:04Z: a restore that errors
    // AFTER the claim is taken must not leave its bytes behind. The claim now
    // names this node, so the next attempt reads the populated volume as the
    // holder's own copy and serves it. That is exactly how us-south-001 served a
    // base with none of its frames applied. Clearing the subjects sends the
    // next attempt back through a full restore. The displaced copy stays aside:
    // putting it back would serve it under this node's claim, which is worse.
    if let Err(e) = restore_all(&req.chain(), req.volume_root, &mut restored, &mut missing).await {
        clear_subjects(req.volume_root, req.subjects);
        return Err(e.context(
            "restore failed after the claim was taken; the partial restore was removed \
             so the next attempt restores again instead of trusting it",
        ));
    }

    // A prefix that holds some subjects and not others is the store-side twin
    // of `TornVolume`, and it is refused for the identical reason: restoring
    // the ones that exist would start the workload with a partial history it
    // cannot tell apart from a complete one.
    if !missing.is_empty() && !restored.is_empty() {
        return Ok(HydrateOutcome::Refused(HydrateRefusal::TornVolume {
            populated: restored.into_iter().map(|r| r.subject).collect(),
            absent: missing,
        }));
    }

    // Step 3: the claim must still be ours, checked *after* the bytes landed.
    if let Err(lost) = claim::assert_holds(&workload_target, epoch).await? {
        return Ok(HydrateOutcome::Refused(HydrateRefusal::FencedMidRestore {
            lost,
            restored,
        }));
    }

    if restored.is_empty() {
        if let Some(d) = displaced {
            undisplace_volume(req.volume_root, req.subjects, &d.suffix)?;
            return Ok(HydrateOutcome::Refused(
                HydrateRefusal::StaleVolumeNothingToRestore {
                    owner: d.owner,
                    epoch: d.epoch,
                },
            ));
        }
        return Ok(HydrateOutcome::NothingInTheStore {
            epoch,
            subjects: missing,
        });
    }
    write_volume_marker(req.volume_root, req.owner, epoch)?;
    Ok(HydrateOutcome::Hydrated {
        epoch,
        subjects: restored,
        seconds: started.elapsed().as_secs_f64(),
        displaced,
    })
}

/// R977-B19 — [`verify`] the chain into a scratch dir next to the volume,
/// taking no claim. `Some` is a refusal: the store's chain is corrupt or torn,
/// and no retry will change that until an operator reseeds it. A verify that
/// errors (unreachable store) stays an `Err`, which callers may retry.
async fn preflight_chain(req: &HydrateRequest<'_>) -> Result<Option<HydrateRefusal>> {
    let scratch = PathBuf::from(format!(
        "{}.hydrate-preflight-{}",
        req.volume_root.display(),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch)
        .with_context(|| format!("creating preflight scratch {}", scratch.display()))?;
    let outcome = verify(&req.chain(), &scratch).await;
    let _ = std::fs::remove_dir_all(&scratch);
    Ok(match outcome? {
        VerifyOutcome::Corrupt { subject, problems } => Some(HydrateRefusal::ChainUnrestorable {
            reason: format!("{subject} fails integrity_check: {}", problems.join("; ")),
        }),
        VerifyOutcome::Torn { present, absent } => Some(HydrateRefusal::ChainUnrestorable {
            reason: format!(
                "store is torn: {} present, {} absent",
                present.join(", "),
                absent.join(", ")
            ),
        }),
        VerifyOutcome::Valid { .. } | VerifyOutcome::NothingInTheStore => None,
    })
}

/// Restore every subject into the volume, recording each one in `restored`, or
/// in `missing` if the store has nothing for it.
async fn restore_all(
    chain: &Chain<'_>,
    volume_root: &Path,
    restored: &mut Vec<SubjectRestore>,
    missing: &mut Vec<String>,
) -> Result<()> {
    for subject in chain.subjects {
        let dest = subject_path(volume_root, subject)?;
        // A subject in a subdirectory of the volume needs that subdirectory to
        // exist; runc's bind mount does not create it and neither does the
        // restore, which writes a file.
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {} for subject {subject}", parent.display()))?;
        }
        let dest_str = dest
            .to_str()
            .with_context(|| format!("subject path {} is not UTF-8", dest.display()))?;

        let target = chain.subject_target(subject);
        let subject_started = Instant::now();
        let Some((source, frames_replayed)) =
            restore_subject(&target, chain.tier, dest_str).await?
        else {
            missing.push(subject.clone());
            continue;
        };
        let bytes = std::fs::metadata(&dest)
            .with_context(|| format!("stat-ing restored subject {}", dest.display()))?
            .len();
        restored.push(SubjectRestore {
            subject: subject.clone(),
            source,
            bytes,
            seconds: subject_started.elapsed().as_secs_f64(),
            frames_replayed,
        });
    }
    Ok(())
}

/// Remove every subject and its sidecars from the volume. Best-effort: a file
/// that is already gone is the state we want.
fn clear_subjects(volume_root: &Path, subjects: &[String]) {
    for subject in subjects {
        let Ok(path) = subject_path(volume_root, subject) else {
            continue;
        };
        for sfx in SIDECARS {
            let mut p = path.clone().into_os_string();
            p.push(sfx);
            let _ = std::fs::remove_file(&p);
        }
    }
}

/// R1013-B2 — file in the volume root recording which claim (owner + epoch) the
/// volume's contents were last written under. Without it "populated" could only
/// be compared by owner label, and a stale copy whose label matched the claim
/// (or a legacy `kamaji-pid-*` claim) was served silently, weeks behind the store.
pub const VOLUME_MARKER: &str = ".turso-backup-claim";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeMarker {
    pub owner: String,
    pub epoch: u64,
}

/// Record that the volume is current as of `(owner, epoch)`. Called after a
/// successful hydrate and by [`crate::tail::start`] once its epoch is known.
pub fn write_volume_marker(volume_root: &Path, owner: &str, epoch: u64) -> Result<()> {
    std::fs::create_dir_all(volume_root)
        .with_context(|| format!("creating volume root {}", volume_root.display()))?;
    let path = volume_root.join(VOLUME_MARKER);
    let tmp = volume_root.join(format!("{VOLUME_MARKER}.tmp"));
    // One line, `<epoch> <owner>`: the crate carries no serde, and this is all it holds.
    let body = format!("{epoch} {owner}\n").into_bytes();
    std::fs::write(&tmp, body).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("installing {}", path.display()))?;
    Ok(())
}

fn read_volume_marker(volume_root: &Path) -> Result<Option<VolumeMarker>> {
    let path = volume_root.join(VOLUME_MARKER);
    match std::fs::read(&path) {
        Ok(b) => Ok((|| {
            let text = String::from_utf8(b).ok()?;
            let (epoch, owner) = text.trim_end_matches('\n').split_once(' ')?;
            Some(VolumeMarker { owner: owner.to_string(), epoch: epoch.parse().ok()? })
        })()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum VolumeCurrency {
    Current,
    /// No usable marker, but the claim names this very owner (or a legacy
    /// process label that cannot prove otherwise): one-time rollout adoption.
    AdoptUnmarked,
    Stale,
}

/// Compare a populated volume's marker with the store's current claim.
fn volume_currency(
    volume_root: &Path,
    held: &claim::ClaimRecord,
    owner: &str,
) -> Result<VolumeCurrency> {
    Ok(match read_volume_marker(volume_root)? {
        Some(m) if m.epoch == held.epoch && m.owner == held.owner => VolumeCurrency::Current,
        Some(_) => VolumeCurrency::Stale,
        None if held.owner == owner || !owner_names_a_node(&held.owner) => {
            VolumeCurrency::AdoptUnmarked
        }
        None => VolumeCurrency::Stale,
    })
}

/// Whether a claim's owner label identifies a NODE, which is the only thing that
/// lets a mismatch prove "someone else streamed after this volume was written".
///
/// R936-B2 follow-on, measured on prod 2026-09-22 23:49:52Z: every claim minted
/// before kamaji's owner label became the hostname carries `kamaji-pid-<pid>`,
/// a PROCESS. Such a label differs from this node's hostname label even when
/// this very node minted it, so treating it as foreign displaced
/// noisetable-account's own (possibly newer) volume on its next redeploy. A
/// process label cannot prove a different owner, so it proves nothing: keep
/// the volume. The tail then re-mints the claim under the node label at its
/// next epoch, so each workload leaves this branch after one start. Removal is
/// tracked as an R936-B2 cleanup.
fn owner_names_a_node(owner: &str) -> bool {
    !owner.starts_with("kamaji-pid-")
}

/// SQLite sidecars that belong to a subject file. Moved with it, or a restored
/// main file would be opened against the stale copy's WAL.
const SIDECARS: [&str; 4] = ["", "-wal", "-shm", "-journal"];

/// Move every populated subject (and its sidecars) to `<file>.<suffix>`.
fn displace_volume(
    volume_root: &Path,
    subjects: &[String],
    held: &claim::ClaimRecord,
) -> Result<Displaced> {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let suffix = format!("displaced-{unix}");
    for subject in subjects {
        let base = subject_path(volume_root, subject)?;
        for side in SIDECARS {
            let from = PathBuf::from(format!("{}{side}", base.display()));
            if from.exists() {
                let to = PathBuf::from(format!("{}{side}.{suffix}", base.display()));
                std::fs::rename(&from, &to).with_context(|| {
                    format!("displacing stale {} to {}", from.display(), to.display())
                })?;
            }
        }
    }
    Ok(Displaced {
        owner: held.owner.clone(),
        epoch: held.epoch,
        suffix,
    })
}

/// Undo [`displace_volume`].
fn undisplace_volume(volume_root: &Path, subjects: &[String], suffix: &str) -> Result<()> {
    for subject in subjects {
        let base = subject_path(volume_root, subject)?;
        for side in SIDECARS {
            let from = PathBuf::from(format!("{}{side}.{suffix}", base.display()));
            if from.exists() {
                let to = PathBuf::from(format!("{}{side}", base.display()));
                std::fs::rename(&from, &to)
                    .with_context(|| format!("restoring displaced {}", from.display()))?;
            }
        }
    }
    Ok(())
}

/// Restore one subject, or `Ok(None)` when the store holds nothing for it.
///
/// "Nothing there" has to be a value rather than an error: a first placement
/// legitimately finds an empty prefix, and the three restore entry points all
/// signal it with an `Err` that is indistinguishable at the type level from a
/// real failure.
async fn restore_subject(
    target: &BackupTarget,
    tier: Tier,
    dest: &str,
) -> Result<Option<(String, Option<u64>)>> {
    match tier {
        Tier::Snapshot => match crate::snapshot::restore_latest(target, dest).await {
            Ok(key) => Ok(Some((key, None))),
            Err(e) if is_absent_source(&e) => Ok(None),
            Err(e) => Err(e),
        },
        Tier::Dedup => match crate::dedup::load_latest_manifest(target).await? {
            None => Ok(None),
            Some((key, manifest)) => {
                crate::dedup::restore_from_manifest(target, &manifest, dest).await?;
                Ok(Some((key, None)))
            }
        },
        Tier::Stream => {
            // A stream prefix between its first tier-1a snapshot and its first
            // tail has a base and no generations. Replaying nothing onto the
            // base is the correct restore, and it is what roadcase's
            // `SinkHydrator` already does — see `restore_stream_from_manifests`.
            let manifests = crate::stream::list_and_parse_generation_manifests(target).await?;
            if manifests.is_empty() {
                return match crate::snapshot::restore_latest(target, dest).await {
                    Ok(key) => Ok(Some((key, Some(0)))),
                    Err(e) if is_absent_source(&e) => Ok(None),
                    Err(e) => Err(e),
                };
            }
            let outcome =
                crate::stream::restore_stream_from_manifests(target, dest, &manifests).await?;
            Ok(Some((
                outcome.base_snapshot_key.clone(),
                Some(outcome.frames_replayed),
            )))
        }
    }
}

/// Whether an error from a restore entry point means "the prefix is empty"
/// rather than "the restore failed".
///
/// String matching, which is ugly, and the honest alternative is worse: the
/// three entry points return `anyhow::Error` and signal absence through
/// `Context` messages, so the only structural fix is to change their return
/// types — a wider change than this ticket, and one that would ripple into
/// roadcase. The two phrases matched here are pinned by
/// `an_empty_prefix_is_nothing_in_the_store_not_an_error`, so a reword on
/// either side fails a test rather than turning a first placement into a
/// startup failure.
fn is_absent_source(e: &anyhow::Error) -> bool {
    let text = format!("{e:#}");
    text.contains("no snapshots found under") || text.contains("no snapshot manifest found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    fn s(names: &[(&str, SubjectState)]) -> Vec<(String, SubjectState)> {
        names.iter().map(|(n, st)| (n.to_string(), *st)).collect()
    }

    const PRESENT: SubjectState = SubjectState::Populated { bytes: 4096 };

    // ── the decision core ───────────────────────────────────────────────────

    #[test]
    fn an_empty_volume_hydrates_and_a_full_one_does_not() {
        assert_eq!(
            assess(&s(&[("a.db", SubjectState::Absent), ("b.db", SubjectState::Absent)])).unwrap(),
            Readiness::Hydrate
        );
        assert_eq!(
            assess(&s(&[("a.db", PRESENT), ("b.db", PRESENT)])).unwrap(),
            Readiness::AlreadyPopulated
        );
    }

    /// The case R850's one-line rule does not cover and the driving question
    /// makes the default. Restoring only `b.db` would bring the workload up
    /// with two databases at different points in time.
    #[test]
    fn a_partly_populated_volume_is_refused_rather_than_topped_up() {
        let r = assess(&s(&[
            ("accounts.db", PRESENT),
            ("passkeys.db", SubjectState::Absent),
            ("sessions.db", PRESENT),
        ]))
        .unwrap();
        assert_eq!(
            r,
            Readiness::TornVolume {
                populated: vec!["accounts.db".into(), "sessions.db".into()],
                absent: vec!["passkeys.db".into()],
            }
        );
    }

    /// A zero-byte file is not a database. Counting one as populated would wedge
    /// a node out of ever hydrating after a truncated write.
    #[test]
    fn a_zero_length_file_counts_as_absent() {
        let dir = tempdir("zero");
        std::fs::write(dir.join("a.db"), b"").unwrap();
        let states = inspect_volume(&dir, &["a.db".to_string()]).unwrap();
        assert_eq!(states[0].1, SubjectState::Absent);
        assert_eq!(assess(&states).unwrap(), Readiness::Hydrate);
    }

    #[test]
    fn inspect_volume_reports_sizes_for_what_is_there() {
        let dir = tempdir("sizes");
        std::fs::write(dir.join("a.db"), b"0123456789").unwrap();
        let states = inspect_volume(&dir, &["a.db".into(), "b.db".into()]).unwrap();
        assert_eq!(states[0].1, SubjectState::Populated { bytes: 10 });
        assert_eq!(states[1].1, SubjectState::Absent);
    }

    /// The declaration already refuses these (`workload_spec`'s
    /// `AbsoluteSubject` / `TraversingSubject`), and this is the belt to that
    /// braces — the argument can come from anywhere and its result decides
    /// where bytes get written.
    #[test]
    fn a_traversing_subject_is_refused_at_the_path_join_too() {
        let dir = tempdir("traverse");
        for bad in ["/etc/passwd", "../../etc/passwd", "./a.db"] {
            let err = inspect_volume(&dir, &[bad.to_string()]).unwrap_err();
            assert!(
                format!("{err:#}").contains(bad),
                "refusal should name the subject: {err:#}"
            );
        }
    }

    #[test]
    fn no_subjects_at_all_is_an_error_not_a_silent_no_op() {
        let err = assess(&[]).unwrap_err();
        assert!(format!("{err:#}").contains("at least one database"), "{err:#}");
    }

    // ── the fenced execution path ───────────────────────────────────────────

    fn tempdir(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "turso-hydrate-test-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// A byte string `upload_base_snapshot` will accept — it refuses anything
    /// without the SQLite magic, so a hand-rolled fixture has to carry it.
    /// A valid one-page empty SQLite database, `tag` written into the page's
    /// unallocated space so images differ. R977-B19: hydrate now verifies the
    /// chain (integrity_check) before claiming, so fixtures must be databases.
    fn db_image(tag: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; 4096];
        v[..16].copy_from_slice(b"SQLite format 3\0");
        v[16..18].copy_from_slice(&4096u16.to_be_bytes());
        v[18] = 1; // write version
        v[19] = 1; // read version
        v[21] = 64;
        v[22] = 32;
        v[23] = 32;
        v[24..28].copy_from_slice(&1u32.to_be_bytes()); // change counter
        v[28..32].copy_from_slice(&1u32.to_be_bytes()); // size in pages
        v[44..48].copy_from_slice(&4u32.to_be_bytes()); // schema format
        v[56..60].copy_from_slice(&1u32.to_be_bytes()); // UTF-8
        v[92..96].copy_from_slice(&1u32.to_be_bytes()); // version-valid-for
        v[96..100].copy_from_slice(&3_045_000u32.to_be_bytes());
        // sqlite_master: empty leaf table page, content area starts at 4096.
        v[100] = 0x0D;
        v[105..107].copy_from_slice(&4096u16.to_be_bytes());
        v[200..200 + tag.len()].copy_from_slice(tag);
        v
    }

    /// Seed a tier-1a snapshot for `subject` under `prefix`, so a hydrate has
    /// something to find. `upload_base_snapshot` is the real writer path.
    async fn seed_snapshot(store: &Arc<dyn ObjectStore>, prefix: &str, tag: &[u8]) -> String {
        let target = BackupTarget {
            store: store.clone(),
            prefix: prefix.to_string(),
        };
        crate::snapshot::upload_base_snapshot(&target, &db_image(tag))
            .await
            .unwrap()
    }

    fn request<'a>(
        store: &Arc<dyn ObjectStore>,
        volume: &'a Path,
        subjects: &'a [String],
        owner: &'a str,
    ) -> HydrateRequest<'a> {
        HydrateRequest {
            store: store.clone(),
            store_prefix: "wl/acct",
            volume_root: volume,
            subjects,
            tier: Tier::Snapshot,
            owner,
        }
    }

    #[tokio::test]
    async fn an_empty_volume_is_filled_from_the_store_under_a_fresh_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"ACCOUNTS").await;
        seed_snapshot(&store, "wl/acct/sessions.db", b"SESSIONS").await;
        let dir = tempdir("fill");
        let subjects = vec!["accounts.db".to_string(), "sessions.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-003"))
            .await
            .unwrap();
        let HydrateOutcome::Hydrated { epoch, subjects: r, .. } = out else {
            panic!("expected Hydrated, got {out:?}");
        };
        assert_eq!(epoch, claim::FIRST_EPOCH);
        assert_eq!(r.len(), 2);
        assert_eq!(
            std::fs::read(dir.join("accounts.db")).unwrap(),
            db_image(b"ACCOUNTS")
        );
        assert_eq!(
            std::fs::read(dir.join("sessions.db")).unwrap(),
            db_image(b"SESSIONS")
        );
        // The report carries measured bytes, not a modelled figure.
        assert_eq!(r[0].bytes, db_image(b"ACCOUNTS").len() as u64);

        // ...and the claim is now readable by anyone else asking who owns this.
        let held = claim::read_claim(&BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(held.owner, "us-west-003");
    }

    /// The single worst thing this module could do. An ordinary restart must
    /// not roll the databases back to the last watermark — and must not even
    /// mint an epoch, since that would fence a streamer still running against
    /// the same volume.
    #[tokio::test]
    async fn an_ordinary_restart_neither_restores_nor_takes_a_claim() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"OLD-BACKUP").await;
        let dir = tempdir("restart");
        std::fs::write(dir.join("accounts.db"), b"LIVE-DATA-WRITTEN-SINCE").unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-east-001"))
            .await
            .unwrap();
        assert_eq!(out, HydrateOutcome::AlreadyPopulated);
        assert_eq!(
            std::fs::read(dir.join("accounts.db")).unwrap(),
            b"LIVE-DATA-WRITTEN-SINCE"
        );
        assert_eq!(
            claim::read_claim(&BackupTarget {
                store: store.clone(),
                prefix: "wl/acct".into()
            })
            .await
            .unwrap(),
            None,
            "a restart must not mint an epoch"
        );
    }

    /// R936-B2, the 2026-09-22 failover: a node holding a leftover copy is
    /// elected while another node holds the claim. The leftover must be moved
    /// aside and the store's copy restored, not served.
    #[tokio::test]
    async fn a_populated_volume_whose_claim_is_held_elsewhere_is_displaced_and_restored() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"CURRENT").await;
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        let ClaimOutcome::Granted { claim: prior, .. } =
            claim::acquire(&target, "us-south-001").await.unwrap()
        else {
            panic!("seeding the claim");
        };
        let dir = tempdir("stale");
        std::fs::write(dir.join("accounts.db"), b"STALE-SEP-6").unwrap();
        std::fs::write(dir.join("accounts.db-wal"), b"STALE-WAL").unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-001"))
            .await
            .unwrap();
        let HydrateOutcome::Hydrated { epoch, displaced: Some(d), .. } = out else {
            panic!("expected Hydrated with a displacement, got {out:?}");
        };
        assert!(epoch > prior.epoch);
        assert_eq!(d.owner, "us-south-001");
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), db_image(b"CURRENT"));
        assert!(!dir.join("accounts.db-wal").exists(), "the stale WAL must not survive");
        assert_eq!(
            std::fs::read(dir.join(format!("accounts.db.{}", d.suffix))).unwrap(),
            b"STALE-SEP-6"
        );
        assert_eq!(
            std::fs::read(dir.join(format!("accounts.db-wal.{}", d.suffix))).unwrap(),
            b"STALE-WAL"
        );
        let held = claim::read_claim(&target).await.unwrap().unwrap();
        assert_eq!(held.owner, "us-west-001");
    }

    /// The holder restarting keeps its volume and mints nothing.
    #[tokio::test]
    async fn the_claim_holder_restarting_keeps_its_volume() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"OLD-BACKUP").await;
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        claim::acquire(&target, "us-west-001").await.unwrap();
        let before = claim::read_claim(&target).await.unwrap().unwrap();
        let dir = tempdir("holder");
        std::fs::write(dir.join("accounts.db"), b"LIVE").unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-001"))
            .await
            .unwrap();
        assert_eq!(out, HydrateOutcome::AlreadyPopulated);
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), b"LIVE");
        assert_eq!(claim::read_claim(&target).await.unwrap().unwrap(), before);
    }

    /// R1013-B2: a marker behind the store's epoch means the volume is stale
    /// even when the owner label matches the claim. Displace, hydrate, restamp.
    #[tokio::test]
    async fn a_marker_behind_the_store_epoch_is_displaced_and_hydrated() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"CURRENT").await;
        let target = BackupTarget { store: store.clone(), prefix: "wl/acct".into() };
        let ClaimOutcome::Granted { claim: held, .. } =
            claim::acquire(&target, "us-east-001").await.unwrap()
        else {
            panic!("seeding the claim");
        };
        let dir = tempdir("marker-behind");
        std::fs::write(dir.join("accounts.db"), b"STALE-SEP-23").unwrap();
        write_volume_marker(&dir, "us-east-001", held.epoch - 1).unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-east-001")).await.unwrap();
        let HydrateOutcome::Hydrated { epoch, displaced: Some(d), .. } = out else {
            panic!("expected Hydrated with a displacement, got {out:?}");
        };
        assert!(epoch > held.epoch);
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), db_image(b"CURRENT"));
        assert_eq!(
            std::fs::read(dir.join(format!("accounts.db.{}", d.suffix))).unwrap(),
            b"STALE-SEP-23"
        );
        assert_eq!(
            read_volume_marker(&dir).unwrap(),
            Some(VolumeMarker { owner: "us-east-001".into(), epoch })
        );
    }

    /// R1013-B2: a marker equal to the store's claim is current.
    #[tokio::test]
    async fn a_marker_matching_the_store_claim_is_already_populated() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"OLD-BACKUP").await;
        let target = BackupTarget { store: store.clone(), prefix: "wl/acct".into() };
        let ClaimOutcome::Granted { claim: held, .. } =
            claim::acquire(&target, "us-west-001").await.unwrap()
        else {
            panic!("seeding the claim");
        };
        let dir = tempdir("marker-current");
        std::fs::write(dir.join("accounts.db"), b"LIVE").unwrap();
        write_volume_marker(&dir, "us-west-001", held.epoch).unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-001")).await.unwrap();
        assert_eq!(out, HydrateOutcome::AlreadyPopulated);
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), b"LIVE");
        assert_eq!(claim::read_claim(&target).await.unwrap().unwrap(), held);
    }

    /// R1013-B2: a non-empty volume with no marker, under a claim held by a
    /// different node, is stale (the only unmarked volume trusted is the claim
    /// holder's own, for the one-time rollout).
    #[tokio::test]
    async fn an_unmarked_volume_under_a_foreign_claim_is_stale() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"CURRENT").await;
        let target = BackupTarget { store: store.clone(), prefix: "wl/acct".into() };
        claim::acquire(&target, "us-south-001").await.unwrap();
        let dir = tempdir("unmarked-foreign");
        std::fs::write(dir.join("accounts.db"), b"STALE").unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-east-001")).await.unwrap();
        assert!(
            matches!(out, HydrateOutcome::Hydrated { displaced: Some(_), .. }),
            "got {out:?}"
        );
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), db_image(b"CURRENT"));
        assert!(read_volume_marker(&dir).unwrap().is_some());
    }

    /// A legacy process-scoped claim (`kamaji-pid-<pid>`) cannot prove another
    /// node streamed after this volume: the node that minted it may be this
    /// one. Keep the volume, restore nothing; the tail re-mints under the node
    /// label. Prod 2026-09-22: noisetable-account was displaced by exactly this.
    #[tokio::test]
    async fn a_legacy_process_label_claim_keeps_the_volume() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"OLDER-BACKUP").await;
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        claim::acquire(&target, "kamaji-pid-794566").await.unwrap();
        let before = claim::read_claim(&target).await.unwrap().unwrap();
        let dir = tempdir("legacy-label");
        std::fs::write(dir.join("accounts.db"), b"NEWER-LOCAL").unwrap();
        std::fs::write(dir.join("accounts.db-wal"), b"LOCAL-WAL").unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "vps-8dba9ff8"))
            .await
            .unwrap();
        assert_eq!(out, HydrateOutcome::AlreadyPopulated);
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), b"NEWER-LOCAL");
        assert_eq!(std::fs::read(dir.join("accounts.db-wal")).unwrap(), b"LOCAL-WAL");
        // 2 files + the adopted volume's freshly stamped marker.
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 3, "nothing displaced");
        assert_eq!(claim::read_claim(&target).await.unwrap().unwrap(), before);
    }

    /// Stale, but nothing to restore: put the files back and refuse.
    #[tokio::test]
    async fn a_stale_volume_with_an_empty_store_is_put_back_and_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        claim::acquire(&target, "us-south-001").await.unwrap();
        let dir = tempdir("stale-empty");
        std::fs::write(dir.join("accounts.db"), b"STALE").unwrap();
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-001"))
            .await
            .unwrap();
        assert!(
            matches!(
                out,
                HydrateOutcome::Refused(HydrateRefusal::StaleVolumeNothingToRestore { .. })
            ),
            "{out:?}"
        );
        assert_eq!(std::fs::read(dir.join("accounts.db")).unwrap(), b"STALE");
        let leftovers = std::fs::read_dir(&dir).unwrap().count();
        assert_eq!(leftovers, 1, "no displaced copy may be left behind");
    }

    #[tokio::test]
    async fn a_torn_volume_is_refused_before_the_claim_is_touched() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"A").await;
        seed_snapshot(&store, "wl/acct/sessions.db", b"S").await;
        let dir = tempdir("torn");
        std::fs::write(dir.join("accounts.db"), b"LIVE").unwrap();
        let subjects = vec!["accounts.db".to_string(), "sessions.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "n1")).await.unwrap();
        let HydrateOutcome::Refused(refusal) = out else {
            panic!("expected a refusal, got {out:?}");
        };
        assert!(refusal.headline().contains("sessions.db"), "{}", refusal.headline());
        assert!(
            !dir.join("sessions.db").exists(),
            "a refused hydrate must not have written anything"
        );
    }

    /// Step 1 of the protocol: another node is placing this workload right now.
    ///
    /// `RivalOn::ClaimPut` stages the interleaving `acquire`'s CAS exists to
    /// catch — the rival's claim lands between our read of the sidecar and our
    /// conditional write of it.
    #[tokio::test]
    async fn losing_the_claim_race_refuses_instead_of_hydrating() {
        let store = racing_store(RivalOn::ClaimPut, 9, "us-east-001");
        seed_snapshot(&store, "wl/acct/accounts.db", b"A").await;
        let dir = tempdir("raced");
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-003"))
            .await
            .unwrap();
        let HydrateOutcome::Refused(HydrateRefusal::ClaimLost { current }) = out else {
            panic!("expected ClaimLost, got {out:?}");
        };
        assert_eq!(current.owner, "us-east-001");
        assert_eq!(current.epoch, 9);
        assert!(
            !dir.join("accounts.db").exists(),
            "a node that lost the claim must not have read a byte of state"
        );
    }

    /// The other half of the same rule, and the one it would be easy to break
    /// by making `acquire` refuse whenever anyone else holds the prefix:
    /// failover must remain possible. A *sequential* takeover hydrates.
    #[tokio::test]
    async fn a_sequential_takeover_still_hydrates() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"A").await;
        let dir = tempdir("failover");
        let subjects = vec!["accounts.db".to_string()];

        claim::acquire(
            &BackupTarget {
                store: store.clone(),
                prefix: "wl/acct".into(),
            },
            "us-east-001",
        )
        .await
        .unwrap();
        let out = hydrate(request(&store, &dir, &subjects, "us-west-003"))
            .await
            .unwrap();
        let HydrateOutcome::Hydrated { epoch, .. } = out else {
            panic!("a sequential takeover must hydrate, got {out:?}");
        };
        assert_eq!(epoch, claim::FIRST_EPOCH + 1);
    }

    /// A first placement finds an empty prefix. That has to be a value, not the
    /// startup failure the underlying `restore_latest` reports it as.
    #[tokio::test]
    async fn an_empty_prefix_is_nothing_in_the_store_not_an_error() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = tempdir("virgin");
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "n1")).await.unwrap();
        let HydrateOutcome::NothingInTheStore { epoch, subjects: missing } = out else {
            panic!("expected NothingInTheStore, got {out:?}");
        };
        assert_eq!(epoch, claim::FIRST_EPOCH);
        assert_eq!(missing, vec!["accounts.db".to_string()]);
        assert!(!dir.join("accounts.db").exists());
    }

    /// The store-side twin of `TornVolume`: half the subjects have backups and
    /// half do not. Starting with the half that restored would look identical
    /// to a complete restore.
    #[tokio::test]
    async fn a_prefix_holding_only_some_subjects_is_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"A").await;
        let dir = tempdir("half");
        let subjects = vec!["accounts.db".to_string(), "sessions.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "n1")).await.unwrap();
        // R977-B19: caught by the pre-claim verify, so no epoch is minted.
        let HydrateOutcome::Refused(HydrateRefusal::ChainUnrestorable { reason }) = out else {
            panic!("expected ChainUnrestorable, got {out:?}");
        };
        assert!(reason.contains("sessions.db"), "{reason}");
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        assert_eq!(claim::read_claim(&target).await.unwrap(), None);
    }

    /// R977-B19: a chain that does not pass integrity_check is refused BEFORE
    /// the claim, so retries leave the live claim and its epoch untouched.
    #[tokio::test]
    async fn a_corrupt_chain_is_refused_without_moving_the_claim_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        claim::acquire(&target, "us-south-001").await.unwrap();
        let before = claim::read_claim(&target).await.unwrap().unwrap();
        crate::snapshot::upload_base_snapshot(
            &BackupTarget {
                store: store.clone(),
                prefix: "wl/acct/accounts.db".into(),
            },
            b"SQLite format 3\0GARBAGE",
        )
        .await
        .unwrap();
        let subjects = vec!["accounts.db".to_string()];
        for attempt in 0..3 {
            let dir = tempdir(&format!("corrupt-{attempt}"));
            let out = hydrate(request(&store, &dir, &subjects, "us-west-003"))
                .await
                .unwrap();
            let HydrateOutcome::Refused(refusal @ HydrateRefusal::ChainUnrestorable { .. }) = out
            else {
                panic!("expected ChainUnrestorable, got {out:?}");
            };
            assert!(refusal.headline().contains("reseed"), "{}", refusal.headline());
            assert!(!dir.join("accounts.db").exists());
        }
        assert_eq!(claim::read_claim(&target).await.unwrap().unwrap(), before);
    }

    /// Step 3 of the protocol, and the check that is easy to leave out: a
    /// takeover landing mid-restore leaves this node holding a complete,
    /// plausible, *stale* copy. Only the post-restore `assert_holds` can see
    /// it — the pre-restore acquire succeeded, and every byte arrived.
    ///
    /// The bytes stay on disk deliberately: deleting a database because a fence
    /// moved is a worse failure than refusing to start with it.
    #[tokio::test]
    async fn a_takeover_during_the_restore_is_caught_after_the_bytes_land() {
        let store = racing_store(RivalOn::SnapshotRead, 9, "us-east-001");
        seed_snapshot(&store, "wl/acct/accounts.db", b"ACCOUNTS").await;
        let dir = tempdir("midrestore");
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "us-west-003"))
            .await
            .unwrap();
        let HydrateOutcome::Refused(HydrateRefusal::FencedMidRestore { lost, restored }) = out
        else {
            panic!("expected FencedMidRestore, got {out:?}");
        };
        let ClaimLost::Superseded { current, ours } = &lost else {
            panic!("expected Superseded, got {lost:?}");
        };
        assert_eq!(*ours, claim::FIRST_EPOCH);
        assert_eq!(current.epoch, 9);
        assert_eq!(current.owner, "us-east-001");
        assert_eq!(restored.len(), 1, "the restore itself completed");
        assert!(
            dir.join("accounts.db").exists(),
            "the restored bytes must be left alone; the refusal is what stops the workload"
        );
        assert!(
            HydrateRefusal::FencedMidRestore { lost, restored }
                .headline()
                .contains("do not start")
        );
    }

    /// A store that lets a rival node's claim land at a chosen point in our own
    /// hydrate — the two interleavings the fence exists for, neither of which
    /// can be staged by calling `acquire` twice (a sequential second acquire is
    /// a legal takeover, not a race).
    ///
    /// Modelled on `stream::tests::RacingStore`, which does the same job for
    /// the watermark CAS.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum RivalOn {
        /// Fire on the first put aimed at the claim key: the rival's record is
        /// already there when our conditional put lands, so `acquire` loses.
        ClaimPut,
        /// Fire on the first listing of a `snapshots/` prefix — i.e. once we
        /// hold the claim and the restore has started.
        SnapshotRead,
    }

    /// The fence rests on the bucket honouring `If-Match` / `If-None-Match`,
    /// which is a deployment property no test of this code can establish. A
    /// bucket that ignores them lets *both* nodes' claims succeed — so a
    /// hydrate against one must refuse rather than proceed under a fence that
    /// is not there.
    #[tokio::test]
    async fn a_sink_that_ignores_conditional_puts_is_refused_before_any_claim() {
        let store: Arc<dyn ObjectStore> = Arc::new(UnconditionalStore {
            inner: Arc::new(InMemory::new()),
        });
        seed_snapshot(&store, "wl/acct/accounts.db", b"A").await;
        let dir = tempdir("degraded");
        let subjects = vec!["accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "n1")).await.unwrap();
        let HydrateOutcome::Refused(refusal) = out else {
            panic!("expected a refusal, got {out:?}");
        };
        assert!(
            matches!(refusal, HydrateRefusal::SinkNotFenced { .. }),
            "{refusal:?}"
        );
        assert!(refusal.headline().contains("conditional writes"), "{}", refusal.headline());
        assert!(
            !dir.join("accounts.db").exists(),
            "nothing may be restored under a fence that is not real"
        );
    }

    /// Drops every put precondition on the floor — a store that has silently
    /// degraded to last-write-wins.
    struct UnconditionalStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Display for UnconditionalStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "UnconditionalStore({})", self.inner)
        }
    }
    impl std::fmt::Debug for UnconditionalStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "UnconditionalStore({:?})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for UnconditionalStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner
                .put_opts(location, payload, object_store::PutOptions::default())
                .await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Fails every GET whose key contains `poison` — a restore that dies after
    /// the claim is taken and after earlier subjects have landed.
    struct FailingGetStore {
        inner: Arc<dyn ObjectStore>,
        poison: &'static str,
        /// Poisoned GETs let through first. R977-B19: 1 lets the pre-claim
        /// verify pass so the failure lands in the post-claim restore.
        pass_first: std::sync::atomic::AtomicUsize,
    }

    impl std::fmt::Display for FailingGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingGetStore({})", self.inner)
        }
    }
    impl std::fmt::Debug for FailingGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingGetStore({:?})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingGetStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if location.as_ref().contains(self.poison)
                && self
                    .pass_first
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |n| n.checked_sub(1),
                    )
                    .is_err()
            {
                return Err(object_store::Error::Generic {
                    store: "FailingGetStore",
                    source: "injected mid-restore failure".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// R936-B2, prod 2026-09-23: a restore that errors after the claim is taken
    /// must leave the volume EMPTY. Otherwise the next attempt sees a populated
    /// volume under this node's own claim and serves the partial restore.
    #[tokio::test]
    async fn a_restore_that_fails_after_the_claim_leaves_nothing_to_trust() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&inner, "wl/acct/a.db", b"A-CURRENT").await;
        seed_snapshot(&inner, "wl/acct/b.db", b"B-CURRENT").await;
        let failing: Arc<dyn ObjectStore> = Arc::new(FailingGetStore {
            inner: inner.clone(),
            poison: "b.db/snapshots/",
            pass_first: 1.into(),
        });
        let dir = tempdir("fail-mid-restore");
        let subjects = vec!["a.db".to_string(), "b.db".to_string()];

        let err = hydrate(request(&failing, &dir, &subjects, "us-south-001"))
            .await
            .expect_err("the injected GET failure must surface");
        assert!(format!("{err:#}").contains("partial restore was removed"), "{err:#}");
        assert!(!dir.join("a.db").exists(), "a.db landed before the failure and must be cleared");
        assert!(!dir.join("b.db").exists());
        let held = claim::read_claim(&BackupTarget {
            store: inner.clone(),
            prefix: "wl/acct".into(),
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(held.owner, "us-south-001", "the claim was taken before the failure");

        // The retry restores both subjects from the store instead of trusting
        // anything left behind.
        let out = hydrate(request(&inner, &dir, &subjects, "us-south-001"))
            .await
            .unwrap();
        let HydrateOutcome::Hydrated { epoch, subjects: restored, .. } = out else {
            panic!("expected a full restore on retry, got {out:?}");
        };
        assert!(epoch > held.epoch);
        assert_eq!(restored.len(), 2);
        assert_eq!(std::fs::read(dir.join("a.db")).unwrap(), db_image(b"A-CURRENT"));
        assert_eq!(std::fs::read(dir.join("b.db")).unwrap(), db_image(b"B-CURRENT"));
    }

    struct RacingStore {
        inner: Arc<dyn ObjectStore>,
        claim_key: object_store::path::Path,
        when: RivalOn,
        rival_epoch: u64,
        rival_owner: String,
        fired: std::sync::atomic::AtomicBool,
        /// R977-B19: snapshot listings seen. The first belongs to the pre-claim
        /// verify; the rival fires on the post-claim restore's.
        snapshot_lists: std::sync::atomic::AtomicUsize,
    }

    impl RacingStore {
        async fn fire(&self) -> object_store::Result<()> {
            if self
                .fired
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(());
            }
            let body = format!("{} 1 {}\n", self.rival_epoch, self.rival_owner);
            self.inner
                .put(&self.claim_key, body.into_bytes().into())
                .await?;
            Ok(())
        }
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
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if self.when == RivalOn::ClaimPut && location == &self.claim_key {
                self.fire().await?;
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            if self.when == RivalOn::SnapshotRead
                && prefix.is_some_and(|p| p.as_ref().ends_with("snapshots"))
                && self
                    .snapshot_lists
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    >= 1
            {
                self.fire().await?;
            }
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn racing_store(when: RivalOn, rival_epoch: u64, rival_owner: &str) -> Arc<dyn ObjectStore> {
        Arc::new(RacingStore {
            inner: Arc::new(InMemory::new()),
            claim_key: object_store::path::Path::from("wl/acct/latest.owner-claim"),
            when,
            rival_epoch,
            rival_owner: rival_owner.to_string(),
            fired: std::sync::atomic::AtomicBool::new(false),
            snapshot_lists: 0.into(),
        })
    }

    /// A subject in a subdirectory of the volume needs that directory created —
    /// runc's bind mount does not create it and the restore writes a file, not
    /// a tree.
    #[tokio::test]
    async fn a_nested_subject_gets_its_directory_created() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/db/accounts.db", b"NESTED").await;
        let dir = tempdir("nested");
        let subjects = vec!["db/accounts.db".to_string()];

        let out = hydrate(request(&store, &dir, &subjects, "n1")).await.unwrap();
        assert!(matches!(out, HydrateOutcome::Hydrated { .. }), "{out:?}");
        assert_eq!(
            std::fs::read(dir.join("db/accounts.db")).unwrap(),
            db_image(b"NESTED")
        );
    }

    // ── verify: restore without the claim (R977-T17) ────────────────────────

    /// A real turso database image, so `integrity_check` has something to pass.
    async fn real_db_image(tag: &str) -> Vec<u8> {
        let dir = tempdir(tag);
        let path = dir.join("src.db");
        let p = path.to_str().unwrap();
        {
            let db = turso::Builder::new_local(p).build().await.unwrap();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
                .await
                .unwrap();
            conn.execute("INSERT INTO t (v) VALUES ('a'), ('b')", ())
                .await
                .unwrap();
            let mut r = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await.unwrap();
            while r.next().await.unwrap().is_some() {}
        }
        std::fs::read(&path).unwrap()
    }

    fn chain<'a>(store: &Arc<dyn ObjectStore>, subjects: &'a [String]) -> Chain<'a> {
        Chain {
            store: store.clone(),
            store_prefix: "wl/acct",
            subjects,
            tier: Tier::Snapshot,
        }
    }

    #[tokio::test]
    async fn verify_restores_a_valid_chain_and_leaves_the_owners_claim_alone() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        claim::acquire(&target, "us-south-001").await.unwrap();
        let before = claim::read_claim(&target).await.unwrap().unwrap();
        let image = real_db_image("verify-src").await;
        crate::snapshot::upload_base_snapshot(
            &BackupTarget {
                store: store.clone(),
                prefix: "wl/acct/accounts.db".into(),
            },
            &image,
        )
        .await
        .unwrap();

        let subjects = vec!["accounts.db".to_string()];
        let scratch = tempdir("verify-ok");
        let out = verify(&chain(&store, &subjects), &scratch).await.unwrap();
        assert!(matches!(out, VerifyOutcome::Valid { ref subjects, .. } if subjects.len() == 1), "{out:?}");
        assert_eq!(
            claim::read_claim(&target).await.unwrap().unwrap(),
            before,
            "verify must not move the live owner's claim or epoch"
        );
        // The owner can still prove it holds the fence at its epoch.
        assert!(claim::assert_holds(&target, before.epoch).await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn verify_takes_no_claim_on_an_unclaimed_chain() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        seed_snapshot(&store, "wl/acct/accounts.db", b"X").await;
        let subjects = vec!["accounts.db".to_string()];
        let scratch = tempdir("verify-noclaim");
        verify(&chain(&store, &subjects), &scratch).await.unwrap();
        let target = BackupTarget {
            store: store.clone(),
            prefix: "wl/acct".into(),
        };
        assert_eq!(claim::read_claim(&target).await.unwrap(), None);
    }

    #[tokio::test]
    async fn verify_reports_a_chain_that_does_not_pass_integrity_check() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Header magic and nothing else: restores, cannot be a database.
        crate::snapshot::upload_base_snapshot(
            &BackupTarget {
                store: store.clone(),
                prefix: "wl/acct/accounts.db".into(),
            },
            b"SQLite format 3\0GARBAGE",
        )
        .await
        .unwrap();
        let subjects = vec!["accounts.db".to_string()];
        let out = verify(&chain(&store, &subjects), &tempdir("verify-bad"))
            .await
            .unwrap();
        assert!(matches!(out, VerifyOutcome::Corrupt { .. }), "{out:?}");
    }

    #[tokio::test]
    async fn verify_refuses_a_populated_scratch_dir() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let scratch = tempdir("verify-full");
        std::fs::write(scratch.join("accounts.db"), b"x").unwrap();
        let subjects = vec!["accounts.db".to_string()];
        assert!(verify(&chain(&store, &subjects), &scratch).await.is_err());
    }
}
