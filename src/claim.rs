//! R850-F1 — **store-side ownership claim**: the fencing token an appliance can
//! mint for itself, on the one authority both sides of a network cut can still
//! reach.
//!
//! # Why this exists next to the fence it feeds
//!
//! [`stream::StreamConfig::epoch`] enforces at-most-one-writer on the R2 path,
//! but it does not *mint* anything — it takes a `u64` the caller was handed by
//! somebody else. For a **tenant** that somebody is yubaba's raft state machine
//! (`YubabaState::tenant_fencing_token`, W245/R732-F1), and
//! `yubaba/crates/tenant-streamer` is the wire between the two.
//!
//! An **appliance** — `archetype = "appliance"` with a yubaba-managed named
//! volume — has no such record. There is no `WorkloadOwnership` in the raft
//! state machine, and adding one would still not answer the question that
//! matters here, because two sovereign groups are independent raft groups that
//! share no epoch counter (the same gap [`stream::StreamConfig::pointer_generation`]
//! exists to cover for tenants). So an appliance that wants to hydrate its
//! state from an object store on a *new* node has no authority to ask whether
//! the old node is still alive.
//!
//! It has exactly one: **the object store the bytes live in.** Two properties
//! make it the right authority rather than merely an available one:
//!
//! - It is the same store the restore reads from, so "cannot reach the fencing
//!   authority" and "cannot hydrate" are the same condition. A fence that can
//!   fail independently of the operation it guards adds a failure mode; this
//!   one cannot.
//! - It already supports compare-and-swap, and turso-backup already refuses to
//!   run against a sink where it silently degrades
//!   ([`stream::probe_conditional_puts`]). The primitive is deployed and
//!   verified, not assumed.
//!
//! # What a claim is and is not
//!
//! A claim is a **monotonically increasing epoch with an owner label**, stored
//! at `<prefix>/latest.owner-claim`, advanced by compare-and-swap. That is all.
//!
//! - It **is not a lease.** There is no TTL and no clock. A lease answers "may
//!   I take over yet?", which is a liveness question, and liveness is a
//!   placement decision that belongs to yubaba — `tenant-streamer`'s
//!   `DEFAULT_LEASE_SECS` is where that decision lives for tenants. Putting a
//!   second, disagreeing answer in the storage layer would give an operator two
//!   timers to reconcile and no way to tell which one fired.
//! - It **is** a total order on takeovers. Whoever the store accepted holds the
//!   highest epoch, every reader agrees on who that is, and the loser of a
//!   concurrent race finds out synchronously ([`ClaimOutcome::Lost`]) rather
//!   than by corrupting the sink.
//! - It says **nothing about the losing node's local disk.** A fenced node
//!   stops being able to *write to the store*; it does not stop serving stale
//!   reads or accepting writes it will never ship. Closing that half is the
//!   supervisor's job — see the protocol below — and pretending otherwise is
//!   the specific lie this module refuses to tell.
//!
//! # The hydrate protocol this is built for
//!
//! The ordering matters, and step 3 is the one that is easy to omit:
//!
//! 1. [`acquire`] → epoch `E`. [`ClaimOutcome::Lost`] means another node is
//!    placing the same appliance *right now*; refuse to hydrate.
//! 2. Restore the declared state into the volume under `E`.
//! 3. [`assert_holds`] with `E` **before starting the workload**. A restore is
//!    seconds to minutes long and is not atomic; if `E+1` was minted while it
//!    ran, the bytes on this disk are already owned by somebody else and
//!    starting would produce the second live writer the whole archetype
//!    forbids.
//! 4. Run, tailing under `StreamConfig::epoch = E`. A [`stream::StreamOutcome::Fenced`]
//!    from any later tail means step 3's race happened after the start, and the
//!    supervisor must stop the workload.
//!
//! [`stream::StreamConfig::epoch`]: crate::stream::StreamConfig::epoch
//! [`stream::StreamConfig::pointer_generation`]: crate::stream::StreamConfig::pointer_generation
//! [`stream::probe_conditional_puts`]: crate::stream::probe_conditional_puts
//! [`stream::StreamOutcome::Fenced`]: crate::stream::StreamOutcome::Fenced

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};

use crate::snapshot::BackupTarget;

/// The epoch an unfenced writer uses, and the value
/// [`stream::StreamConfig::epoch`] documents as "unfenced".
///
/// No claim ever carries it: [`acquire`] on a virgin prefix mints
/// [`FIRST_EPOCH`]. It exists here so a reader of this module does not have to
/// go and check whether `0` is a legal claim (it is not) and so
/// [`ClaimRecord::parse`] can reject it by name.
///
/// [`stream::StreamConfig::epoch`]: crate::stream::StreamConfig::epoch
pub const UNFENCED_EPOCH: u64 = 0;

/// The epoch the first successful [`acquire`] against a prefix mints.
///
/// `1`, for the same reason `yah_tenant_pointer::FIRST_GENERATION` is 1: `0`
/// means "nobody has claimed this", and a real owner must never be mistakable
/// for one.
pub const FIRST_EPOCH: u64 = 1;

/// Sidecar leaf holding the claim. Sits beside `latest.stream-watermark` and
/// the two fingerprint sidecars, under the same prefix, deliberately: an
/// operator listing a prefix should see the ownership record next to the data
/// it governs rather than in a separate administrative namespace.
const CLAIM_LEAF: &str = "latest.owner-claim";

/// A parsed ownership claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRecord {
    /// The fencing token. Feed this to
    /// [`StreamConfig::epoch`](crate::stream::StreamConfig::epoch).
    pub epoch: u64,
    /// Opaque label for *who* holds `epoch` — a node id, a hostname, a
    /// `<machine>/<workload>` pair. Never interpreted here, and never used to
    /// decide anything: two acquires from the same label are still two
    /// takeovers, because a restarted process and its own zombie predecessor
    /// share a label and are exactly the pair the fence has to separate.
    pub owner: String,
    /// Wall-clock nanos at the acquire that wrote this record. Diagnostic
    /// only — nothing compares it, which is what keeps this a claim and not a
    /// lease.
    pub claimed_at_nanos: u128,
}

impl ClaimRecord {
    /// Serialize to the one-line whitespace form the watermark sidecar uses.
    ///
    /// `owner` goes last and is validated at [`acquire`] to contain no
    /// whitespace, so the format stays splittable without quoting.
    fn render(&self) -> String {
        format!("{} {} {}\n", self.epoch, self.claimed_at_nanos, self.owner)
    }

    /// Parse the sidecar body.
    ///
    /// Loud on every deviation. A claim that cannot be read is *not* treated as
    /// an absent claim: absent means "free to take", and silently taking a
    /// prefix whose owner record was merely garbled is precisely the
    /// two-live-writers outcome this module exists to prevent.
    fn parse(text: &str) -> Result<Self> {
        let mut fields = text.split_whitespace();
        let epoch: u64 = fields
            .next()
            .context("owner-claim sidecar is empty; expected '<epoch> <nanos> <owner>'")?
            .parse()
            .context("owner-claim epoch")?;
        if epoch == UNFENCED_EPOCH {
            bail!(
                "owner-claim sidecar records epoch 0, which means unfenced and is never \
                 written by acquire(); the sidecar has been tampered with or hand-edited"
            );
        }
        let claimed_at_nanos: u128 = fields
            .next()
            .context("owner-claim sidecar must be '<epoch> <nanos> <owner>'")?
            .parse()
            .context("owner-claim claimed_at_nanos")?;
        let owner = fields
            .next()
            .context("owner-claim sidecar must be '<epoch> <nanos> <owner>'")?
            .to_string();
        Ok(ClaimRecord {
            epoch,
            claimed_at_nanos,
            owner,
        })
    }
}

/// What an [`acquire`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The store accepted our compare-and-swap. `claim` is now the record every
    /// reader of this prefix will see, and `previous` is whoever we displaced
    /// (`None` on a virgin prefix).
    ///
    /// Displacing a live owner is *legal and expected* — that is what a
    /// takeover is. What it is not is silent: the displaced writer's next tail
    /// bounces [`Fenced`](crate::stream::StreamOutcome::Fenced).
    Granted {
        claim: ClaimRecord,
        previous: Option<ClaimRecord>,
    },
    /// Somebody else's write landed between our read and our conditional put,
    /// so we minted nothing. `current` is the record we lost to, re-read after
    /// the fact.
    ///
    /// This is the *concurrent* case only. A sequentially-later acquire always
    /// succeeds — the caller who wants "refuse if anyone else holds it" wants
    /// [`assert_holds`], not a retry loop here.
    Lost { current: ClaimRecord },
}

impl ClaimOutcome {
    /// The epoch to stream under, or `None` if we did not get one.
    pub fn granted_epoch(&self) -> Option<u64> {
        match self {
            ClaimOutcome::Granted { claim, .. } => Some(claim.epoch),
            ClaimOutcome::Lost { .. } => None,
        }
    }
}

/// Object key of `target`'s claim sidecar.
fn claim_key(target: &BackupTarget) -> ObjPath {
    join_key(&target.prefix, CLAIM_LEAF)
}

/// Read the current claim, with the object version needed to CAS over it.
async fn read_claim_versioned(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
) -> Result<Option<(ClaimRecord, UpdateVersion)>> {
    match store.get(key).await {
        Ok(res) => {
            // Capture the version before consuming the body — `bytes()` takes
            // `res` by value. Both fields are kept because stores differ in
            // which one they honour for a conditional put (`object_store`'s own
            // `UpdateVersion` docs say to preserve both). Same shape as
            // `stream::read_watermark`.
            let version = UpdateVersion {
                e_tag: res.meta.e_tag.clone(),
                version: res.meta.version.clone(),
            };
            let bytes = res.bytes().await.context("reading owner-claim sidecar")?;
            let record = ClaimRecord::parse(&String::from_utf8_lossy(&bytes))
                .with_context(|| format!("parsing owner-claim sidecar {key}"))?;
            Ok(Some((record, version)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e).context("fetching owner-claim sidecar"),
    }
}

/// Who holds this prefix right now, if anyone.
///
/// `Ok(None)` means the sidecar is absent — an unclaimed prefix. It does *not*
/// mean unreadable: a garbled sidecar is an `Err`, because "free to take" is
/// the one answer that must never be produced by a failure.
pub async fn read_claim(target: &BackupTarget) -> Result<Option<ClaimRecord>> {
    Ok(read_claim_versioned(&target.store, &claim_key(target))
        .await?
        .map(|(record, _)| record))
}

/// Take ownership of `target`'s prefix, advancing the epoch by one.
///
/// The compare-and-swap is against the version read at the top of this call, so
/// two nodes acquiring at the same instant produce exactly one
/// [`ClaimOutcome::Granted`] — the other gets [`ClaimOutcome::Lost`] naming the
/// winner. On a virgin prefix the put is [`PutMode::Create`], which is the same
/// guard one level down: two bootstrappers cannot both mint [`FIRST_EPOCH`].
///
/// **This does not ask permission.** A sequentially-later acquire displaces a
/// live owner on purpose; deciding *whether* a takeover should happen is
/// placement's job, and a storage primitive that refused would make failover
/// impossible. What this guarantees is that the takeover is ordered, visible,
/// and immediately fatal to the displaced writer's next tail.
///
/// `owner` must be non-empty and whitespace-free — it is the last field of a
/// whitespace-split sidecar line, and a label with a space in it would parse
/// back as a truncated one.
pub async fn acquire(target: &BackupTarget, owner: &str) -> Result<ClaimOutcome> {
    if owner.is_empty() || owner.split_whitespace().count() != 1 {
        bail!(
            "claim owner label {owner:?} must be one non-empty whitespace-free token; it is the \
             last field of the sidecar line and would not parse back"
        );
    }
    let key = claim_key(target);
    let existing = read_claim_versioned(&target.store, &key).await?;

    let (next_epoch, previous, mode) = match &existing {
        Some((record, version)) => (
            record
                .epoch
                .checked_add(1)
                .context("owner-claim epoch would overflow u64")?,
            Some(record.clone()),
            PutMode::Update(version.clone()),
        ),
        // Nothing when we read: assert that is *still* true, so two nodes
        // bootstrapping the same fresh prefix cannot both proceed.
        None => (FIRST_EPOCH, None, PutMode::Create),
    };

    let claim = ClaimRecord {
        epoch: next_epoch,
        owner: owner.to_string(),
        claimed_at_nanos: unix_nanos(),
    };
    let opts = PutOptions {
        mode,
        ..Default::default()
    };
    match target
        .store
        .put_opts(&key, claim.render().into_bytes().into(), opts)
        .await
    {
        Ok(_) => Ok(ClaimOutcome::Granted { claim, previous }),
        Err(object_store::Error::Precondition { .. })
        | Err(object_store::Error::AlreadyExists { .. }) => {
            // Re-read to name the winner. A vanished sidecar here means someone
            // deleted the claim between our failed CAS and this read, which is
            // not a state any code in this crate produces — report it rather
            // than inventing a winner.
            let (current, _) = read_claim_versioned(&target.store, &key)
                .await?
                .context(
                    "lost the owner-claim compare-and-swap, but the sidecar is now absent; \
                     something outside turso-backup is deleting claims",
                )?;
            Ok(ClaimOutcome::Lost { current })
        }
        Err(e) => Err(e).with_context(|| format!("writing owner-claim sidecar {key}")),
    }
}

/// Why a [`assert_holds`] check failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimLost {
    /// Somebody minted a higher epoch. The common case, and the one step 3 of
    /// the hydrate protocol is looking for.
    Superseded { current: ClaimRecord, ours: u64 },
    /// The sidecar is gone. Not a state this crate produces; surfaced
    /// separately so an operator reads "somebody deleted the claim" rather than
    /// "you were superseded by nobody".
    Vanished { ours: u64 },
}

impl std::fmt::Display for ClaimLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClaimLost::Superseded { current, ours } => write!(
                f,
                "epoch {ours} was superseded by epoch {} (owner {})",
                current.epoch, current.owner
            ),
            ClaimLost::Vanished { ours } => write!(
                f,
                "epoch {ours} can no longer be verified — the owner-claim sidecar is absent"
            ),
        }
    }
}

/// Confirm `epoch` is still the highest claim on this prefix.
///
/// `Ok(Ok(()))` means yes. `Ok(Err(ClaimLost))` means the check ran and the
/// answer is no. `Err` means the check could not run at all — and the caller
/// must treat that as *not holding*, because an unreachable store is
/// indistinguishable from the partition this fence exists for.
///
/// A record at exactly `epoch` passes regardless of its owner label: labels are
/// diagnostic and a node that re-reads its own claim after a process restart
/// must not fence itself out over a hostname that changed shape.
pub async fn assert_holds(
    target: &BackupTarget,
    epoch: u64,
) -> Result<std::result::Result<(), ClaimLost>> {
    match read_claim(target).await? {
        Some(current) if current.epoch == epoch => Ok(Ok(())),
        Some(current) => Ok(Err(ClaimLost::Superseded {
            current,
            ours: epoch,
        })),
        None => Ok(Err(ClaimLost::Vanished { ours: epoch })),
    }
}

/// Join an object-store prefix and a leaf into a normalized [`ObjPath`],
/// tolerating empty/slash-padded prefixes. Same helper as `dedup`/`stream`;
/// duplicated rather than shared because each module keeps its own private copy
/// and hoisting it is a wider change than this ticket.
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

    fn target(prefix: &str) -> BackupTarget {
        BackupTarget {
            store: Arc::new(InMemory::new()),
            prefix: prefix.to_string(),
        }
    }

    /// Two targets sharing one store — the two-nodes-one-bucket shape every
    /// fencing test here needs.
    fn shared(prefix: &str) -> (BackupTarget, BackupTarget) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        (
            BackupTarget {
                store: store.clone(),
                prefix: prefix.to_string(),
            },
            BackupTarget {
                store,
                prefix: prefix.to_string(),
            },
        )
    }

    #[tokio::test]
    async fn a_virgin_prefix_is_unclaimed_and_the_first_acquire_mints_epoch_one() {
        let t = target("acct");
        assert_eq!(read_claim(&t).await.unwrap(), None);

        let out = acquire(&t, "us-east-001").await.unwrap();
        let ClaimOutcome::Granted { claim, previous } = out else {
            panic!("first acquire on a virgin prefix must be granted, got {out:?}");
        };
        assert_eq!(claim.epoch, FIRST_EPOCH);
        assert_eq!(claim.owner, "us-east-001");
        assert_eq!(previous, None);

        // And it round-trips through the sidecar, not just through the return
        // value — the next node reads bytes, not our struct.
        assert_eq!(read_claim(&t).await.unwrap(), Some(claim));
    }

    #[tokio::test]
    async fn a_takeover_advances_the_epoch_and_names_who_it_displaced() {
        let (a, b) = shared("acct");
        acquire(&a, "us-east-001").await.unwrap();

        let out = acquire(&b, "us-west-003").await.unwrap();
        let ClaimOutcome::Granted { claim, previous } = out else {
            panic!("a sequentially-later acquire is a takeover and must be granted, got {out:?}");
        };
        assert_eq!(claim.epoch, FIRST_EPOCH + 1);
        assert_eq!(claim.owner, "us-west-003");
        assert_eq!(previous.unwrap().owner, "us-east-001");
    }

    /// The property the whole module exists for: the displaced node can no
    /// longer prove it holds the prefix, and finds out by asking rather than by
    /// corrupting anything.
    #[tokio::test]
    async fn the_displaced_owner_fails_assert_holds_and_is_told_who_won() {
        let (a, b) = shared("acct");
        let ours = acquire(&a, "us-east-001")
            .await
            .unwrap()
            .granted_epoch()
            .unwrap();
        assert!(assert_holds(&a, ours).await.unwrap().is_ok());

        acquire(&b, "us-west-003").await.unwrap();

        let lost = assert_holds(&a, ours).await.unwrap().unwrap_err();
        let ClaimLost::Superseded { current, ours: had } = lost else {
            panic!("expected Superseded, got {lost:?}");
        };
        assert_eq!(had, ours);
        assert_eq!(current.epoch, ours + 1);
        assert_eq!(current.owner, "us-west-003");
    }

    /// A restarted process re-reading its own claim must not fence itself out.
    /// The label is diagnostic; only the epoch decides.
    #[tokio::test]
    async fn assert_holds_ignores_the_owner_label() {
        let t = target("acct");
        let e = acquire(&t, "node-a").await.unwrap().granted_epoch().unwrap();
        assert!(assert_holds(&t, e).await.unwrap().is_ok());
    }

    /// Step 1 of the hydrate protocol. Both nodes read an absent sidecar, both
    /// try to bootstrap; `PutMode::Create` lets exactly one through, and the
    /// loser is told so synchronously instead of hydrating over the winner.
    #[tokio::test]
    async fn two_bootstrappers_racing_a_fresh_prefix_produce_exactly_one_winner() {
        let (a, b) = shared("acct");
        let key = claim_key(&a);

        // Both read empty (the real concurrent interleaving) before either
        // writes: `acquire` re-reads internally, so the race is staged by
        // letting A's acquire complete and forcing B to have already decided it
        // saw nothing. Drive B's put directly with the Create mode `acquire`
        // would have chosen for an absent sidecar.
        acquire(&a, "node-a").await.unwrap();
        let stale_create = b
            .store
            .put_opts(
                &key,
                b"1 0 node-b".as_slice().into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        assert!(
            stale_create.is_err(),
            "PutMode::Create must be refused once the sidecar exists — if this passes, the \
             store has degraded to unconditional writes and the fence is not real"
        );
        assert_eq!(read_claim(&a).await.unwrap().unwrap().owner, "node-a");
    }

    /// The concurrent-takeover race, staged the only way it can be
    /// deterministically: B holds a version that A has already replaced.
    #[tokio::test]
    async fn a_lost_compare_and_swap_reports_the_winner_rather_than_minting() {
        let (a, b) = shared("acct");
        acquire(&a, "node-a").await.unwrap();

        // B reads, capturing version v1 — this is the state a concurrent
        // acquire is in at the moment it computes its next epoch.
        let key = claim_key(&b);
        let (_, v1) = read_claim_versioned(&b.store, &key).await.unwrap().unwrap();

        // A takes over first, superseding v1.
        acquire(&a, "node-a").await.unwrap();

        // B's conditional put on the stale version must bounce, and `acquire`'s
        // recovery path must name the winner.
        let bounced = b
            .store
            .put_opts(
                &key,
                b"2 0 node-b".as_slice().into(),
                PutOptions {
                    mode: PutMode::Update(v1),
                    ..Default::default()
                },
            )
            .await;
        assert!(bounced.is_err(), "stale-version update must be refused");

        // ...and a fresh acquire from B now sees epoch 2 and mints 3, i.e. the
        // sequential path is unblocked once the race is over.
        let out = acquire(&b, "node-b").await.unwrap();
        assert_eq!(out.granted_epoch(), Some(3));
    }

    /// "Free to take" must never be produced by a failure. A sidecar that
    /// cannot be parsed is an error, not an absent claim.
    #[tokio::test]
    async fn a_garbled_sidecar_is_an_error_not_an_unclaimed_prefix() {
        let t = target("acct");
        t.store
            .put(&claim_key(&t), b"not a claim at all".as_slice().into())
            .await
            .unwrap();
        let err = read_claim(&t).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("owner-claim"),
            "error should name the sidecar: {err:#}"
        );
    }

    /// Epoch 0 is `StreamConfig`'s "unfenced" sentinel. A sidecar recording it
    /// is either hand-edited or written by something that is not this module,
    /// and reading it as a real owner would let a genuine claim at epoch 1
    /// appear to be a *takeover* of it.
    #[tokio::test]
    async fn epoch_zero_in_a_sidecar_is_rejected() {
        let t = target("acct");
        t.store
            .put(&claim_key(&t), b"0 123 node-a".as_slice().into())
            .await
            .unwrap();
        let err = read_claim(&t).await.unwrap_err();
        assert!(format!("{err:#}").contains("epoch 0"), "{err:#}");
    }

    #[tokio::test]
    async fn an_owner_label_with_whitespace_is_refused_before_anything_is_written() {
        let t = target("acct");
        let err = acquire(&t, "us east 001").await.unwrap_err();
        assert!(format!("{err:#}").contains("whitespace-free"), "{err:#}");
        assert_eq!(
            read_claim(&t).await.unwrap(),
            None,
            "a refused label must not leave a sidecar behind"
        );
    }

    #[tokio::test]
    async fn an_empty_prefix_claims_at_the_bucket_root() {
        let t = target("");
        acquire(&t, "node-a").await.unwrap();
        assert_eq!(claim_key(&t), ObjPath::from(CLAIM_LEAF));
        assert!(read_claim(&t).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn assert_holds_on_a_deleted_sidecar_is_vanished_not_held() {
        let t = target("acct");
        let e = acquire(&t, "node-a").await.unwrap().granted_epoch().unwrap();
        t.store.delete(&claim_key(&t)).await.unwrap();
        assert_eq!(
            assert_holds(&t, e).await.unwrap().unwrap_err(),
            ClaimLost::Vanished { ours: e }
        );
    }
}
