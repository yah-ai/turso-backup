//! Recover a spliced durability chain: cut a fresh tier-1a snapshot from a
//! chosen volume and move the spliced manifests aside.
//!
//! R977-T18. A stream chain that two live copies wrote into (W364 §8, the
//! 2026-10-13 drill) or whose source WAL was recreated mid-stream cannot be
//! restored: [`crate::stream::restore_latest_stream`] lists *every* generation
//! manifest whatever the base snapshot's age, so a fresh snapshot alone does
//! not help. R932-T3 and R977-B15 each recovered one by hand from the Mac with
//! hand-copied bucket creds. This is that procedure as one verb:
//!
//! 1. **Claim check.** Any claim on the workload prefix is treated as held by a
//!    live owner — claims carry no lease, so "live" cannot be told from the
//!    store, and a tail streaming into the chain while it is reseeded would
//!    splice it again. Refused unless `fence_owner` names the current owner,
//!    in which case the reseed takes the claim (epoch + 1) and fences it.
//! 2. **Archive.** Every object under the prefix except snapshot images is
//!    copied to the sibling `<prefix>.reseed-archive-<label>/`.
//! 3. **Snapshot.** Each subject (db + `-wal`) is copied out of the source
//!    volume, the copy is checked to have held still, and the snapshot writer
//!    (`snapshot::vacuum_into` + `snapshot::upload_base_snapshot`) publishes it.
//!    The live file is never opened.
//! 4. **Supersede.** `generations/*` and `latest.stream-watermark` move to
//!    `<subject>/superseded-<label>/` — copied, then the original removed.
//!    Nothing is deleted without a byte-identical copy in place first.
//! 5. **Verify.** [`crate::hydrate::verify`] restores the result into scratch;
//!    anything but `Valid` is an `Err`.
//!
//! Dry-run ([`ReseedRequest::apply`] false) reads the claim and lists the
//! store, and writes nothing anywhere.
//!
//! @arch:see(.yah/docs/working/W364-workload-replica-reconcile.md)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};

use store_claim::claim::{self, ClaimOutcome, ClaimRecord};
use store_claim::BackupTarget;

use crate::hydrate::{subject_path, verify, Chain, Tier, VerifyOutcome};

/// Everything [`reseed`] needs.
pub struct ReseedRequest<'a> {
    pub chain: Chain<'a>,
    /// Volume dir holding the subjects to seed from. Only ever copied.
    pub source_root: &'a Path,
    /// Empty-or-absent working dir for the quiesced copies and the verify.
    pub scratch_root: &'a Path,
    /// Suffix for `superseded-<label>` and `reseed-archive-<label>`, e.g. `20261015`.
    pub label: &'a str,
    /// Claim owner label recorded when `fence_owner` takes the claim.
    pub owner: &'a str,
    /// Current claim owner the operator accepts fencing. Must match exactly.
    pub fence_owner: Option<&'a str>,
    pub apply: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubjectPlan {
    pub subject: String,
    /// Size of the subject's main file in the source volume.
    pub source_bytes: u64,
    /// Keys moved under `superseded-<label>/`.
    pub supersede: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReseedPlan {
    pub claim: Option<ClaimRecord>,
    /// True when the claim will be (or was) taken to fence `claim`'s owner.
    pub fence: bool,
    pub archive_prefix: String,
    /// Keys copied to `archive_prefix`.
    pub archive: Vec<String>,
    pub subjects: Vec<SubjectPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReseedRefusal {
    /// A claim is held and no `fence_owner` was given.
    OwnerHoldsClaim { current: ClaimRecord },
    /// `fence_owner` names someone other than the claim's owner.
    FenceOwnerMismatch { current: ClaimRecord, named: String },
}

impl ReseedRefusal {
    pub fn kind(&self) -> &'static str {
        match self {
            ReseedRefusal::OwnerHoldsClaim { .. } => "owner_holds_claim",
            ReseedRefusal::FenceOwnerMismatch { .. } => "fence_owner_mismatch",
        }
    }

    pub fn headline(&self) -> String {
        match self {
            ReseedRefusal::OwnerHoldsClaim { current } => format!(
                "the chain's claim is held by {} at epoch {}; its tail may still be streaming \
                 into this chain, and reseeding under it would splice the chain again. Stop that \
                 copy, or pass --fence-owner {} to take the claim and fence it first",
                current.owner, current.epoch, current.owner
            ),
            ReseedRefusal::FenceOwnerMismatch { current, named } => format!(
                "--fence-owner names {named}, but the claim is held by {} at epoch {}; \
                 refusing to fence an owner the operator did not name",
                current.owner, current.epoch
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReseedOutcome {
    /// Dry-run: what `--apply` would do. Nothing was written.
    Planned(ReseedPlan),
    Applied {
        plan: ReseedPlan,
        /// Epoch taken when fencing, if any.
        fenced_at: Option<u64>,
        /// One new snapshot key per subject.
        snapshots: Vec<String>,
        /// Always `Valid` — anything else is returned as `Err`.
        verify: VerifyOutcome,
    },
    Refused(ReseedRefusal),
}

/// Plan, and with `apply`, carry out the reseed. See the module doc.
pub async fn reseed(req: ReseedRequest<'_>) -> Result<ReseedOutcome> {
    validate_label(req.label)?;
    let chain = &req.chain;
    let prefix = chain.store_prefix.trim_matches('/');
    if prefix.is_empty() {
        bail!("reseed needs a non-empty store prefix; the archive is written beside it");
    }
    match chain.tier {
        Tier::Stream | Tier::Snapshot => {}
        Tier::Dedup => bail!("reseed covers stream and snapshot chains; a dedup chain has no generations to splice"),
    }
    let store = &chain.store;
    let workload = BackupTarget {
        store: store.clone(),
        prefix: prefix.to_string(),
    };

    // 1. Claim.
    let current = claim::read_claim(&workload).await?;
    let fence = match (&current, req.fence_owner) {
        (None, _) => false,
        (Some(c), None) => {
            return Ok(ReseedOutcome::Refused(ReseedRefusal::OwnerHoldsClaim { current: c.clone() }))
        }
        (Some(c), Some(named)) if named != c.owner => {
            return Ok(ReseedOutcome::Refused(ReseedRefusal::FenceOwnerMismatch {
                current: c.clone(),
                named: named.to_string(),
            }))
        }
        (Some(_), Some(_)) => true,
    };

    // Plan: source sizes, keys to archive and to supersede, collisions.
    let mut subjects = Vec::new();
    for s in chain.subjects {
        let path = subject_path(req.source_root, s)?;
        let meta = std::fs::metadata(&path)
            .with_context(|| format!("source subject {} is not readable", path.display()))?;
        let sp = format!("{prefix}/{s}");
        let mut supersede: Vec<String> = Vec::new();
        if chain.tier == Tier::Stream {
            let gens = crate::stream::list_recursive(store, &ObjPath::from(format!("{sp}/generations"))).await?;
            supersede.extend(gens.into_iter().map(|m| m.location.to_string()));
            let wm = ObjPath::from(format!("{sp}/latest.stream-watermark"));
            if exists(store, &wm).await? {
                supersede.push(wm.to_string());
            }
            supersede.sort();
        }
        let dest = ObjPath::from(format!("{sp}/superseded-{}", req.label));
        if !crate::stream::list_recursive(store, &dest).await?.is_empty() {
            bail!("{dest} already exists; pick another --label rather than mixing two reseeds");
        }
        subjects.push(SubjectPlan {
            subject: s.clone(),
            source_bytes: meta.len(),
            supersede,
        });
    }
    let archive_prefix = format!("{prefix}.reseed-archive-{}", req.label);
    if !crate::stream::list_recursive(store, &ObjPath::from(archive_prefix.as_str())).await?.is_empty() {
        bail!("{archive_prefix} already exists; pick another --label");
    }
    let mut archive: Vec<String> = crate::stream::list_recursive(store, &ObjPath::from(prefix))
        .await?
        .into_iter()
        .map(|m| m.location.to_string())
        .filter(|k| !k.split('/').any(|seg| seg == "snapshots"))
        .collect();
    archive.sort();
    let plan = ReseedPlan {
        claim: current,
        fence,
        archive_prefix,
        archive,
        subjects,
    };
    if !req.apply {
        return Ok(ReseedOutcome::Planned(plan));
    }

    // Scratch must start empty so the verify proves the store, not leftovers.
    if req.scratch_root.exists()
        && std::fs::read_dir(req.scratch_root)?.next().is_some()
    {
        bail!("scratch dir {} is not empty", req.scratch_root.display());
    }
    let copies = req.scratch_root.join("copy");
    let verify_dir = req.scratch_root.join("verify");
    std::fs::create_dir_all(&verify_dir)
        .with_context(|| format!("creating {}", verify_dir.display()))?;

    // Fence before writing anything the old owner could race.
    let fenced_at = if fence {
        match claim::acquire(&workload, req.owner).await? {
            ClaimOutcome::Granted { claim, .. } => Some(claim.epoch),
            ClaimOutcome::Lost { current } => bail!(
                "lost the claim race to {} at epoch {} while fencing; nothing was written",
                current.owner,
                current.epoch
            ),
        }
    } else {
        None
    };

    // 2. Archive.
    for key in &plan.archive {
        let rel = &key[prefix.len() + 1..];
        copy_create(store, key, &format!("{}/{rel}", plan.archive_prefix)).await?;
    }

    // 3. Snapshot from quiesced copies.
    let mut snapshots = Vec::new();
    for (i, sp) in plan.subjects.iter().enumerate() {
        let src = subject_path(req.source_root, &sp.subject)?;
        let copy = subject_path(&copies, &sp.subject)?;
        quiesced_copy(&src, &copy)?;
        let image_path = req.scratch_root.join(format!("image-{i}.db"));
        let copy_str = copy
            .to_str()
            .with_context(|| format!("path {} is not UTF-8", copy.display()))?;
        crate::snapshot::vacuum_into(copy_str, &image_path).await?;
        let image = std::fs::read(&image_path)
            .with_context(|| format!("reading {}", image_path.display()))?;
        let target = BackupTarget {
            store: store.clone(),
            prefix: format!("{prefix}/{}", sp.subject),
        };
        snapshots.push(crate::snapshot::upload_base_snapshot(&target, &image).await?);
    }

    // 4. Supersede: copy, confirm, then remove the original.
    for sp in &plan.subjects {
        let base = format!("{prefix}/{}/", sp.subject);
        for key in &sp.supersede {
            let rel = &key[base.len()..];
            let dest = format!("{base}superseded-{}/{rel}", req.label);
            copy_create(store, key, &dest).await?;
            store
                .delete(&ObjPath::from(key.as_str()))
                .await
                .with_context(|| format!("removing {key} after copying it to {dest}"))?;
        }
    }

    // 5. Verify.
    let outcome = verify(chain, &verify_dir).await?;
    if !matches!(outcome, VerifyOutcome::Valid { .. }) {
        bail!(
            "RESEED APPLIED BUT THE CHAIN DOES NOT VERIFY: {outcome:?}. New snapshots {snapshots:?}; \
             archive at {}; superseded manifests under <subject>/superseded-{}/",
            plan.archive_prefix,
            req.label
        );
    }
    Ok(ReseedOutcome::Applied {
        plan,
        fenced_at,
        snapshots,
        verify: outcome,
    })
}

fn validate_label(label: &str) -> Result<()> {
    if label.is_empty()
        || !label
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!("label {label:?} must be non-empty [A-Za-z0-9_-]");
    }
    Ok(())
}

async fn exists(store: &Arc<dyn ObjectStore>, key: &ObjPath) -> Result<bool> {
    match store.head(key).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e).with_context(|| format!("checking {key}")),
    }
}

/// Copy `from` to `to`, refusing to overwrite, and confirm the copy's bytes.
async fn copy_create(store: &Arc<dyn ObjectStore>, from: &str, to: &str) -> Result<()> {
    let from_p = ObjPath::from(from);
    let to_p = ObjPath::from(to);
    let bytes = store
        .get(&from_p)
        .await
        .with_context(|| format!("reading {from}"))?
        .bytes()
        .await
        .with_context(|| format!("reading body of {from}"))?;
    let opts = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    store
        .put_opts(&to_p, bytes.clone().into(), opts)
        .await
        .with_context(|| format!("copying {from} to {to} (refusing to overwrite)"))?;
    let back = store
        .get(&to_p)
        .await
        .with_context(|| format!("re-reading {to}"))?
        .bytes()
        .await?;
    if back != bytes {
        bail!("copy of {from} at {to} does not match the original; original left in place");
    }
    Ok(())
}

fn sidecars(p: &Path) -> [PathBuf; 2] {
    let s = p.as_os_str().to_owned();
    let mut wal = s.clone();
    wal.push("-wal");
    [p.to_path_buf(), PathBuf::from(wal)]
}

fn stamp(paths: &[PathBuf]) -> Vec<Option<(u64, SystemTime)>> {
    paths
        .iter()
        .map(|p| {
            std::fs::metadata(p)
                .ok()
                .map(|m| (m.len(), m.modified().unwrap_or(SystemTime::UNIX_EPOCH)))
        })
        .collect()
}

/// Copy `src` and its `-wal` to `dest`, accepting the copy only if neither
/// file's size or mtime moved across it. The live file is never opened by
/// SQLite/turso — only read as bytes.
fn quiesced_copy(src: &Path, dest: &Path) -> Result<()> {
    let from = sidecars(src);
    let to = sidecars(dest);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    for _ in 0..5 {
        let before = stamp(&from);
        for (f, t) in from.iter().zip(&to) {
            let _ = std::fs::remove_file(t);
            if f.exists() {
                std::fs::copy(f, t)
                    .with_context(|| format!("copying {} to {}", f.display(), t.display()))?;
            }
        }
        if stamp(&from) == before {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    bail!(
        "{} kept changing across 5 copy attempts; stop the workload or point --source at a stopped copy",
        src.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn tempdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "reseed-{tag}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    async fn make_source_db(dir: &Path, name: &str) {
        let p = dir.join(name);
        let db = turso::Builder::new_local(p.to_str().unwrap()).build().await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", ())
            .await
            .unwrap();
        conn.execute("INSERT INTO t (v) VALUES ('a'), ('b')", ())
            .await
            .unwrap();
    }

    /// A spliced stream chain: an old base plus manifests that do not parse
    /// into a chain, which is what made the 10-13 prefixes unrestorable.
    async fn spliced_store() -> Arc<dyn ObjectStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (k, v) in [
            ("wl/issues/issues.db/generations/gen-00000000000000000001.manifest", "spliced-1"),
            ("wl/issues/issues.db/generations/gen-00000000000000000002.manifest", "spliced-2"),
            ("wl/issues/issues.db/latest.stream-watermark", "wm"),
            ("wl/issues/issues.db/snapshots/snapshot-00000000000000000001.db", "old"),
        ] {
            store.put(&ObjPath::from(k), v.as_bytes().to_vec().into()).await.unwrap();
        }
        store
    }

    async fn all_keys(store: &Arc<dyn ObjectStore>) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        for m in crate::stream::list_recursive(store, &ObjPath::from("")).await.unwrap() {
            let b = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            out.push((m.location.to_string(), b.to_vec()));
        }
        out.sort();
        out
    }

    fn req<'a>(
        store: &Arc<dyn ObjectStore>,
        subjects: &'a [String],
        source: &'a Path,
        scratch: &'a Path,
        fence_owner: Option<&'a str>,
        apply: bool,
    ) -> ReseedRequest<'a> {
        ReseedRequest {
            chain: Chain {
                store: store.clone(),
                store_prefix: "wl/issues",
                subjects,
                tier: Tier::Stream,
            },
            source_root: source,
            scratch_root: scratch,
            label: "20261015",
            owner: "reseed-test",
            fence_owner,
            apply,
        }
    }

    #[tokio::test]
    async fn dry_run_writes_nothing() {
        let store = spliced_store().await;
        let src = tempdir("dry-src");
        make_source_db(&src, "issues.db").await;
        let scratch = tempdir("dry-scratch").join("s");
        let subjects = vec!["issues.db".to_string()];
        let before = all_keys(&store).await;
        let out = reseed(req(&store, &subjects, &src, &scratch, None, false)).await.unwrap();
        let ReseedOutcome::Planned(plan) = out else { panic!("{out:?}") };
        assert_eq!(plan.subjects[0].supersede.len(), 3, "{plan:?}");
        assert_eq!(all_keys(&store).await, before, "dry-run must not touch the store");
        assert!(!scratch.exists(), "dry-run must not create scratch");
    }

    #[tokio::test]
    async fn apply_archives_and_never_deletes_and_the_result_verifies() {
        let store = spliced_store().await;
        let src = tempdir("apply-src");
        make_source_db(&src, "issues.db").await;
        let scratch = tempdir("apply-scratch").join("s");
        let subjects = vec!["issues.db".to_string()];
        let chain = Chain {
            store: store.clone(),
            store_prefix: "wl/issues",
            subjects: &subjects,
            tier: Tier::Stream,
        };
        // Precondition: the spliced chain does not restore.
        let pre = verify(&chain, &tempdir("pre-verify")).await;
        assert!(!matches!(pre, Ok(VerifyOutcome::Valid { .. })), "{pre:?}");

        let before = all_keys(&store).await;
        let out = reseed(req(&store, &subjects, &src, &scratch, None, true)).await.unwrap();
        let ReseedOutcome::Applied { snapshots, verify: v, .. } = out else { panic!("{out:?}") };
        assert!(matches!(v, VerifyOutcome::Valid { .. }));
        assert_eq!(snapshots.len(), 1);

        let after: std::collections::HashMap<_, _> = all_keys(&store).await.into_iter().collect();
        // Every original object's bytes survive somewhere: in place, or moved
        // under superseded-<label>/, and also in the archive (snapshots excepted).
        for (k, bytes) in &before {
            let rel = k.strip_prefix("wl/issues/issues.db/").unwrap();
            let moved = format!("wl/issues/issues.db/superseded-20261015/{rel}");
            assert!(
                after.get(k) == Some(bytes) || after.get(&moved) == Some(bytes),
                "{k} vanished"
            );
            if !rel.starts_with("snapshots/") {
                let arch = format!("wl/issues.reseed-archive-20261015/issues.db/{rel}");
                assert_eq!(after.get(&arch), Some(bytes), "{arch} not archived");
            }
        }
        assert!(!after.keys().any(|k| k.starts_with("wl/issues/issues.db/generations/")));

        // Independent re-verify of the store.
        let again = verify(&chain, &tempdir("post-verify")).await.unwrap();
        assert!(matches!(again, VerifyOutcome::Valid { .. }), "{again:?}");
        // The source volume is untouched.
        assert!(src.join("issues.db").exists());
    }

    #[tokio::test]
    async fn refuses_when_an_owner_holds_the_claim_unless_named() {
        let store = spliced_store().await;
        let workload = BackupTarget {
            store: store.clone(),
            prefix: "wl/issues".into(),
        };
        claim::acquire(&workload, "us-south-001").await.unwrap();
        let src = tempdir("claim-src");
        make_source_db(&src, "issues.db").await;
        let subjects = vec!["issues.db".to_string()];
        let before = all_keys(&store).await;

        let scratch = tempdir("claim-scratch").join("s");
        let out = reseed(req(&store, &subjects, &src, &scratch, None, true)).await.unwrap();
        let ReseedOutcome::Refused(r) = out else { panic!("{out:?}") };
        assert_eq!(r.kind(), "owner_holds_claim");
        assert!(r.headline().contains("us-south-001") && r.headline().contains("--fence-owner"));

        let out = reseed(req(&store, &subjects, &src, &scratch, Some("us-west-001"), true))
            .await
            .unwrap();
        assert!(matches!(out, ReseedOutcome::Refused(ReseedRefusal::FenceOwnerMismatch { .. })));
        assert_eq!(all_keys(&store).await, before, "a refusal writes nothing");

        // Named correctly: fences (epoch + 1) and proceeds.
        let out = reseed(req(&store, &subjects, &src, &scratch, Some("us-south-001"), true))
            .await
            .unwrap();
        let ReseedOutcome::Applied { fenced_at, .. } = out else { panic!("{out:?}") };
        assert_eq!(fenced_at, Some(2));
        assert!(claim::assert_holds(&workload, 1).await.unwrap().is_err());
    }
}
