//! `turso-backup-hydrate` — one-shot CLI that drives [`hydrate`] from
//! environment variables and prints the verdict as a single JSON line.
//!
//! R850-F1. Sibling of `turso-backup-snapshot`, and the **process seam** that
//! answers R850-F1's dep-direction question.
//!
//! # Why a process, not a kamaji dependency
//!
//! Hydrate-on-place has to happen before kamaji starts a container, so the
//! obvious wiring is a `turso-backup` dependency in `kamaji-bin`. That is the
//! wrong direction, for the reason `yubaba/crates/tenant-streamer`'s module doc
//! already gives about the *other* half of this system: W253 tenet 1 separates
//! the control plane from the data plane, and kamaji is the control plane on a
//! node. Linking this in would put `turso` + `turso_core` — a database engine —
//! into the supervisor that runs every workload on every box, coupling their
//! failure domains, their memory profiles and their build times, so that a
//! turso bump rebuilds and re-tests the process supervisor.
//!
//! yubaba already manages WAL-shipping as a sidecar rather than in-process
//! (`litestream.rs`, then `tenant-streamer`). This is the same shape for the
//! restore side: kamaji execs this, reads one JSON line and an exit code, and
//! decides whether to start the container.
//!
//! # Environment
//!
//! Required, all of them — every one names something whose default would be a
//! guess about the only copy of somebody's data:
//!
//! - `VOLUME_ROOT` — host directory the named volume binds from, e.g.
//!   `/var/lib/yah/kamaji/volumes/<name>`.
//! - `SUBJECTS` — comma-separated volume-relative database paths, exactly
//!   `yah.durability.subjects` from the workload spec.
//! - `TIER` — `snapshot` | `dedup` | `stream`, exactly `yah.durability.tier`.
//! - `OWNER` — label recorded in the ownership claim; a node id or hostname.
//! - `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `BACKUP_PREFIX` — the
//!   store. `BACKUP_PREFIX` has no default here (unlike the snapshot bin's
//!   `snap`) for the same reason the declaration refuses a default store: a
//!   hydrate against the wrong prefix restores the wrong database.
//!
//! Optional: `S3_ENDPOINT` (default `http://minio:9000`), `S3_REGION`
//! (default `us-east-1`; `auto` for R2).
//!
//! # Exit codes — the contract with the supervisor
//!
//! | exit | meaning | supervisor |
//! |------|---------|------------|
//! | 0 | `hydrated` \| `already_populated` \| `nothing_in_the_store` | start the workload |
//! | 2 | `refused` — a verdict was reached and it is no | **do not start**; do not retry blindly |
//! | 1 | `error` — no verdict (store unreachable, volume unreadable) | **do not start**; retry is meaningful |
//!
//! 2 and 1 are separate because they want different handling: a torn volume or
//! a lost fence will still be true on the next attempt and needs an operator,
//! while an unreachable store may not be. Both mean "do not start" — an
//! unreachable store is indistinguishable from the partition the fence exists
//! for, which is exactly when starting is worst.
//!
//! # `verify` — check a chain restores, without the claim (R977-T17)
//!
//! `turso-backup-hydrate verify` restores the chain into `SCRATCH_DIR` (must
//! not already hold the subjects) and runs `PRAGMA integrity_check` on each,
//! reading and writing **no** ownership claim or fence, so it is safe on any
//! node while the owner streams. Same store env as hydrate; `VOLUME_ROOT` and
//! `OWNER` are not read. Exit 0 = `valid`, 2 = `corrupt` | `torn` |
//! `nothing_in_the_store`, 1 = `error`.
//!
//! # `reseed` — recover a spliced chain (R977-T18)
//!
//! `turso-backup-hydrate reseed --source DIR --scratch DIR [--label YYYYMMDD]
//! [--fence-owner OWNER] [--apply]` — see `turso_backup::reseed`. Dry-run by
//! default: prints the plan and writes nothing. Env: `SUBJECTS`, `TIER`,
//! `BACKUP_PREFIX`, `S3_BUCKET`, `S3_ENDPOINT`, `S3_REGION`; `OWNER` only with
//! `--fence-owner`. Credentials are the node's own, the same pair kamaji hands
//! the tail (R936-B13): `S3_ACCESS_KEY__<BUCKET>`/`S3_SECRET_KEY__<BUCKET>` from
//! the environment, else read in place from
//! `<cloud-conf-dir>/durability-cred-<bucket>.env`. A bare `S3_ACCESS_KEY` is not
//! read, and there is no flag for another cred file. Exit 0 = `planned` |
//! `applied` (verified), 2 = `refused`, 1 = `error` (including a failed verify).
//!
//! [`hydrate`]: turso_backup::hydrate::hydrate

use anyhow::{bail, Context, Result};
use object_store::aws::AmazonS3Builder;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use turso_backup::hydrate::{
    hydrate, verify, Chain, HydrateOutcome, HydrateRequest, Tier, VerifyOutcome,
};
use turso_backup::reseed::{reseed, ReseedOutcome, ReseedPlan, ReseedRequest};

/// Exit code for a reached verdict of "do not start". See the module doc.
const EXIT_REFUSED: u8 = 2;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match env::args().nth(1).as_deref() {
        None => {}
        Some("verify") => return verify_main().await,
        Some("reseed") => return reseed_main().await,
        Some(other) => {
            eprintln!("unknown verb {other:?}; usage: turso-backup-hydrate [verify|reseed]");
            return ExitCode::FAILURE;
        }
    }
    match run().await {
        Ok(outcome) => {
            println!("{}", outcome_to_json(&outcome));
            match outcome {
                HydrateOutcome::Refused(_) => ExitCode::from(EXIT_REFUSED),
                _ => ExitCode::SUCCESS,
            }
        }
        Err(err) => {
            println!(
                "{{\"outcome\":\"error\",\"message\":{}}}",
                json_string(&format!("{err:#}"))
            );
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<HydrateOutcome> {
    let volume_root = PathBuf::from(required("VOLUME_ROOT")?);
    let subjects = parse_subjects(&required("SUBJECTS")?)?;
    let tier = parse_tier(&required("TIER")?)?;
    let owner = required("OWNER")?;
    let (store, prefix) = store_from_env()?;

    hydrate(HydrateRequest {
        store,
        store_prefix: &prefix,
        volume_root: &volume_root,
        subjects: &subjects,
        tier,
        owner: &owner,
    })
    .await
    .with_context(|| format!("hydrating {} from {prefix}", volume_root.display()))
}

async fn verify_main() -> ExitCode {
    match run_verify().await {
        Ok(outcome) => {
            println!("{}", verify_to_json(&outcome));
            match outcome {
                VerifyOutcome::Valid { .. } => ExitCode::SUCCESS,
                _ => ExitCode::from(EXIT_REFUSED),
            }
        }
        Err(err) => {
            println!(
                "{{\"outcome\":\"error\",\"message\":{}}}",
                json_string(&format!("{err:#}"))
            );
            ExitCode::FAILURE
        }
    }
}

async fn run_verify() -> Result<VerifyOutcome> {
    let scratch = PathBuf::from(required("SCRATCH_DIR")?);
    let subjects = parse_subjects(&required("SUBJECTS")?)?;
    let tier = parse_tier(&required("TIER")?)?;
    let (store, prefix) = store_from_env()?;
    std::fs::create_dir_all(&scratch)
        .with_context(|| format!("creating scratch dir {}", scratch.display()))?;
    let chain = Chain {
        store,
        store_prefix: &prefix,
        subjects: &subjects,
        tier,
    };
    verify(&chain, &scratch)
        .await
        .with_context(|| format!("verifying {prefix} into {}", scratch.display()))
}

async fn reseed_main() -> ExitCode {
    match run_reseed().await {
        Ok(outcome) => {
            println!("{}", reseed_to_json(&outcome));
            match outcome {
                ReseedOutcome::Refused(_) => ExitCode::from(EXIT_REFUSED),
                _ => ExitCode::SUCCESS,
            }
        }
        Err(err) => {
            println!(
                "{{\"outcome\":\"error\",\"message\":{}}}",
                json_string(&format!("{err:#}"))
            );
            ExitCode::FAILURE
        }
    }
}

struct ReseedArgs {
    source: PathBuf,
    scratch: PathBuf,
    label: Option<String>,
    fence_owner: Option<String>,
    apply: bool,
}

fn parse_reseed_args(args: &[String]) -> Result<ReseedArgs> {
    let (mut source, mut scratch, mut label, mut fence_owner, mut apply) =
        (None, None, None, None, false);
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut val = || it.next().cloned().with_context(|| format!("{a} needs a value"));
        match a.as_str() {
            "--source" => source = Some(PathBuf::from(val()?)),
            "--scratch" => scratch = Some(PathBuf::from(val()?)),
            "--label" => label = Some(val()?),
            "--fence-owner" => fence_owner = Some(val()?),
            "--apply" => apply = true,
            other => bail!("unknown reseed argument {other:?}"),
        }
    }
    Ok(ReseedArgs {
        source: source.context("--source <volume dir> is required")?,
        scratch: scratch.context("--scratch <empty dir> is required")?,
        label,
        fence_owner,
        apply,
    })
}

/// The bucket-scoped env-var suffix kamaji uses (R936-B13 `bucket_env_suffix`).
fn bucket_env_suffix(bucket: &str) -> String {
    bucket
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_uppercase() } else { '_' })
        .collect()
}

/// The node's own credential for `bucket`: scoped env first, else the node's
/// cred file read in place. Never a bare pair, never a caller-named file.
fn node_credentials(bucket: &str) -> Result<(String, String)> {
    let sfx = bucket_env_suffix(bucket);
    let (ak, sk) = (format!("S3_ACCESS_KEY__{sfx}"), format!("S3_SECRET_KEY__{sfx}"));
    if let (Ok(a), Ok(s)) = (env::var(&ak), env::var(&sk)) {
        return Ok((a, s));
    }
    let file = format!("/etc/yah-cloud/durability-cred-{bucket}.env");
    let text = std::fs::read_to_string(&file).with_context(|| {
        format!(
            "no {ak}/{sk} in the environment and {file} is not readable \
             (run as root on a node that holds the bucket's cred)"
        )
    })?;
    let (a, s) = parse_cred_env(&text, &ak, &sk);
    Ok((
        a.with_context(|| format!("{file} has no {ak}"))?,
        s.with_context(|| format!("{file} has no {sk}"))?,
    ))
}

/// `KEY=value` lines, systemd EnvironmentFile shape; optional quotes stripped.
fn parse_cred_env(text: &str, ak: &str, sk: &str) -> (Option<String>, Option<String>) {
    let (mut a, mut s) = (None, None);
    for line in text.lines() {
        let Some((k, v)) = line.trim().split_once('=') else {
            continue;
        };
        let v = v.trim().trim_matches('"').trim_matches('\'').to_string();
        match k.trim() {
            k if k == ak => a = Some(v),
            k if k == sk => s = Some(v),
            _ => {}
        }
    }
    (a, s)
}

/// Today's UTC date as `YYYYMMDD`, the default label.
fn today_label() -> String {
    let days = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        / 86_400) as i64;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}{m:02}{d:02}")
}

async fn run_reseed() -> Result<ReseedOutcome> {
    let args: Vec<String> = env::args().skip(2).collect();
    let a = parse_reseed_args(&args)?;
    let subjects = parse_subjects(&required("SUBJECTS")?)?;
    let tier = parse_tier(&required("TIER")?)?;
    let bucket = required("S3_BUCKET")?;
    let prefix = required("BACKUP_PREFIX")?;
    let owner = match a.fence_owner {
        Some(_) => required("OWNER")?,
        None => String::new(),
    };
    let (access, secret) = node_credentials(&bucket)?;
    let store = build_store(&bucket, &access, &secret)?;
    let label = a.label.unwrap_or_else(today_label);
    reseed(ReseedRequest {
        chain: Chain {
            store,
            store_prefix: &prefix,
            subjects: &subjects,
            tier,
        },
        source_root: &a.source,
        scratch_root: &a.scratch,
        label: &label,
        owner: &owner,
        fence_owner: a.fence_owner.as_deref(),
        apply: a.apply,
    })
    .await
    .with_context(|| format!("reseeding {prefix} from {}", a.source.display()))
}

fn reseed_plan_json(p: &ReseedPlan) -> String {
    let list = |v: &[String]| v.iter().map(|s| json_string(s)).collect::<Vec<_>>().join(",");
    format!(
        "\"claim\":{},\"fence\":{},\"archive_prefix\":{},\"archive\":[{}],\"subjects\":[{}]",
        p.claim.as_ref().map_or("null".to_string(), |c| format!(
            "{{\"owner\":{},\"epoch\":{}}}",
            json_string(&c.owner),
            c.epoch
        )),
        p.fence,
        json_string(&p.archive_prefix),
        list(&p.archive),
        p.subjects
            .iter()
            .map(|s| format!(
                "{{\"subject\":{},\"source_bytes\":{},\"supersede\":[{}]}}",
                json_string(&s.subject),
                s.source_bytes,
                list(&s.supersede)
            ))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn reseed_to_json(outcome: &ReseedOutcome) -> String {
    match outcome {
        ReseedOutcome::Planned(p) => {
            format!("{{\"outcome\":\"planned\",{}}}", reseed_plan_json(p))
        }
        ReseedOutcome::Applied {
            plan,
            fenced_at,
            snapshots,
            verify,
        } => format!(
            "{{\"outcome\":\"applied\",{},\"fenced_at\":{},\"snapshots\":[{}],\"verify\":{}}}",
            reseed_plan_json(plan),
            fenced_at.map_or("null".to_string(), |e| e.to_string()),
            snapshots
                .iter()
                .map(|s| json_string(s))
                .collect::<Vec<_>>()
                .join(","),
            verify_to_json(verify)
        ),
        ReseedOutcome::Refused(r) => format!(
            "{{\"outcome\":\"refused\",\"reason\":{},\"message\":{}}}",
            json_string(r.kind()),
            json_string(&r.headline())
        ),
    }
}

fn verify_to_json(outcome: &VerifyOutcome) -> String {
    let list = |v: &[String]| v.iter().map(|s| json_string(s)).collect::<Vec<_>>().join(",");
    match outcome {
        VerifyOutcome::Valid { subjects, seconds } => format!(
            "{{\"outcome\":\"valid\",\"seconds\":{seconds:.3},\"restored\":[{}]}}",
            subjects
                .iter()
                .map(|s| format!(
                    "{{\"subject\":{},\"source\":{},\"bytes\":{},\"frames_replayed\":{}}}",
                    json_string(&s.subject),
                    json_string(&s.source),
                    s.bytes,
                    s.frames_replayed.map_or("null".to_string(), |n| n.to_string())
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        VerifyOutcome::Corrupt { subject, problems } => format!(
            "{{\"outcome\":\"corrupt\",\"subject\":{},\"problems\":[{}]}}",
            json_string(subject),
            list(problems)
        ),
        VerifyOutcome::Torn { present, absent } => format!(
            "{{\"outcome\":\"torn\",\"present\":[{}],\"absent\":[{}]}}",
            list(present),
            list(absent)
        ),
        VerifyOutcome::NothingInTheStore => "{\"outcome\":\"nothing_in_the_store\"}".to_string(),
    }
}

fn required(key: &str) -> Result<String> {
    let v = env::var(key).with_context(|| format!("{key} is required"))?;
    if v.trim().is_empty() {
        bail!("{key} is required and must not be empty");
    }
    Ok(v)
}

/// Split `SUBJECTS` the same way `workload_spec` splits
/// `yah.durability.subjects`, and refuse the same shapes.
///
/// Deliberately duplicated rather than imported: this crate takes no dependency
/// on yah types (see `hydrate::Tier`), and the validation that matters —
/// traversal — is re-applied inside `inspect_volume` regardless, so the copy
/// here is a better error message, not the guard.
fn parse_subjects(raw: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let s = part.trim();
        if s.is_empty() {
            bail!("SUBJECTS has an empty entry (a stray or trailing comma): {raw:?}");
        }
        if out.contains(&s.to_string()) {
            bail!("SUBJECTS names {s:?} twice");
        }
        out.push(s.to_string());
    }
    Ok(out)
}

fn parse_tier(raw: &str) -> Result<Tier> {
    match raw.trim() {
        "snapshot" => Ok(Tier::Snapshot),
        "dedup" => Ok(Tier::Dedup),
        "stream" => Ok(Tier::Stream),
        // `none` is deliberately not accepted: a tier that ships no bytes has
        // nothing to hydrate from, and silently exiting 0 on one would let a
        // misconfigured caller believe a restore happened.
        other => bail!(
            "TIER = {other:?} is not a tier that ships bytes (snapshot|dedup|stream); \
             tier \"none\" has nothing to hydrate from and must not reach this binary"
        ),
    }
}

fn store_from_env() -> Result<(Arc<dyn object_store::ObjectStore>, String)> {
    let bucket = required("S3_BUCKET")?;
    let access = required("S3_ACCESS_KEY")?;
    let secret = required("S3_SECRET_KEY")?;
    let prefix = required("BACKUP_PREFIX")?;
    Ok((build_store(&bucket, &access, &secret)?, prefix))
}

fn build_store(
    bucket: &str,
    access: &str,
    secret: &str,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://minio:9000".to_string());
    let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let store = turso_backup::bounded_s3(AmazonS3Builder::new())
        .with_endpoint(&endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access)
        .with_secret_access_key(secret)
        .with_region(&region)
        // The claim's compare-and-swap rests on these headers being emitted.
        // Setting this is necessary and not sufficient — `hydrate` probes the
        // live sink and refuses when the backend ignores them.
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .with_context(|| format!("building S3-shaped store for {endpoint} bucket={bucket}"))?;
    Ok(Arc::new(store))
}

fn outcome_to_json(outcome: &HydrateOutcome) -> String {
    match outcome {
        HydrateOutcome::Hydrated {
            epoch,
            subjects,
            seconds,
            displaced,
        } => {
            let bytes: u64 = subjects.iter().map(|s| s.bytes).sum();
            let displaced = match displaced {
                Some(d) => format!(
                    "{{\"owner\":{},\"epoch\":{},\"suffix\":{}}}",
                    json_string(&d.owner),
                    d.epoch,
                    json_string(&d.suffix)
                ),
                None => "null".to_string(),
            };
            format!(
                "{{\"outcome\":\"hydrated\",\"epoch\":{epoch},\"subjects\":{},\"bytes\":{bytes},\
                 \"seconds\":{seconds:.3},\"displaced\":{displaced},\"restored\":[{}]}}",
                subjects.len(),
                subjects
                    .iter()
                    .map(|s| format!(
                        "{{\"subject\":{},\"source\":{},\"bytes\":{},\"seconds\":{:.3}}}",
                        json_string(&s.subject),
                        json_string(&s.source),
                        s.bytes,
                        s.seconds
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        HydrateOutcome::AlreadyPopulated => "{\"outcome\":\"already_populated\"}".to_string(),
        HydrateOutcome::NothingInTheStore { epoch, subjects } => format!(
            "{{\"outcome\":\"nothing_in_the_store\",\"epoch\":{epoch},\"subjects\":[{}]}}",
            subjects
                .iter()
                .map(|s| json_string(s))
                .collect::<Vec<_>>()
                .join(",")
        ),
        HydrateOutcome::Refused(refusal) => format!(
            "{{\"outcome\":\"refused\",\"reason\":{},\"message\":{}}}",
            json_string(refusal_kind(refusal)),
            json_string(&refusal.headline())
        ),
    }
}

/// A stable machine-readable tag per refusal, so a supervisor can branch
/// without parsing the prose headline.
fn refusal_kind(refusal: &turso_backup::hydrate::HydrateRefusal) -> &'static str {
    use turso_backup::hydrate::HydrateRefusal as R;
    match refusal {
        R::TornVolume { .. } => "torn_volume",
        R::ClaimLost { .. } => "claim_lost",
        R::FencedMidRestore { .. } => "fenced_mid_restore",
        R::SinkNotFenced { .. } => "sink_not_fenced",
        R::StaleVolumeNothingToRestore { .. } => "stale_volume_nothing_to_restore",
        R::ChainUnrestorable { .. } => "chain_unrestorable",
    }
}

/// JSON-string-encode `s`. Same helper as the snapshot bin's, and duplicated
/// for the same reason it is private there: two 20-line binaries do not justify
/// a shared module, and a serde dependency for four escapes does not either.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_claim::claim::{ClaimLost, ClaimRecord};
    use turso_backup::hydrate::{HydrateRefusal, SubjectRestore};

    #[test]
    fn reseed_credentials_use_kamajis_bucket_scoped_names() {
        assert_eq!(bucket_env_suffix("noisetable-account-backup"), "NOISETABLE_ACCOUNT_BACKUP");
        let (a, s) = parse_cred_env(
            "# c\nS3_ACCESS_KEY__B=ak\nS3_SECRET_KEY__B=\"sk\"\nS3_ACCESS_KEY=bare\n",
            "S3_ACCESS_KEY__B",
            "S3_SECRET_KEY__B",
        );
        assert_eq!((a.as_deref(), s.as_deref()), (Some("ak"), Some("sk")));
    }

    #[test]
    fn reseed_is_dry_run_unless_apply_is_given() {
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let a = parse_reseed_args(&args(&["--source", "/v", "--scratch", "/s"])).unwrap();
        assert!(!a.apply && a.fence_owner.is_none());
        let a = parse_reseed_args(&args(&["--source", "/v", "--scratch", "/s", "--apply"])).unwrap();
        assert!(a.apply);
        assert!(parse_reseed_args(&args(&["--scratch", "/s"])).is_err());
        assert_eq!(today_label().len(), 8);
    }

    #[test]
    fn subjects_split_and_trim() {
        assert_eq!(
            parse_subjects("accounts.db, passkeys.db ,sessions.db").unwrap(),
            vec!["accounts.db", "passkeys.db", "sessions.db"]
        );
        assert!(parse_subjects("a.db,").is_err());
        assert!(parse_subjects("a.db,a.db").is_err());
    }

    /// `none` must not reach this binary. Accepting it and exiting 0 would tell
    /// a supervisor a restore happened when nothing was even looked for.
    #[test]
    fn tier_none_is_refused_rather_than_treated_as_a_no_op() {
        assert_eq!(parse_tier("stream").unwrap(), Tier::Stream);
        let err = parse_tier("none").unwrap_err();
        assert!(format!("{err:#}").contains("nothing to hydrate from"), "{err:#}");
    }

    #[test]
    fn a_hydrated_outcome_reports_measured_bytes_and_seconds() {
        let line = outcome_to_json(&HydrateOutcome::Hydrated {
            epoch: 4,
            seconds: 12.5,
            displaced: None,
            subjects: vec![SubjectRestore {
                subject: "accounts.db".into(),
                source: "wl/acct/accounts.db/snapshots/snapshot-000.db".into(),
                bytes: 1024,
                seconds: 12.25,
                frames_replayed: Some(17),
            }],
        });
        assert!(line.contains("\"outcome\":\"hydrated\""), "{line}");
        assert!(line.contains("\"epoch\":4"), "{line}");
        assert!(line.contains("\"bytes\":1024"), "{line}");
        // The measured figure is the point: this is what a recovery estimate
        // built from a declared `state-mb` and a constant should be replaced by.
        assert!(line.contains("\"seconds\":12.500"), "{line}");

        // R850-T2: yubaba's `cloud::recovery_journal` parses THIS line to turn
        // the extrapolated `RecoveryEstimate::Hydrate` into a measured one, and
        // `cloud` deliberately takes no dependency on this crate. The coupling
        // is therefore pinned from both sides by this literal, which is
        // byte-identical to `REAL_HYDRATE_LINE` in
        // `oss/yubaba/crates/cloud/src/recovery_journal.rs`. Changing this
        // format string without changing that constant fails here first.
        assert_eq!(
            line,
            concat!(
                r#"{"outcome":"hydrated","epoch":4,"subjects":1,"bytes":1024,"seconds":12.500,"#,
                r#""displaced":null,"restored":[{"subject":"accounts.db","#,
                r#""source":"wl/acct/accounts.db/snapshots/snapshot-000.db","#,
                r#""bytes":1024,"seconds":12.250}]}"#,
            )
        );
    }

    /// A supervisor branches on `reason`, not on the prose — so every refusal
    /// carries a stable tag and the exit code is the same for all of them.
    #[test]
    fn every_refusal_carries_a_machine_readable_reason() {
        let cases = [
            (
                HydrateRefusal::TornVolume {
                    populated: vec!["a.db".into()],
                    absent: vec!["b.db".into()],
                },
                "torn_volume",
            ),
            (
                HydrateRefusal::ClaimLost {
                    current: ClaimRecord {
                        epoch: 2,
                        owner: "us-east-001".into(),
                        claimed_at_nanos: 0,
                    },
                },
                "claim_lost",
            ),
            (
                HydrateRefusal::FencedMidRestore {
                    lost: ClaimLost::Vanished { ours: 2 },
                    restored: vec![],
                },
                "fenced_mid_restore",
            ),
            (
                HydrateRefusal::SinkNotFenced {
                    stage: store_claim::PreflightStage::Update,
                },
                "sink_not_fenced",
            ),
            (
                HydrateRefusal::ChainUnrestorable {
                    reason: "a.db fails integrity_check".into(),
                },
                "chain_unrestorable",
            ),
        ];
        for (refusal, tag) in cases {
            assert_eq!(refusal_kind(&refusal), tag);
            let line = outcome_to_json(&HydrateOutcome::Refused(refusal));
            assert!(line.contains(&format!("\"reason\":\"{tag}\"")), "{line}");
        }
    }

    #[test]
    fn already_populated_and_nothing_in_the_store_are_distinguishable() {
        assert_eq!(
            outcome_to_json(&HydrateOutcome::AlreadyPopulated),
            "{\"outcome\":\"already_populated\"}"
        );
        let line = outcome_to_json(&HydrateOutcome::NothingInTheStore {
            epoch: 1,
            subjects: vec!["accounts.db".into()],
        });
        assert!(line.contains("\"outcome\":\"nothing_in_the_store\""), "{line}");
        assert!(line.contains("\"accounts.db\""), "{line}");
    }
}
