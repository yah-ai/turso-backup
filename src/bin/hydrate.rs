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
//! [`hydrate`]: turso_backup::hydrate::hydrate

use anyhow::{bail, Context, Result};
use object_store::aws::AmazonS3Builder;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use turso_backup::hydrate::{hydrate, HydrateOutcome, HydrateRequest, Tier};

/// Exit code for a reached verdict of "do not start". See the module doc.
const EXIT_REFUSED: u8 = 2;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
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
    let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://minio:9000".to_string());
    let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let bucket = required("S3_BUCKET")?;
    let access = required("S3_ACCESS_KEY")?;
    let secret = required("S3_SECRET_KEY")?;
    let prefix = required("BACKUP_PREFIX")?;

    let store = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(&bucket)
        .with_access_key_id(&access)
        .with_secret_access_key(&secret)
        .with_region(&region)
        // The claim's compare-and-swap rests on these headers being emitted.
        // Setting this is necessary and not sufficient — `hydrate` probes the
        // live sink and refuses when the backend ignores them.
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .with_context(|| format!("building S3-shaped store for {endpoint} bucket={bucket}"))?;
    Ok((Arc::new(store), prefix))
}

fn outcome_to_json(outcome: &HydrateOutcome) -> String {
    match outcome {
        HydrateOutcome::Hydrated {
            epoch,
            subjects,
            seconds,
        } => {
            let bytes: u64 = subjects.iter().map(|s| s.bytes).sum();
            format!(
                "{{\"outcome\":\"hydrated\",\"epoch\":{epoch},\"subjects\":{},\"bytes\":{bytes},\
                 \"seconds\":{seconds:.3},\"restored\":[{}]}}",
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
    use turso_backup::claim::{ClaimLost, ClaimRecord};
    use turso_backup::hydrate::{HydrateRefusal, SubjectRestore};

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
                    stage: turso_backup::stream::PreflightStage::Update,
                },
                "sink_not_fenced",
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
