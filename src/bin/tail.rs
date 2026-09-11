//! `turso-backup-tail` — the long-running sibling of `turso-backup-hydrate`:
//! keeps a placed workload's declared databases shipped to the object store,
//! printing one JSON line per round.
//!
//! R850-F1. Same process-seam argument the hydrate bin makes at length, and it
//! applies harder here: this one links `turso` + `turso_core` *and* keeps them
//! resident for the workload's whole life. Putting that inside the node's
//! process supervisor would give every workload on the box a database engine's
//! memory profile and failure domain. kamaji spawns this, reads its lines, and
//! acts on its exit code.
//!
//! # Environment
//!
//! Identical to `turso-backup-hydrate` — deliberately, because both are driven
//! from the same `yah.durability.*` declaration and a divergence between them is
//! a backup written where no restore will look:
//!
//! - `VOLUME_ROOT`, `SUBJECTS`, `TIER`, `OWNER`
//! - `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `BACKUP_PREFIX`
//! - optional `S3_ENDPOINT` (default `http://minio:9000`), `S3_REGION`
//!   (default `us-east-1`; `auto` for R2)
//!
//! Tail-only, all optional:
//!
//! - `PAGE_SIZE` (default 4096) — the source databases' page size.
//! - `TAIL_INTERVAL_SECS` (default 30) — seconds between rounds.
//! - `RPO_SECS` — the recovery-point objective the interval promises, carried
//!   into the watermark so a missed round reads as a breach rather than as
//!   silence. Defaults to `4 * TAIL_INTERVAL_SECS`, which is a scheduler-slack
//!   allowance and not a policy; declare `yah.durability.rpo-seconds` to mean
//!   something by it.
//! - `ROUNDS` — stop after N rounds instead of running forever. For tests and
//!   one-shot operator runs; `0` (the default) means forever.
//!
//! # Exit codes — the contract with the supervisor
//!
//! | exit | meaning | supervisor |
//! |------|---------|------------|
//! | 0 | `ROUNDS` was reached | nothing; this is only ever an operator run |
//! | 2 | `fenced` — **another node owns this workload's state** | **stop the workload** |
//! | 1 | `error` — no verdict (store unreachable, source unreadable) | restart the tail; the workload may keep running |
//!
//! **There is no graceful-shutdown path and no signal handler**, which is a
//! decision rather than an omission: every round is already crash-atomic. A tail
//! killed mid-upload leaves frames under keys no manifest references — invisible
//! to restore, exactly like `StreamOutcome::Shed` — and the next round re-derives
//! everything from the sidecar. A handler could only make that slower.
//!
//! Exit 2 is the one that matters and it is not advisory. A fenced node cannot
//! ship a byte, so every write its application accepts afterwards is a write
//! nobody can ever recover. That is the supervisor half of at-most-one-live, and
//! it is why this exits rather than logging and carrying on.

use anyhow::{bail, Context, Result};
use object_store::aws::AmazonS3Builder;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use turso_backup::hydrate::Tier;
use turso_backup::tail::{round, start, RoundOutcome, SubjectOutcome, SubjectReport, TailRequest};

/// Exit code for "another node owns this state" — see the module doc.
const EXIT_FENCED: u8 = 2;

const DEFAULT_INTERVAL_SECS: u64 = 30;
const DEFAULT_PAGE_SIZE: usize = 4096;

/// Multiplier turning the tail interval into a default RPO target. Four rounds
/// of slack: one missed round is a scheduler hiccup, four in a row is a stopped
/// tail, and only the second should light up.
const DEFAULT_RPO_ROUNDS: u64 = 4;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(Verdict::RoundsExhausted) => ExitCode::SUCCESS,
        Ok(Verdict::Fenced) => ExitCode::from(EXIT_FENCED),
        Err(err) => {
            println!(
                "{{\"outcome\":\"error\",\"message\":{}}}",
                json_string(&format!("{err:#}"))
            );
            ExitCode::FAILURE
        }
    }
}

/// The only two ways this returns without an error.
enum Verdict {
    Fenced,
    RoundsExhausted,
}

async fn run() -> Result<Verdict> {
    let volume_root = PathBuf::from(required("VOLUME_ROOT")?);
    let subjects = parse_subjects(&required("SUBJECTS")?)?;
    let tier = parse_tier(&required("TIER")?)?;
    let owner = required("OWNER")?;
    let (store, prefix) = store_from_env()?;
    let page_size = parse_num("PAGE_SIZE", DEFAULT_PAGE_SIZE as u64)? as usize;
    let interval = Duration::from_secs(parse_num("TAIL_INTERVAL_SECS", DEFAULT_INTERVAL_SECS)?);
    let rpo = Duration::from_secs(parse_num(
        "RPO_SECS",
        interval.as_secs().saturating_mul(DEFAULT_RPO_ROUNDS),
    )?);
    let rounds = parse_num("ROUNDS", 0)?;

    let mut session = match start(TailRequest {
        store,
        store_prefix: &prefix,
        volume_root: &volume_root,
        subjects: &subjects,
        tier,
        owner: &owner,
        page_size,
        rpo_target: Some(rpo),
    })
    .await
    .with_context(|| format!("starting a tail of {} into {prefix}", volume_root.display()))?
    {
        Ok(session) => session,
        // A refusal at start is a reached verdict of "do not stream", but it is
        // NOT the fence: nothing here says another node owns the workload, so
        // the supervisor should not stop it on this. It is an error the operator
        // has to fix — a bucket that cannot fence, or a genuinely concurrent
        // start — and the exit code says so.
        Err(refusal) => bail!("{}", refusal.headline()),
    };
    println!(
        "{{\"outcome\":\"started\",\"epoch\":{},\"tier\":{},\"subjects\":{},\"displaced\":{}}}",
        session.epoch(),
        json_string(tier.as_str()),
        subjects.len(),
        match session.displaced() {
            Some(c) => format!(
                "{{\"epoch\":{},\"owner\":{}}}",
                c.epoch,
                json_string(&c.owner)
            ),
            None => "null".to_string(),
        }
    );

    let mut done = 0u64;
    loop {
        match round(&mut session).await? {
            RoundOutcome::Backed(reports) => println!("{}", round_json(&reports)),
            RoundOutcome::Fenced { detail } => {
                println!(
                    "{{\"outcome\":\"fenced\",\"message\":{}}}",
                    json_string(&detail)
                );
                return Ok(Verdict::Fenced);
            }
        }
        done += 1;
        if rounds != 0 && done >= rounds {
            return Ok(Verdict::RoundsExhausted);
        }
        tokio::time::sleep(interval).await;
    }
}

fn round_json(reports: &[SubjectReport]) -> String {
    let subjects: Vec<String> = reports
        .iter()
        .map(|r| {
            format!(
                "{{\"subject\":{},\"seconds\":{:.3},{}}}",
                json_string(&r.subject),
                r.seconds,
                subject_body(&r.outcome)
            )
        })
        .collect();
    format!(
        "{{\"outcome\":\"round\",\"subjects\":[{}]}}",
        subjects.join(",")
    )
}

/// The per-subject body. `state` is the field a consumer switches on, and it is
/// deliberately flat rather than a nested tagged union: the reader is a log
/// pipeline or a supervisor, not a deserializer with the crate's types.
fn subject_body(outcome: &SubjectOutcome) -> String {
    use turso_backup::dedup::DedupOutcome;
    use turso_backup::snapshot::SnapshotOutcome;
    use turso_backup::stream::StreamOutcome;

    match outcome {
        SubjectOutcome::Absent => "\"state\":\"absent\"".to_string(),
        SubjectOutcome::SourceTooHot { detail } => format!(
            "\"state\":\"source_too_hot\",\"message\":{}",
            json_string(detail)
        ),
        SubjectOutcome::Snapshot(SnapshotOutcome::Uploaded { key, bytes, .. }) => format!(
            "\"state\":\"uploaded\",\"key\":{},\"bytes\":{bytes}",
            json_string(key)
        ),
        SubjectOutcome::Snapshot(SnapshotOutcome::Deduplicated { .. })
        | SubjectOutcome::Snapshot(SnapshotOutcome::Unchanged { .. }) => {
            "\"state\":\"unchanged\"".to_string()
        }
        SubjectOutcome::Dedup(DedupOutcome::Snapshotted {
            manifest_key,
            total_pages,
            uploaded_pages,
            ..
        }) => format!(
            "\"state\":\"uploaded\",\"key\":{},\"pages\":{total_pages},\"uploaded_pages\":{uploaded_pages}",
            json_string(manifest_key)
        ),
        SubjectOutcome::Dedup(DedupOutcome::Unchanged { .. }) => {
            "\"state\":\"unchanged\"".to_string()
        }
        SubjectOutcome::Stream {
            base_snapshot_key,
            base_published,
            outcome,
            ..
        } => {
            let base = format!(
                "\"base\":{},\"base_published\":{base_published}",
                json_string(base_snapshot_key)
            );
            match outcome {
                StreamOutcome::Empty { watermark, .. } => format!(
                    "\"state\":\"current\",{base},\"last_frame\":{}",
                    watermark.last_frame
                ),
                StreamOutcome::Streamed { first_frame, last_frame, frame_count, .. } => format!(
                    "\"state\":\"streamed\",{base},\"first_frame\":{first_frame},\
                     \"last_frame\":{last_frame},\"frames\":{frame_count}"
                ),
                StreamOutcome::Restarted { first_frame, last_frame, frame_count, .. } => format!(
                    "\"state\":\"restarted\",{base},\"first_frame\":{first_frame},\
                     \"last_frame\":{last_frame},\"frames\":{frame_count}"
                ),
                StreamOutcome::Shed { first_frame, last_frame, .. } => format!(
                    "\"state\":\"shed\",{base},\"first_frame\":{first_frame},\
                     \"last_frame\":{last_frame}"
                ),
                // Unreachable in practice — `round` converts a fenced subject
                // into `RoundOutcome::Fenced` before it can reach a report — and
                // rendered rather than `unreachable!()` because a panic in a
                // formatter is a worse way to learn that invariant broke.
                StreamOutcome::Fenced { current_epoch, our_epoch, .. } => format!(
                    "\"state\":\"fenced\",{base},\"current_epoch\":{current_epoch},\
                     \"our_epoch\":{our_epoch}"
                ),
            }
        }
    }
}

fn required(key: &str) -> Result<String> {
    let v = env::var(key).with_context(|| format!("{key} is required"))?;
    if v.trim().is_empty() {
        bail!("{key} is required and must not be empty");
    }
    Ok(v)
}

fn parse_num(key: &str, default: u64) -> Result<u64> {
    match env::var(key) {
        Err(_) => Ok(default),
        Ok(v) if v.trim().is_empty() => Ok(default),
        Ok(v) => v
            .trim()
            .parse()
            .with_context(|| format!("{key} = {v:?} is not a non-negative integer")),
    }
}

/// Split `SUBJECTS` the same way `turso-backup-hydrate` does — see that bin's
/// note on why this is duplicated rather than shared with `workload_spec`.
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
        other => bail!(
            "TIER = {other:?} is not a tier that ships bytes (snapshot|dedup|stream); \
             tier \"none\" ships nothing and must not reach this binary"
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
        // Necessary, not sufficient — `tail::start` probes the live sink and
        // refuses when the backend ignores them.
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .with_context(|| format!("building S3-shaped store for {endpoint} bucket={bucket}"))?;
    Ok((Arc::new(store), prefix))
}

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
    use turso_backup::stream::{RpoStatus, Watermark};

    #[test]
    fn subjects_split_and_trim() {
        assert_eq!(
            parse_subjects(" a.db , b/c.db ").unwrap(),
            vec!["a.db".to_string(), "b/c.db".to_string()]
        );
        assert!(parse_subjects("a.db,,b.db").is_err());
        assert!(parse_subjects("a.db,a.db").is_err(), "a duplicate subject is a typo, not a plan");
    }

    /// `none` must not reach this binary: a tier that ships no bytes has nothing
    /// to tail, and accepting it would let a misconfigured caller believe a
    /// backup was running.
    #[test]
    fn tier_none_is_refused() {
        assert!(parse_tier("none").is_err());
        assert!(parse_tier("streem").is_err());
        assert_eq!(parse_tier(" stream ").unwrap(), Tier::Stream);
    }

    #[test]
    fn numeric_env_defaults_and_refuses_junk() {
        assert_eq!(parse_num("TURSO_BACKUP_TAIL_TEST_UNSET_KEY", 7).unwrap(), 7);
        env::set_var("TURSO_BACKUP_TAIL_TEST_JUNK", "soon");
        assert!(parse_num("TURSO_BACKUP_TAIL_TEST_JUNK", 7).is_err());
        env::remove_var("TURSO_BACKUP_TAIL_TEST_JUNK");
    }

    /// The supervisor reads `state`, so each one has to be distinct and stable.
    #[test]
    fn subject_states_are_distinguishable() {
        assert!(subject_body(&SubjectOutcome::Absent).contains("\"state\":\"absent\""));
        assert!(subject_body(&SubjectOutcome::SourceTooHot {
            detail: "moved".into()
        })
        .contains("\"state\":\"source_too_hot\""));
        let current = subject_body(&SubjectOutcome::Stream {
            base_snapshot_key: "p/snapshots/snapshot-1.db".into(),
            base_published: false,
            base_attempts: 0,
            outcome: turso_backup::stream::StreamOutcome::Empty {
                watermark: Watermark { checkpoint_seq: 0, last_frame: 9 },
                rpo: RpoStatus { target: None, watermark_age: None, breached: false },
            },
        });
        assert!(current.contains("\"state\":\"current\""), "{current}");
        assert!(current.contains("\"last_frame\":9"), "{current}");
        assert!(current.contains("\"base_published\":false"), "{current}");
    }

    /// A round's JSON has to survive a subject name with a quote in it, because
    /// the name comes from a declaration and a broken line breaks the pipeline
    /// that reads every OTHER subject too.
    #[test]
    fn round_json_escapes_subject_names() {
        let line = round_json(&[SubjectReport {
            subject: "we\"ird.db".into(),
            outcome: SubjectOutcome::Absent,
            seconds: 0.5,
        }]);
        assert!(line.contains(r#""subject":"we\"ird.db""#), "{line}");
    }
}
