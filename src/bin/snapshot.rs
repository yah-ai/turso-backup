//! `turso-backup-snapshot` — one-shot CLI that drives [`snapshot_and_upload`]
//! from environment variables and prints the outcome as a single JSON line.
//!
//! R006-F2 — the executable primitive an almanac-pattern scheduler points its
//! `command` at. Wraps the library's [`snapshot_and_upload`] in a process
//! boundary so any caller (a system cron, an embedded `tokio::spawn` loop, a
//! `task-runs::TaskDriver` with `Initiator::Cron`, a future
//! `Workload::Almanac` reconciler) can fire snapshots the same way.
//!
//! ## Why a process boundary
//!
//! - **Almanac shape:** `workload_spec::AlmanacManifest.command` is a shell
//!   string; it needs an exec-able to call.
//! - **Task-runs shape:** `task_runs::TaskDriver::spawn_run` records exit
//!   code + stdout chunks per run. A long-lived in-process snapshot loop
//!   would have to invent its own success/failure record; one process per
//!   run gets it for free.
//! - **Isolation:** a panic, OOM, or stuck FD in one snapshot run can't take
//!   down the host service.
//!
//! ## Environment
//!
//! Required:
//! - `DB_PATH` — local turso database file to snapshot.
//! - `S3_BUCKET` — destination bucket.
//! - `S3_ACCESS_KEY` / `S3_SECRET_KEY` — credentials.
//!
//! Optional (defaults match `harness::backup_target` so the same env shape
//! works in the docker harness and a production almanac job):
//! - `S3_ENDPOINT` (default `http://minio:9000` — set this for R2/AWS)
//! - `S3_REGION` (default `us-east-1` — `auto` for R2)
//! - `BACKUP_PREFIX` (default `snap`)
//!
//! ## Output
//!
//! Exactly one line of JSON on stdout, plus an exit code:
//!
//! | exit | outcome JSON |
//! |------|--------------|
//! | 0    | `{"outcome":"uploaded","key":...,"bytes":...,"snapshot_hash":...}` |
//! | 0    | `{"outcome":"unchanged","source_hash":...}` |
//! | 0    | `{"outcome":"deduplicated","snapshot_hash":...}` |
//! | 1    | `{"outcome":"error","message":...}` |
//!
//! All three success variants exit 0 — a missed-backup signal (F3) MUST come
//! from "no Uploaded/Unchanged/Deduplicated in the last N intervals," not
//! from exit code alone. `Unchanged` and `Deduplicated` are normal idle
//! outcomes for a quiescent or auto-checkpointed DB.

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use turso_backup::snapshot::{snapshot_and_upload, BackupTarget, SnapshotOutcome};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(outcome) => {
            println!("{}", outcome_to_json(&outcome));
            ExitCode::SUCCESS
        }
        Err(err) => {
            // anyhow's Display walks the full cause chain; JSON-escape to keep
            // the line parseable even when the message contains quotes/newlines.
            println!(
                "{{\"outcome\":\"error\",\"message\":{}}}",
                json_string(&format!("{err:#}"))
            );
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<SnapshotOutcome> {
    let db_path = env::var("DB_PATH").context("DB_PATH is required")?;
    let target = backup_target_from_env()?;
    snapshot_and_upload(&db_path, &target)
        .await
        .with_context(|| format!("snapshot_and_upload({db_path})"))
}

fn backup_target_from_env() -> Result<BackupTarget> {
    let endpoint = env_or("S3_ENDPOINT", "http://minio:9000");
    let bucket = env::var("S3_BUCKET").context("S3_BUCKET is required")?;
    let access = env::var("S3_ACCESS_KEY").context("S3_ACCESS_KEY is required")?;
    let secret = env::var("S3_SECRET_KEY").context("S3_SECRET_KEY is required")?;
    let region = env_or("S3_REGION", "us-east-1");
    let prefix = env_or("BACKUP_PREFIX", "snap");

    let store = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(&bucket)
        .with_access_key_id(&access)
        .with_secret_access_key(&secret)
        .with_region(&region)
        // Permissive: HTTPS endpoints stay TLS-signed; this only opts in to
        // plain-HTTP MinIO when the endpoint is http://. R2 and real S3 are
        // https:// and ride TLS regardless.
        .with_allow_http(true)
        // Path-style (endpoint/bucket/key) works against every S3-compatible
        // backend we target (MinIO, R2, AWS path-style). Virtual-hosted style
        // (bucket.endpoint/key) is rejected by some MinIO configs and adds no
        // value here.
        .with_virtual_hosted_style_request(false)
        .build()
        .with_context(|| format!("building S3-shaped store for {endpoint} bucket={bucket}"))?;

    Ok(BackupTarget {
        store: Arc::new(store),
        prefix,
    })
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn outcome_to_json(outcome: &SnapshotOutcome) -> String {
    match outcome {
        SnapshotOutcome::Uploaded {
            key,
            bytes,
            snapshot_hash,
        } => format!(
            "{{\"outcome\":\"uploaded\",\"key\":{},\"bytes\":{bytes},\"snapshot_hash\":{}}}",
            json_string(key),
            json_string(snapshot_hash)
        ),
        SnapshotOutcome::Unchanged { source_hash } => format!(
            "{{\"outcome\":\"unchanged\",\"source_hash\":{}}}",
            json_string(source_hash)
        ),
        SnapshotOutcome::Deduplicated { snapshot_hash } => format!(
            "{{\"outcome\":\"deduplicated\",\"snapshot_hash\":{}}}",
            json_string(snapshot_hash)
        ),
    }
}

/// JSON-string-encode `s`: wrap in quotes, escape `"` `\` and the control
/// characters JSON requires. The values this bin emits are well-formed
/// (hex hashes, ObjectStore paths, anyhow display) so the slow path here is
/// the error message — keep it correct, not fast.
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

    #[test]
    fn uploaded_outcome_serialises() {
        let line = outcome_to_json(&SnapshotOutcome::Uploaded {
            key: "snap/snapshots/snapshot-00000000001700000000000000000.db".into(),
            bytes: 446 * 1024,
            snapshot_hash: "abc123".into(),
        });
        assert!(line.starts_with("{\"outcome\":\"uploaded\""));
        assert!(line.contains("\"bytes\":456704"));
        assert!(line.contains("\"snapshot_hash\":\"abc123\""));
    }

    #[test]
    fn unchanged_outcome_serialises() {
        let line = outcome_to_json(&SnapshotOutcome::Unchanged {
            source_hash: "deadbeef".into(),
        });
        assert_eq!(line, "{\"outcome\":\"unchanged\",\"source_hash\":\"deadbeef\"}");
    }

    #[test]
    fn deduplicated_outcome_serialises() {
        let line = outcome_to_json(&SnapshotOutcome::Deduplicated {
            snapshot_hash: "cafef00d".into(),
        });
        assert_eq!(
            line,
            "{\"outcome\":\"deduplicated\",\"snapshot_hash\":\"cafef00d\"}"
        );
    }

    #[test]
    fn json_string_escapes_problem_characters() {
        // The path of an error message is the only place these tend to show up.
        assert_eq!(json_string("simple"), "\"simple\"");
        assert_eq!(json_string("with\"quote"), "\"with\\\"quote\"");
        assert_eq!(json_string("back\\slash"), "\"back\\\\slash\"");
        assert_eq!(json_string("new\nline"), "\"new\\nline\"");
        assert_eq!(json_string("tab\there"), "\"tab\\there\"");
        // Control character below 0x20 — \u escape.
        assert_eq!(json_string("\x01"), "\"\\u0001\"");
    }
}
