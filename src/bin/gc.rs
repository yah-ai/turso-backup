//! `turso-backup-gc` — reclaim the tier-2 objects a rebase orphans.
//!
//! R850-T3. [`turso_backup::stream::gc_stream`] is the sweep; this is the
//! process boundary an operator or a scheduler points at a live bucket, for the
//! same three reasons `turso-backup-snapshot` has one (almanac `command`
//! strings are shell strings, `task_runs` records exit code + stdout per run,
//! and a stuck GC must not be able to take a host service with it).
//!
//! ## It is a dry run unless you say otherwise
//!
//! `GC_APPLY` is unset by default, and unset means **report and delete
//! nothing**. That is deliberate for a tool whose entire job is deleting
//! objects out of a bucket somebody's restore depends on: the first run tells
//! you what would go, and you authorize the second.
//!
//! ## Environment
//!
//! Required:
//! - `S3_BUCKET` — bucket holding the sink.
//! - `S3_ACCESS_KEY` / `S3_SECRET_KEY` — credentials.
//!
//! Optional (same defaults as `turso-backup-snapshot`, so one env shape drives
//! the whole crate's bins):
//! - `S3_ENDPOINT` (default `http://minio:9000` — set this for R2/AWS)
//! - `S3_REGION` (default `us-east-1` — `auto` for R2)
//! - `BACKUP_PREFIX` (default `snap`) — the sink prefix to sweep. One subject
//!   per invocation: a tail that ships several databases gives each its own
//!   prefix, so sweeping them means running this once per prefix.
//! - `GC_GRACE_SECS` (default 86400) — never collect an object younger than
//!   this. See `turso_backup::stream::DEFAULT_STREAM_GC_GRACE` for why a day.
//! - `GC_APPLY` (`1`/`true`/`yes` to actually delete; anything else, or unset,
//!   is a dry run)
//!
//! ## Output
//!
//! Exactly one line of JSON on stdout, plus an exit code:
//!
//! | exit | outcome JSON |
//! |------|--------------|
//! | 0    | `{"outcome":"swept","dry_run":...,"collected_snapshots":[...],...}` |
//! | 1    | `{"outcome":"error","message":...}` |
//!
//! A dry run and a real sweep both exit 0 — read `dry_run` in the line, not the
//! exit code, to know whether anything was actually deleted.

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;
use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use turso_backup::snapshot::BackupTarget;
use turso_backup::stream::{gc_stream, StreamGcConfig, StreamGcOutcome};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(outcome) => {
            println!("{}", outcome_to_json(&outcome));
            ExitCode::SUCCESS
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

async fn run() -> Result<StreamGcOutcome> {
    let target = backup_target_from_env()?;
    let cfg = gc_config_from_env()?;
    gc_stream(&target, &cfg)
        .await
        .with_context(|| format!("gc_stream({})", target.prefix))
}

fn gc_config_from_env() -> Result<StreamGcConfig> {
    let grace = match env::var("GC_GRACE_SECS") {
        Ok(raw) => Duration::from_secs(
            raw.trim()
                .parse::<u64>()
                .with_context(|| format!("GC_GRACE_SECS must be a whole number of seconds, got {raw:?}"))?,
        ),
        Err(_) => turso_backup::stream::DEFAULT_STREAM_GC_GRACE,
    };
    Ok(StreamGcConfig {
        grace,
        dry_run: !is_truthy(env::var("GC_APPLY").ok().as_deref()),
    })
}

/// Only an explicit affirmative applies. Every other value — including a typo,
/// an empty string, and `GC_APPLY=0` — leaves the dry run in place, because the
/// failure mode of guessing wrong in the other direction is deleted objects.
fn is_truthy(v: Option<&str>) -> bool {
    matches!(
        v.map(|s| s.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes")
    )
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
        .with_allow_http(true)
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

fn outcome_to_json(o: &StreamGcOutcome) -> String {
    format!(
        "{{\"outcome\":\"swept\",\"dry_run\":{},\"live_base_snapshot_keys\":{},\
         \"collected_snapshots\":{},\"collected_frame_objects\":{},\
         \"collected_bytes\":{},\"retained_snapshots\":{},\
         \"retained_frame_objects\":{},\"spared_by_grace\":{},\
         \"base_snapshots_skipped\":{}}}",
        o.dry_run,
        json_array(&o.live_base_snapshot_keys),
        json_array(&o.collected_snapshots),
        json_array(&o.collected_frame_objects),
        o.collected_bytes,
        o.retained_snapshots,
        o.retained_frame_objects,
        o.spared_by_grace,
        o.base_snapshots_skipped,
    )
}

fn json_array(items: &[String]) -> String {
    let body: Vec<String> = items.iter().map(|s| json_string(s)).collect();
    format!("[{}]", body.join(","))
}

/// JSON-string-encode `s` — same encoder as `turso-backup-snapshot`'s, for the
/// same reason: the object keys and anyhow messages this bin emits must not be
/// able to break the single parseable line.
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
    fn only_an_explicit_affirmative_applies() {
        assert!(is_truthy(Some("1")));
        assert!(is_truthy(Some("true")));
        assert!(is_truthy(Some(" YES ")));
        // Everything else keeps the dry run.
        assert!(!is_truthy(None));
        assert!(!is_truthy(Some("")));
        assert!(!is_truthy(Some("0")));
        assert!(!is_truthy(Some("false")));
        assert!(!is_truthy(Some("ture")));
    }

    #[test]
    fn swept_outcome_serialises() {
        let line = outcome_to_json(&StreamGcOutcome {
            dry_run: true,
            live_base_snapshot_keys: vec!["snap/snapshots/snapshot-2.db".into()],
            collected_snapshots: vec!["snap/snapshots/snapshot-1.db".into()],
            collected_frame_objects: vec!["snap/frames/0000000004/1-6".into()],
            collected_bytes: 4096,
            retained_snapshots: 1,
            retained_frame_objects: 3,
            spared_by_grace: 2,
            base_snapshots_skipped: false,
        });
        assert!(line.starts_with("{\"outcome\":\"swept\",\"dry_run\":true"));
        assert!(line.contains("\"collected_snapshots\":[\"snap/snapshots/snapshot-1.db\"]"));
        assert!(line.contains("\"collected_bytes\":4096"));
        assert!(line.contains("\"spared_by_grace\":2"));
    }

    #[test]
    fn empty_arrays_render_as_empty_json_arrays() {
        let line = outcome_to_json(&StreamGcOutcome::default());
        assert!(line.contains("\"collected_snapshots\":[]"));
        assert!(line.contains("\"dry_run\":false"));
    }

    #[test]
    fn json_string_escapes_problem_characters() {
        assert_eq!(json_string("with\"quote"), "\"with\\\"quote\"");
        assert_eq!(json_string("\x01"), "\"\\u0001\"");
    }
}
