//! Conditional-put preflight: prove the configured sink enforces
//! `If-None-Match` / `If-Match` before anything relies on it. Moved verbatim
//! from `turso-backup`'s `stream` module (R971-T6).

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};

use crate::{join_key, unix_nanos, BackupTarget};

/// Which conditional-put mode a preflight probe found unenforced.
///
/// Both matter and they fail independently — a store can honour `If-None-Match`
/// (guarding the bootstrap put) while ignoring `If-Match` (guarding every
/// steady-state advance), or the reverse. Naming which one degraded is the
/// difference between an operator fixing a bucket setting in a minute and
/// bisecting a corruption in a week.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightStage {
    /// [`PutMode::Create`] / `If-None-Match` — the guard on two writers
    /// bootstrapping the same fresh sink.
    Create,
    /// [`PutMode::Update`] / `If-Match` — the guard on every subsequent
    /// watermark advance, and therefore the one the steady state rests on.
    Update,
}

impl PreflightStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            PreflightStage::Create => "PutMode::Create (If-None-Match)",
            PreflightStage::Update => "PutMode::Update (If-Match)",
        }
    }
}

/// What [`probe_conditional_puts`] observed at a real sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreconditionSupport {
    /// Both conditional modes were enforced — a put that should have been
    /// rejected was rejected. The watermark CAS is real at this sink.
    Honoured,
    /// A put that *must* have failed its precondition succeeded instead, so
    /// this backend has silently degraded to unconditional writes.
    Degraded { stage: PreflightStage },
}

impl PreconditionSupport {
    pub fn is_honoured(&self) -> bool {
        matches!(self, PreconditionSupport::Honoured)
    }
}

/// R732-T4: prove, against the *real* configured sink, that conditional puts
/// are actually enforced — and hand the caller a hard answer so it can refuse
/// to start when they are not.
///
/// [turso-backup's `write_watermark`] carries a deployment caveat that no test can close:
/// if `AmazonS3Builder` is pointed at a store that does not honour
/// `If-Match` / `If-None-Match`, every conditional put silently succeeds, the
/// CAS degrades to last-write-wins, and the concurrent split-brain window
/// W245 exists to shut reopens — while every unit test stays green, because
/// the in-memory store used in tests does honour them. Support is a property
/// of configuration, not of code, so a runtime probe is the only guard that
/// can exist.
///
/// The probe writes a uniquely-keyed canary under `<prefix>/preflight/`,
/// exercises both modes with puts that MUST be rejected, and deletes it. It
/// touches no watermark, no manifest and no frame, so it is safe to run at
/// startup against a live sink another node owns — and it is deliberately
/// keyed per-process-per-nanosecond so two nodes probing at once cannot fail
/// each other.
///
/// `Ok(Degraded)` is the interesting return and is NOT an error: the probe
/// worked perfectly, and reported that the store is unsafe. An `Err` means the
/// probe could not reach a verdict at all (sink unreachable, credentials
/// wrong), which is also a refuse-to-start condition but a different one for
/// an operator to read.
pub async fn probe_conditional_puts(target: &BackupTarget) -> Result<PreconditionSupport> {
    let key = join_key(
        &target.prefix,
        &format!("preflight/conditional-put-{:020}-{}.canary", unix_nanos(), std::process::id()),
    );
    let result = probe_at_key(&target.store, &key).await;
    // Best-effort cleanup: a leaked canary is inert (nothing reads
    // `preflight/`), so a delete failure must not mask the verdict — which is
    // the whole reason the caller ran this.
    if let Err(e) = target.store.delete(&key).await {
        tracing_delete_failure(&key, &e);
    }
    result
}

/// The probe body, split out so the canary is deleted on every path.
async fn probe_at_key(
    store: &Arc<dyn ObjectStore>,
    key: &ObjPath,
) -> Result<PreconditionSupport> {
    let create = PutOptions { mode: PutMode::Create, ..Default::default() };

    // 1. Claim the key. This one is *expected* to succeed; if it doesn't, the
    //    probe cannot reach a verdict (and `AlreadyExists` here means the
    //    per-process-per-nanosecond key collided, which is a bug, not a store
    //    property — so it stays an error rather than a `Degraded` verdict).
    let first = store
        .put_opts(key, b"preflight-1".as_slice().into(), create.clone())
        .await
        .with_context(|| format!("preflight canary could not be created at {key}"))?;
    let v1 = UpdateVersion { e_tag: first.e_tag.clone(), version: first.version.clone() };

    // 2. Create again over a key that now exists. A store honouring
    //    `If-None-Match` rejects this; one that succeeds has degraded.
    match store.put_opts(key, b"preflight-2".as_slice().into(), create).await {
        Err(object_store::Error::AlreadyExists { .. })
        | Err(object_store::Error::Precondition { .. }) => {}
        Ok(_) => return Ok(PreconditionSupport::Degraded { stage: PreflightStage::Create }),
        Err(e) => {
            return Err(e).with_context(|| format!("preflight Create probe failed at {key}"))
        }
    }

    // 3. Advance the canary conditionally on the version we hold, to obtain a
    //    *superseded* version. Expected to succeed — it is the ordinary
    //    steady-state write turso-backup's `write_watermark` makes.
    store
        .put_opts(
            key,
            b"preflight-3".as_slice().into(),
            PutOptions { mode: PutMode::Update(v1.clone()), ..Default::default() },
        )
        .await
        .with_context(|| format!("preflight Update probe could not advance {key}"))?;

    // 4. Update again on the now-stale version — exactly the shape of a fenced
    //    writer losing the watermark race. A store honouring `If-Match`
    //    rejects it.
    match store
        .put_opts(
            key,
            b"preflight-4".as_slice().into(),
            PutOptions { mode: PutMode::Update(v1), ..Default::default() },
        )
        .await
    {
        Err(object_store::Error::Precondition { .. })
        | Err(object_store::Error::AlreadyExists { .. }) => Ok(PreconditionSupport::Honoured),
        Ok(_) => Ok(PreconditionSupport::Degraded { stage: PreflightStage::Update }),
        Err(e) => Err(e).with_context(|| format!("preflight Update probe failed at {key}")),
    }
}

/// turso-backup takes no logging dependency (it is a library consumed by
/// binaries that pick their own), so a failed canary cleanup goes to stderr
/// rather than through `tracing`.
fn tracing_delete_failure(key: &ObjPath, e: &object_store::Error) {
    eprintln!("turso-backup: preflight canary {key} could not be deleted: {e}");
}
