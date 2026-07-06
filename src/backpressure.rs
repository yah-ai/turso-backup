//! Explicit R2 backpressure for the tier-2 WAL-frame drain loop (R574-F2).
//!
//! `stream::tail_frames`'s upload loop used to have no retry, no bounded
//! buffer, no shed — an R2 error bubbled immediately (see the W248 gotcha:
//! "turso-backup deliberately has NO retry/buffer/shed"). That was correct
//! for a bare primitive; this module is the explicit decision the doc's §5
//! calls for: a bounded in-memory spill buffer of frames read-but-not-yet-
//! confirmed-uploaded, an explicit overflow policy (block / shed / fail),
//! and retry-with-backoff on 429/503-shaped errors *inside* the drain loop
//! rather than bubbled raw.
//!
//! Implements R574-F2 (relay R574, "WAL→R2 streamer hardening"); the ticket
//! itself — status, assignee, handoff — is tracked in the W248 working doc,
//! not duplicated as an in-source annotation here.
//! @arch:see(.yah/docs/working/W248-wal-streamer-hardening.md)
//!
//! ## Design notes (for the reviewer, not the board)
//!
//! - `BackpressureConfig{spill_buffer_frames, policy, backoff}` is a
//!   required field on `StreamConfig`. The drain loop
//!   (`stream::drain_frames_with_backpressure`) reads WAL frames into a
//!   `VecDeque` bounded at `spill_buffer_frames`; once full it must resolve
//!   the head frame before reading further. A throttling-shaped `put` error
//!   retries with backoff (`put_with_backoff`); `Block` never gives up on a
//!   throttling error (bubbles instantly on any non-throttling error, same
//!   as `Fail`/`Shed`); `Fail`/`Shed` give up after `backoff.max_retries`.
//!   On give-up: `Fail` bubbles the error (nothing persists — matches
//!   pre-F2 behavior, which is why `Fail` is `BackpressurePolicy`'s
//!   `#[default]`, so every pre-existing `StreamConfig` call site is
//!   behavior-preserving); `Shed` drops the whole buffered backlog with a
//!   loud `BackpressureReport` and returns `Ok(StreamOutcome::Shed{..})`
//!   with no manifest/watermark write, so the next `tail_frames` call
//!   re-attempts the same range — no corruption, RPO regresses for the
//!   shed window instead.
//! - `StreamOutcome::Streamed`/`Restarted` gained a `backpressure:
//!   BackpressureReport` field. The new `StreamOutcome::Shed` variant
//!   covers "every buffered frame dropped, nothing persisted" (`Shed`
//!   policy only) — kept distinct from `Empty` (engine watermark itself
//!   has no new frames) so callers can tell "nothing new" from "had new
//!   frames, all shed".
//! - `is_throttling_error` is a best-effort string scan over the
//!   `object_store::Error` display + source chain for
//!   "429"/"503"/"Too Many Requests"/"Service Unavailable"/"SlowDown"/
//!   "RequestThrottled" — `object_store`'s own status-aware `RetryError`
//!   (`client::retry`) is `pub(crate)` to that crate and unreachable from
//!   here, so there is no public typed classifier to hook; this matches how
//!   a real S3-compatible backend's `Display` text reads once
//!   `object_store`'s own internal retry budget is exhausted.
//! - Test fake `fault_injection::FaultyStore` (`cfg(test)` only) wraps an
//!   inner `Arc<dyn ObjectStore>` and fails a configured queue of
//!   `put_opts` calls with a throttling-shaped `Error::Generic` before
//!   delegating through — mirrors this crate's existing
//!   mock-the-seam-not-the-engine convention (`stream.rs`'s `MockWal`).
//!   Needed `async-trait` + `futures-util` as dev-dependencies (both
//!   already resolved transitively via `object_store`'s own aws/http
//!   client and its own `throttle` module) since `object_store::ObjectStore`
//!   is itself `#[async_trait]` and its `delete_stream`/`list` methods are
//!   typed in terms of `futures_util::stream::BoxStream`.
//! - The doc left the exact `spill_buffer_frames` bound and backoff numbers
//!   open. Chose `spill_buffer_frames = 256` and
//!   `BackoffConfig{initial_delay=200ms, max_delay=10s, multiplier=2.0,
//!   max_retries=5}` as the `Default` — no measured workload to size
//!   against yet; both are plain struct fields so a caller overrides them
//!   per-tenant SLA without a code change.

use std::time::Duration;

/// What [`crate::stream::tail_frames`]'s drain loop does once the bounded
/// spill buffer is full and the head frame's upload keeps failing with a
/// throttling-shaped error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Keep retrying the throttled upload with backoff indefinitely —
    /// RPO-preserving (no frame is ever dropped), at the cost of a
    /// `tail_frames` call that can run long — or hang — under a sustained
    /// R2 outage. Right choice for SLA-tier tenants where a slow drain beats
    /// losing/delaying frames. Retries never give up *on a throttling
    /// error*; a non-throttling error (auth, not-found, …) still bubbles
    /// immediately regardless of policy.
    Block,
    /// Once backoff is exhausted for the head frame, drop the entire
    /// buffered backlog (loudly — see [`BackpressureReport::frames_shed`])
    /// and return whatever prefix already persisted. Nothing is corrupted:
    /// no manifest/watermark is written for the dropped tail, so the next
    /// `tail_frames` call resumes the same range from the last real
    /// watermark. RPO regresses for the shed window instead of blocking.
    Shed,
    /// Once backoff is exhausted, fail the whole `tail_frames` call —
    /// today's implicit "errors bubble immediately" behavior, made
    /// explicit. Right choice for non-SLA tiers. Chosen as [`Default`] so
    /// existing callers see no behavior change until they opt in to
    /// `Block`/`Shed`.
    #[default]
    Fail,
}

/// Exponential backoff parameters for retrying a throttled (429/503-shaped)
/// R2 `put`. Applied per-frame inside the drain loop.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackoffConfig {
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Hard cap on any single retry's delay.
    pub max_delay: Duration,
    /// Multiplier applied per retry (`initial_delay * multiplier^attempt`,
    /// capped at `max_delay`).
    pub multiplier: f64,
    /// Retries allowed before [`BackpressurePolicy::Fail`]/[`BackpressurePolicy::Shed`]
    /// give up. Ignored by [`BackpressurePolicy::Block`], which never gives
    /// up on a throttling error.
    pub max_retries: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            max_retries: 5,
        }
    }
}

impl BackoffConfig {
    /// Delay before retry number `attempt` (0-based), exponential with a
    /// hard `max_delay` cap.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.powi(attempt.min(32) as i32);
        let millis = (self.initial_delay.as_millis() as f64 * factor) as u64;
        Duration::from_millis(millis).min(self.max_delay)
    }
}

/// Bounded spill buffer + overflow policy + retry backoff for the frame
/// drain loop. See `.yah/docs/working/W248-wal-streamer-hardening.md`
/// ("Explicit backpressure").
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackpressureConfig {
    /// Max frames buffered in memory — read from the WAL but not yet
    /// confirmed uploaded — before the overflow policy applies. Once full,
    /// the drain loop must resolve the head frame before reading further.
    pub spill_buffer_frames: usize,
    pub policy: BackpressurePolicy,
    pub backoff: BackoffConfig,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            spill_buffer_frames: 256,
            policy: BackpressurePolicy::default(),
            backoff: BackoffConfig::default(),
        }
    }
}

/// Backpressure activity for a single `tail_frames` call — surfaced on
/// [`crate::stream::StreamOutcome`] so an orchestrator can alert on the
/// high-water mark or a nonzero shed count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BackpressureReport {
    pub policy: BackpressurePolicy,
    /// Peak number of frames buffered-but-unconfirmed during this call.
    pub high_water_frames: usize,
    /// Frames dropped by a `Shed` overflow (always 0 under `Block`/`Fail`).
    pub frames_shed: u64,
    /// Throttling retries consumed across the whole call.
    pub throttle_retries: u32,
}

/// True if `err` looks like a 429/503-shaped throttling response rather
/// than a hard failure (auth, not-found, corrupt request, …).
///
/// `object_store`'s own retry classification
/// (`object_store::client::retry::RetryError::status`) is `pub(crate)` to
/// that crate and unreachable from here, so this is a best-effort string
/// classifier over the error's `Display` and `source()` chain. Every
/// observed backend (including object_store's own internal
/// retry-exhausted error, whose `Display` embeds the HTTP status text)
/// surfaces the status this way.
pub(crate) fn is_throttling_error(err: &object_store::Error) -> bool {
    let mut msg = err.to_string();
    let mut cause = std::error::Error::source(err);
    while let Some(c) = cause {
        msg.push_str(" | ");
        msg.push_str(&c.to_string());
        cause = c.source();
    }
    const NEEDLES: [&str; 6] = [
        "429",
        "503",
        "Too Many Requests",
        "Service Unavailable",
        "SlowDown",
        "RequestThrottled",
    ];
    NEEDLES.iter().any(|n| msg.contains(n))
}

/// Upload `payload` to `key`, retrying throttling-shaped errors with
/// [`BackoffConfig`] backoff. Non-throttling errors bubble immediately (no
/// retry, regardless of policy). Under [`BackpressurePolicy::Block`],
/// retries never give up on a throttling error; under `Fail`/`Shed` they
/// stop after `backoff.max_retries` and the caller (the drain loop) decides
/// what "stop" means for its policy.
pub(crate) async fn put_with_backoff(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    key: &object_store::path::Path,
    payload: object_store::PutPayload,
    cfg: &BackpressureConfig,
    retries_used: &mut u32,
) -> Result<object_store::PutResult, object_store::Error> {
    use object_store::ObjectStoreExt;
    let mut attempt: u32 = 0;
    loop {
        match store.put(key, payload.clone()).await {
            Ok(r) => return Ok(r),
            Err(e) if is_throttling_error(&e) => {
                let unbounded = matches!(cfg.policy, BackpressurePolicy::Block);
                if !unbounded && attempt >= cfg.backoff.max_retries {
                    return Err(e);
                }
                tokio::time::sleep(cfg.backoff.delay_for(attempt)).await;
                attempt += 1;
                *retries_used += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Test-only fake R2 client: an [`object_store::ObjectStore`] wrapper that
/// fails a configured queue of `put_opts` calls with a throttling-shaped
/// error before delegating to the wrapped store.
#[cfg(test)]
pub(crate) mod fault_injection {
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use object_store::{
        path::Path as ObjPath, CopyOptions, Error as OsError, GetOptions, GetResult, ListResult,
        MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, Result as OsResult,
    };
    use std::collections::VecDeque;
    use std::fmt;
    use std::sync::{Arc, Mutex};

    /// A single queued fault: fail the next `put_opts` call with this
    /// throttling shape, then move on to the next queued fault (or the real
    /// store, once the queue is empty).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Fault {
        TooManyRequests,
        ServiceUnavailable,
    }

    #[derive(Debug)]
    struct SimulatedThrottle(&'static str);
    impl fmt::Display for SimulatedThrottle {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for SimulatedThrottle {}

    pub(crate) struct FaultyStore {
        inner: Arc<dyn ObjectStore>,
        faults: Mutex<VecDeque<Fault>>,
    }

    impl FaultyStore {
        pub(crate) fn new(
            inner: Arc<dyn ObjectStore>,
            faults: impl IntoIterator<Item = Fault>,
        ) -> Self {
            Self {
                inner,
                faults: Mutex::new(faults.into_iter().collect()),
            }
        }
    }

    impl fmt::Display for FaultyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FaultyStore({})", self.inner)
        }
    }
    impl fmt::Debug for FaultyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FaultyStore({:?})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for FaultyStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            let next = self.faults.lock().unwrap().pop_front();
            if let Some(fault) = next {
                let msg: &'static str = match fault {
                    Fault::TooManyRequests => {
                        "HTTP status client error (429 Too Many Requests) for url"
                    }
                    Fault::ServiceUnavailable => {
                        "HTTP status server error (503 Service Unavailable) for url"
                    }
                };
                return Err(OsError::Generic {
                    store: "faulty-test-store",
                    source: Box::new(SimulatedThrottle(msg)),
                });
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &ObjPath, options: GetOptions) -> OsResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<ObjPath>>,
        ) -> BoxStream<'static, OsResult<ObjPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_delay_grows_and_caps() {
        let cfg = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            multiplier: 2.0,
            max_retries: 10,
        };
        assert_eq!(cfg.delay_for(0), Duration::from_millis(100));
        assert_eq!(cfg.delay_for(1), Duration::from_millis(200));
        assert_eq!(cfg.delay_for(2), Duration::from_millis(400));
        assert_eq!(cfg.delay_for(3), Duration::from_millis(500), "capped at max_delay");
        assert_eq!(cfg.delay_for(10), Duration::from_millis(500));
    }

    #[test]
    fn classifies_429_and_503_as_throttling_but_not_other_errors() {
        let e429 = object_store::Error::Generic {
            store: "t",
            source: Box::new(std::io::Error::other("429 Too Many Requests")),
        };
        let e503 = object_store::Error::Generic {
            store: "t",
            source: Box::new(std::io::Error::other("503 Service Unavailable")),
        };
        let e_not_found = object_store::Error::NotFound {
            path: "x".into(),
            source: Box::new(std::io::Error::other("nope")),
        };
        assert!(is_throttling_error(&e429));
        assert!(is_throttling_error(&e503));
        assert!(!is_throttling_error(&e_not_found));
    }
}
