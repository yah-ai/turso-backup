//! Large-object uploads that survive a flaky uplink (R975-F28).
//!
//! A base snapshot is one object, and `task-runs.turso`'s is ~589 MB. A single
//! `put` of that size has to cross the wire in one unbroken request; on a link
//! that drops a TLS record every few seconds (R975-F26: `BadRecordMac` cutting
//! uploads at 0.4-11 s) every attempt restarts from byte zero and none ever
//! finishes. [`put_large`] instead ships anything over [`MULTIPART_THRESHOLD`]
//! as an S3 multipart upload of [`PART_SIZE`] parts, so a dropped connection
//! costs one part, not the object.
//!
//! **Why the retry lives in [`PartRetryStore`] and not in `put_large`.**
//! `object_store`'s `MultipartUpload::put_part` assigns the part number when it
//! is *called*, and the S3 implementation's `complete` refuses ("Missing part")
//! unless every number it handed out came back. So a caller holding only
//! `dyn ObjectStore` cannot retry a part: a second `put_part` is a *new* part,
//! and the failed number stays a hole. Retrying the same part number needs
//! `MultipartStore::put_part(path, id, idx, ..)`, which only the concrete
//! backend implements. [`PartRetryStore`] wraps such a backend and hands out a
//! `MultipartUpload` that retries each part under its own index with bounded
//! backoff, and aborts the upload when a part finally fails or the upload is
//! dropped unfinished. Callers stay on `Arc<dyn ObjectStore>`.
//!
//! Without the wrapper, `put_large` still uploads in parts (each part gets
//! `object_store`'s own per-request retry) and still aborts on failure; it just
//! has no extra part-level retry budget of its own.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

/// Objects strictly larger than this go multipart; smaller ones keep the single `put`.
pub const MULTIPART_THRESHOLD: usize = 32 * 1024 * 1024;
/// Bytes per multipart part (S3/R2 require >= 5 MiB for every part but the last).
pub const PART_SIZE: usize = 16 * 1024 * 1024;
/// Attempts per part in [`PartRetryStore`], including the first.
pub const PART_ATTEMPTS: u32 = 6;
/// Backoff before the first part retry; doubles each retry, capped at [`PART_BACKOFF_MAX`].
#[cfg(not(test))]
pub const PART_BACKOFF_BASE: Duration = Duration::from_millis(250);
#[cfg(test)]
pub const PART_BACKOFF_BASE: Duration = Duration::from_millis(1);
/// Ceiling on one part-retry backoff.
pub const PART_BACKOFF_MAX: Duration = Duration::from_secs(8);

/// Upload `bytes` to `path`: one `put` up to [`MULTIPART_THRESHOLD`], else a
/// multipart upload of [`PART_SIZE`] parts, aborted on any failure so the bucket
/// does not keep orphan parts. Reusable by any caller with a large body
/// (base snapshots today; camp_files' file-mirror bodies are the planned next).
pub async fn put_large(
    store: &dyn ObjectStore,
    path: &ObjPath,
    bytes: Bytes,
) -> object_store::Result<PutResult> {
    put_large_with(store, path, bytes, MULTIPART_THRESHOLD, PART_SIZE).await
}

async fn put_large_with(
    store: &dyn ObjectStore,
    path: &ObjPath,
    bytes: Bytes,
    threshold: usize,
    part_size: usize,
) -> object_store::Result<PutResult> {
    if bytes.len() <= threshold {
        return store.put(path, bytes.into()).await;
    }
    let mut upload = store.put_multipart(path).await?;
    // Sequential: one part in flight bounds memory to the caller's buffer and
    // keeps a flaky link from fighting itself.
    let mut off = 0;
    while off < bytes.len() {
        let end = (off + part_size).min(bytes.len());
        if let Err(e) = upload.put_part(bytes.slice(off..end).into()).await {
            let _ = upload.abort().await;
            return Err(e);
        }
        off = end;
    }
    match upload.complete().await {
        Ok(r) => Ok(r),
        Err(e) => {
            let _ = upload.abort().await;
            Err(e)
        }
    }
}

/// An `ObjectStore` whose multipart uploads retry each failed part under its
/// own part number. Everything else delegates to `inner`. See the module doc.
#[derive(Debug)]
pub struct PartRetryStore<S> {
    inner: Arc<S>,
}

impl<S> PartRetryStore<S> {
    pub fn new(inner: S) -> Self {
        Self { inner: Arc::new(inner) }
    }
}

impl<S: std::fmt::Display> std::fmt::Display for PartRetryStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartRetryStore({})", self.inner)
    }
}

fn backoff(retry: u32) -> Duration {
    PART_BACKOFF_BASE.saturating_mul(1u32 << retry.min(16)).min(PART_BACKOFF_MAX)
}

#[derive(Debug)]
struct RetryingUpload<S: MultipartStore> {
    store: Arc<S>,
    path: ObjPath,
    id: MultipartId,
    next_idx: usize,
    parts: Arc<Mutex<Vec<Option<PartId>>>>,
    done: bool,
}

#[async_trait::async_trait]
impl<S: MultipartStore + std::fmt::Debug> MultipartUpload for RetryingUpload<S> {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let idx = self.next_idx;
        self.next_idx += 1;
        self.parts.lock().unwrap().push(None);
        let (store, path, id, parts) =
            (self.store.clone(), self.path.clone(), self.id.clone(), self.parts.clone());
        Box::pin(async move {
            let mut attempt = 0;
            loop {
                match store.put_part(&path, &id, idx, data.clone()).await {
                    Ok(pid) => {
                        parts.lock().unwrap()[idx] = Some(pid);
                        return Ok(());
                    }
                    Err(_) if attempt + 1 < PART_ATTEMPTS => {
                        tokio::time::sleep(backoff(attempt)).await;
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                }
            }
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts: Option<Vec<PartId>> = self.parts.lock().unwrap().iter().cloned().collect();
        let parts = parts.ok_or_else(|| object_store::Error::Generic {
            store: "PartRetryStore",
            source: "complete called with a part that never uploaded".into(),
        })?;
        let r = self.store.complete_multipart(&self.path, &self.id, parts).await?;
        self.done = true;
        Ok(r)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.done = true;
        self.store.abort_multipart(&self.path, &self.id).await
    }
}

impl<S: MultipartStore> Drop for RetryingUpload<S> {
    /// An upload dropped without `complete`/`abort` would leave its parts
    /// billed in the bucket forever; abort it best-effort.
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let (store, path, id) = (self.store.clone(), self.path.clone(), self.id.clone());
            rt.spawn(async move {
                let _ = store.abort_multipart(&path, &id).await;
            });
        }
    }
}

#[async_trait::async_trait]
impl<S> ObjectStore for PartRetryStore<S>
where
    S: ObjectStore + MultipartStore + std::fmt::Display,
{
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let id = self.inner.create_multipart_opts(location, opts).await?;
        Ok(Box::new(RetryingUpload {
            store: self.inner.clone(),
            path: location.clone(),
            id,
            next_idx: 0,
            parts: Arc::new(Mutex::new(Vec::new())),
            done: false,
        }))
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// InMemory whose `MultipartStore::put_part` fails every `nth` call
    /// (the failing call's retry then succeeds), or always when `nth == 1`.
    #[derive(Debug)]
    struct FlakyParts {
        inner: InMemory,
        nth: usize,
        calls: AtomicUsize,
        aborts: AtomicUsize,
    }
    impl FlakyParts {
        fn new(nth: usize) -> Self {
            Self { inner: InMemory::new(), nth, calls: AtomicUsize::new(0), aborts: AtomicUsize::new(0) }
        }
    }
    impl std::fmt::Display for FlakyParts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyParts")
        }
    }
    #[async_trait::async_trait]
    impl MultipartStore for FlakyParts {
        async fn create_multipart(&self, path: &ObjPath) -> object_store::Result<MultipartId> {
            self.inner.create_multipart(path).await
        }
        async fn put_part(
            &self,
            path: &ObjPath,
            id: &MultipartId,
            part_idx: usize,
            data: PutPayload,
        ) -> object_store::Result<PartId> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n % self.nth == 0 {
                return Err(object_store::Error::Generic {
                    store: "FlakyParts",
                    source: "injected BadRecordMac".into(),
                });
            }
            self.inner.put_part(path, id, part_idx, data).await
        }
        async fn complete_multipart(
            &self,
            path: &ObjPath,
            id: &MultipartId,
            parts: Vec<PartId>,
        ) -> object_store::Result<PutResult> {
            self.inner.complete_multipart(path, id, parts).await
        }
        async fn abort_multipart(&self, path: &ObjPath, id: &MultipartId) -> object_store::Result<()> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            self.inner.abort_multipart(path, id).await
        }
    }
    #[async_trait::async_trait]
    impl ObjectStore for FlakyParts {
        async fn put_opts(&self, l: &ObjPath, p: PutPayload, o: PutOptions) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &ObjPath,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &ObjPath, o: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(l, o).await
        }
        fn delete_stream(
            &self,
            s: BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(s)
        }
        fn list(&self, p: Option<&ObjPath>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&ObjPath>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &ObjPath, t: &ObjPath, o: object_store::CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
    }

    fn image(len: usize) -> Vec<u8> {
        let mut v = b"SQLite format 3\0".to_vec();
        v.extend((0..len - 16).map(|i| (i % 251) as u8));
        v
    }

    #[tokio::test]
    async fn base_snapshot_completes_when_every_third_part_fails_once() {
        let store = Arc::new(PartRetryStore::new(FlakyParts::new(3)));
        let target = store_claim::BackupTarget { store: store.clone(), prefix: "db".into() };
        // Real constants: 40 MiB > MULTIPART_THRESHOLD -> 3 parts of PART_SIZE.
        let img = image(40 * 1024 * 1024 + 7);
        let key = crate::snapshot::upload_base_snapshot(&target, &img).await.unwrap();
        let got = store.get(&ObjPath::from(key)).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), img.as_slice());
        let inner = &store.inner;
        assert!(inner.calls.load(Ordering::SeqCst) > 3, "a part was retried");
        assert_eq!(inner.aborts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn part_that_never_succeeds_aborts_the_upload() {
        let store = PartRetryStore::new(FlakyParts::new(1));
        let path = ObjPath::from("big");
        let err = put_large_with(&store, &path, Bytes::from(vec![1u8; 300]), 100, 100).await;
        assert!(err.is_err());
        assert_eq!(store.inner.calls.load(Ordering::SeqCst), PART_ATTEMPTS as usize);
        assert_eq!(store.inner.aborts.load(Ordering::SeqCst), 1);
        assert!(store.head(&path).await.is_err(), "no object published");
    }

    #[tokio::test]
    async fn small_object_is_a_single_put() {
        let store = PartRetryStore::new(FlakyParts::new(1));
        let path = ObjPath::from("small");
        put_large(&store, &path, Bytes::from_static(b"hi")).await.unwrap();
        assert_eq!(store.inner.calls.load(Ordering::SeqCst), 0);
    }
}
