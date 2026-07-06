//! PLAYBACK request path: ytId → cached faststart MP4 (yt-dlp + ffmpeg), served with HTTP range
//! support. A cached file is served instantly; a cold id downloads to completion first (prewarm at
//! /meta keeps the cache warm ahead of play, so cold is the exception).

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use futures_util::{FutureExt, TryStreamExt};
use hyper::body::Frame;
use hyper::header::HeaderMap;
use hyper::{Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::config::Config;
use crate::httputil::{self, parse_range, Body, RangeReq};
use crate::state::{AppState, BoxFuture, SharedDownload};
use crate::ytdlp::{self, PlayError};

/// Read buffer for streaming a cached file out. 256 KiB (vs ReaderStream's 4 KiB default) — one
/// big buffer per stream keeps syscalls/wakeups low so time-to-first-frame isn't throttled by the
/// serve path. Matches (exceeds) Node's 64 KiB createReadStream highWaterMark.
const STREAM_BUF: usize = 256 * 1024;

fn cache_path(cfg: &Config, vid: &str) -> PathBuf {
    cfg.cache_dir.join(format!("{vid}.mp4"))
}

/// Is the on-disk cache usable? `create_dir_all` is idempotent and cheap when the dir already
/// exists, so this doubles as a self-healing check — a volume that comes back after boot recovers
/// without a restart. `/play` and `/crop` gate on it to return a clean 503 instead of a murky 502.
pub async fn cache_available(cfg: &Config) -> bool {
    tokio::fs::create_dir_all(&cfg.cache_dir).await.is_ok()
        && tokio::fs::create_dir_all(&cfg.ytdlp_cache).await.is_ok()
}

/// Bump a cached file's atime so the LRU eviction sees it as recently used. Fire-and-forget so the
/// hot serve path isn't slowed; a rare eviction/serve race is handled by the open-miss refetch in
/// `handle_play`.
fn touch_atime(fp: PathBuf) {
    tokio::task::spawn_blocking(move || {
        if let Ok(f) = std::fs::File::open(&fp) {
            let times = std::fs::FileTimes::new().set_accessed(SystemTime::now());
            let _ = f.set_times(times);
        }
    });
}

/// Evict least-recently-used cached files until under the size cap (bounded cache). Sync fs, run
/// off the runtime thread via spawn_blocking. Skips dotfiles so an in-progress `.<vid>.…partial.mp4`
/// is neither counted nor deleted out from under its writer.
pub(crate) fn evict_if_needed(cfg: &Config) {
    let mut files: Vec<(PathBuf, u64, SystemTime)> = match std::fs::read_dir(&cfg.cache_dir) {
        Ok(rd) => rd
            .flatten()
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                !name.starts_with('.') && name.ends_with(".mp4")
            })
            .filter_map(|e| {
                let md = e.metadata().ok()?;
                let atime = md.accessed().unwrap_or(SystemTime::UNIX_EPOCH);
                Some((e.path(), md.len(), atime))
            })
            .collect(),
        Err(_) => return,
    };
    let mut total: u64 = files.iter().map(|f| f.1).sum();
    if total <= cfg.cache_max_bytes {
        return;
    }
    files.sort_by_key(|f| f.2); // oldest atime first
    for (p, size, _) in &files {
        if total <= cfg.cache_max_bytes {
            break;
        }
        if std::fs::remove_file(p).is_ok() {
            total -= size;
        }
    }
}

/// Download+mux a faststart MP4 for `vid`, cached. De-dupes concurrent requests via `in_flight`:
/// the first caller creates one shared download, everyone else awaits it.
pub async fn fetch_trailer(state: Arc<AppState>, vid: String) -> Result<PathBuf, PlayError> {
    let fp = cache_path(&state.cfg, &vid);
    if let Ok(md) = tokio::fs::metadata(&fp).await {
        if md.len() > 0 {
            touch_atime(fp.clone()); // bump atime for LRU
            return Ok(fp);
        }
    }

    // Each created download gets a unique generation and a DETACHED driver task that owns it: the
    // driver polls the download to completion and clears the map entry regardless of any requester's
    // lifetime. So a client disconnecting mid-download can't orphan the entry (which would wedge the
    // vid until restart) or leak the subprocess — this fn is now a pure waiter. The generation guard
    // means the driver only ever removes its own entry.
    let shared: SharedDownload = {
        let mut map = state.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, existing)) = map.get(&vid) {
            existing.clone()
        } else {
            let gen = state.dl_gen.fetch_add(1, Ordering::Relaxed);
            let fut: BoxFuture<Result<PathBuf, PlayError>> = {
                let st = state.clone();
                let v = vid.clone();
                Box::pin(async move { download_cached(st, v, gen).await })
            };
            let shared = fut.shared();
            map.insert(vid.clone(), (gen, shared.clone()));
            let driver = shared.clone();
            let st = state.clone();
            let v = vid.clone();
            tokio::spawn(async move {
                let _ = driver.await; // drive to completion even if every requester goes away
                let mut map = st.in_flight.lock().unwrap_or_else(|e| e.into_inner());
                if matches!(map.get(&v), Some((g, _)) if *g == gen) {
                    map.remove(&v);
                }
            });
            shared
        }
    };
    shared.await
}

/// The actual yt-dlp download for a cold `vid`: mux to a per-generation temp file, bake the clap on
/// the temp, then atomically rename into place and evict if we blew the cap. `gen` makes the temp
/// name unique so even a de-dupe miss can't put two writers on one path. Bounded by `download_sem`.
async fn download_cached(state: Arc<AppState>, vid: String, gen: u64) -> Result<PathBuf, PlayError> {
    let fp = cache_path(&state.cfg, &vid);
    // Temp MUST end in .mp4 — yt-dlp derives the merge output name from the extension. Leading dot
    // keeps it out of eviction's LRU scan.
    let tmp = state
        .cfg
        .cache_dir
        .join(format!(".{vid}.{}.{gen}.partial.mp4", std::process::id()));

    // Global cap on concurrent downloads (bounds CPU/disk/fd for a burst of distinct ids).
    let _permit = state.download_sem.acquire().await;

    if let Err(e) = ytdlp::download_to(&state.cfg, &vid, &tmp).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        // /health signal (moved here from the old resolve-time probe): a SYSTEMIC extraction failure
        // (YouTube BotGuard / a broken nsig-JS runtime) bumps the counter; a per-video geo-block/removal
        // does not. A successful download below clears it. `extractor_unavailable` trips past the threshold.
        if e.reason == "extraction_failed" {
            state.extract_fails.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        return Err(e);
    }
    state.extract_fails.store(0, std::sync::atomic::Ordering::Relaxed); // extraction worked → clear the signal

    // Detect the content rect (cached for /crop) and bake a `clap` box — on the TEMP file, BEFORE
    // publishing. So the file that appears at `fp` is already final and immutable: no request can
    // serve it mid-clap-write, and a crash/kill during the bake leaves the temp (not a corrupt
    // cached file). Best-effort — a play must never break because crop detection did.
    if let Some(report) = crate::crop::detect(&state.cfg, &vid, &tmp).await {
        crate::crop::cache_report(&state, &vid, report.clone());
        crate::crop::bake_clap(&state.cfg, &tmp, &report).await;
    }

    tokio::fs::rename(&tmp, &fp).await.map_err(|e| {
        let _ = std::fs::remove_file(&tmp); // don't leak the temp on a rename failure
        PlayError {
            status: 502,
            reason: "extraction_failed".into(),
            message: "Could not fetch this trailer.".into(),
            detail: format!("rename {}: {e}", tmp.display()),
        }
    })?;

    let cfg = state.cfg.clone();
    let _ = tokio::task::spawn_blocking(move || evict_if_needed(&cfg)).await;
    Ok(fp)
}

/// Wrap an async reader as a streaming response body with a large read buffer.
fn stream_body<R>(reader: R) -> Body
where
    R: tokio::io::AsyncRead + Send + Sync + 'static,
{
    let stream = ReaderStream::with_capacity(reader, STREAM_BUF).map_ok(Frame::data);
    StreamBody::new(stream).boxed()
}

/// Serve a file with HTTP range support (so the player can scrub). Always answers Content-Length +
/// Accept-Ranges (+206 on Range) — which tvOS AVPlayer REQUIRES for a progressive MP4.
///
/// `Err(())` means the file vanished before we could open it (evicted between fetch and serve) —
/// the caller retries with a fresh fetch. Every other outcome is a finished `Response`.
async fn serve_file(range: Option<&str>, fp: &Path, vid: &str) -> Result<Response<Body>, ()> {
    let file = match tokio::fs::File::open(fp).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("serve_file open {}: {e}", fp.display());
            return Err(());
        }
    };
    let size = match file.metadata().await {
        Ok(m) => m.len(),
        Err(_) => return Ok(httputil::text(StatusCode::INTERNAL_SERVER_ERROR, "stat failed")),
    };
    // The cached MP4 for a given id is byte-stable + immutable (a new extraction would be a new id),
    // so it can be cached hard. A strong ETag from id+size lets a caller/proxy revalidate cheaply.
    let etag = httputil::etag_of(format!("{vid}:{size}").as_bytes());

    let resp = match parse_range(range, size) {
        Some(RangeReq::Unsatisfiable) => Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header("content-range", format!("bytes */{size}"))
            .body(httputil::full(""))
            .unwrap(),
        Some(RangeReq::Satisfiable { start, end }) => {
            let len = end - start + 1;
            let mut file = file;
            if file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
                return Ok(httputil::text(StatusCode::INTERNAL_SERVER_ERROR, "seek failed"));
            }
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("content-range", format!("bytes {start}-{end}/{size}"))
                .header("accept-ranges", "bytes")
                .header("content-length", len)
                .header("content-type", "video/mp4")
                .header("cache-control", "public, max-age=31536000, immutable")
                .header("etag", &etag)
                .body(stream_body(file.take(len)))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::OK)
            .header("content-length", size)
            .header("content-type", "video/mp4")
            .header("accept-ranges", "bytes")
            .header("cache-control", "public, max-age=31536000, immutable")
            .header("etag", &etag)
            .body(stream_body(file))
            .unwrap(),
    };
    Ok(resp)
}

/// Typed /play failure body (geo_blocked 451 / restricted 403 / unavailable 404 / 502).
fn play_error(vid: &str, e: &PlayError) -> Response<Body> {
    let body = serde_json::json!({ "error": e.reason, "message": e.message, "id": vid });
    httputil::json(
        StatusCode::from_u16(e.status).unwrap_or(StatusCode::BAD_GATEWAY),
        &body,
        &[],
    )
}

pub async fn handle_play(state: Arc<AppState>, headers: &HeaderMap, vid: String) -> Response<Body> {
    if !cache_available(&state.cfg).await {
        return httputil::error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cache_unavailable",
            "Trailer cache is unavailable.",
        );
    }
    let range = headers.get("range").and_then(|v| v.to_str().ok()).map(str::to_string);
    // At most two attempts: if the cached file is evicted between fetch and open, re-fetch once.
    for attempt in 0..2 {
        match fetch_trailer(state.clone(), vid.clone()).await {
            Ok(fp) => match serve_file(range.as_deref(), &fp, &vid).await {
                Ok(resp) => return resp,
                Err(()) if attempt == 0 => continue, // evicted mid-serve — retry a fresh fetch
                Err(()) => break,
            },
            Err(e) => {
                eprintln!("[{vid}] {}", e.detail);
                return play_error(&vid, &e);
            }
        }
    }
    httputil::text(StatusCode::INTERNAL_SERVER_ERROR, "serve failed")
}
