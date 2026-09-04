//! PLAYBACK request path: ytId → cached faststart MP4 (yt-dlp + ffmpeg), served with HTTP range
//! support. A cached file is served instantly; a cold id downloads to completion first (prewarm at
//! /meta keeps the cache warm ahead of play, so cold is the exception).

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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

/// A partial older than this cannot still be downloading: the download timeout is 240s.
const PARTIAL_GRACE: Duration = Duration::from_secs(30 * 60);

/// Reclaim partials nothing is writing any more.
///
/// They are dot-prefixed to keep eviction from deleting a live download, which also kept them out
/// of the size cap — so a crash, an OOM kill or a redeploy mid-download left a file that nothing
/// counted and nothing ever removed, on a persistent volume.
pub(crate) fn sweep_partials(cfg: &Config) {
    let Ok(rd) = std::fs::read_dir(&cfg.cache_dir) else { return };
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Anything that is not a published trailer is scratch. Naming the shapes one at a time kept
        // missing one: first only `.partial.mp4` (yt-dlp actually writes `<tmp>.part` and
        // `<tmp>.f<id>.<ext>.part`), then only dotfiles (MP4Box's `-tmp` file is `_libgpac_…`, no
        // dot, no extension). A published trailer is `<vid>.mp4` and nothing else is.
        if is_published_trailer(&name) {
            continue;
        }
        // Directories are not ours to remove — yt-dlp's own cache lives in one here.
        if !entry.metadata().map(|m| m.is_file()).unwrap_or(false) {
            continue;
        }
        let stale = entry
            .metadata()
            .ok()
            .and_then(|md| md.modified().ok())
            .and_then(|m| m.elapsed().ok())
            .is_some_and(|age| age > PARTIAL_GRACE);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Remove `tmp` and every scratch file yt-dlp derived from it. Unlinking only `tmp` left the rest
/// on disk, where the size cap neither counts nor evicts them, so a run of failing downloads pushed
/// real usage past the cap until the hourly sweep's 30-min grace expired.
///
/// Matched on the name WITHOUT the extension, because yt-dlp puts `.f<id>` on either side of it:
/// `prepend_extension` inserts before the extension when the stream's ext matches the output's and
/// appends otherwise. The ladder forces avc1+mp4a, so video is always `.mp4` (inserted:
/// `<stem>.f137.mp4`) and audio always `.m4a` (appended: `<stem>.mp4.f140`). Matching the full
/// filename therefore reclaimed the small audio partial and left the large video one — the leak
/// this exists to close. `<stem>` is unique per download (pid + generation), so it cannot reach
/// another in-flight temp.
pub(crate) async fn remove_temp_set(cfg: &Config, tmp: &std::path::Path) {
    let Some(stem) = tmp.file_stem().and_then(|n| n.to_str()).map(String::from) else { return };
    let _ = tokio::fs::remove_file(tmp).await;
    let Ok(mut rd) = tokio::fs::read_dir(&cfg.cache_dir).await else { return };
    while let Ok(Some(entry)) = rd.next_entry().await {
        if entry.file_name().to_string_lossy().starts_with(&stem) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

/// Remove the scratch THIS process was writing, on the way out.
///
/// Temps are named with our pid, so once we exit nothing can tell them from another instance's
/// live work — `sweep_partials` has to wait out its 30-minute grace before touching them, during
/// which they sit on the cache volume uncounted by nothing and unreclaimable. A redeploy is the
/// common case, and it is exactly when the volume is under pressure.
pub(crate) fn sweep_own_temps(cfg: &Config) {
    let Ok(rd) = std::fs::read_dir(&cfg.cache_dir) else { return };
    let mine = format!(".{}.", std::process::id());
    let mut removed = 0usize;
    for entry in rd.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // `.{vid}.{pid}.{gen}.partial.mp4` — match on the pid segment, wherever the vid puts it.
        if is_published_trailer(&name) || !name.contains(&mine) {
            continue;
        }
        if entry.metadata().map(|m| m.is_file()).unwrap_or(false)
            && std::fs::remove_file(entry.path()).is_ok()
        {
            removed += 1;
        }
    }
    if removed > 0 {
        eprintln!("shutdown: reclaimed {removed} partial file(s)");
    }
}

/// `<vid>.mp4`, the only shape this service publishes.
fn is_published_trailer(name: &str) -> bool {
    name.strip_suffix(".mp4").is_some_and(crate::is_valid_vid)
}

/// Evict least-recently-used cached files until under the size cap (bounded cache). Sync fs, run
/// off the runtime thread via spawn_blocking. Skips dotfiles so an in-progress `.<vid>.…partial.mp4`
/// is neither counted nor deleted out from under its writer; `sweep_partials` reclaims stale ones.
pub(crate) fn evict_if_needed(cfg: &Config) {
    // Everything on the volume counts toward the cap, but only published trailers are EVICTABLE.
    // Counting just `<vid>.mp4` made the cap a floor on real usage rather than a ceiling: an
    // in-progress download or a leaked MP4Box temp is trailer-sized and was invisible here, so the
    // volume could sit well over its limit while this function believed it was under. Scratch is
    // reclaimed by sweep_partials once it is provably abandoned — deleting it here would race a
    // live download — but the space it occupies has to be subtracted from what trailers may use.
    let mut files: Vec<(PathBuf, u64, SystemTime)> = Vec::new();
    let mut scratch_bytes: u64 = 0;
    match std::fs::read_dir(&cfg.cache_dir) {
        Ok(rd) => {
            for e in rd.flatten() {
                let Ok(md) = e.metadata() else { continue };
                if !md.is_file() {
                    continue; // yt-dlp's own cache lives in a subdirectory here
                }
                let name = e.file_name();
                if is_published_trailer(&name.to_string_lossy()) {
                    let atime = md.accessed().unwrap_or(SystemTime::UNIX_EPOCH);
                    files.push((e.path(), md.len(), atime));
                } else {
                    scratch_bytes += md.len();
                }
            }
        }
        Err(_) => return,
    }
    // TTL pass: drop anything not accessed within cache_ttl, independent of the size cap. atime is
    // bumped on every serve (touch_atime), so a rewatched trailer keeps a fresh timestamp and survives;
    // only genuinely-stale ones age out. cache_ttl == 0 (CACHE_TTL_DAYS=0) disables it.
    if !cfg.cache_ttl.is_zero() {
        if let Some(cutoff) = SystemTime::now().checked_sub(cfg.cache_ttl) {
            files.retain(|(p, _size, atime)| {
                if *atime < cutoff {
                    let _ = std::fs::remove_file(p);
                    false
                } else {
                    true
                }
            });
        }
    }
    // Trailers get whatever the cap has left after scratch. Adding scratch into the running total
    // and then subtracting only trailer bytes meant that once scratch alone cleared the cap the
    // loop could never satisfy it — so it deleted EVERY published trailer, on every call, including
    // the one the download that triggered it had just published. That is the death spiral the
    // cache_max_bytes floor exists to prevent, reintroduced through the other side.
    //
    // When scratch alone exceeds the cap, evicting trailers cannot fix it: the space is held by
    // downloads in flight or by leftovers younger than sweep_partials' grace, and both resolve on
    // their own. Say so and leave the cache alone.
    let Some(budget) = cfg.cache_max_bytes.checked_sub(scratch_bytes).filter(|b| *b > 0) else {
        eprintln!(
            "warning: in-flight/abandoned scratch ({scratch_bytes} B) fills CACHE_MAX_BYTES ({} B) \
             on its own; evicting trailers cannot help — sweep_partials reclaims it",
            cfg.cache_max_bytes
        );
        return;
    };
    let mut total: u64 = files.iter().map(|f| f.1).sum();
    if total <= budget {
        return;
    }
    files.sort_by_key(|f| f.2); // oldest atime first
    for (p, size, _) in &files {
        if total <= budget {
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
    // Checked here as well as at the routes, because `vid` also arrives from TMDB/KinoCheck via
    // prewarm, and it becomes a filename and a yt-dlp -o path. One `..` writes outside the cache.
    if !crate::is_valid_vid(&vid) {
        return Err(PlayError {
            status: 400,
            reason: "bad_id".into(),
            message: "Not a YouTube id.".into(),
            detail: format!("rejected vid {vid:?}"),
        });
    }
    let fp = cache_path(&state.cfg, &vid);
    if let Ok(md) = tokio::fs::metadata(&fp).await {
        // is_file, not just non-empty: a directory reports a non-zero length, so anything that left
        // one at a trailer's path was served as a cache hit that serve_file could then never open.
        if md.is_file() && md.len() > 0 {
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
        remove_temp_set(&state.cfg, &tmp).await;
        // /health signal (moved here from the old resolve-time probe): a SYSTEMIC extraction failure
        // (YouTube BotGuard / a broken nsig-JS runtime) bumps the counter; a per-video geo-block/removal
        // does not. A successful download below clears it. `extractor_unavailable` trips past the threshold.
        if e.reason == "extraction_failed" {
            state.extract_fails.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // A local failure is not the extractor's fault, but it is still a total outage from the
        // viewer's side, and it used to move nothing at all.
        if e.reason == "incomplete_download" {
            state.local_fails.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        return Err(e);
    }
    state.extract_fails.store(0, std::sync::atomic::Ordering::Relaxed); // extraction worked → clear the signal

    // Detect the content rect (cached for /crop) and bake a `clap` box — on the TEMP file, BEFORE
    // publishing, so no request can serve it mid-write. Best-effort: a play must not break because
    // crop detection did, and a bake that never ran leaves the file exactly as it was.
    //
    // A bake that was KILLED part-way is different, and the distinction was being thrown away with
    // the return value. MP4Box rewrites in place, on the same inode, so an interrupted one leaves a
    // half-rewritten trailer — which was then renamed into the cache and served immutable for a
    // year, never re-fetched. Better to lose the download and re-fetch than to cache that.
    if let Some(report) = crate::crop::detect(&state.cfg, &vid, &tmp).await {
        crate::crop::cache_report(&state, &vid, report.clone());
        if crate::crop::bake_clap(&state.cfg, &tmp, &report).await == crate::crop::Bake::Damaged {
            remove_temp_set(&state.cfg, &tmp).await;
            state.local_fails.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(PlayError::bake_interrupted());
        }
    }

    tokio::fs::rename(&tmp, &fp).await.map_err(|e| {
        let _ = std::fs::remove_file(&tmp); // by here yt-dlp has merged and cleaned its own siblings
        // A rename failure is ours — a full or read-only volume, {vid}.mp4 already there as a
        // directory — not the extractor's. Calling it `extraction_failed` sent the operator after
        // yt-dlp, and because the extraction counter was cleared just above and this error is built
        // here rather than in download_to, an instance failing EVERY download at the rename moved
        // no counter at all and reported ok.
        state.local_fails.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        PlayError::incomplete(format!("rename {}: {e}", tmp.display()))
    })?;
    // Cleared only once a trailer is actually in the cache — before the rename it was cleared by a
    // download that could still fail.
    state.local_fails.store(0, std::sync::atomic::Ordering::Relaxed);

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
