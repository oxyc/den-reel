//! HLS stream-remux path (EXPERIMENTAL — gated by `STREAM_REMUX`). Instead of downloading the whole
//! trailer and then serving it (`/play`), extract the two DASH stream URLs and **stream-copy** remux
//! them into fMP4 HLS segments as the bytes arrive — the client plays segment 0 while the rest is
//! still muxing, so first-frame is a first-segment wait, not a whole-file wait. No re-encode (copy),
//! so CPU is trivial. den-reel still proxies the bytes (the googlevideo URLs are IP-locked to THIS
//! server), so this is a latency win, not a bandwidth one. Baked-in letterbox is handled client-side
//! (billboard `resizeAspectFill`), so there's no cropdetect/clap here.
//!
//! Files live under `cache_dir/hls/<vid>/`: `index.m3u8` (the playlist, grows then gets an
//! `EXT-X-ENDLIST`), `init.mp4` (fMP4 init segment), `seg_NNN.m4s` (media segments). Concurrency is
//! de-duped like `/play`: the first caller starts one ffmpeg driver, everyone else shares its
//! readiness. A detached driver owns the ffmpeg child so a client disconnect can't orphan it.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::FutureExt;
use hyper::{Response, StatusCode};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use crate::config::Config;
use crate::httputil::{self, Body};
use crate::state::{AppState, BoxFuture, SharedHls, UrlEntry};
use crate::ytdlp::{self, PlayError};

const READY_TIMEOUT_SECS: u64 = 60; // wait this long for the first segment before giving up
const REMUX_TIMEOUT_SECS: u64 = 300; // backstop the whole ffmpeg remux
const TARGET_SEGMENT_SECS: &str = "4"; // HLS segment duration target (ffmpeg splits on keyframes)

/// Per-vid HLS working directory: `cache_dir/hls/<vid>/`.
fn hls_dir(state: &AppState, vid: &str) -> PathBuf {
    state.cfg.cache_dir.join("hls").join(vid)
}

/// A playlist carrying `EXT-X-ENDLIST` is a finished VOD — safe to serve straight from cache with no
/// new remux. Absent/partial → not complete.
async fn is_complete(dir: &Path) -> bool {
    matches!(tokio::fs::read_to_string(dir.join("index.m3u8")).await, Ok(s) if s.contains("#EXT-X-ENDLIST"))
}

/// The direct googlevideo URL(s), cached briefly (URL_CACHE_TTL_MS < the ~6h signature life) so a
/// re-play skips a fresh yt-dlp extraction. Bounded like `yt_cache`.
async fn cached_urls(state: &Arc<AppState>, vid: &str) -> Result<Vec<String>, PlayError> {
    {
        let cache = state.url_cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = cache.get(vid) {
            if e.exp > (state.clock)() {
                return Ok(e.urls.clone());
            }
        }
    }
    let urls = ytdlp::extract_urls(&state.cfg, vid).await?;
    {
        let mut cache = state.url_cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= crate::YT_CACHE_MAX {
            let now = (state.clock)();
            cache.retain(|_, e| e.exp > now);
        }
        cache.insert(vid.to_string(), UrlEntry { urls: urls.clone(), exp: (state.clock)() + crate::URL_CACHE_TTL_MS });
    }
    Ok(urls)
}

/// Build the ffmpeg stream-copy → HLS command. Runs with `current_dir(dir)` so the playlist references
/// its segments by bare relative names (`init.mp4`, `seg_000.m4s`) — exactly the paths the app fetches
/// back from `/hls/<vid>/`. Two inputs (DASH v+a) → map both; one input (progressive) → map its audio
/// optionally.
fn spawn_ffmpeg(cfg: &Config, urls: &[String], dir: &Path) -> std::io::Result<Child> {
    let mut cmd = Command::new(&cfg.ffmpeg);
    cmd.args(["-hide_banner", "-loglevel", "error", "-nostdin"]);
    for u in urls {
        cmd.args(["-i", u]);
    }
    cmd.args(["-map", "0:v:0"]);
    if urls.len() > 1 {
        cmd.args(["-map", "1:a:0"]);
    } else {
        cmd.args(["-map", "0:a:0?"]); // progressive: audio is in the same input (optional if silent)
    }
    cmd.args([
        "-c",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        TARGET_SEGMENT_SECS,
        "-hls_playlist_type",
        "event", // append-only, so the app sees new segments as they land; we add ENDLIST on completion
        "-hls_segment_type",
        "fmp4",
        "-hls_flags",
        "independent_segments",
        "-hls_fmp4_init_filename",
        "init.mp4",
        "-hls_segment_filename",
        "seg_%03d.m4s",
        "index.m3u8",
    ])
    .current_dir(dir)
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    cmd.spawn()
}

/// Reconstruct the readiness-wait error from the driver's `.failed` sentinel: `status\nreason\nmessage`.
pub(crate) fn parse_sentinel(s: &str) -> PlayError {
    let mut it = s.splitn(3, '\n');
    let status = it.next().and_then(|s| s.trim().parse().ok()).unwrap_or(502);
    let reason = it.next().unwrap_or("extraction_failed").to_string();
    let message = it.next().unwrap_or("Could not fetch this trailer.").to_string();
    PlayError { status, reason, message, detail: "hls remux failed".into() }
}

/// Poll the working dir until the first segment (and playlist + init) exist → ready to serve; or the
/// driver drops a `.failed` sentinel → surface it; or we hit READY_TIMEOUT.
async fn wait_ready(dir: &Path) -> Result<(), PlayError> {
    let seg0 = dir.join("seg_000.m4s");
    let index = dir.join("index.m3u8");
    let init = dir.join("init.mp4");
    let failed = dir.join(".failed");
    for _ in 0..(READY_TIMEOUT_SECS * 10) {
        if let Ok(reason) = tokio::fs::read_to_string(&failed).await {
            return Err(parse_sentinel(&reason));
        }
        let ready = tokio::fs::try_exists(&seg0).await.unwrap_or(false)
            && tokio::fs::try_exists(&init).await.unwrap_or(false)
            && tokio::fs::try_exists(&index).await.unwrap_or(false);
        if ready {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(PlayError {
        status: 504,
        reason: "timeout".into(),
        message: "This trailer took too long to start.".into(),
        detail: format!("hls first segment not ready in {READY_TIMEOUT_SECS}s"),
    })
}

/// The detached driver: owns the ffmpeg child + the concurrency permit for the whole remux. On clean
/// exit it appends `EXT-X-ENDLIST` (marking the VOD complete + cacheable); on failure it writes a
/// `.failed` sentinel so a waiter fails fast. Always clears its own job-map entry (generation-guarded)
/// so it can't wedge the vid.
async fn drive_remux(
    state: Arc<AppState>,
    vid: String,
    gen: u64,
    dir: PathBuf,
    mut child: Child,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let stderr_pipe = child.stderr.take();
    let drain = tokio::spawn(async move {
        let mut buf = String::new();
        if let Some(mut p) = stderr_pipe {
            let _ = p.read_to_string(&mut buf).await;
        }
        buf
    });

    let wait_result = tokio::time::timeout(Duration::from_secs(REMUX_TIMEOUT_SECS), child.wait()).await;
    if wait_result.is_err() {
        let _ = child.start_kill(); // timeout: reap the child (kill_on_drop also covers a later drop)
    }
    let stderr = drain.await.unwrap_or_default();
    let result: Result<(), PlayError> = match wait_result {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(ytdlp::classify(status.code(), &stderr)),
        Ok(Err(e)) => Err(PlayError { status: 502, reason: "extraction_failed".into(), message: "Could not fetch this trailer.".into(), detail: format!("ffmpeg wait: {e}") }),
        Err(_) => Err(PlayError { status: 504, reason: "timeout".into(), message: "This trailer took too long to fetch.".into(), detail: format!("ffmpeg exceeded {REMUX_TIMEOUT_SECS}s") }),
    };

    match &result {
        Ok(()) => {
            // Append ENDLIST so the app stops polling and the playlist is a finished, cacheable VOD.
            append_endlist(&dir).await;
            state.extract_fails.store(0, Ordering::Relaxed);
        }
        Err(e) => {
            eprintln!("[hls {vid}] remux failed: {} — {}", e.detail, stderr_tail(&stderr));
            // A systemic extraction fault drives the same /health signal as /play (BotGuard/nsig).
            if e.reason == "extraction_failed" {
                state.extract_fails.fetch_add(1, Ordering::Relaxed);
            }
            let sentinel = format!("{}\n{}\n{}", e.status, e.reason, e.message);
            let _ = tokio::fs::write(dir.join(".failed"), sentinel).await;
        }
    }

    // Clear our own job entry (only if still ours) so a later re-play re-evaluates from disk.
    {
        let mut jobs = state.hls_jobs.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(jobs.get(&vid), Some((g, _)) if *g == gen) {
            jobs.remove(&vid);
        }
    }
    if result.is_ok() {
        let cfg = state.cfg.clone();
        let _ = tokio::task::spawn_blocking(move || evict_hls_if_needed(&cfg)).await;
    }
}

fn stderr_tail(s: &str) -> String {
    let tail: String = s.chars().rev().take(200).collect::<Vec<_>>().into_iter().rev().collect();
    tail.replace('\n', " ").trim().to_string()
}

/// Append `#EXT-X-ENDLIST` to the event playlist (ffmpeg's `event` type doesn't write it) so the app
/// knows the VOD is complete and the file can be cached hard.
async fn append_endlist(dir: &Path) {
    let index = dir.join("index.m3u8");
    if let Ok(mut s) = tokio::fs::read_to_string(&index).await {
        if !s.contains("#EXT-X-ENDLIST") {
            if !s.ends_with('\n') {
                s.push('\n');
            }
            s.push_str("#EXT-X-ENDLIST\n");
            let _ = tokio::fs::write(&index, s).await;
        }
    }
}

/// Ensure an HLS stream exists (or is being produced) for `vid`, returning its dir once it's READY to
/// serve (init + first segment written). De-dupes concurrent callers onto one ffmpeg driver.
pub async fn ensure_hls(state: Arc<AppState>, vid: String) -> Result<PathBuf, PlayError> {
    let dir = hls_dir(&state, &vid);
    if is_complete(&dir).await {
        touch_dir(dir.clone());
        return Ok(dir);
    }
    let shared: SharedHls = {
        let mut jobs = state.hls_jobs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, existing)) = jobs.get(&vid) {
            existing.clone()
        } else {
            let gen = state.hls_gen.fetch_add(1, Ordering::Relaxed);
            let fut: BoxFuture<Result<(), PlayError>> = {
                let st = state.clone();
                let v = vid.clone();
                Box::pin(async move { start_remux(st, v, gen).await })
            };
            let shared = fut.shared();
            jobs.insert(vid.clone(), (gen, shared.clone()));
            shared
        }
    };
    shared.await.map(|()| dir)
}

/// Start a remux for `vid`: fresh working dir, resolve URLs, spawn ffmpeg under a concurrency permit,
/// hand the child to a detached driver, and return once the stream is READY. On an early error
/// (extraction / spawn) it clears its own job entry so the failure isn't cached as an in-flight job.
async fn start_remux(state: Arc<AppState>, vid: String, gen: u64) -> Result<(), PlayError> {
    let dir = hls_dir(&state, &vid);
    match start_remux_inner(&state, &vid, gen, &dir).await {
        // Spawn succeeded — the detached driver now owns cleanup. Wait for the first segment.
        Ok(()) => wait_ready(&dir).await,
        // Early error (mkdir / extraction / spawn): no driver will run to clear us, so drop the entry
        // now (generation-guarded) — a retry can re-attempt instead of re-awaiting a cached failure.
        Err(e) => {
            let mut jobs = state.hls_jobs.lock().unwrap_or_else(|e| e.into_inner());
            if matches!(jobs.get(&vid), Some((g, _)) if *g == gen) {
                jobs.remove(&vid);
            }
            Err(e)
        }
    }
}

/// The fallible setup up to (and including) spawning ffmpeg + its detached driver. Returns once the
/// driver is launched (which then owns the child + permit + job-map cleanup).
async fn start_remux_inner(state: &Arc<AppState>, vid: &str, gen: u64, dir: &Path) -> Result<(), PlayError> {
    let _ = tokio::fs::remove_dir_all(dir).await; // clear any partial from a prior aborted run
    tokio::fs::create_dir_all(dir).await.map_err(|e| PlayError {
        status: 503,
        reason: "cache_unavailable".into(),
        message: "Trailer cache is unavailable.".into(),
        detail: format!("mkdir {}: {e}", dir.display()),
    })?;

    let urls = cached_urls(state, vid).await?;
    let permit = state
        .download_sem
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| PlayError { status: 503, reason: "shutting_down".into(), message: "Server is restarting.".into(), detail: format!("sem: {e}") })?;

    let child = spawn_ffmpeg(&state.cfg, &urls, dir).map_err(PlayError::spawn)?;

    let st = state.clone();
    let v = vid.to_string();
    let d = dir.to_path_buf();
    tokio::spawn(async move { drive_remux(st, v, gen, d, child, permit).await });
    Ok(())
}

/// Bump a dir's mtime so HLS eviction sees it as recently used. Fire-and-forget.
fn touch_dir(dir: PathBuf) {
    tokio::task::spawn_blocking(move || {
        let _ = std::fs::File::open(&dir).and_then(|f| f.set_times(std::fs::FileTimes::new().set_accessed(SystemTime::now())));
    });
}

/// LRU-evict completed HLS dirs (those with an ENDLIST'd playlist) until under the byte cap. Skips
/// in-progress dirs (no ENDLIST) so a live remux is never deleted out from under its writer. Sync fs,
/// run off the runtime via spawn_blocking by the caller.
pub(crate) fn evict_hls_if_needed(cfg: &Config) {
    let root = cfg.cache_dir.join("hls");
    let entries = match std::fs::read_dir(&root) {
        Ok(rd) => rd,
        Err(_) => return,
    };
    let mut dirs: Vec<(PathBuf, u64, SystemTime, bool)> = Vec::new();
    for e in entries.flatten() {
        let p = e.path();
        if !p.is_dir() {
            continue;
        }
        let index = p.join("index.m3u8");
        let complete = std::fs::read_to_string(&index).map(|s| s.contains("#EXT-X-ENDLIST")).unwrap_or(false);
        let (mut size, mut atime) = (0u64, SystemTime::UNIX_EPOCH);
        if let Ok(rd) = std::fs::read_dir(&p) {
            for f in rd.flatten() {
                if let Ok(md) = f.metadata() {
                    size += md.len();
                    if let Ok(a) = md.accessed() {
                        atime = atime.max(a);
                    }
                }
            }
        }
        dirs.push((p, size, atime, complete));
    }
    let mut total: u64 = dirs.iter().map(|d| d.1).sum();
    if total <= cfg.cache_max_bytes {
        return;
    }
    dirs.sort_by_key(|d| d.2); // oldest atime first
    for (p, size, _, complete) in &dirs {
        if total <= cfg.cache_max_bytes {
            break;
        }
        if *complete && std::fs::remove_dir_all(p).is_ok() {
            total -= size;
        }
    }
}

/// The three file kinds served under `/hls/<vid>/`, each with the right content-type + caching. The
/// playlist is `no-cache` (it grows until ENDLIST); init/segments are immutable (a new extraction is a
/// new vid). Returns `None` for a filename that isn't one of ours (routing already validated it, but
/// this is the last guard against path traversal).
fn classify_file(file: &str) -> Option<(&'static str, &'static str)> {
    if file == "index.m3u8" {
        Some(("application/vnd.apple.mpegurl", "no-cache"))
    } else if file == "init.mp4" {
        Some(("video/mp4", "public, max-age=31536000, immutable"))
    } else if is_segment(file) {
        Some(("video/iso.segment", "public, max-age=31536000, immutable"))
    } else {
        None
    }
}

/// `seg_NNN.m4s` with exactly three digits — the only segment names ffmpeg writes with our pattern.
pub(crate) fn is_segment(file: &str) -> bool {
    file.len() == 11 // "seg_" + 3 digits + ".m4s"
        && file.starts_with("seg_")
        && file.ends_with(".m4s")
        && file[4..7].bytes().all(|b| b.is_ascii_digit())
}

/// Serve one HLS artifact for `vid`. The `index.m3u8` request is what kicks off (and awaits readiness
/// of) the remux; init/segment requests then just serve the files the driver is writing.
pub async fn handle_hls(state: Arc<AppState>, vid: String, file: String) -> Response<Body> {
    let Some((ctype, cache_control)) = classify_file(&file) else {
        return httputil::text(StatusCode::NOT_FOUND, "not found");
    };
    if !crate::play::cache_available(&state.cfg).await {
        return httputil::error(StatusCode::SERVICE_UNAVAILABLE, "cache_unavailable", "Trailer cache is unavailable.");
    }
    let dir = hls_dir(&state, &vid);

    if file == "index.m3u8" {
        if let Err(e) = ensure_hls(state.clone(), vid.clone()).await {
            let body = serde_json::json!({ "error": e.reason, "message": e.message, "id": vid });
            return httputil::json(StatusCode::from_u16(e.status).unwrap_or(StatusCode::BAD_GATEWAY), &body, &[("cache-control", "no-store")]);
        }
    }
    // Serve the file (playlist is ready after ensure_hls; init/segments exist once the app asks for
    // them, since it only requests names it saw in the playlist). A missing file → 404 (app retries).
    let fp = dir.join(&file);
    match tokio::fs::read(&fp).await {
        Ok(bytes) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", ctype)
            .header("content-length", bytes.len())
            .header("cache-control", cache_control)
            .body(httputil::full(bytes))
            .unwrap(),
        Err(_) => httputil::text(StatusCode::NOT_FOUND, "not ready"),
    }
}
