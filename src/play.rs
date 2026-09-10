//! PLAYBACK request path: ytId → cached faststart MP4 (yt-dlp + ffmpeg), served with HTTP range
//! support. A cached file is served instantly; a cold id downloads to completion first (prewarm at
//! /meta keeps the cache warm ahead of play, so cold is the exception).

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::{FutureExt, TryStreamExt};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::header::HeaderMap;
use hyper::{Response, StatusCode};
use tokio::io::AsyncReadExt;
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

/// How long a POSITIVE cache-availability answer stands before we probe the volume again.
const CACHE_OK_TTL_MS: u64 = 5_000;

/// Milliseconds since this process started. MONOTONIC, unlike `default_clock`.
///
/// The memo below is an absolute deadline, and comparing one against the wall clock means a
/// backward step — chrony past its slew threshold, a VM restored from a snapshot, a host correcting
/// after a dead RTC battery — holds the deadline in the future for the size of the step rather than
/// for five seconds. What that suppresses is not merely a check: `create_dir_all` is also the repair,
/// so a directory one call would have recreated stays missing, and none of those requests observe a
/// failure that would clear the memo. `Instant` cannot step.
fn monotonic_ms() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64
}

/// Is the on-disk cache usable? `create_dir_all` is idempotent, so this doubles as a self-healing
/// check — a volume that comes back after boot recovers without a restart. `/play` and `/crop` gate
/// on it to return a clean 503 instead of a murky 502.
///
/// Idempotent is not the same as free. `tokio::fs` is `spawn_blocking` underneath, so this was two
/// dispatches to the blocking pool and four syscalls on EVERY request — including the ones answered
/// entirely from memory, ahead of the disk hit, the failure cache and the crop caches. On a
/// single-threaded runtime that is two full task handoffs, which is an order of magnitude more than
/// everything those fast paths do put together.
///
/// So hold a positive answer for a few seconds. Only the positive one: a volume that has just been
/// seen to fail is re-probed on the very next request, so nothing delays recovery — which is the
/// property this function exists for. The exposure is the reverse case, a volume that disappears and
/// is not noticed for up to five seconds; the requests in that window fail on the read instead, which
/// they already have to handle.
/// The memo lives on the `Config` rather than in a static, because it is an answer ABOUT that
/// config's volume. Production has exactly one, so it behaves identically either way — but a static
/// would let one test's verdict about its own temp directory answer another test's question about a
/// different one.
pub async fn cache_available(cfg: &Config) -> bool {
    if monotonic_ms() < cfg.cache_ok_until.load(Ordering::Relaxed) {
        return true;
    }
    // Read the epoch BEFORE probing. Both calls below are `spawn_blocking` underneath, so each
    // yields the single runtime thread — long enough for a download to fail, conclude the volume is
    // gone and invalidate the memo. Publishing a verdict formed before that failure existed would
    // re-arm the memo for five seconds and reopen exactly the window the invalidation closes.
    let epoch = cfg.cache_epoch.load(Ordering::Relaxed);
    let ok = tokio::fs::create_dir_all(&cfg.cache_dir).await.is_ok()
        && tokio::fs::create_dir_all(&cfg.ytdlp_cache).await.is_ok();
    // A failure stores nothing, so it is not remembered and the next request asks again.
    // The deadline is measured from AFTER the probe, not before it: a slow mount could otherwise
    // return an already-expired deadline, and the memo would never take effect at all.
    if ok && cfg.cache_epoch.load(Ordering::Relaxed) == epoch {
        cfg.cache_ok_until.store(monotonic_ms() + CACHE_OK_TTL_MS, Ordering::Relaxed);
    }
    ok
}

/// Drop the memo, so the next `/play` or `/crop` probes the volume for real.
///
/// Called when a download fails for a reason that is OURS rather than YouTube's. Without it the memo
/// converted a volume that vanished into up to five seconds of requests that sail past the 503 gate
/// into `download_cached`, and each of those does lasting damage: the failure is pinned per-id for
/// two to five minutes, and the counter it moves reports as `extractor_unavailable` — sending the
/// operator to bump yt-dlp over a disk that went away. That mis-routing is the exact thing the
/// local/extraction split exists to prevent. One observed local failure now costs one request.
pub(crate) fn invalidate_cache_availability(cfg: &Config) {
    // Epoch first, then the deadline. A probe suspended in its awaits checks the epoch before
    // publishing, so bumping it is what stops that probe from undoing this.
    cfg.cache_epoch.fetch_add(1, Ordering::Relaxed);
    cfg.cache_ok_until.store(0, Ordering::Relaxed);
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
            // ...unless it is not actually a trailer. `fs::metadata` FOLLOWS symlinks, which
            // `DirEntry::metadata` does not — and the serve path follows, so anything else here
            // would unlink a symlinked trailer that plays perfectly well.
            if !std::fs::metadata(entry.path()).map(|m| m.is_file()).unwrap_or(true) {
                // Say what happened, not what was intended: a removal that fails (EACCES on a
                // directory a root-run container left in a volume we read as nonroot, EROFS, or a
                // node where remove_dir_all returns ENOTDIR) otherwise logs success hourly while
                // every request for that id keeps re-downloading and failing at the rename.
                match std::fs::remove_dir_all(entry.path()) {
                    Ok(()) => eprintln!("sweep: removed {name}, which was not a trailer"),
                    Err(e) => eprintln!("sweep: {name} is not a trailer and cannot be removed: {e}"),
                }
            }
            continue;
        }
        // Directories are not ours to remove — yt-dlp's own cache lives in one here. Except one at
        // a PUBLISHED trailer's path, which nothing else will ever clear: the cache-hit check now
        // rejects it (a directory reports a non-zero length), so every request for that id
        // re-downloads and then fails at the rename, forever, dragging /health with it.
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

/// What the volume held when eviction last looked, so `/stats` can answer without walking the cache
/// directory on a request. Measured after any eviction, so it describes the state we left behind.
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheUsage {
    pub trailer_bytes: u64,
    pub trailer_count: u64,
    /// In-flight partials and anything else not named `<vid>.mp4`. Counted against the cap but never
    /// evictable here; `sweep_partials` reclaims it once it is provably abandoned.
    pub scratch_bytes: u64,
}

/// Evict least-recently-used cached files until under the size cap (bounded cache). Sync fs, run
/// off the runtime thread via spawn_blocking. Skips dotfiles so an in-progress `.<vid>.…partial.mp4`
/// is neither counted nor deleted out from under its writer; `sweep_partials` reclaims stale ones.
///
/// Returns what it saw, or `None` if the cache directory could not be read at all. It already walks
/// the directory, so reporting the totals costs nothing and spares `/stats` from doing it again on
/// the request path.
pub(crate) fn evict_if_needed(cfg: &Config) -> Option<CacheUsage> {
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
        Err(_) => return None,
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
        return Some(usage(&files, scratch_bytes));
    };
    let mut total: u64 = files.iter().map(|f| f.1).sum();
    if total <= budget {
        return Some(usage(&files, scratch_bytes));
    }
    files.sort_by_key(|f| f.2); // oldest atime first
    let mut evicted = 0usize;
    for (p, size, _) in &files {
        if total <= budget {
            break;
        }
        if std::fs::remove_file(p).is_ok() {
            total -= size;
            evicted += 1;
        }
    }
    // Report what SURVIVED. `files` still lists the evicted ones, and the sort put them first.
    Some(CacheUsage { trailer_bytes: total, trailer_count: (files.len() - evicted) as u64, scratch_bytes })
}

/// Totals for a directory listing nothing was evicted from.
fn usage(files: &[(PathBuf, u64, SystemTime)], scratch_bytes: u64) -> CacheUsage {
    CacheUsage {
        trailer_bytes: files.iter().map(|f| f.1).sum(),
        trailer_count: files.len() as u64,
        scratch_bytes,
    }
}

/// How long a failed `/play` stands before we spend another download permit on the same id.
///
/// Reason-aware because a single TTL is wrong in both directions. "Removed by the uploader" is a
/// fact about the world that will not change this afternoon, and re-asking it every request costs a
/// yt-dlp process and one of three permits. A timeout is the opposite: it is the reason that burns a
/// permit for the full DOWNLOAD_TIMEOUT_SECS, and also the one most likely to be a slow network
/// rather than a dead video — pinning it for hours would turn a bad minute into a dead trailer.
///
/// So: facts cache long, verdicts about YouTube's mood cache briefly, and a local fault caches just
/// long enough to stop a hot loop while the operator fixes the disk.
pub(crate) fn fail_ttl_ms(reason: &str) -> u64 {
    match reason {
        // The video is gone or shut to us. Nothing we retry changes that — but an hour, not the six
        // this wants to be, because of what happens if the classification is wrong. `classify` routes
        // anything whose stderr says "video unavailable" here, and that string is not exclusive to a
        // removed video: it is also what a REJECTED EXTRACTOR gets told, which is a whole-library
        // event rather than a per-video one. This bucket is also the one /health is explicitly built
        // to ignore, and nothing flushes the map short of a restart — so a misread here is a green
        // /health over an empty service for as long as the TTL says. An hour still removes
        // essentially every repeat extraction within a browsing session; six would price one
        // misclassification at most of an evening.
        "unavailable" | "restricted" => 60 * 60 * 1000,
        // A region block can lift, and the client has alternates to try meanwhile.
        "geo_blocked" => 30 * 60 * 1000,
        // A systemic extractor outage ends when yt-dlp is bumped — which is a redeploy, so this map
        // is gone anyway. Kept short so a recovery inside one process is visible quickly.
        "extraction_failed" => 5 * 60 * 1000,
        // Ours, not YouTube's: a full volume, a killed bake. Retrying fast helps nobody, but the fix
        // can land without a restart, so do not sit on it.
        "incomplete_download" => 2 * 60 * 1000,
        // Long enough to stop one stuck id monopolising a permit every request, short enough that a
        // network that comes back is served within the minute.
        "timeout" => 60 * 1000,
        // A shape we do not recognise: assume the least and re-ask soon.
        _ => 60 * 1000,
    }
}

/// The still-standing failure for `vid`, if any. Cheap: one lock, one lookup, off the hot path for
/// every cache hit (which returns before this).
fn cached_failure(state: &AppState, vid: &str, now: u64) -> Option<PlayError> {
    let map = state.play_fails.lock().unwrap_or_else(|e| e.into_inner());
    map.get(vid).filter(|(_, exp)| *exp > now).map(|(e, _)| e.clone())
}

/// Remember why this id failed, so the next request answers from memory instead of from yt-dlp.
///
/// The stderr tail is dropped on the way in. It was already logged once, in full, by the request
/// that actually failed; keeping it would re-log a stale extractor message on every subsequent hit
/// and hold up to 300 bytes per entry for the privilege.
pub(crate) fn record_failure(state: &AppState, vid: &str, e: &PlayError) {
    let now = (state.clock)();
    let ttl = fail_ttl_ms(&e.reason);
    let mut map = state.play_fails.lock().unwrap_or_else(|e| e.into_inner());
    if map.len() >= crate::PLAY_FAIL_MAX {
        map.retain(|_, (_, exp)| *exp > now);
        // Still full of live entries. This map is an optimisation, not an answer — dropping it costs
        // one repeated download per id and nothing else — so take the O(n) clear rather than carry
        // machinery to evict the nearest-to-expiry.
        if map.len() >= crate::PLAY_FAIL_MAX {
            map.clear();
        }
    }
    let compact = PlayError {
        status: e.status,
        reason: e.reason.clone(),
        message: e.message.clone(),
        detail: format!("cached {} for {}ms", e.reason, ttl),
    };
    map.insert(vid.to_string(), (compact, now + ttl));
}

/// Forget a failure once the id has actually produced a trailer.
fn clear_failure(state: &AppState, vid: &str) {
    let mut map = state.play_fails.lock().unwrap_or_else(|e| e.into_inner());
    map.remove(vid);
}

/// What `demote_known_dead` learned while it was holding the lock, so the caller does not have to
/// take it again to ask a second question about the same map.
#[derive(Clone, Copy, Debug, Default)]
pub struct Demotion {
    /// The sort actually MOVED something — so this response's order reflects a signal whose
    /// shortest life is 60 seconds, and must not be cached for a week.
    ///
    /// Not merely "something is dead". A list that is already in the right order — a live candidate
    /// ahead of a dead one — produces a body byte-for-byte identical to the one the untouched path
    /// would emit, and downgrading that to `max-age=3600` makes every client re-ask 168 times more
    /// often for an answer that cannot have changed.
    pub reordered: bool,
    /// The candidate the client will play first is itself dead, which after the sort means they all
    /// are. Nothing here is worth a speculative download.
    pub head_dead: bool,
}

/// Move candidates we currently know are unplayable behind the ones that might work, preserving
/// relative order otherwise (`sort_by_key` is stable, and `false` sorts before `true`).
///
/// This is the only playability signal the discovery path has — `/meta` deliberately does not probe,
/// so a dead candidate stays first in TMDB's rank order and every client rediscovers it one at a
/// time. Applied at RESPONSE time rather than at resolve time on purpose: the resolve is cached for
/// 24h, while a geo-block can lift inside that, so the stored order must stay the upstream's.
///
/// One lock for the whole thing, and an early return when nothing has ever failed — which is the
/// normal case, and the one that must cost nothing. The answers the caller needs come back with it
/// rather than being asked for separately, which is a second lock and a second clock read on a path
/// that runs per request.
pub fn demote_known_dead(state: &AppState, ids: &mut [String]) -> Demotion {
    if ids.is_empty() {
        return Demotion::default();
    }
    let map = state.play_fails.lock().unwrap_or_else(|e| e.into_inner());
    // The empty check first, so the normal case — nothing has ever failed — really does cost
    // nothing, rather than a clock read it is about to throw away.
    if map.is_empty() {
        return Demotion::default();
    }
    let now = (state.clock)();
    let is_dead = |id: &str| map.get(id).is_some_and(|(_, exp)| *exp > now);
    let Some(first_dead) = ids.iter().position(|id| is_dead(id)) else {
        return Demotion::default();
    };
    // Would the stable sort actually move anything? Only if a live candidate sits behind a dead one.
    // Everything already in order comes out identical, and saying otherwise costs the response six
    // days of cacheability for nothing.
    let reordered = ids[first_dead..].iter().any(|id| !is_dead(id));
    if reordered {
        ids.sort_by_key(|id| is_dead(id));
    }
    Demotion { reordered, head_dead: is_dead(&ids[0]) }
}

/// Drop a FINISHED download from the in-flight map, so the next `fetch_trailer` starts a new one
/// instead of being handed the old one's answer.
///
/// The evicted-mid-serve retry could not work without this. The driver task clears the entry only
/// after its own `await` returns, and every waiter wakes on that same completion — so a request that
/// re-entered on the driver's heels joined the still-present entry, got the same already-resolved
/// future, was handed the same path that had just been evicted from under it, and fell through to a
/// 500. The one retry in the serve path was dead code in exactly the case it exists for.
///
/// `peek` is what makes this safe to do unconditionally: it is `Some` only once the shared future
/// has produced a value, so a download that is genuinely still running is never disturbed and
/// concurrent callers keep de-duplicating onto it.
fn drop_if_finished(state: &AppState, vid: &str) {
    let mut map = state.in_flight.lock().unwrap_or_else(|e| e.into_inner());
    if map.get(vid).is_some_and(|(_, shared)| shared.peek().is_some()) {
        map.remove(vid);
    }
}

/// Download+mux a faststart MP4 for `vid`, cached. De-dupes concurrent requests via `in_flight`:
/// the first caller creates one shared download, everyone else awaits it.
pub async fn fetch_trailer(state: Arc<AppState>, vid: String) -> Result<PathBuf, PlayError> {
    fetch_trailer_inner(state, vid, true).await
}

/// [`fetch_trailer`] for a caller that has just tried to open the file and found it absent.
///
/// Skips the disk check, because that caller already paid for it. `/play` tries `open_for_serve`
/// first and only lands here when it failed, so asking `metadata` for the same answer was a second
/// dispatch to the blocking pool on every cold play — and on every failure-cache hit, which is the
/// path that is supposed to cost a hash lookup and nothing else.
///
/// The window this gives up: if the file appears between that open and this call, the disk check
/// would have caught it and now the `in_flight` join has to. It nearly always does, since the entry
/// outlives the download it describes; when it does not, the cost is one redundant download that
/// publishes by atomic rename over an identical file.
pub(crate) async fn fetch_trailer_cold(state: Arc<AppState>, vid: String) -> Result<PathBuf, PlayError> {
    fetch_trailer_inner(state, vid, false).await
}

async fn fetch_trailer_inner(
    state: Arc<AppState>,
    vid: String,
    check_disk: bool,
) -> Result<PathBuf, PlayError> {
    // Checked here as well as at the routes, because `vid` also arrives from TMDB/KinoCheck via
    // prewarm, and it becomes a filename and a yt-dlp -o path. One `..` writes outside the cache.
    if !crate::is_valid_vid(&vid) {
        // Logged at the point of rejection: this one never reaches the download future that does the
        // logging for every other failure.
        eprintln!("[{vid}] rejected: not a YouTube id");
        return Err(PlayError {
            status: 400,
            reason: "bad_id".into(),
            message: "Not a YouTube id.".into(),
            detail: format!("rejected vid {vid:?}"),
        });
    }
    let fp = cache_path(&state.cfg, &vid);
    if check_disk {
        if let Ok(md) = tokio::fs::metadata(&fp).await {
            // is_file, not just non-empty: a directory reports a non-zero length, so anything that
            // left one at a trailer's path was served as a cache hit that could never produce bytes.
            if md.is_file() && md.len() > 0 {
                // Unconditionally, and that is load-bearing rather than lazy.
                //
                // This touch is the ONLY thing that makes atime mean "recently served" — under
                // relatime a read does not refresh it — and `evict_if_needed`'s size-cap pass sorts
                // on exactly that field, oldest first, after every completed download. Stamping
                // `now` on every request is what makes the file currently streaming the freshest
                // thing on the volume, and therefore structurally the last candidate for eviction.
                //
                // Two attempts to skip it when the stamp was "recent enough" both broke that. The
                // gate reads "touch only if atime is ALREADY stale", so a trailer downloaded at T
                // and played at T+5s is never touched at all: it keeps its download-time stamp
                // while prewarms landing behind it get fresher ones, and the next eviction takes
                // the one being watched. Any non-zero threshold admits that.
                //
                // `/play` reaches its serves through `open_for_serve` instead, which stamps atime
                // on the handle it already holds; this path is what `/crop` and prewarm come
                // through, and they read the file too.
                touch_atime(fp.clone());
                return Ok(fp);
            }
        }
    }

    // A failure we already paid for. AFTER the disk check, so a file that arrived by any other route
    // still wins, and BEFORE the in-flight join, so a request for a removed video costs a hash
    // lookup instead of a download permit and a yt-dlp process. Joining a download that is genuinely
    // running is still the right thing, which is why this sits between the two.
    if let Some(e) = cached_failure(&state, &vid, (state.clock)()) {
        return Err(e);
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
        } else if map.len() >= crate::IN_FLIGHT_MAX {
            // Only a NEW id is refused. Joining a download already in flight costs nothing and is
            // exactly what the de-duplication is for, so a viewer waiting on a trailer someone else
            // triggered is never turned away by this.
            let outstanding = map.len();
            // Guard released BEFORE logging. Once the map is full every new id takes this branch, so
            // the line is request-driven and unbounded — and stderr is a pipe someone else drains.
            // A write that blocks would block it holding this mutex, stalling every /play, /crop and
            // prewarm on a single-threaded runtime; a write that fails would panic with the guard
            // held. Neither is worth risking to save a `drop`.
            drop(map);
            eprintln!("[{vid}] refused: {outstanding} downloads already outstanding");
            return Err(PlayError::overloaded());
        } else {
            let gen = state.dl_gen.fetch_add(1, Ordering::Relaxed);
            let fut: BoxFuture<Result<PathBuf, PlayError>> = {
                let st = state.clone();
                let v = vid.clone();
                // Record the verdict INSIDE the shared future, not in the driver task below. Both
                // are woken by the same completion, so a waiter that re-entered on the driver's
                // heels could miss a record that had not run yet — and re-enter is exactly what the
                // evicted-mid-serve retry does. Here it is ordered: anyone who can observe the
                // result can observe the record. Runs once, because `Shared` polls the inner future
                // once however many waiters there are.
                Box::pin(async move {
                    let out = download_cached(st.clone(), v.clone(), gen).await;
                    match &out {
                        Ok(_) => clear_failure(&st, &v),
                        Err(e) => {
                            // Logged HERE, once per download that actually happened, rather than at
                            // every request that observes the result. `record_failure` drops the
                            // stderr tail precisely so a stale extractor message is not re-logged on
                            // every later hit — but the line itself was still being written by the
                            // serve path, so an id inside a one-hour window produced a syscall and a
                            // log line per request saying the same thing. Repeats are supposed to be
                            // free; this is the last part of them that was not.
                            eprintln!("[{v}] {}", e.detail);
                            record_failure(&st, &v, e);
                        }
                    }
                    out
                })
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
    let tmp = state.cfg.cache_dir.join(format!(".{vid}.{}.{gen}.partial.mp4", std::process::id()));

    // Global cap on concurrent downloads (bounds CPU/disk/fd for a burst of distinct ids).
    let _permit = state.download_sem.acquire().await;

    if let Err(e) = ytdlp::download_to(&state.cfg, &vid, &tmp).await {
        remove_temp_set(&state.cfg, &tmp).await;
        // Re-probe the volume on the next request. Deliberately on ANY failure, not just the ones
        // classified local: a cache dir that has gone away makes yt-dlp fail on its own `-o` path
        // with a message `classify` does not recognise, so the very case this exists to catch
        // arrives wearing `extraction_failed`. Getting it wrong the other way costs one pair of
        // idempotent create_dir_all calls after a failed download, which is nothing.
        invalidate_cache_availability(&state.cfg);
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
            invalidate_cache_availability(&state.cfg);
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
        invalidate_cache_availability(&state.cfg);
        PlayError::incomplete(format!("rename {}: {e}", tmp.display()))
    })?;
    // Cleared only once a trailer is actually in the cache — before the rename it was cleared by a
    // download that could still fail.
    state.local_fails.store(0, std::sync::atomic::Ordering::Relaxed);

    let cfg = state.cfg.clone();
    if let Ok(Some(u)) = tokio::task::spawn_blocking(move || evict_if_needed(&cfg)).await {
        state.record_cache_usage(u);
    }
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

/// A cached trailer, opened and ready to serve: everything the response needs from the filesystem,
/// obtained in ONE blocking call.
struct Opened {
    file: std::fs::File,
    size: u64,
    /// Already resolved against the real size, and already seeked to when satisfiable.
    range: Option<RangeReq>,
}

/// Open a cached trailer for serving — open, fstat, reject anything that is not a real file, bump
/// atime, resolve the Range header and seek to it — in a single `spawn_blocking`.
///
/// Serving a warm file used to cost four or five dispatches to the blocking pool: `metadata` in
/// `fetch_trailer`, an `open` + `set_times` for the atime touch, then `File::open` and `metadata`
/// again here, plus a `seek` on a Range request. On a current-thread runtime each of those is a full
/// task handoff, and one playback is many range requests. Every one of them is answering a question
/// about the same file, so ask once.
///
/// `set_times` goes through the handle we already hold rather than opening the path again, and the
/// `is_file` check that used to live in `fetch_trailer` comes along for free — `open` on a directory
/// succeeds on Linux, so something has to reject it, and the fstat is right here.
fn open_for_serve(fp: &Path, range: Option<&str>) -> std::io::Result<Opened> {
    use std::io::{Seek, SeekFrom};

    let mut file = std::fs::File::open(fp)?;
    let md = file.metadata()?;
    if !md.is_file() || md.len() == 0 {
        // A directory reports a non-zero length, so anything that left one at a trailer's path would
        // otherwise be served as a cache hit that can never produce bytes.
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "not a published trailer"));
    }
    // Best-effort: the LRU wants to know this was served, but failing to record it must not fail the
    // playback. See `evict_if_needed` for why the stamp matters.
    let _ = file.set_times(std::fs::FileTimes::new().set_accessed(SystemTime::now()));

    let size = md.len();
    let parsed = parse_range(range, size);
    if let Some(RangeReq::Satisfiable { start, .. }) = parsed {
        file.seek(SeekFrom::Start(start))?;
    }
    Ok(Opened { file, size, range: parsed })
}

/// Run [`open_for_serve`] off the runtime thread. `None` means "not servable" — usually simply not
/// cached yet, which is the normal cold path and not worth a log line.
async fn try_open(cfg: &Config, vid: &str, range: Option<&str>) -> Option<Opened> {
    let fp = cache_path(cfg, vid);
    let range = range.map(str::to_string);
    let opened = tokio::task::spawn_blocking(move || open_for_serve(&fp, range.as_deref())).await;
    match opened {
        Ok(Ok(o)) => Some(o),
        // A cold id is the common case and says nothing; anything else is worth seeing.
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
        Ok(Err(e)) => {
            eprintln!("[{vid}] open for serve: {e}");
            None
        }
        Err(e) => {
            eprintln!("[{vid}] open for serve panicked: {e}");
            None
        }
    }
}

/// Build the response for an already-opened trailer. Always answers Content-Length + Accept-Ranges
/// (+206 on Range) — which tvOS AVPlayer REQUIRES for a progressive MP4.
///
/// Pure: no I/O, no awaits. Everything it needs was settled by `open_for_serve`.
fn serve_opened(opened: Opened, vid: &str) -> Response<Body> {
    let Opened { file, size, range } = opened;
    let file = tokio::fs::File::from_std(file);
    // The cached MP4 for a given id is byte-stable + immutable (a new extraction would be a new id),
    // so it can be cached hard. A strong ETag from id+size lets a caller/proxy revalidate cheaply.
    let etag = httputil::etag_of(format!("{vid}:{size}").as_bytes());

    match range {
        Some(RangeReq::Unsatisfiable) => Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header("content-range", format!("bytes */{size}"))
            .body(httputil::full(""))
            .unwrap(),
        Some(RangeReq::Satisfiable { start, end }) => {
            // Already seeked, in the same blocking call that opened it.
            let len = end - start + 1;
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
    }
}

/// What is LEFT of this id's failure window, in ms. `None` when nothing is standing — the entry was
/// dropped under `PLAY_FAIL_MAX` pressure, or this is a reason we do not cache.
fn remaining_fail_ms(state: &AppState, vid: &str) -> Option<u64> {
    let now = (state.clock)();
    let map = state.play_fails.lock().unwrap_or_else(|e| e.into_inner());
    map.get(vid).map(|(_, exp)| exp.saturating_sub(now)).filter(|r| *r > 0)
}

/// Typed /play failure body (geo_blocked 451 / restricted 403 / unavailable 404 / 502).
///
/// `Retry-After` is what is LEFT of the failure window, not the full TTL. The full TTL is only
/// correct for the first response — the one where the extraction actually happened. Every later
/// request is answered from the cache, and recomputing the whole TTL there tells a client asking at
/// 5h59m of a 6h window to wait another six hours: up to twice the real cooldown, and unbounded if
/// it keeps polling. The remainder is right there in the entry, so use it.
fn play_error(state: &AppState, vid: &str, e: &PlayError) -> Response<Body> {
    let body = serde_json::json!({ "error": e.reason, "message": e.message, "id": vid });
    // No entry means nothing is being cached for this id, so the full TTL is the honest estimate of
    // when asking again could help. Never zero: a client reading `Retry-After: 0` will come straight
    // back, which is the one answer that is never useful here.
    let ms = remaining_fail_ms(state, vid).unwrap_or_else(|| fail_ttl_ms(&e.reason));
    // Round UP. `record_failure` and this read take separate millisecond clock readings, so dividing
    // down reports one second short whenever the millisecond ticks between them — and a client that
    // comes back a second early finds the window still closed. Rounding up can only ever be right.
    let retry_after = ms.div_ceil(1000).max(1).to_string();
    httputil::json(
        StatusCode::from_u16(e.status).unwrap_or(StatusCode::BAD_GATEWAY),
        &body,
        &[("retry-after", &retry_after)],
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

    // Try to serve first, ask questions later. The overwhelmingly common request is for a trailer
    // that is already cached — prewarm exists to make it so — and this reaches it in ONE dispatch to
    // the blocking pool. Going through `fetch_trailer` first meant a stat and an atime touch before
    // the open and fstat that actually serve, all to establish what a single open would have told us.
    if let Some(opened) = try_open(&state.cfg, &vid, range.as_deref()).await {
        return serve_opened(opened, &vid);
    }

    // Not servable: cold, or evicted out from under us. Two attempts, because the file can be
    // evicted again between the download completing and us opening it.
    for attempt in 0..2 {
        // `_cold`: the open above already asked the disk and it said no. Going through the checking
        // variant asked the same question a second time, on every cold play and — worse — on every
        // request the failure cache answers, which is meant to cost a hash lookup and nothing else.
        if let Err(e) = fetch_trailer_cold(state.clone(), vid.clone()).await {
            // Not logged here: a failure is logged once by the download that produced it, and this
            // arm is also reached by every request the failure cache answers from memory.
            return play_error(&state, &vid, &e);
        }
        if let Some(opened) = try_open(&state.cfg, &vid, range.as_deref()).await {
            return serve_opened(opened, &vid);
        }
        // Retire the finished entry before retrying, or `fetch_trailer` joins it and hands back the
        // same success for the file that just vanished.
        if attempt == 0 {
            drop_if_finished(&state, &vid);
        }
    }
    httputil::text(StatusCode::INTERNAL_SERVER_ERROR, "serve failed")
}
