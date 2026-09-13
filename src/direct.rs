//! DIRECT URLS: hand a browser the googlevideo URL itself, instead of downloading the trailer and
//! proxying it back.
//!
//! `/play` exists because the Apple TV cannot do this. It downloads, muxes a faststart MP4, bakes the
//! `clap` crop and serves the bytes — minutes of yt-dlp and ffmpeg, a 4 GB cache volume, and the
//! trailer crossing this box twice. For a muted picture running behind a billboard that is a great
//! deal of machinery, and all of it is spent because AVPlayer needs one file with sound in it.
//!
//! A browser does not. So: resolve with yt-dlp (`--print`, no download — the `--simulate` class of
//! work, which is why it takes a probe permit rather than a download one) and answer with the URLs.
//! The page streams them from Google directly, at full height, with no wait and no bytes through here.
//!
//! **The URLs work away from this server.** They carry `ip=<this box>` inside the signed `sparams`
//! set, which reads like an IP binding and is why `/play` proxies — but Google does not enforce it:
//! a URL resolved here plays from an unrelated address. (Verified against both `videoplayback` and
//! `manifest.googlevideo.com`.) They do expire, in about six hours, which is what `expires` and the
//! response's `max-age` are for.
//!
//! **Two streams, usually.** YouTube still lists the muxed itag 18, but no longer serves it — every
//! current answer is adaptive, so `video` is video-only and `audio` is a separate track. A muted
//! surface can ignore `audio` entirely, and a `<video>` element cannot combine the two at all.
//!
//! **Which is what `hls` is for.** YouTube also publishes an HLS master playlist carrying video,
//! audio and subtitles as renditions, adaptive across every height — one URL, and the only way a
//! browser gets the trailer's SOUND. WebKit plays it natively from a bare `<video>`, which covers
//! every browser on iOS. Elsewhere it needs MSE, and googlevideo sends no `Access-Control-Allow-Origin`,
//! so hls.js cannot fetch the segments: Chrome and Firefox take the progressive `video` instead, and
//! anything there that wants sound stays on `/play`. AetherEngine takes one source per session with
//! no external-audio API, so the Apple TV keeps `/play` throughout.
//!
//! **No crop.** The `clap` box is baked into the cached MP4 by the download path, so a trailer with
//! baked-in letterbox keeps its bars here. `/crop` cannot help — it reads the downloaded file.

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Response, StatusCode};
use serde_json::json;
use tokio::process::Command;

use crate::config::Config;
use crate::httputil::{self, Body};
use crate::state::AppState;
use crate::ytdlp::{classify, PlayError};

/// yt-dlp `--print` is a metadata round-trip, not a download; well under the probe's own backstop.
const RESOLVE_TIMEOUT_SECS: u64 = 30;

/// Stop handing out a URL this long before it expires, so a page that starts playing on the last
/// answer still has time to finish loading it.
const EXPIRY_MARGIN_MS: u64 = 5 * 60 * 1000;

/// What to cache when the URL carries no expiry we can read. Short: an unreadable expiry means the
/// shape changed, and serving a dead URL for an hour is worse than resolving again in five minutes.
const UNKNOWN_EXPIRY_TTL_MS: u64 = 5 * 60 * 1000;

/// Bound on the resolve cache. Same reasoning as `PLAY_FAIL_MAX`: every entry is an optimisation,
/// each expires on its own, and losing one costs a single yt-dlp run.
pub const DIRECT_CACHE_MAX: usize = 512;

/// A resolved set of direct URLs, and when they stop working.
#[derive(Clone, Debug)]
pub struct Direct {
    /// The video stream — video-only whenever YouTube answers adaptively, which is now always.
    pub video: String,
    /// The separate audio track, absent only if a muxed format did come back.
    pub audio: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    /// YouTube's own HLS master playlist: one URL carrying video, audio and subtitles as separate
    /// renditions, adaptive across every height. WebKit plays it natively from a bare `<video>` —
    /// which is every browser on iOS — and it is the only way a browser gets the trailer's SOUND,
    /// since the progressive pair above cannot be combined by a `<video>` element.
    pub hls: Option<String>,
    /// Epoch ms after which the URLs stop working, as far as we could read it.
    pub expires: u64,
}

/// One cached answer: a resolve that worked, or the reason it did not, and when to ask again.
pub type CachedDirect = (Result<Direct, PlayError>, u64);

/// Read `expire=<unix seconds>` out of a googlevideo URL. It appears as a query parameter on
/// `videoplayback` and as a `/expire/<n>/` path segment on `manifest.googlevideo.com`, so both
/// shapes are accepted. Returns epoch MILLISECONDS, to match every other clock here.
pub fn parse_expiry_ms(url: &str) -> Option<u64> {
    let secs = url
        .split(['?', '&'])
        .skip(1)
        .find_map(|p| p.strip_prefix("expire="))
        .or_else(|| {
            let at = url.find("/expire/")?;
            url[at + "/expire/".len()..].split('/').next()
        })
        .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))?;
    secs.parse::<u64>().ok()?.checked_mul(1000)
}

/// The soonest expiry across every URL in the answer — an answer is only good while ALL of its
/// streams are, since a page playing video with a dead audio track is not playing the trailer.
fn earliest_expiry(urls: &[&str], now: u64) -> u64 {
    urls.iter().filter_map(|u| parse_expiry_ms(u)).min().unwrap_or(now + UNKNOWN_EXPIRY_TTL_MS)
}

/// Parse yt-dlp's two `--print` lines: `"<width> <height>"`, then one URL per selected format.
/// yt-dlp writes `NA` for a field it does not know, which is not a failure — an answer with no
/// dimensions is still a playable URL.
pub fn parse_resolve(stdout: &str, now: u64) -> Option<Direct> {
    let mut lines = stdout.lines().map(str::trim).filter(|l| !l.is_empty());
    let dims = lines.next()?;
    let (mut urls, mut hls): (Vec<&str>, Option<String>) = (Vec::new(), None);
    for line in lines {
        if line.starts_with("https://") {
            urls.push(line);
        } else if hls.is_none() {
            hls = first_url_in(line).map(str::to_string);
        }
    }
    let (video, audio) = match urls.as_slice() {
        // One URL is a muxed format — rare now, but it is still what the ladder's last rung asks for.
        [only] => (only.to_string(), None),
        // yt-dlp prints the selected formats in the order the selector named them: video, then audio.
        [video, audio, ..] => (video.to_string(), Some(audio.to_string())),
        [] => return None,
    };
    let mut dim = dims.split_whitespace().map(|v| v.parse::<u32>().ok());
    // The HLS master expires on its own clock, and an answer is only good while everything in it is.
    let mut dated = urls.clone();
    if let Some(master) = hls.as_deref() {
        dated.push(master);
    }
    Some(Direct {
        expires: earliest_expiry(&dated, now),
        video,
        audio,
        hls,
        width: dim.next().flatten(),
        height: dim.next().flatten(),
    })
}

/// The first `https://…` held in a line that is not itself a URL.
///
/// `--print "%(formats.:.manifest_url)s"` answers with a Python-style list, most of whose entries are
/// `None` — only the HLS formats carry one, and every one of them names the same master playlist, so
/// the first is the answer. Terminated on the quote or separator that closes the entry.
fn first_url_in(line: &str) -> Option<&str> {
    let rest = &line[line.find("https://")?..];
    Some(&rest[..rest.find(['\'', '"', ',', ']', ' ']).unwrap_or(rest.len())])
}

/// Ask yt-dlp for the URLs, without downloading anything.
///
/// The format string is `cfg.ytdlp_format` — the very same ladder `/play` extracts with. That is
/// deliberate: it pins avc1 + mp4a under `MAX_HEIGHT`, which is what a browser's own hardware
/// decoder wants for the same reasons AVPlayer does, and it means this endpoint cannot start
/// answering with a VP9/AV1 stream that only some browsers can play.
pub async fn resolve(cfg: &Config, vid: &str, now: u64) -> Result<Direct, PlayError> {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--no-warnings",
        "--no-playlist",
        "--socket-timeout",
        "15",
        "--cache-dir",
        &cache, // the same nsig/player-JS work a probe or a download already paid for
        "-f",
        &cfg.ytdlp_format,
        "--print",
        "%(width)s %(height)s",
        "--print",
        "urls",
        // The progressive selection above carries no manifest of its own, so this is what makes one
        // run answer both transports instead of two. Every HLS format names the same master.
        "--print",
        "%(formats.:.manifest_url)s",
        &format!("https://www.youtube.com/watch?v={vid}"),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped()) // captured, never swallowed: `classify` reads it to say WHY
    .kill_on_drop(true);
    if let Some(ea) = &cfg.ytdlp_extractor_args {
        cmd.args(["--extractor-args", ea]);
    }
    let out = match tokio::time::timeout(Duration::from_secs(RESOLVE_TIMEOUT_SECS), cmd.output()).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            return Err(PlayError {
                status: 502,
                reason: "extraction_failed".into(),
                message: "Could not fetch this trailer.".into(),
                detail: format!("spawn yt-dlp: {e}"),
            })
        }
        Err(_) => {
            return Err(PlayError {
                status: 504,
                reason: "timeout".into(),
                message: "This trailer took too long to resolve.".into(),
                detail: format!("yt-dlp --print exceeded {RESOLVE_TIMEOUT_SECS}s"),
            })
        }
    };
    if !out.status.success() {
        return Err(classify(out.status.code(), &String::from_utf8_lossy(&out.stderr)));
    }
    // Exit 0 with nothing we can use is not an extraction failure — the extractor worked and we
    // could not read it — so it does not wear a reason that sends the operator to bump yt-dlp.
    parse_resolve(&String::from_utf8_lossy(&out.stdout), now).ok_or_else(|| PlayError {
        status: 502,
        reason: "no_direct_url".into(),
        message: "Could not fetch this trailer.".into(),
        detail: format!("yt-dlp exited 0 with no usable URL: {}", crate::ytdlp::stderr_tail(&out.stdout)),
    })
}

/// The still-standing cached answer for `vid`, if it has not expired.
fn cached(state: &AppState, vid: &str, now: u64) -> Option<Result<Direct, PlayError>> {
    let map = state.direct_cache.lock().unwrap_or_else(|e| e.into_inner());
    map.get(vid).filter(|(_, exp)| *exp > now).map(|(r, _)| r.clone())
}

/// Remember an answer until `exp`. Bounded exactly like `play_fails`, and for the same reason.
fn remember(state: &AppState, vid: &str, entry: CachedDirect, now: u64) {
    let mut map = state.direct_cache.lock().unwrap_or_else(|e| e.into_inner());
    if map.len() >= DIRECT_CACHE_MAX {
        map.retain(|_, (_, exp)| *exp > now);
        if map.len() >= DIRECT_CACHE_MAX {
            map.clear();
        }
    }
    map.insert(vid.to_string(), entry);
}

/// How long this answer may stand: until its URLs are close to expiring, and never past the moment
/// they stop working. A failure keeps `/play`'s reason-aware cooldown, which already knows that
/// "removed" is a fact and "timed out" is a mood.
fn ttl_ms(answer: &Result<Direct, PlayError>, now: u64) -> u64 {
    match answer {
        Ok(d) => d.expires.saturating_sub(now).saturating_sub(EXPIRY_MARGIN_MS),
        Err(e) => crate::play::fail_ttl_ms(&e.reason),
    }
}

/// This id's answer, from memory or from yt-dlp. `None` timing means it came from the cache.
///
/// Shared by the request path and by the warm-up `/meta` fires, so a speculative resolve and a real
/// one cannot drift apart — and so the warm-up genuinely fills the cache the request then reads.
pub(crate) async fn answer(
    state: &Arc<AppState>,
    vid: &str,
) -> (Result<Direct, PlayError>, Option<std::time::Duration>) {
    if let Some(answer) = cached(state, vid, (state.clock)()) {
        return (answer, None);
    }
    // A probe permit, not a download one: this is a metadata round-trip of the same weight as
    // `ytdlp::probe`, and it must not be able to queue behind — or in front of — a real download.
    let started = std::time::Instant::now();
    let _permit = state.probe_sem.acquire().await;
    // Asked again under the permit. A burst for one id all miss the cache together and then queue;
    // without this every one spends its own yt-dlp run on an answer the first has already written.
    if let Some(answer) = cached(state, vid, (state.clock)()) {
        return (answer, None);
    }
    let answer = resolve(&state.cfg, vid, (state.clock)()).await;
    let now = (state.clock)();
    if let Err(e) = &answer {
        // Once per resolve that actually ran, at most once a minute per reason — the same shape the
        // download path logs with, and for the same reason: in an outage every one fails alike.
        crate::log_limited(&format!("direct {}", e.reason), || format!("[{vid}] {}", e.detail));
    }
    remember(state, vid, (answer.clone(), now + ttl_ms(&answer, now)), now);
    (answer, Some(started.elapsed()))
}

/// Resolve ahead of the request that will want it, so `/direct` costs a hash lookup instead of a
/// yt-dlp run.
///
/// `/meta` already prewarms the DOWNLOAD, which is what made `/play` feel instant — and is exactly
/// why the direct path felt slower for a title that had been browsed: it traded a warm file for a
/// cold resolve. This puts the resolve on the same footing. Fire-and-forget, and it takes the same
/// probe permit, so a browse cannot spend more of the budget than a probe would.
pub fn warm(state: Arc<AppState>, vid: String) {
    if !crate::is_valid_vid(&vid) {
        return;
    }
    // Nothing to do if the answer is already standing — checked before spawning, so a browse over
    // titles that are all cached costs no tasks at all.
    if cached(&state, &vid, (state.clock)()).is_some() {
        return;
    }
    tokio::spawn(async move {
        let _ = answer(&state, &vid).await;
    });
}

pub async fn handle_direct(state: Arc<AppState>, vid: String) -> Response<Body> {
    let (answer, spent) = answer(&state, &vid).await;
    let now = (state.clock)();
    let timing = match spent {
        Some(d) => httputil::timing("resolve", d),
        None => "cache;desc=hit".to_string(),
    };
    httputil::timed(respond(&state, &vid, &answer, now), &timing)
}

/// The JSON body, cacheable for exactly as long as the URLs in it are good for.
fn respond(state: &AppState, vid: &str, answer: &Result<Direct, PlayError>, now: u64) -> Response<Body> {
    let d = match answer {
        Ok(d) => d,
        Err(e) => return crate::play::play_error(state, vid, e),
    };
    let body = json!({
        "id": vid,
        "video": d.video,
        "audio": d.audio,
        "hls": d.hls,
        "width": d.width,
        "height": d.height,
        "expires": d.expires / 1000,
    });
    // Never past the URLs' own life. A client holding this after that point has a 403 from Google
    // and no way to know why, so the answer must go stale before the thing it describes does.
    let max_age = ttl_ms(answer, now) / 1000;
    httputil::json(
        StatusCode::OK,
        &body,
        &[("cache-control", &format!("private, max-age={max_age}") as &str)],
    )
}
