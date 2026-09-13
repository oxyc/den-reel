//! den-reel — the whole trailer path for Den in one small binary:
//!
//!   1. ADDON (Den/Fusion protocol):  imdbId -> TMDB /videos (KinoCheck fallback) -> ytId
//!      GET /manifest.json                     -> addon manifest
//!      GET /meta/<movie|series>/<imdbId>.json -> { meta: { links:[{ trailers: <play url> }] } }
//!
//!   2. PLAYBACK (yt-dlp + ffmpeg proxy):  ytId -> App-Store-safe, seekable MP4
//!      GET /play/<id>.mp4  (or /play?v=<id>)  -> 200/206 video/mp4
//!      GET /direct/<id>.json                  -> the googlevideo URLs themselves (see `direct.rs`)
//!      GET /health                            -> 200 ok
//!
//! Extraction: yt-dlp rotates innertube clients that don't need a BotGuard poToken; ffmpeg muxes a
//! faststart H.264/AAC MP4, which we cache and PROXY.
//!
//! The proxy is for AVPlayer's sake, not the URL's. A googlevideo URL carries `ip=<this box>` inside
//! its signed `sparams`, which reads like a binding to this server — but Google does not enforce it,
//! and `/direct` hands that same URL to a browser, which plays it. What the Apple TV cannot do is
//! take a video stream plus a separate audio one, which is what YouTube now answers with.

mod addon;
mod config;
mod crop;
mod direct;
mod httputil;
mod play;
mod seal;
mod sign;
mod state;
mod upstream;
mod userconfig;
mod ytdlp;

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::httputil::{query_param, Body};
use crate::state::AppState;
use std::sync::Arc;

pub const MAX_PROBE: usize = 6; // cap how many trailer candidates we validate per movie
pub const SEARCH_MAX: usize = 4; // YouTube-search fallback: how many results to consider (then probe)
                                 // Strictly below DOWNLOAD_CONCURRENCY, and that relationship is the point: the prewarm permit and
                                 // the download permit are different semaphores, so equal caps let three speculative prewarms take
                                 // every download permit and — the semaphore being FIFO-fair — park the /play the viewer is actually
                                 // waiting on behind them, for up to DOWNLOAD_TIMEOUT_SECS. One permit stays reserved for real work.
pub const PREWARM_MAX: usize = 2;
pub const YT_TTL_MS: u64 = 24 * 60 * 60 * 1000;
pub const YT_NEG_TTL_MS: u64 = 60 * 60 * 1000; // "nothing playable" caches shorter (geo/transient may lift)
                                               // A lookup that FAILED, rather than one that answered "nothing": long enough to stop a browse from
                                               // stampeding a sick upstream, short enough that a recovery is visible in about a minute.
pub const YT_FAIL_TTL_MS: u64 = 60 * 1000;
// How long PAST its normal expiry a known-good answer may keep standing in for a failing lookup,
// measured from when it was last CONFIRMED (re-serving rewrites the expiry, so that is the only
// clock that still means anything). Serving the last answer beats serving none during an outage,
// but a trailer that was REMOVED upstream has to stop being handed out eventually — and /meta ships
// it with a 7-day max-age, so "eventually" cannot mean "while anything is still faulting".
pub const STALE_GRACE_MS: u64 = 24 * 60 * 60 * 1000;
const _: () =
    assert!(YT_FAIL_TTL_MS < YT_NEG_TTL_MS, "a failure must be re-asked sooner than a real 'no trailer'");
pub const YT_CACHE_MAX: usize = 10_000; // sweep expired entries once the resolve cache grows past this
pub const CROP_CACHE_MAX: usize = 10_000; // bound the crop-report cache the same way
                                          // Bound the /play failure cache. Far smaller than the two above on purpose: those hold answers worth
                                          // keeping, this holds a reason to not re-spawn yt-dlp for a minute or six hours, every entry expires
                                          // on its own, and losing one costs exactly one repeated download. A homelab sees a few hundred
                                          // distinct trailers, so 512 covers the working set without reserving memory for a library.
pub const PLAY_FAIL_MAX: usize = 512;
// Same shape, same reasoning, for the ids cropdetect could not read. A vid and a timestamp each.
pub const CROP_UNKNOWN_MAX: usize = 512;
// How long an unreadable crop stands before ffmpeg is spent on it again. The cached MP4 is immutable,
// so a second pass over the same bytes usually reaches the same nothing — but the other way to land
// here is a cropdetect that timed out under load, and that deserves another go before long.
pub const CROP_UNKNOWN_TTL_MS: u64 = 10 * 60 * 1000;
pub const DOWNLOAD_CONCURRENCY: usize = 3; // global cap on concurrent yt-dlp downloads (bounds CPU/disk/fd)
const _: () =
    assert!(PREWARM_MAX < DOWNLOAD_CONCURRENCY, "prewarm must leave a download permit for a real /play");
pub const PROBE_CONCURRENCY: usize = 6; // global cap on concurrent yt-dlp --simulate probes
                                        // Cap on DISTINCT ids with a download outstanding. `download_sem` bounds how many run at once, but
                                        // the permit is taken inside `download_cached` — so every new id got a map entry and a spawned
                                        // driver that could sit queued for up to DOWNLOAD_TIMEOUT_SECS. That is request-driven growth: on an
                                        // instance without PLAY_SECRET, anyone who can reach /play can add to it by asking for ids that
                                        // are merely well-formed.
                                        //
                                        // Sized by what the queue can plausibly SERVE, not just by memory. The wait is un-timed — only the
                                        // yt-dlp run itself has a timeout — so a queue this deep is also a promise: at three at a time and a
                                        // 240s worst case per download, 24 drains in about half an hour, where 64 would take an hour and a
                                        // half of clients waiting on requests that will mostly have been abandoned. Still many times what
                                        // legitimate use puts in flight (three downloading, two prewarming, a few waiting their turn).
pub const IN_FLIGHT_MAX: usize = 24;
const _: () = assert!(
    IN_FLIGHT_MAX > DOWNLOAD_CONCURRENCY + PREWARM_MAX,
    "the queue has to be able to hold everything that may legitimately be running at once"
);

/// The /configure page, embedded so the binary is self-contained (seals a BYOK TMDB key into the URL).
const CONFIGURE_PAGE: &str = include_str!("configure.html");

/// A YouTube id as it appears in a /play path or `?v=`: `[A-Za-z0-9_-]{11}`.
///
/// Exactly 11, not a 6..=15 window. A YouTube video id is a base64url-encoded 64-bit value and has
/// been 11 characters for the life of the service. This is the ONLY gate on /play and /crop — both
/// of which spend a download permit and a yt-dlp process on whatever they are handed — and it also
/// decides what the cache sweep considers a published trailer, so the loosest thing that still
/// accepts every real id is the right thing.
pub fn is_valid_vid(id: &str) -> bool {
    id.len() == 11 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// How often one failure condition may write a line.
const LOG_EVERY: Duration = Duration::from_secs(60);

/// Log a failure at most once per `LOG_EVERY` per `condition`, saying how many were held back since
/// the last line. The log is for state changes: in an outage every lookup and every download fails
/// the same way, and one line a minute says so as well as a thousand.
///
/// No timer. A held-back count is reported when the condition next occurs after its window, and a
/// condition that has stopped occurring has nothing left to say. `line` is only built when it is
/// written. Conditions are a closed set — an upstream and a status, a failure reason — so the list
/// stays a handful long.
pub fn log_limited(condition: &str, line: impl FnOnce() -> String) {
    static SEEN: std::sync::Mutex<Vec<(String, std::time::Instant, u32)>> = std::sync::Mutex::new(Vec::new());
    let now = std::time::Instant::now();
    let held = {
        let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
        match seen.iter_mut().find(|(c, _, _)| c == condition) {
            Some((_, at, held)) if now.duration_since(*at) < LOG_EVERY => {
                *held += 1;
                return;
            }
            Some((_, at, held)) => {
                *at = now;
                std::mem::take(held)
            }
            None => {
                seen.push((condition.to_string(), now, 0));
                0
            }
        }
    };
    // Written after the lock is released: stderr is a pipe someone else drains.
    match held {
        0 => eprintln!("{}", line()),
        n => eprintln!("{} ({n} more like it since the last line)", line()),
    }
}

/// Consecutive hard upstream faults before /health reports `degraded` (ADDON-02).
const HEALTH_FAIL_THRESHOLD: u32 = 3;

/// Build the /health JSON body (ADDON-02). Pure so the branch logic is unit-testable without app
/// state: `degraded` when trailers can't work at all (no server TMDB key AND no sealed-config keyring,
/// so no install can supply one), or when TMDB has been failing
/// (>= HEALTH_FAIL_THRESHOLD consecutive hard faults); otherwise `ok`. `/health` is addon-level (no
/// per-install config), so a keyring being present is enough to consider trailers workable.
fn health_body(
    tmdb_available: bool,
    recent_failures: u32,
    extract_fails: u32,
    local_fails: u32,
) -> serde_json::Value {
    match health_verdict(tmdb_available, recent_failures, extract_fails, local_fails) {
        Some((reason, detail)) => {
            serde_json::json!({"status": "degraded", "reason": reason, "detail": detail})
        }
        None => serde_json::json!({"status": "ok"}),
    }
}

/// The /health verdict: `None` when ok, else its reason and detail. Shared by the body above and by
/// the line `AppState::note_health` writes when the verdict changes.
fn health_verdict(
    tmdb_available: bool,
    recent_failures: u32,
    extract_fails: u32,
    local_fails: u32,
) -> Option<(&'static str, &'static str)> {
    if !tmdb_available {
        Some(("tmdb_key_missing", "set CONFIG_KEY (per-install BYOK) or TMDB_KEY"))
    } else if recent_failures >= HEALTH_FAIL_THRESHOLD {
        Some(("upstream_unavailable", "TMDB has been failing"))
    } else if extract_fails >= HEALTH_FAIL_THRESHOLD {
        // Trailers resolve upstream but yt-dlp can't extract any of them here — YouTube BotGuard or a
        // stale yt-dlp / broken nsig-JS runtime. Bumping YTDLP_VERSION is the fix that usually works,
        // and is named FIRST on purpose: pinning a client with YTDLP_PLAYER_CLIENTS is the advice that
        // produced a dead client name silently degrading extraction for who knows how long.
        Some((
            "extractor_unavailable",
            "yt-dlp can't extract YouTube here — bump YTDLP_VERSION first; pin YTDLP_PLAYER_CLIENTS only as a stopgap",
        ))
    } else if local_fails >= HEALTH_FAIL_THRESHOLD {
        // Downloads are failing for a reason that is ours, not YouTube's — no output file, or a
        // clap bake killed part-way. Named separately because "bump yt-dlp" is the wrong advice.
        Some((
            "downloads_failing",
            "yt-dlp extracts fine but no trailer file is being produced — check the cache volume and MP4Box",
        ))
    } else {
        None
    }
}

/// May this request read `/metrics`? Only when a token is configured and the request presents it as
/// `Authorization: Bearer <token>` — the prefix required, the token trimmed, as every Den addon reads
/// it. Compared in constant time, so a caller cannot recover the token a byte at a time from how
/// long a refusal took.
fn metrics_authorized(state: &AppState, headers: &hyper::HeaderMap) -> bool {
    use subtle::ConstantTimeEq;
    let Some(token) = state.cfg.metrics_token.as_deref() else { return false };
    let presented = headers
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    presented.is_some_and(|p| p.as_bytes().ct_eq(token.as_bytes()).into())
}

/// The `/metrics` body in the Prometheus text exposition format, written by hand: the format is a
/// few dozen lines of printing, and a client library would be a dependency for it. Every number is
/// either an atomic load or the length of a map we hold briefly — no I/O, no directory walk — and
/// none of it is computed until a scrape asks.
fn metrics_body(state: &AppState) -> String {
    use std::fmt::Write;
    use std::sync::atomic::Ordering::Relaxed;
    fn len<K, V>(m: &std::sync::Mutex<std::collections::HashMap<K, V>>) -> u64 {
        m.lock().unwrap_or_else(|e| e.into_inner()).len() as u64
    }
    let mut b = String::with_capacity(4096);
    // One HELP/TYPE block and its samples; an empty label set is an unlabelled series. Every series
    // here is a gauge — a level read now. Even the failure counts go down: a success resets them.
    let mut gauge = |name: &str, help: &str, samples: &[(&str, u64)]| {
        let _ = writeln!(b, "# HELP {name} {help}\n# TYPE {name} gauge");
        for (labels, v) in samples {
            if labels.is_empty() {
                let _ = writeln!(b, "{name} {v}");
            } else {
                let _ = writeln!(b, "{name}{{{labels}}} {v}");
            }
        }
    };
    let trailer_bytes = state.cache_trailer_bytes.load(Relaxed);
    let scratch_bytes = state.cache_scratch_bytes.load(Relaxed);
    let cap = state.cfg.cache_max_bytes;

    gauge(
        "reel_build_info",
        "The running build.",
        &[(concat!("version=\"", env!("CARGO_PKG_VERSION"), "\""), 1)],
    );
    // The cache figures come from the last eviction pass, not from this request. 0 means no pass has
    // run yet, which on a fresh process lasts until the first download or the first hourly tick.
    gauge(
        "reel_cache_measured_at_seconds",
        "When the eviction pass last measured the cache volume, unix seconds (0 = not yet).",
        &[("", state.cache_measured_at.load(Relaxed) / 1000)],
    );
    gauge(
        "reel_cache_trailers",
        "Trailers on the cache volume.",
        &[("", state.cache_trailer_count.load(Relaxed))],
    );
    gauge("reel_cache_trailer_bytes", "Bytes of trailers on the cache volume.", &[("", trailer_bytes)]);
    gauge(
        "reel_cache_scratch_bytes",
        "Bytes of partial downloads and other scratch, counted against the cap.",
        &[("", scratch_bytes)],
    );
    gauge("reel_cache_max_bytes", "The cache size cap (CACHE_MAX_BYTES).", &[("", cap)]);
    // What a new trailer can still take. Scratch counts against the cap, so this is the number that
    // actually decides whether the next download evicts something.
    gauge(
        "reel_cache_free_bytes",
        "What the cap leaves after trailers and scratch.",
        &[("", cap.saturating_sub(trailer_bytes + scratch_bytes))],
    );
    gauge(
        "reel_downloads_in_flight",
        "Distinct ids with a download outstanding.",
        &[("", len(&state.in_flight))],
    );
    gauge(
        "reel_downloads_in_flight_max",
        "Outstanding ids past which /play answers 503 busy.",
        &[("", IN_FLIGHT_MAX as u64)],
    );
    gauge("reel_downloads_max", "Downloads that may run at once.", &[("", DOWNLOAD_CONCURRENCY as u64)]);
    gauge(
        "reel_prewarm_permits_available",
        "Speculative downloads that could start now.",
        &[("", state.prewarm_sem.available_permits() as u64)],
    );
    gauge(
        "reel_probe_permits_available",
        "yt-dlp probes or searches that could start now.",
        &[("", state.probe_sem.available_permits() as u64)],
    );
    gauge("reel_resolve_cache_entries", "Titles in the resolve cache.", &[("", len(&state.yt_cache))]);
    gauge(
        "reel_resolve_cache_max",
        "Resolve cache size past which expired entries are swept.",
        &[("", YT_CACHE_MAX as u64)],
    );
    gauge("reel_crop_cache_entries", "Crop reports cached.", &[("", len(&state.crop_cache))]);
    gauge(
        "reel_crop_unreadable_entries",
        "Ids cropdetect could not read, waiting out their retry.",
        &[("", len(&state.crop_unknown))],
    );
    // Ids /play has found unplayable and is not re-extracting yet.
    gauge(
        "reel_play_failure_cache_entries",
        "Ids /play is refusing to re-extract yet.",
        &[("", len(&state.play_fails))],
    );
    gauge("reel_play_failure_cache_max", "Bound on the /play failure cache.", &[("", PLAY_FAIL_MAX as u64)]);
    // The same three counters /health turns into a one-word verdict.
    gauge(
        "reel_consecutive_failures",
        "Consecutive failures by kind; /health reports degraded at 3.",
        &[
            ("kind=\"upstream\"", state.upstream.recent_failures() as u64),
            ("kind=\"extract\"", state.extract_fails.load(Relaxed) as u64),
            ("kind=\"local\"", state.local_fails.load(Relaxed) as u64),
        ],
    );
    b
}

// Generic over the request body: this handler routes on path/query only and discards the body, so tests
// can drive it with a `Request<()>` while `run()` passes the real `Request<Incoming>`.
pub async fn handle_request<B>(state: Arc<AppState>, req: Request<B>) -> Response<Body> {
    let start = std::time::Instant::now();
    let log = state.cfg.log_requests;
    let (parts, _body) = req.into_parts();
    let mut resp = if parts.method == hyper::Method::OPTIONS {
        // CORS preflight, on any path: everything here is credential-free, and a browser-based
        // client asks before it sends anything with a header of its own.
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("access-control-allow-origin", "*")
            .header("access-control-allow-methods", "GET, HEAD, OPTIONS")
            .header("access-control-allow-headers", "*")
            // A day, so a browser stops preflighting every request.
            .header("access-control-max-age", "86400")
            .body(httputil::full(""))
            .unwrap()
    } else if !matches!(parts.method, hyper::Method::GET | hyper::Method::HEAD) {
        // Every route here is a read. Routing on the path alone meant a POST to /play started a
        // download like a GET would.
        httputil::json(
            StatusCode::METHOD_NOT_ALLOWED,
            &serde_json::json!({"error": "method_not_allowed"}),
            &[("allow", "GET, HEAD, OPTIONS"), ("cache-control", "no-store")],
        )
    } else {
        let resp = route(state, &parts).await;
        // Honor a conditional GET/HEAD: any cacheable 200 carries an ETag, so an `If-None-Match` hit
        // collapses to a 304 (a no-op for unsafe methods, errors, and `no-store` bodies).
        httputil::apply_conditional(&parts.method, &parts.headers, resp)
    };
    // Stamped here rather than in each builder, so no response can go out without it — the video,
    // the page, a plain-text error and a 304 included. A browser that cannot read an error body
    // reports a CORS failure instead of the error.
    resp.headers_mut().insert("access-control-allow-origin", hyper::header::HeaderValue::from_static("*"));
    // The debug headers readable too: a cross-origin fetch sees only the CORS-safelisted headers unless
    // Expose-Headers names more, and Resource Timing hides Server-Timing without Timing-Allow-Origin.
    resp.headers_mut().insert(
        "access-control-expose-headers",
        hyper::header::HeaderValue::from_static("Server-Timing, X-Den-Degraded"),
    );
    resp.headers_mut().insert("timing-allow-origin", hyper::header::HeaderValue::from_static("*"));
    // Time to headers: a streamed /play body is still being written when this runs.
    let elapsed = start.elapsed();
    // `total` only where a handler named its phases, so it is never the whole of the header.
    let timing = resp
        .headers()
        .get("server-timing")
        .and_then(|v| v.to_str().ok())
        .map(|phases| format!("{phases}, {}", httputil::timing("total", elapsed)));
    if let Some(v) = timing.and_then(|t| hyper::header::HeaderValue::from_str(&t).ok()) {
        resp.headers_mut().insert("server-timing", v);
    }
    if log {
        eprintln!("{}", request_line(&parts, resp.status(), elapsed));
    }
    resp
}

/// `<METHOD> <redacted path> <status> <ms>ms[ rid=<id>]`. The `rid` is the caller's `X-Request-Id`, so a
/// line here can be matched to the one the app logged for the same request.
fn request_line(
    parts: &hyper::http::request::Parts,
    status: StatusCode,
    elapsed: std::time::Duration,
) -> String {
    let mut line = format!(
        "{} {} {} {}ms",
        parts.method,
        redact_path(parts.uri.path()),
        status.as_u16(),
        elapsed.as_millis()
    );
    if let Some(rid) = request_id(&parts.headers) {
        line.push_str(" rid=");
        line.push_str(&rid);
    }
    line
}

/// The caller's `X-Request-Id`, reduced to `[A-Za-z0-9_-]` and 32 characters: it is written into the
/// log verbatim, so nothing that could forge a line or carry a secret gets through. Empty = absent.
fn request_id(headers: &hyper::HeaderMap) -> Option<String> {
    let raw = headers.get("x-request-id")?.to_str().ok()?;
    let id: String =
        raw.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_').take(32).collect();
    (!id.is_empty()).then_some(id)
}

/// The request path as the request log may show it. The query string never gets this far — `?s=`
/// is a play signature — and a per-install config segment carries a BYOK key, sealed or not, so any
/// first segment that is not one of our own routes is written as `<config>`. That covers
/// `/<config>/manifest.json` and `/<config>/meta/…`, and also a client probing `/<config>/configure`
/// or pasting the bare segment, which would otherwise put the key in the log through a 404.
fn redact_path(path: &str) -> std::borrow::Cow<'_, str> {
    const ROUTES: [&str; 10] = [
        "",
        "health",
        "metrics",
        "manifest.json",
        "configure",
        "config-key",
        "meta",
        "crop",
        "play",
        "direct",
    ];
    let rest = path.strip_prefix('/').unwrap_or(path);
    let (first, tail) = match rest.split_once('/') {
        Some((first, tail)) => (first, Some(tail)),
        None => (rest, None),
    };
    if ROUTES.contains(&first) {
        return path.into();
    }
    match tail {
        Some(tail) => format!("/<config>/{tail}").into(),
        None => "/<config>".into(),
    }
}

async fn route(state: Arc<AppState>, parts: &hyper::http::request::Parts) -> Response<Body> {
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");

    if path == "/health" {
        // Standard Den addon health (ADDON-02): 200 for liveness, `degraded` when trailers can't work —
        // no server TMDB key AND no sealed-config keyring, or TMDB failing. KinoCheck is a
        // fallback: its outage does not mean trailers are broken, so it does not move this.
        let (tmdb_available, recent_failures, extract_fails, local_fails) = state.health_inputs();
        let body = health_body(tmdb_available, recent_failures, extract_fails, local_fails);
        return httputil::json(StatusCode::OK, &body, &[("cache-control", "no-store")]);
    }
    // Operational detail /health has no room for. /health answers one question — can this instance
    // serve trailers — and answers it in three words; everything behind that verdict (how full the
    // volume is, how many downloads are in flight, how much of the resolve cache is standing) was
    // visible only by reading logs.
    //
    // Deliberately does NOT walk the cache directory: the figures come from the eviction pass, which
    // already walks it after every download and once an hour. An ops endpoint that stats a few
    // thousand files per request is a way to make a busy box busier.
    //
    // Behind a bearer token, and OFF when none is configured. In-flight downloads and cache
    // occupancy polled over time are a timeline of when the household is watching, which /health
    // does not give away. A refusal is the same 404 an unknown path gets, with or without a token
    // configured, so nobody is told there is something here to poke at.
    if path == "/metrics" {
        if !metrics_authorized(&state, &parts.headers) {
            return httputil::not_found();
        }
        let body = metrics_body(&state);
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
            .header("content-length", body.len())
            .header("cache-control", "no-store")
            .body(httputil::full(body))
            .unwrap();
    }
    if path == "/manifest.json" {
        return httputil::json(
            StatusCode::OK,
            &addon::manifest(None),
            &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
        );
    }
    // The /configure UI seals a BYOK TMDB key into the install URL (den-scout/docs/SEALED-CONFIG.md).
    if path == "/" || path == "/configure" || path == "/configure/" {
        return httputil::html(
            StatusCode::OK,
            CONFIGURE_PAGE,
            &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
        );
    }
    // The current X25519 public key (base64) so /configure can seal the config to it; 404 when sealed
    // configs are disabled (no key) — the page then keeps plaintext. Five minutes, not an hour: the key
    // rotates, and the ETag lets a revalidation after that cost nothing. Both answers carry
    // CONFIG_EPOCH, which the page stamps into every link it builds, sealed or not: a link stamped
    // below it would be refused the moment it was built.
    if path == "/config-key" {
        let epoch = state.cfg.revocation.epoch();
        return match state.config_keyring.as_ref().map(|kr| kr.current_pub_b64()) {
            Some(k) if !k.is_empty() => httputil::json(
                StatusCode::OK,
                &serde_json::json!({"key": k, "epoch": epoch}),
                &[("cache-control", "public, max-age=300")],
            ),
            _ => httputil::json(
                StatusCode::NOT_FOUND,
                &serde_json::json!({"error": "no_key", "epoch": epoch}),
                &[("cache-control", "no-store")],
            ),
        };
    }

    // Legacy config-less discovery: /meta/(movie|series)/(.+).json — resolves with the env TMDB key.
    if let Some(rest) = path.strip_prefix("/meta/") {
        if let Some(resp) = meta_from_rest(&state, &parts.headers, None, rest, query).await {
            return resp;
        }
    }

    // Config-scoped discovery: /<config>/manifest.json and /<config>/meta/(movie|series)/(.+).json,
    // where <config> carries a BYOK TMDB key (sealed or legacy plaintext). The app pastes the manifest
    // URL; Stremio then derives the /meta calls from the same base. Fail CLOSED on a bad config.
    if let Some((cfg_seg, rest)) = path.strip_prefix('/').and_then(|p| p.split_once('/')) {
        if rest == "manifest.json" || rest.starts_with("meta/") {
            let cfg = match userconfig::decode_checked(
                state.config_keyring.as_ref(),
                &state.cfg.revocation,
                cfg_seg,
            ) {
                Ok(c) => c,
                Err(why) => {
                    // Say something. A key rolled out of CONFIG_KEYS_PREV makes every install
                    // 400 at once, and this path logged nothing at all — leaving the operator to
                    // guess. Length only: the segment carries the key. Once a minute, because that
                    // is also every request those installs make. A revoked install gets the same
                    // 400 as an undecodable one; this line is the only place the two differ.
                    log_limited(refusal_condition(&why), || match why {
                        userconfig::Rejected::Undecodable => {
                            format!("bad_config: {rest} rejected a {}-byte config segment", cfg_seg.len())
                        }
                        refused => format!("bad_config: {rest} refused — {refused}"),
                    });
                    return httputil::json(
                        StatusCode::BAD_REQUEST,
                        &serde_json::json!({"error": "bad_config"}),
                        &[("cache-control", "no-store")],
                    );
                }
            };
            if rest == "manifest.json" {
                return httputil::json(
                    StatusCode::OK,
                    &addon::manifest(cfg.iid.as_deref()),
                    &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
                );
            }
            let meta_rest = &rest["meta/".len()..];
            if let Some(resp) = meta_from_rest(&state, &parts.headers, Some(&cfg), meta_rest, query).await {
                return resp;
            }
            return httputil::not_found();
        }
    }

    // crop hint: /crop/<id>.json → detected content rect so the app can trim baked-in letterbox.
    if let Some(id) = path.strip_prefix("/crop/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
            // An unsigned /crop degrades instead of refusing. Nothing this server emits carries a
            // signed crop URL — the tag rides on the play URL, and it is the client that has to
            // carry it across — so a hard 403 here would turn "the app did not propagate `s`" into
            // de-letterboxing that silently disappears, with no error to report and /health green.
            // The gate exists to protect the DOWNLOAD, and `unsigned_response` reaches none of it.
            if !signature_ok(&state, id, query) {
                return crop::unsigned_response(id);
            }
            return crop::handle_crop(state, id.to_string()).await;
        }
    }

    // direct URLs: /direct/<id>.json → the googlevideo URLs themselves, for a client that can play
    // them without this server in the middle (the web app; see `direct.rs`). Refuses like /play
    // rather than degrading like /crop: an unsigned caller would otherwise get a free yt-dlp run,
    // and unlike a crop hint there is no useful constant to answer with.
    if let Some(id) = path.strip_prefix("/direct/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
            if !signature_ok(&state, id, query) {
                return bad_signature();
            }
            return direct::handle_direct(state, id.to_string()).await;
        }
    }

    // playback: id from /play/<id>.mp4 (overrides ?v=), else ?v= on the bare /play path.
    let mut vid = query_param(query, "v");
    let play_match =
        path.strip_prefix("/play/").and_then(|r| r.strip_suffix(".mp4")).filter(|id| is_valid_vid(id));
    if let Some(id) = play_match {
        vid = Some(id.to_string());
    } else if path != "/play" {
        return httputil::not_found();
    }
    let vid = match vid {
        Some(v) if is_valid_vid(&v) => v,
        _ => {
            return httputil::error(
                StatusCode::BAD_REQUEST,
                "bad_video_id",
                "Expected an 11-character YouTube id.",
            )
        }
    };
    if !signature_ok(&state, &vid, query) {
        return bad_signature();
    }
    play::handle_play(state, &parts.headers, vid).await
}

/// The log condition for each way a config or a bound link is refused, so each is rate-limited apart.
fn refusal_condition(why: &userconfig::Rejected) -> &'static str {
    match why {
        userconfig::Rejected::Undecodable => "bad_config",
        userconfig::Rejected::Plaintext => "plaintext_refused",
        userconfig::Rejected::Revoked { .. } => "install_revoked",
        userconfig::Rejected::EpochTooOld { .. } => "install_epoch_too_old",
        userconfig::Rejected::NoInstallId => "install_no_iid",
    }
}

/// May this request spend a download on this id? `true` for every request when `PLAY_SECRET` is
/// unset, which is the default. Otherwise the tag must verify over the id and the install named by
/// `i`/`e` (see `sign.rs`), and that install must still be admitted by `REVOKED_INSTALLS` /
/// `CONFIG_EPOCH` / `REQUIRE_INSTALL_ID`. While `PLAY_SIGNING_GRACE_UNTIL` is ahead, a request with
/// no tag, or with a tag over the id alone from before install binding, is served too.
///
/// A predicate, not a response. The two callers disagree about what a refusal looks like — `/play`
/// says 403, `/crop` degrades to "play the full frame" — and the `/crop` refusal is the EXPECTED
/// case there, since nothing this server emits is a signed crop URL. Returning a built response
/// meant serializing a JSON body and a header map on that path and dropping both.
///
/// Both endpoints authorise the same work for the same id, and the tag does not cover the path, so a
/// client can carry the query it was handed on the play URL straight over to `/crop`.
fn signature_ok(state: &Arc<AppState>, vid: &str, query: &str) -> bool {
    let Some(secret) = state.cfg.play_secret.as_deref() else { return true };
    let prev = &state.cfg.play_secrets_prev;
    let Some(presented) = query_param(query, "s") else {
        // A missing tag rides the grace window: a URL issued before signing was turned on carries none.
        if in_grace(&state.cfg, (state.clock)()) {
            note_served_in_grace(vid, "without a tag");
            return true;
        }
        return false;
    };
    let iid = query_param(query, "i");
    let binding = match query_param(query, "e") {
        None if iid.is_none() => sign::Binding::Unbound,
        // Never minted: an install id always travels with its epoch.
        None => return false,
        Some(ep) => match ep.parse() {
            Ok(ep) => sign::Binding::Install { iid: iid.as_deref(), ep },
            Err(_) => return false,
        },
    };
    if sign::verify_any(secret, prev, &sign::message(vid, binding), Some(&presented)) {
        return install_admitted(state, vid, binding);
    }
    // A tag over the id alone, as releases before install binding minted it. It names no install,
    // so nothing can revoke it: honoured only inside the grace, like an untagged URL. A bound link
    // stripped of `i`/`e` does not land here, because its tag covers the binding.
    if binding == sign::Binding::Unbound
        && in_grace(&state.cfg, (state.clock)())
        && sign::verify_any(secret, prev, vid, Some(&presented))
    {
        note_served_in_grace(vid, "on a tag from before install binding");
        return true;
    }
    false
}

/// Is the install a verified link was minted for still admitted? An unbound link, from the
/// config-less `/meta`, names none and is. A refusal gets the same answer as a bad tag; this log
/// line, in the style of the config routes' `bad_config`, is the only place the two differ.
fn install_admitted(state: &AppState, vid: &str, binding: sign::Binding) -> bool {
    let sign::Binding::Install { iid, ep } = binding else { return true };
    match state.cfg.revocation.check_install(iid, ep) {
        Ok(()) => true,
        Err(why) => {
            log_limited(&format!("play_{}", refusal_condition(&why)), || {
                format!("bad_signature: link for {vid} refused — {why}")
            });
            false
        }
    }
}

fn in_grace(cfg: &Config, now_ms: u64) -> bool {
    cfg.play_signing_grace.as_ref().is_some_and(|g| now_ms < g.until_ms)
}

/// Ids remembered by `note_served_in_grace`. Anyone who can reach `/play` can invent ids, so the
/// set is bounded; past it the rest share one rate-limited line.
const GRACE_LOGGED_IDS: usize = 4096;

/// Say, once per id and `how`, that a URL was served only because of the grace window, so the
/// operator can see which trailers clients are still holding old URLs for — and that the
/// stragglers have stopped before the deadline arrives.
fn note_served_in_grace(vid: &str, how: &str) {
    static SEEN: std::sync::Mutex<std::collections::BTreeSet<String>> =
        std::sync::Mutex::new(std::collections::BTreeSet::new());
    let key = format!("{vid} {how}");
    let fresh = {
        let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
        if seen.contains(&key) {
            return;
        }
        seen.len() < GRACE_LOGGED_IDS && seen.insert(key)
    };
    if fresh {
        eprintln!("play signing grace: served {vid} {how}");
    } else {
        log_limited("served_in_grace", || {
            format!("play signing grace: served {vid} {how} (past {GRACE_LOGGED_IDS} distinct ids)")
        });
    }
}

/// The startup line's `play_signing=` value: `off`, `on`, or `grace(until=<ts>)` while untagged
/// requests are still served. A grace whose moment has passed reads `on`, because that is what it is.
fn play_signing_state(cfg: &Config, now_ms: u64) -> String {
    match &cfg.play_signing_grace {
        _ if cfg.play_secret.is_none() => "off".into(),
        Some(g) if now_ms < g.until_ms => format!("grace(until={})", g.until),
        _ => "on".into(),
    }
}

/// The `/play` refusal: terse, and no hint about what a correct tag would look like.
fn bad_signature() -> Response<Body> {
    httputil::json(
        StatusCode::FORBIDDEN,
        &serde_json::json!({"error": "bad_signature", "detail": "This trailer URL is not signed for this server."}),
        &[("cache-control", "no-store")],
    )
}

/// Parse `<movie|series>/<imdbId>.json` (the part after `meta/`) and dispatch to the meta handler.
/// `None` if the shape doesn't match, so the caller can fall through to the next route. `cfg` carries
/// the per-install BYOK keys (`None` = legacy config-less, resolve with the env key).
async fn meta_from_rest(
    state: &Arc<AppState>,
    headers: &hyper::HeaderMap,
    cfg: Option<&userconfig::UserConfig>,
    rest: &str,
    query: &str,
) -> Option<Response<Body>> {
    let (seg, tail) = rest.split_once('/')?;
    if (seg == "movie" || seg == "series") && tail.ends_with(".json") && tail.len() > 5 {
        let raw = httputil::percent_decode(&tail[..tail.len() - 5]);
        return Some(addon::handle_meta(state, headers, cfg, seg, &raw, query).await);
    }
    None
}

async fn run(cfg: Config) -> std::io::Result<()> {
    // A bad CACHE_DIR must NOT crash-loop the process: discovery (/health, /manifest, /meta) doesn't
    // need the cache, only /play and /crop do — and those return a structured 503 when it's missing.
    if let Err(e) = std::fs::create_dir_all(&cfg.cache_dir) {
        eprintln!(
            "warning: cache dir {} is unusable ({e}); /play and /crop will 503 until it's writable",
            cfg.cache_dir.display()
        );
    }
    let port = cfg.port;
    let cache_disp = cfg.cache_dir.display().to_string();
    let max_h = cfg.max_height.clone();

    let state = AppState::new(cfg);
    let cfg_for_shutdown = state.cfg.clone();
    // A verdict that is degraded from the start — no discovery key at all — is said once, here.
    state.note_health();

    // Pick up where the last process left off. A resolve is a TMDB round-trip per title, and a
    // redeploy otherwise makes the next browse pay for every title on screen again.
    {
        let restored = state::load_resolve_cache(&state.cfg, (state.clock)());
        if !restored.is_empty() {
            *state.yt_cache.lock().unwrap_or_else(|e| e.into_inner()) = restored;
        }
    }

    // Periodic cache sweep so the last-access TTL is enforced during idle stretches too — eviction
    // otherwise only runs after a download. Hourly is ample for a day-scale TTL, and interval's first
    // tick fires immediately so a cache left stale over a long downtime is trimmed on boot.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let cfg = state.cfg.clone();
                let measured = tokio::task::spawn_blocking(move || {
                    crate::play::sweep_partials(&cfg);
                    crate::play::evict_if_needed(&cfg)
                })
                .await;
                if let Ok(Some(u)) = measured {
                    state.record_cache_usage(u);
                }
            }
        });
    }

    // Built ONCE, before serving — and before the startup line, since until the handlers are
    // registered SIGTERM keeps its default disposition and a stop in that window killed the process
    // outright. Constructing it per accept dropped the Signal each time accept() won the select, and
    // tokio's signal subscribes at the current watch version — so a SIGTERM delivered while no
    // Signal existed was simply not seen by the next one.
    //
    // A redeploy sends SIGTERM. Without handling it the process is killed outright: every in-flight
    // subprocess keeps running in its own process group, and the partial files it was writing sit on
    // the cache volume — invisible to the size cap and unreclaimable until the sweep's 30-minute
    // grace, under a pid that no longer exists.
    let shutdown = shutdown_signal();

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let on = |b: bool| if b { "on" } else { "off" };
    eprintln!(
        "den-reel {} listening on :{port} — metrics={} log_requests={} sealed={} revoked={} epoch={} \
         require_iid={} play_signing={} env_tmdb_key={} cache={cache_disp} max_height={max_h}",
        env!("CARGO_PKG_VERSION"),
        on(state.cfg.metrics_token.is_some()),
        on(state.cfg.log_requests),
        on(state.config_keyring.is_some()),
        state.cfg.revocation.revoked_count(),
        state.cfg.revocation.epoch(),
        on(state.cfg.revocation.requires_install_id()),
        play_signing_state(&state.cfg, (state.clock)()),
        on(state.cfg.tmdb_key.is_some()),
    );

    let drained = serve_until(listener, state.clone(), shutdown, DRAIN_GRACE, HEADER_READ_TIMEOUT).await;

    // In-flight responses have finished, or run out of time.
    //
    // Kill the downloads EXPLICITLY. Dropping the state does not do it: `in_flight` holds a Shared
    // clone of a future that captures the very Arc<AppState> the map lives in, so the cycle keeps
    // each Child alive past runtime teardown and neither kill-on-drop nor the group guard fires.
    // The sweep would then race a yt-dlp that is still writing, and recreate the file it deleted.
    // In the container this was masked by den-reel being PID 1 — namespace teardown SIGKILLs the
    // orphans just after — which is not a mechanism to rely on, and is absent outside a container.
    let killed = crate::ytdlp::kill_live_groups();
    if killed > 0 {
        eprintln!("shutdown: killed {killed} in-flight download(s)");
    }
    crate::play::sweep_own_temps(&cfg_for_shutdown);
    state::save_resolve_cache(&state);
    if drained {
        eprintln!("shut down cleanly");
    }
    Ok(())
}

/// How long in-flight requests get to finish after SIGTERM. Under podman's default 10s stop timeout,
/// as den-atlas's and den-embed's are, so the drain works whether or not the Quadlet's --stop-timeout
/// has reached the box.
const DRAIN_GRACE: Duration = Duration::from_secs(8);

/// How long a client may take to send a request head. Without it a connection that sends half a
/// head and goes quiet is held open for as long as the client likes — and at a stop, holds the drain
/// until the deadline.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve until `shutdown` resolves, then let in-flight requests finish for at most `grace`; `true`
/// when they all did. Before this, SIGTERM stopped the loop and returned at once, cutting every
/// response mid-stream.
///
/// The bound is the point: a graceful shutdown waits for every connection, and a client that sends
/// half a request head and stops — or a /play parked on a download that has minutes left — would
/// otherwise decide how long a restart takes. The caller kills the downloads once this returns.
async fn serve_until(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()>,
    grace: Duration,
    header_timeout: Duration,
) -> bool {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    loop {
        // A transient accept error (e.g. EMFILE under an fd-exhausting burst) must not take the
        // whole server down — log and keep accepting.
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    log_limited("accept", || format!("accept: {e}"));
                    // And back off before trying again. The listener stays readable while the process is
                    // out of descriptors, so an immediate retry fails at once and the loop spins a core
                    // at 100% until one frees up — on the one runtime thread that serves every request.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = &mut shutdown => break,
        };
        let state = state.clone();
        let service = service_fn(move |req| {
            let state = state.clone();
            async move { Ok::<_, Infallible>(handle_request(state, req).await) }
        });
        let conn = hyper::server::conn::http1::Builder::new()
            .timer(TokioTimer::new())
            .header_read_timeout(header_timeout)
            .serve_connection(TokioIo::new(stream), service);
        let conn = graceful.watch(conn);
        // A client hanging up mid-response is normal; don't log it.
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => true,
        _ = tokio::time::sleep(grace) => {
            eprintln!("drain deadline ({grace:?}) reached with requests still in flight");
            false
        }
    }
}

/// Resolves on SIGTERM (a redeploy) or SIGINT (a terminal).
///
/// Both are registered NOW, by the caller, not lazily when the future is first polled — polling
/// starts once the server is already accepting, and until then SIGTERM keeps its default
/// disposition. They are registered independently, so one failing does not discard the other.
fn shutdown_signal() -> impl Future<Output = ()> {
    use tokio::signal::unix::{signal, SignalKind};
    let term = signal(SignalKind::terminate());
    let int = signal(SignalKind::interrupt());
    async move {
        tokio::select! {
            _ = wait_for(term, "SIGTERM") => {}
            _ = wait_for(int, "SIGINT") => {}
        }
        // A SECOND signal ends it now. Both handles above are dropped by here, and tokio does not
        // restore the default disposition when a `Signal` drops — so every later SIGTERM and ^C would
        // be caught and discarded, and an operator could not get out of the drain short of SIGKILL.
        // Exit 0: asking twice is a deliberate choice, not a failure.
        tokio::spawn(async move {
            tokio::select! {
                _ = quietly(signal(SignalKind::terminate())) => {}
                _ = quietly(signal(SignalKind::interrupt())) => {}
            }
            eprintln!("second signal — exiting without finishing the drain");
            std::process::exit(0);
        });
    }
}

/// Resolve when this signal arrives, or never if it could not be registered — returning at once
/// would stop the server the moment it started, so an unregisterable signal stays a hard kill.
async fn wait_for(registered: std::io::Result<tokio::signal::unix::Signal>, name: &str) {
    match registered {
        Ok(mut sig) => {
            sig.recv().await;
            eprintln!("{name} — draining in-flight requests");
        }
        Err(e) => {
            eprintln!("{name} handler unavailable ({e}); it will be a hard kill");
            std::future::pending::<()>().await
        }
    }
}

/// Like `wait_for`, but says nothing — for a caller that prints its own, different message.
async fn quietly(registered: std::io::Result<tokio::signal::unix::Signal>) {
    match registered {
        Ok(mut sig) => {
            sig.recv().await;
        }
        Err(_) => std::future::pending::<()>().await,
    }
}

fn main() {
    let cfg = Config::from_env();
    // current_thread: one runtime thread keeps idle RAM low; the heavy lifting is in subprocesses.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");

    if let Err(e) = rt.block_on(run(cfg)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
