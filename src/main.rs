//! den-reel — the whole trailer path for Den in one small binary:
//!
//!   1. ADDON (Den/Fusion protocol):  imdbId -> TMDB /videos (KinoCheck fallback) -> ytId
//!      GET /manifest.json                     -> addon manifest
//!      GET /meta/<movie|series>/<imdbId>.json -> { meta: { links:[{ trailers: <play url> }] } }
//!
//!   2. PLAYBACK (yt-dlp + ffmpeg proxy):  ytId -> App-Store-safe, seekable MP4
//!      GET /play/<id>.mp4  (or /play?v=<id>)  -> 200/206 video/mp4
//!      GET /health                            -> 200 ok
//!
//! Extraction: yt-dlp rotates innertube clients that don't need a BotGuard poToken; ffmpeg muxes a
//! faststart H.264/AAC MP4; we cache and PROXY it (the googlevideo URL is IP-bound to THIS server,
//! so the Apple TV must hit us, not YouTube).

mod addon;
mod config;
mod crop;
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

use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
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
const _: () = assert!(
    YT_FAIL_TTL_MS < YT_NEG_TTL_MS,
    "a failure must be re-asked sooner than a real 'no trailer'"
);
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
const _: () = assert!(
    PREWARM_MAX < DOWNLOAD_CONCURRENCY,
    "prewarm must leave a download permit for a real /play"
);
pub const PROBE_CONCURRENCY: usize = 6; // global cap on concurrent yt-dlp --simulate probes

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

/// Consecutive hard upstream faults before /health reports `degraded` (ADDON-02).
const HEALTH_FAIL_THRESHOLD: u32 = 3;

/// Build the /health JSON body (ADDON-02). Pure so the branch logic is unit-testable without app
/// state: `degraded` when trailers can't work at all (no server TMDB key AND no sealed-config keyring,
/// so no install can supply one), or when TMDB has been failing
/// (>= HEALTH_FAIL_THRESHOLD consecutive hard faults); otherwise `ok`. `/health` is addon-level (no
/// per-install config), so a keyring being present is enough to consider trailers workable.
fn health_body(tmdb_available: bool, recent_failures: u32, extract_fails: u32, local_fails: u32) -> serde_json::Value {
    if !tmdb_available {
        serde_json::json!({"status": "degraded", "reason": "tmdb_key_missing", "detail": "set REEL_CONFIG_KEY (per-install BYOK) or TMDB_KEY"})
    } else if recent_failures >= HEALTH_FAIL_THRESHOLD {
        serde_json::json!({"status": "degraded", "reason": "upstream_unavailable", "detail": "TMDB has been failing"})
    } else if extract_fails >= HEALTH_FAIL_THRESHOLD {
        // Trailers resolve upstream but yt-dlp can't extract any of them here — YouTube BotGuard or a
        // stale yt-dlp / broken nsig-JS runtime. Bump YTDLP_VERSION (Dockerfile) or tune YTDLP_PLAYER_CLIENTS.
        serde_json::json!({"status": "degraded", "reason": "extractor_unavailable", "detail": "yt-dlp can't extract YouTube here — bump yt-dlp or set YTDLP_PLAYER_CLIENTS"})
    } else if local_fails >= HEALTH_FAIL_THRESHOLD {
        // Downloads are failing for a reason that is ours, not YouTube's — no output file, or a
        // clap bake killed part-way. Named separately because "bump yt-dlp" is the wrong advice.
        serde_json::json!({"status": "degraded", "reason": "downloads_failing", "detail": "yt-dlp extracts fine but no trailer file is being produced — check the cache volume and MP4Box"})
    } else {
        serde_json::json!({"status": "ok"})
    }
}

// Generic over the request body: this handler routes on path/query only and discards the body, so tests
// can drive it with a `Request<()>` while `run()` passes the real `Request<Incoming>`.
pub async fn handle_request<B>(state: Arc<AppState>, req: Request<B>) -> Response<Body> {
    let (parts, _body) = req.into_parts();
    let resp = route(state, &parts).await;
    // Honor a conditional GET/HEAD: any cacheable 200 carries an ETag, so an `If-None-Match` hit
    // collapses to a 304 (a no-op for unsafe methods, errors, and `no-store` bodies).
    httputil::apply_conditional(&parts.method, &parts.headers, resp)
}

async fn route(state: Arc<AppState>, parts: &hyper::http::request::Parts) -> Response<Body> {
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");

    if path == "/health" {
        // Standard Den addon health (ADDON-02): 200 for liveness, `degraded` when trailers can't work —
        // no server TMDB key AND no sealed-config keyring, or TMDB failing. KinoCheck is a
        // fallback: its outage does not mean trailers are broken, so it does not move this.
        let tmdb_available = state.cfg.tmdb_key.is_some() || state.config_keyring.is_some();
        let extract_fails = state.extract_fails.load(std::sync::atomic::Ordering::Relaxed);
        let local_fails = state.local_fails.load(std::sync::atomic::Ordering::Relaxed);
        let body = health_body(tmdb_available, state.upstream.recent_failures(), extract_fails, local_fails);
        return httputil::json(StatusCode::OK, &body, &[("cache-control", "no-store")]);
    }
    if path == "/manifest.json" {
        return httputil::json(
            StatusCode::OK,
            &addon::manifest(),
            &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
        );
    }
    // The /configure UI seals a BYOK TMDB key into the install URL (den-scout/docs/SEALED-CONFIG.md).
    if path == "/" || path == "/configure" || path == "/configure/" {
        return httputil::html(StatusCode::OK, CONFIGURE_PAGE, &[("cache-control", "public, max-age=3600")]);
    }
    // The current X25519 public key (base64) so /configure can seal the config to it; 404 when sealed
    // configs are disabled (no key) — the page then keeps plaintext.
    if path == "/config-key" {
        return match state.config_keyring.as_ref().map(|kr| kr.current_pub_b64()) {
            Some(k) if !k.is_empty() => httputil::json(
                StatusCode::OK,
                &serde_json::json!({"key": k}),
                &[("cache-control", "public, max-age=3600")],
            ),
            _ => httputil::json(
                StatusCode::NOT_FOUND,
                &serde_json::json!({"error": "no_key"}),
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
            let cfg = match userconfig::decode(state.config_keyring.as_ref(), cfg_seg) {
                Some(c) => c,
                None => {
                    // Say something. A key rolled out of REEL_CONFIG_KEYS_PREV makes every install
                    // 400 at once, and this path logged nothing at all — leaving the operator to
                    // guess. Length only: the segment carries the key.
                    eprintln!("bad_config: {rest} rejected a {}-byte config segment", cfg_seg.len());
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
                    &addon::manifest(),
                    &[("cache-control", "public, max-age=3600, stale-while-revalidate=600")],
                );
            }
            let meta_rest = &rest["meta/".len()..];
            if let Some(resp) = meta_from_rest(&state, &parts.headers, Some(&cfg), meta_rest, query).await {
                return resp;
            }
            return httputil::text(StatusCode::NOT_FOUND, "not found");
        }
    }

    // crop hint: /crop/<id>.json → detected content rect so the app can trim baked-in letterbox.
    if let Some(id) = path.strip_prefix("/crop/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
            if let Some(denied) = signature_check(&state, id, query) {
                return denied;
            }
            return crop::handle_crop(state, id.to_string()).await;
        }
    }

    // playback: id from /play/<id>.mp4 (overrides ?v=), else ?v= on the bare /play path.
    let mut vid = query_param(query, "v");
    let play_match = path
        .strip_prefix("/play/")
        .and_then(|r| r.strip_suffix(".mp4"))
        .filter(|id| is_valid_vid(id));
    if let Some(id) = play_match {
        vid = Some(id.to_string());
    } else if path != "/play" {
        return httputil::text(StatusCode::NOT_FOUND, "not found");
    }
    let vid = match vid {
        Some(v) if is_valid_vid(&v) => v,
        _ => return httputil::text(StatusCode::BAD_REQUEST, "bad video id"),
    };
    if let Some(denied) = signature_check(&state, &vid, query) {
        return denied;
    }
    play::handle_play(state, &parts.headers, vid).await
}

/// Gate the two routes that spend a download on whatever id they are given. `None` means carry on —
/// which is every request when `REEL_PLAY_SECRET` is unset, the default. `Some(response)` is the
/// refusal.
///
/// Both endpoints authorise the same work for the same id, and the tag covers the id alone, so a
/// client can carry the `s` it was handed on the play URL straight over to `/crop`.
fn signature_check(state: &Arc<AppState>, vid: &str, query: &str) -> Option<Response<Body>> {
    let secret = state.cfg.play_secret.as_deref()?;
    if sign::verify(secret, vid, query_param(query, "s").as_deref()) {
        return None;
    }
    // Deliberately terse, and no hint about what a correct tag would look like.
    Some(httputil::json(
        StatusCode::FORBIDDEN,
        &serde_json::json!({"error": "bad_signature", "message": "This trailer URL is not signed for this server."}),
        &[("cache-control", "no-store")],
    ))
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
    let addon_on = cfg.tmdb_key.is_some();
    let cache_disp = cfg.cache_dir.display().to_string();
    let max_h = cfg.max_height.clone();

    let state = AppState::new(cfg);
    let cfg_for_shutdown = state.cfg.clone();

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
                let _ = tokio::task::spawn_blocking(move || {
                    crate::play::sweep_partials(&cfg);
                    crate::play::evict_if_needed(&cfg);
                })
                .await;
            }
        });
    }

    // Built ONCE, outside the loop. Constructing it per iteration dropped the Signal each time
    // accept() won the select, and tokio's signal subscribes at the current watch version — so a
    // SIGTERM delivered while no Signal existed was simply not seen by the next one.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    println!(
        "den-reel on :{port} (cache {cache_disp}, \u{2264}{max_h}p, addon {})",
        if addon_on { "on" } else { "off \u{2014} set TMDB_KEY" }
    );

    loop {
        // A transient accept error (e.g. EMFILE under an fd-exhausting burst) must not take the
        // whole server down — log and keep accepting.
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("accept: {e}");
                    continue;
                }
            },
            // A redeploy (`podman auto-update`) sends SIGTERM. Without handling it the process is
            // killed outright: every in-flight subprocess keeps running in its own process group,
            // and the partial files it was writing sit on the cache volume — invisible to the size
            // cap and unreclaimable until the sweep's 30-minute grace, under a pid that no longer
            // exists. See the shutdown block below for what stopping the loop actually does.
            _ = &mut shutdown => {
                eprintln!("shutting down: stopping accepts, killing in-flight downloads");
                break;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle_request(state, req).await) }
            });
            // A client hanging up mid-response is normal; don't log it.
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await;
        });
    }
    // Only the shutdown branch breaks — an accept error continues — so reaching here means SIGTERM.
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
    Ok(())
}

/// Resolves on SIGTERM (a redeploy) or SIGINT (a terminal). On a platform without unix signals only
/// ctrl-c is available, which is what a dev run sends anyway.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("cannot listen for SIGTERM ({e}); a redeploy will strand in-flight work");
                return std::future::pending().await;
            }
        };
        tokio::select! {
            _ = term.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// `den-reel healthcheck` — used by the container HEALTHCHECK so the slim image needs no curl.
async fn healthcheck(port: u16) -> i32 {
    let url = format!("http://127.0.0.1:{port}/health");
    match reqwest::get(&url).await {
        Ok(r) if r.status().is_success() => 0,
        _ => 1,
    }
}

fn main() {
    let cfg = Config::from_env();
    // current_thread: one runtime thread keeps idle RAM low; the heavy lifting is in subprocesses.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        std::process::exit(rt.block_on(healthcheck(cfg.port)));
    }

    if let Err(e) = rt.block_on(run(cfg)) {
        eprintln!("fatal: {e}");
        std::process::exit(1);
    }
}
