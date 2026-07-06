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
mod hls;
mod httputil;
mod play;
mod seal;
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
pub const PREWARM_MAX: usize = 3; // cap concurrent prewarm downloads (bounds a burst of /meta calls)
pub const YT_TTL_MS: u64 = 24 * 60 * 60 * 1000;
pub const YT_NEG_TTL_MS: u64 = 60 * 60 * 1000; // "nothing playable" caches shorter (geo/transient may lift)
pub const YT_CACHE_MAX: usize = 10_000; // sweep expired entries once the resolve cache grows past this
pub const URL_CACHE_TTL_MS: u64 = 5 * 60 * 60 * 1000; // HLS googlevideo URLs cached <6h signature life
pub const CROP_CACHE_MAX: usize = 10_000; // bound the crop-report cache the same way
pub const DOWNLOAD_CONCURRENCY: usize = 3; // global cap on concurrent yt-dlp downloads (bounds CPU/disk/fd)
pub const PROBE_CONCURRENCY: usize = 6; // global cap on concurrent yt-dlp --simulate probes

/// The /configure page, embedded so the binary is self-contained (seals a BYOK TMDB key into the URL).
const CONFIGURE_PAGE: &str = include_str!("configure.html");

/// A YouTube id as it appears in a /play path or `?v=`: `[A-Za-z0-9_-]{6,15}`.
pub fn is_valid_vid(id: &str) -> bool {
    (6..=15).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Consecutive hard upstream faults before /health reports `degraded` (ADDON-02).
const HEALTH_FAIL_THRESHOLD: u32 = 3;

/// Build the /health JSON body (ADDON-02). Pure so the branch logic is unit-testable without app
/// state: `degraded` when trailers can't work at all (no server TMDB key AND no sealed-config keyring,
/// so no install can supply one), or when the upstreams (TMDB/KinoCheck) have been failing
/// (>= HEALTH_FAIL_THRESHOLD consecutive hard faults); otherwise `ok`. `/health` is addon-level (no
/// per-install config), so a keyring being present is enough to consider trailers workable.
fn health_body(tmdb_available: bool, recent_failures: u32, extract_fails: u32) -> serde_json::Value {
    if !tmdb_available {
        serde_json::json!({"status": "degraded", "reason": "tmdb_key_missing", "detail": "set REEL_CONFIG_KEY (per-install BYOK) or TMDB_KEY"})
    } else if recent_failures >= HEALTH_FAIL_THRESHOLD {
        serde_json::json!({"status": "degraded", "reason": "upstream_unavailable", "detail": "TMDB/KinoCheck have been failing"})
    } else if extract_fails >= HEALTH_FAIL_THRESHOLD {
        // Trailers resolve upstream but yt-dlp can't extract any of them here — YouTube BotGuard or a
        // stale yt-dlp / broken nsig-JS runtime. Bump YTDLP_VERSION (Dockerfile) or tune YTDLP_PLAYER_CLIENTS.
        serde_json::json!({"status": "degraded", "reason": "extractor_unavailable", "detail": "yt-dlp can't extract YouTube here — bump yt-dlp or set YTDLP_PLAYER_CLIENTS"})
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
        // no server TMDB key AND no sealed-config keyring, or the upstreams (TMDB/KinoCheck) failing.
        let tmdb_available = state.cfg.tmdb_key.is_some() || state.config_keyring.is_some();
        let extract_fails = state.extract_fails.load(std::sync::atomic::Ordering::Relaxed);
        let body = health_body(tmdb_available, state.upstream.recent_failures(), extract_fails);
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
                    return httputil::json(
                        StatusCode::BAD_REQUEST,
                        &serde_json::json!({"error": "bad_config"}),
                        &[("cache-control", "no-store")],
                    )
                }
            };
            if rest == "manifest.json" {
                return httputil::json(StatusCode::OK, &addon::manifest(), &[]);
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
            return crop::handle_crop(state, id.to_string()).await;
        }
    }

    // HLS stream-remux (EXPERIMENTAL, STREAM_REMUX): /hls/<vid>/<index.m3u8|init.mp4|seg_NNN.m4s>.
    // Only live when the flag is on; a single path segment for the file (no `/`) guards traversal, and
    // hls::handle_hls further restricts to the exact three filenames.
    if let Some(rest) = path.strip_prefix("/hls/") {
        if state.cfg.stream_remux {
            if let Some((vid, file)) = rest.split_once('/') {
                if is_valid_vid(vid) && !file.is_empty() && !file.contains('/') {
                    return hls::handle_hls(state, vid.to_string(), file.to_string()).await;
                }
            }
        }
        return httputil::text(StatusCode::NOT_FOUND, "not found");
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
    play::handle_play(state, &parts.headers, vid).await
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
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    println!(
        "den-reel on :{port} (cache {cache_disp}, \u{2264}{max_h}p, addon {})",
        if addon_on { "on" } else { "off \u{2014} set TMDB_KEY" }
    );

    loop {
        // A transient accept error (e.g. EMFILE under an fd-exhausting burst) must not take the
        // whole server down — log and keep accepting.
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("accept: {e}");
                continue;
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
