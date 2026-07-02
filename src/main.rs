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
mod state;
mod upstream;
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
pub const PREWARM_MAX: usize = 3; // cap concurrent prewarm downloads (bounds a burst of /meta calls)
pub const YT_TTL_MS: u64 = 24 * 60 * 60 * 1000;
pub const YT_NEG_TTL_MS: u64 = 60 * 60 * 1000; // "nothing playable" caches shorter (geo/transient may lift)
pub const YT_CACHE_MAX: usize = 10_000; // sweep expired entries once the resolve cache grows past this

/// A YouTube id as it appears in a /play path or `?v=`: `[A-Za-z0-9_-]{6,15}`.
pub fn is_valid_vid(id: &str) -> bool {
    (6..=15).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

pub async fn handle_request(state: Arc<AppState>, req: Request<hyper::body::Incoming>) -> Response<Body> {
    let (parts, _body) = req.into_parts();
    let path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");

    if path == "/health" {
        return httputil::text(StatusCode::OK, "ok");
    }
    if path == "/manifest.json" {
        return httputil::json(StatusCode::OK, &addon::manifest(), &[]);
    }

    // /meta/(movie|series)/(.+).json
    if let Some(rest) = path.strip_prefix("/meta/") {
        if let Some((seg, tail)) = rest.split_once('/') {
            if (seg == "movie" || seg == "series") && tail.ends_with(".json") && tail.len() > 5 {
                let raw = httputil::percent_decode(&tail[..tail.len() - 5]);
                return addon::handle_meta(&state, &parts.headers, seg, &raw, query).await;
            }
        }
    }

    // crop hint: /crop/<id>.json → detected content rect so the app can trim baked-in letterbox.
    if let Some(id) = path.strip_prefix("/crop/").and_then(|r| r.strip_suffix(".json")) {
        if is_valid_vid(id) {
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
    play::handle_play(state, &parts.headers, vid).await
}

async fn run(cfg: Config) -> std::io::Result<()> {
    std::fs::create_dir_all(&cfg.cache_dir)?;
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
