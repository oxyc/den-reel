//! ADDON request path: resolve (and cache) the first PLAYABLE trailer for an imdb id, then build
//! the Fusion `meta` payload whose play URL points back at THIS host.

use std::collections::HashSet;
use std::sync::Arc;

use hyper::header::HeaderMap;
use hyper::{Response, StatusCode};
use serde_json::{json, Value};

use crate::httputil::{self, query_param, Body};
use crate::state::{AppState, YtEntry};
use crate::{MAX_PROBE, YT_CACHE_MAX, YT_NEG_TTL_MS, YT_TTL_MS};

pub fn manifest() -> Value {
    json!({
        "id": "fi.oxy.den-reel",
        // Single source of truth: the Cargo package version (CI asserts it == the v* tag). So the
        // manifest can't drift from Cargo.toml, nor the tag from either.
        "version": env!("CARGO_PKG_VERSION"),
        "name": "Den Reel",
        "description": "Direct-URL trailers (TMDB/KinoCheck → yt-dlp service) for inline playback.",
        "resources": ["meta"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        // A BYOK TMDB key is entered (and sealed) at /configure — advertise it so a Stremio client shows
        // the Configure button. The Den app builds the sealed URL directly, so this is just for parity.
        "behaviorHints": { "configurable": true },
    })
}

fn is_imdb(id: &str) -> bool {
    id.strip_prefix("tt").is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
}

/// `^[a-z]{2}$` (case-insensitive), else the caller falls back to "en".
fn valid_lang(l: &str) -> bool {
    l.len() == 2 && l.bytes().all(|b| b.is_ascii_alphabetic())
}

/// The base URL this server is reachable at (for building play URLs the device will fetch).
fn self_base(cfg_public: Option<&str>, headers: &HeaderMap, port: u16) -> String {
    if let Some(b) = cfg_public {
        return b.trim_end_matches('/').to_string();
    }
    let hdr = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let proto = hdr("x-forwarded-proto")
        .map(|p| p.split(',').next().unwrap_or("http").trim().to_string())
        .unwrap_or_else(|| "http".to_string());
    // Only reflect a sane Host charset into the play URL we hand back (a spoofed Host would otherwise
    // point the app at an attacker origin). PUBLIC_BASE_URL short-circuits this in prod.
    let host = hdr("x-forwarded-host")
        .or_else(|| hdr("host"))
        .filter(|h| is_sane_host(h))
        .map(|h| h.to_string())
        .unwrap_or_else(|| format!("localhost:{port}"));
    format!("{proto}://{host}")
}

/// A hostname/authority we're willing to reflect into a returned URL: alnum + the punctuation a
/// host+port uses. Rejects spaces, slashes, `@`, etc.
fn is_sane_host(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 255
        && h.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'_'))
}

/// Build the Fusion `meta` payload — one `links[]` entry per resolved trailer, best-first, so the client
/// can fall back to the next on a playback failure. Empty ids → no links.
pub fn build_meta(ty: &str, imdb: &str, base: &str, yt_ids: &[String], stream: bool) -> Value {
    let base = base.trim_end_matches('/');
    let links: Vec<Value> = yt_ids
        .iter()
        .map(|id| {
            // STREAM_REMUX on → an HLS playlist the client streams (plays segment 0 while the rest
            // muxes); off → the proven download-then-serve MP4 proxy. Both are best-first candidates.
            let trailer = if stream {
                format!("{base}/hls/{id}/index.m3u8")
            } else {
                format!("{base}/play/{id}.mp4")
            };
            json!({
                "name": "Trailer",
                "category": "Trailer",
                "trailers": trailer,
                "provider": "Den Reel",
            })
        })
        .collect();
    json!({ "meta": { "id": imdb, "type": ty, "links": links } })
}

/// Resolve (and cache) trailer ytIds for an imdb id, **best-playable first** then the remaining
/// candidates as unprobed fallbacks (so the client can try the next on a playback failure). Empty =
/// nothing playable (cached shorter, in case transient). `tmdb_key`/`kinocheck_key` are the effective
/// per-request BYOK credentials (URL config, or env fallback). The cache is keyed by `imdb:lang` only —
/// the resolved trailer is public and key-independent, so installs with different keys share one entry.
pub async fn resolve_youtube_ids(
    state: &Arc<AppState>,
    tmdb_key: &str,
    kinocheck_key: Option<&str>,
    imdb: &str,
    ty: &str,
    lang: &str,
) -> Vec<String> {
    let cache_key = format!("{imdb}:{lang}");
    {
        let cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = cache.get(&cache_key) {
            if e.exp > (state.clock)() {
                return e.ids.clone();
            }
        }
    }
    // TMDB + KinoCheck concurrently (KinoCheck is only a fallback source, but fetching it in
    // parallel costs no extra wall-clock). Official trailer first, KinoCheck appended.
    let (tmdb, kc) = tokio::join!(
        state.upstream.tmdb_candidates(tmdb_key, imdb, ty, lang),
        state.upstream.kinocheck_youtube_id(kinocheck_key, imdb, ty, lang),
    );
    let mut seen = HashSet::new();
    let mut candidates: Vec<String> = Vec::new();
    for c in tmdb.into_iter().chain(kc) {
        if seen.insert(c.clone()) {
            candidates.push(c);
        }
    }
    candidates.truncate(MAX_PROBE);
    // NO probe: return the TMDB/KinoCheck candidates in rank order. The client plays the first that is
    // playable AND landscape, advancing past a portrait/dead pick — so yt-dlp stays OFF the /meta critical
    // path (a resolve is a TMDB call, ~200 ms, not a 2–4 s extraction). Playability + de-letterboxing are
    // validated lazily on /play (whose download outcome now drives the /health extraction signal).
    let mut ids = candidates;
    // Fallback: NO TMDB/KinoCheck candidate at all (a brand-new title TMDB hasn't linked a video for) →
    // search YouTube for "<title year> trailer". Still no probe — the results are returned as candidates.
    if ids.is_empty() {
        if let Some(title) = state.upstream.tmdb_title(tmdb_key, imdb, ty).await {
            let query = format!("{title} trailer");
            for c in (state.searcher)(query.clone()).await {
                if seen.insert(c.clone()) {
                    ids.push(c);
                }
            }
            ids.truncate(MAX_PROBE);
            eprintln!("trailer {imdb} ({ty}/{lang}): no candidates → search {query:?} → {} result(s)", ids.len());
        }
    }
    // A title with no trailer at all is a normal empty (short-cached), not an extraction failure.
    if ids.is_empty() {
        eprintln!("trailer {imdb} ({ty}/{lang}): no TMDB/KinoCheck candidates + search found nothing");
    }
    let ttl = if ids.is_empty() { YT_NEG_TTL_MS } else { YT_TTL_MS };
    {
        let mut cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
        // Bound growth: when the map gets large, sweep expired entries before inserting so a
        // long-running instance with many distinct lookups doesn't leak unboundedly.
        if cache.len() >= YT_CACHE_MAX {
            let now = (state.clock)();
            cache.retain(|_, e| e.exp > now);
        }
        cache.insert(cache_key, YtEntry { ids: ids.clone(), exp: (state.clock)() + ttl });
    }
    ids
}

pub async fn handle_meta(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    cfg: Option<&crate::userconfig::UserConfig>,
    ty: &str,
    raw_id: &str,
    query: &str,
) -> Response<Body> {
    let imdb = raw_id.split(':').next().unwrap_or(""); // series may arrive as tt…:S:E — trailers are show-level
    let base = self_base(state.cfg.public_base_url.as_deref(), headers, state.cfg.port);
    // Only imdb ids reach the upstreams (and our URLs) — reject anything else so a crafted id
    // can't be interpolated into a TMDB/KinoCheck request.
    if !is_imdb(imdb) {
        return httputil::json(StatusCode::OK, &build_meta(ty, imdb, &base, &[], state.cfg.stream_remux), &[]);
    }
    // Effective BYOK credentials: the per-install URL config wins; the server env keys are only a
    // migration fallback for legacy config-less installs (den-scout/docs/SEALED-CONFIG.md).
    let tmdb_key = cfg
        .map(|c| c.tmdb_key.as_str())
        .or(state.cfg.tmdb_key.as_deref())
        .unwrap_or("");
    let kinocheck_key = cfg
        .and_then(|c| c.kinocheck_key.as_deref())
        .or(state.cfg.kinocheck_key.as_deref());
    let raw_lang = query_param(query, "lang").unwrap_or_else(|| "en".to_string());
    let lang = if valid_lang(&raw_lang) { raw_lang } else { "en".to_string() };
    let yt_ids = resolve_youtube_ids(state, tmdb_key, kinocheck_key, imdb, ty, &lang).await;
    // Prewarm only the primary (the one the client plays first) UNLESS the caller opted out (?prewarm=0);
    // the alternates are downloaded on demand only if that first one fails.
    if let Some(primary) = yt_ids.first() {
        if query_param(query, "prewarm").as_deref() != Some("0") {
            (state.prewarm)(state.clone(), primary.clone());
        }
    }
    let payload = build_meta(ty, imdb, &base, &yt_ids, state.cfg.stream_remux);
    // A SUCCESSFUL resolution (a real trailer) is cacheable 7d; an empty result (no trailer /
    // geo-blocked / a transient upstream fault) is no-store so the client re-checks a miss.
    let has_link = payload["meta"]["links"].as_array().is_some_and(|a| !a.is_empty());
    let extra: &[(&str, &str)] = if has_link {
        &[("cache-control", "public, max-age=604800, stale-while-revalidate=86400")]
    } else {
        &[("cache-control", "no-store")]
    };
    httputil::json(StatusCode::OK, &payload, extra)
}
