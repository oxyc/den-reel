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

/// Build the Fusion `meta` payload for a resolved (or missing) trailer.
pub fn build_meta(ty: &str, imdb: &str, base: &str, yt_id: &str) -> Value {
    if yt_id.is_empty() {
        return json!({ "meta": { "id": imdb, "type": ty, "links": [] } });
    }
    let trailers = format!("{}/play/{yt_id}.mp4", base.trim_end_matches('/'));
    json!({
        "meta": {
            "id": imdb,
            "type": ty,
            "links": [{
                "name": "Trailer",
                "category": "Trailer",
                "trailers": trailers,
                "provider": "Den Reel",
            }],
        }
    })
}

/// First candidate yt-dlp can actually extract here, preserving rank order. The common case (top
/// trailer plays) costs ONE probe; only on a miss do we probe the rest concurrently and take the
/// highest-ranked that passes — so a geo-blocked top pick no longer serialises N×3s.
async fn first_playable(state: &Arc<AppState>, candidates: &[String]) -> String {
    if candidates.is_empty() {
        return String::new();
    }
    if (state.prober)(candidates[0].clone()).await {
        return candidates[0].clone();
    }
    let rest = &candidates[1..];
    // Spawn so the probes actually run concurrently (futures are lazy)...
    let mut handles: Vec<_> = rest
        .iter()
        .map(|c| tokio::spawn((state.prober)(c.clone())))
        .collect();
    for i in 0..handles.len() {
        if (&mut handles[i]).await.unwrap_or(false) {
            // ...but await in rank order → highest-ranked winner. Cancel the probes we no longer
            // need (their yt-dlp --simulate processes die with the task via kill_on_drop).
            for h in &handles[i + 1..] {
                h.abort();
            }
            return rest[i].clone();
        }
    }
    String::new()
}

/// Resolve (and cache) the first PLAYABLE trailer ytId for an imdb id. "" = looked up, nothing
/// playable (cached shorter, in case it's transient). `tmdb_key`/`kinocheck_key` are the effective
/// per-request BYOK credentials (URL config, or env fallback). The cache is keyed by `imdb:lang` only —
/// the resolved trailer is public and key-independent, so installs with different keys share one entry.
pub async fn resolve_youtube_id(
    state: &Arc<AppState>,
    tmdb_key: &str,
    kinocheck_key: Option<&str>,
    imdb: &str,
    ty: &str,
    lang: &str,
) -> String {
    let cache_key = format!("{imdb}:{lang}");
    {
        let cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = cache.get(&cache_key) {
            if e.exp > (state.clock)() {
                return e.id.clone();
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
    let id = first_playable(state, &candidates).await;
    let ttl = if id.is_empty() { YT_NEG_TTL_MS } else { YT_TTL_MS };
    {
        let mut cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
        // Bound growth: when the map gets large, sweep expired entries before inserting so a
        // long-running instance with many distinct lookups doesn't leak unboundedly.
        if cache.len() >= YT_CACHE_MAX {
            let now = (state.clock)();
            cache.retain(|_, e| e.exp > now);
        }
        cache.insert(cache_key, YtEntry { id: id.clone(), exp: (state.clock)() + ttl });
    }
    id
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
        return httputil::json(StatusCode::OK, &build_meta(ty, imdb, &base, ""), &[]);
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
    let yt_id = resolve_youtube_id(state, tmdb_key, kinocheck_key, imdb, ty, &lang).await;
    // Prewarm the download UNLESS the caller opted out (?prewarm=0).
    if !yt_id.is_empty() && query_param(query, "prewarm").as_deref() != Some("0") {
        (state.prewarm)(state.clone(), yt_id.clone());
    }
    let payload = build_meta(ty, imdb, &base, &yt_id);
    // A SUCCESSFUL resolution (a real trailer) is cacheable 7d; an empty result (no trailer /
    // geo-blocked / a transient upstream fault) is no-store so the client re-checks a miss.
    let has_link = payload["meta"]["links"].as_array().is_some_and(|a| !a.is_empty());
    let extra: &[(&str, &str)] = if has_link {
        &[("cache-control", "public, max-age=604800")]
    } else {
        &[("cache-control", "no-store")]
    };
    httputil::json(StatusCode::OK, &payload, extra)
}
