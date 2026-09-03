//! ADDON request path: resolve (and cache) the first PLAYABLE trailer for an imdb id, then build
//! the Fusion `meta` payload whose play URL points back at THIS host.

use std::collections::HashSet;
use std::sync::Arc;

use hyper::header::HeaderMap;
use hyper::{Response, StatusCode};
use serde_json::{json, Value};

use crate::httputil::{self, query_param, Body};
use crate::state::{AppState, YtEntry};
use crate::{MAX_PROBE, STALE_GRACE_MS, YT_CACHE_MAX, YT_FAIL_TTL_MS, YT_NEG_TTL_MS, YT_TTL_MS};

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
pub(crate) fn self_base(cfg_public: Option<&str>, headers: &HeaderMap, port: u16) -> String {
    if let Some(b) = cfg_public {
        return b.trim_end_matches('/').to_string();
    }
    let hdr = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    // A scheme, not whatever the header says. Reflected unchecked it produced URLs like
    // `javascript://host/play/...` — the same spoofing the Host filter below exists for, on the
    // field next to it.
    let proto = match hdr("x-forwarded-proto").map(|p| p.split(',').next().unwrap_or("").trim()) {
        Some(p) if p.eq_ignore_ascii_case("https") => "https",
        _ => "http",
    };
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
pub fn build_meta(ty: &str, imdb: &str, base: &str, yt_ids: &[String]) -> Value {
    let base = base.trim_end_matches('/');
    let links: Vec<Value> = yt_ids
        .iter()
        .map(|id| {
            json!({
                "name": "Trailer",
                "category": "Trailer",
                "trailers": format!("{base}/play/{id}.mp4"),
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
    // A keyless request gets its OWN namespace. The key is otherwise deliberately credential-free
    // (a resolved trailer is public and key-independent) — true for a lookup that ran, false for one
    // that could not: with no TMDB key only KinoCheck is consulted, and sharing that thinner answer
    // with keyed installs let a config-less /meta blank titles for everyone. Namespacing it means
    // the answer can be cached normally instead of re-asked forever, which is what the failure
    // cooldown was being stretched to cover — badly, since a missing key is not a transient blip.
    let cache_key = if tmdb_key.is_empty() {
        format!("{imdb}:{lang}:nokey")
    } else {
        format!("{imdb}:{lang}")
    };
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
    // Whether we got an ANSWER, per call. With a key TMDB decides — KinoCheck is a fallback whose
    // outage means only that we lost the fallback. Without one TMDB is never consulted, so
    // KinoCheck is the sole source and its outage is a total failure to get an answer.
    let sources_answered = if tmdb_key.is_empty() { kc.is_ok() } else { tmdb.is_ok() };
    let tmdb = tmdb.unwrap_or_default();
    let kc = kc.unwrap_or_default();
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
    let mut search_failed = false;
    // Fallback: NO TMDB/KinoCheck candidate at all (a brand-new title TMDB hasn't linked a video for) →
    // search YouTube for "<title year> trailer". Still no probe — the results are returned as candidates.
    if ids.is_empty() {
        let title = state.upstream.tmdb_title(tmdb_key, imdb, ty).await;
        // The title lookup is the gate on the search: if IT could not be asked, no search ran, and
        // the empty result below is not an answer either.
        search_failed = title.is_err();
        if let Ok(Some(title)) = title {
            let query = format!("{title} trailer");
            match (state.searcher)(query.clone()).await {
                Some(found) => {
                    for c in found {
                        if seen.insert(c.clone()) {
                            ids.push(c);
                        }
                    }
                    ids.truncate(MAX_PROBE);
                    eprintln!("trailer {imdb} ({ty}/{lang}): no candidates → search {query:?} → {} result(s)", ids.len());
                }
                // The third source has the same two-failures-one-value problem as the other two: a
                // broken yt-dlp returned the same empty list as "YouTube has nothing".
                None => search_failed = true,
            }
        }
    }
    // A title with no trailer at all is a normal empty (short-cached), not an extraction failure.
    if ids.is_empty() {
        // Two different failures, and the log used to call both the second one: if `tmdb_title`
        // returned None no search ever ran, which means TMDB does not know this id at all.
        if search_failed {
            eprintln!("trailer {imdb} ({ty}/{lang}): the search could not run (see above)");
        } else {
            eprintln!("trailer {imdb} ({ty}/{lang}): nothing found");
        }
    }
    // An empty result is only an ANSWER if we actually got one. Every failure mode — transport
    // error, a wrong BYOK key's 401, a 429, a 5xx — arrives here as the same empty Vec as a title
    // with no trailer, and caching that pinned "no trailer" for an hour under a key that
    // deliberately excludes the credential. So one install with a typo'd key, or a single TMDB
    // blip, blanked trailers for every install, with /health still green and a log line identical
    // to a real miss. A keyless request is not a failure but a narrower question — cached under its
    // own key, so it neither blanks keyed installs nor re-asks on every browse.
    //
    // Every source that was consulted actually answered. This used to be a process-wide counter
    // sampled before and after the join, which could not tell WHICH lookup faulted: an unrelated
    // title's outage landing in the window was read as this title's answer, and one install's bad
    // key marked every concurrent resolve failed. The signal travels with the call now.
    //
    // A short cooldown rather than no entry at all: skipping the cache entirely meant a persistent
    // fault turned every browse of a trailer-less title into two TMDB calls plus a yt-dlp search,
    // with nothing to rate-limit it — trading a stale answer for a stampede.
    let asked_and_got_an_answer = sources_answered && !search_failed;
    let ttl = match (ids.is_empty(), asked_and_got_an_answer) {
        (false, _) => YT_TTL_MS,
        (true, true) => YT_NEG_TTL_MS,
        (true, false) => YT_FAIL_TTL_MS,
    };
    let now = (state.clock)();
    let mut confirmed = now;
    let mut substituted = false;
    {
        let mut cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
        // A failure with nothing to show falls back to the last answer we had. The key is
        // credential-free and shared, so publishing an empty list here is a cache HIT for every
        // install — the title shows no trailer, and no upstream call happens to correct it.
        //
        // Only when the lookup produced NOTHING. A fresh non-empty result is the better answer even
        // if some other resolve faulted inside our window — the fault counter is process-wide, so
        // that says nothing about this lookup. Substituting there served a stale id while holding
        // the current one, and /meta ships it with a 7-day max-age.
        //
        // And only while the answer is still worth trusting. Re-serving rewrites `exp` to the retry
        // cooldown, so without an independent clock a title whose trailer was REMOVED upstream is
        // handed out forever, for as long as anything in the process keeps faulting.
        if ids.is_empty() && !asked_and_got_an_answer {
            if let Some(e) = cache
                .get(&cache_key)
                .filter(|e| !e.ids.is_empty() && now < e.confirmed + YT_TTL_MS + STALE_GRACE_MS)
            {
                // A live entry is a better answer than ours and already has its own expiry; taking
                // it without rewriting stops a slow failing resolve from downgrading a fast good
                // one's 24h entry to the 60s cooldown.
                if e.exp > now {
                    return e.ids.clone();
                }
                ids = e.ids.clone();
                confirmed = e.confirmed;
                substituted = true;
            }
        }
        // Bound growth: when the map gets large, sweep expired entries before inserting so a
        // long-running instance with many distinct lookups doesn't leak unboundedly.
        if cache.len() >= YT_CACHE_MAX {
            cache.retain(|_, e| e.exp > now);
        }
        cache.insert(cache_key, YtEntry { ids: ids.clone(), exp: now + ttl, confirmed });
    }
    if substituted {
        eprintln!("trailer {imdb} ({ty}/{lang}): lookup failed, serving the last known answer");
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
        return httputil::json(
            StatusCode::OK,
            &build_meta(ty, imdb, &base, &[]),
            &[("cache-control", "no-store")],
        );
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
    // Lowercased, not just accepted: the cache key and KinoCheck's language pick are both
    // case-sensitive, so "DE" got its own cache entry AND silently fell through to English.
    let lang = if valid_lang(&raw_lang) { raw_lang.to_ascii_lowercase() } else { "en".to_string() };
    let yt_ids = resolve_youtube_ids(state, tmdb_key, kinocheck_key, imdb, ty, &lang).await;
    // Prewarm only the primary (the one the client plays first) UNLESS the caller opted out (?prewarm=0);
    // the alternates are downloaded on demand only if that first one fails.
    if let Some(primary) = yt_ids.first() {
        if query_param(query, "prewarm").as_deref() != Some("0") {
            (state.prewarm)(state.clone(), primary.clone());
        }
    }
    let payload = build_meta(ty, imdb, &base, &yt_ids);
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
