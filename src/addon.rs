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

/// `tt` + digits, and a BOUNDED number of them. The longest real IMDb id is 8 digits; 11 leaves
/// room for a decade of growth and still bounds everything downstream that this string becomes —
/// the upstream request path, and the resolve cache key it is interpolated into. Unbounded, a
/// caller could put a 60-digit id in a cache key and inflate the parked cache past the size the
/// loader will read, which silently disables persistence from then on. `/meta` has no gate in
/// front of it, so "nobody would do that" is not a bound.
fn is_imdb(id: &str) -> bool {
    id.strip_prefix("tt")
        .is_some_and(|d| (1..=11).contains(&d.len()) && d.bytes().all(|b| b.is_ascii_digit()))
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
///
/// `secret` is `REEL_PLAY_SECRET` when the operator has set it: the play URL then carries the tag
/// that `/play` and `/crop` will demand (see `sign.rs`). `None` — the default — emits exactly the
/// bare URL this has always emitted.
pub fn build_meta(ty: &str, imdb: &str, base: &str, yt_ids: &[String], secret: Option<&str>) -> Value {
    let base = base.trim_end_matches('/');
    // Derived once, not once per link: the MAC key depends only on the secret, and this signs up to
    // MAX_PROBE ids per response.
    let signer = secret.map(crate::sign::Signer::new);
    let links: Vec<Value> = yt_ids
        .iter()
        .map(|id| {
            let url = match &signer {
                Some(s) => format!("{base}/play/{id}.mp4?s={}", s.tag(id)),
                None => format!("{base}/play/{id}.mp4"),
            };
            json!({
                "name": "Trailer",
                "category": "Trailer",
                "trailers": url,
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
/// What a resolve produced. `stale` marks a last-known-good answer standing in for a lookup that
/// could not be made — correct to serve, but not something to pin in a client for a week.
pub struct Resolved {
    pub ids: Vec<String>,
    pub stale: bool,
}

pub async fn resolve_youtube_ids(
    state: &Arc<AppState>,
    tmdb_key: &str,
    kinocheck_key: Option<&str>,
    imdb: &str,
    ty: &str,
    lang: &str,
) -> Resolved {
    // The key names WHICH SOURCES this request could ask, and nothing else. It is deliberately
    // credential-free — a resolved trailer is public and key-independent, so installs that can ask
    // the same sources share an entry — but "same sources" is the part that has to be in the key.
    // An install that cannot ask a source gets a THINNER answer, and publishing that under the
    // shared key hands it to installs that could have asked: a config-less /meta blanked titles for
    // everyone, which is why the TMDB half was namespaced.
    //
    // KinoCheck needs the same treatment for the same reason. It is queried with or without a key
    // (upstream.rs only adds the X-Api-Key header when there is one), so a keyless install usually
    // gets an answer — until KinoCheck rate-limits or rejects it, and then `kc` is a fault, the
    // candidate list loses its fallback id, and that shorter list is what every keyed install reads
    // for a full YT_TTL_MS. It costs an alternate rather than a primary, which is exactly why it
    // went unnoticed: the trailer still plays, there is just no second one to fall back to.
    //
    // Presence, never the value: two installs with different keys still share, as they should.
    let sources = match (tmdb_key.is_empty(), kinocheck_key.is_some()) {
        (false, true) => "",
        (false, false) => ":nokc",
        (true, true) => ":nokey",
        (true, false) => ":nokey:nokc",
    };
    let cache_key = format!("{imdb}:{lang}{sources}");
    {
        let cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
        let now = (state.clock)();
        if let Some(e) = cache.get(&cache_key) {
            if e.exp > now {
                // Past when it would normally have expired means these ids are a stand-in written
                // by a failed lookup, not a fresh answer — the caller caches it in the client for
                // hours, not a week.
                return Resolved { ids: e.ids.clone(), stale: now >= e.confirmed + YT_TTL_MS };
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
                    eprintln!(
                        "trailer {imdb} ({ty}/{lang}): no candidates → search {query:?} → {} result(s)",
                        ids.len()
                    );
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
                    return Resolved { ids: e.ids.clone(), stale: true };
                }
                ids = e.ids.clone();
                confirmed = e.confirmed;
                substituted = true;
            }
        }
        // Bound growth. Sweeping only EXPIRED entries is not a bound: once that many are live the
        // map keeps growing and every later insert pays a full scan under this mutex for nothing.
        // Drop the nearest-to-expiry until under, so the number is a cap rather than a threshold.
        if cache.len() >= YT_CACHE_MAX {
            cache.retain(|_, e| e.exp > now);
            if cache.len() >= YT_CACHE_MAX {
                let mut exps: Vec<u64> = cache.values().map(|e| e.exp).collect();
                exps.sort_unstable();
                let cutoff = exps[cache.len() - YT_CACHE_MAX / 2];
                cache.retain(|_, e| e.exp > cutoff);
            }
        }
        cache.insert(cache_key, YtEntry { ids: ids.clone(), exp: now + ttl, confirmed });
    }
    if substituted {
        eprintln!("trailer {imdb} ({ty}/{lang}): lookup failed, serving the last known answer");
    }
    Resolved { ids, stale: substituted }
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
            &build_meta(ty, imdb, &base, &[], state.cfg.play_secret.as_deref()),
            &[("cache-control", "no-store")],
        );
    }
    // Effective BYOK credentials: the per-install URL config wins; the server env keys are only a
    // migration fallback for legacy config-less installs (den-scout/docs/SEALED-CONFIG.md).
    let tmdb_key = cfg.map(|c| c.tmdb_key.as_str()).or(state.cfg.tmdb_key.as_deref()).unwrap_or("");
    let kinocheck_key = cfg.and_then(|c| c.kinocheck_key.as_deref()).or(state.cfg.kinocheck_key.as_deref());
    let raw_lang = query_param(query, "lang").unwrap_or_else(|| "en".to_string());
    // Lowercased, not just accepted: the cache key and KinoCheck's language pick are both
    // case-sensitive, so "DE" got its own cache entry AND silently fell through to English.
    let lang = if valid_lang(&raw_lang) { raw_lang.to_ascii_lowercase() } else { "en".to_string() };
    let resolved = resolve_youtube_ids(state, tmdb_key, kinocheck_key, imdb, ty, &lang).await;
    let mut yt_ids = resolved.ids;
    // What /play learned, applied to what /meta hands out. Discovery does not probe, so without this
    // a candidate that is geo-blocked or removed keeps its upstream rank forever and every client
    // rediscovers it — one download permit and one yt-dlp process at a time.
    let demotion = crate::play::demote_known_dead(state, &mut yt_ids);
    // Prewarm only the primary (the one the client plays first) UNLESS the caller opted out (?prewarm=0);
    // the alternates are downloaded on demand only if that first one fails. After the demotion above,
    // a primary that is STILL known dead means every candidate is — so there is nothing worth
    // speculatively fetching, and the permit is better left for a /play someone is waiting on.
    if let Some(primary) = yt_ids.first() {
        if query_param(query, "prewarm").as_deref() != Some("0") && !demotion.head_dead {
            (state.prewarm)(state.clone(), primary.clone());
        }
    }
    let payload = build_meta(ty, imdb, &base, &yt_ids, state.cfg.play_secret.as_deref());
    // A SUCCESSFUL resolution (a real trailer) is cacheable 7d; an empty result (no trailer /
    // geo-blocked / a transient upstream fault) is no-store so the client re-checks a miss.
    let has_link = payload["meta"]["links"].as_array().is_some_and(|a| !a.is_empty());
    let extra: &[(&str, &str)] = if has_link && (resolved.stale || demotion.reordered) {
        // Two ways to get here, one reason. A last-known-good answer standing in for a lookup we
        // could not make: the server stops trusting it after a day, so pinning it in every client
        // for a week outlives that by six.
        //
        // And an order that reflects a /play failure. That signal lives 60 seconds for a timeout —
        // after which this server has forgotten it entirely — while the body it shaped would be
        // held by every client that fetched inside that window for seven days. The demotion is
        // deliberately applied per response rather than baked into the 24h resolve entry, on the
        // grounds that a block can lift; a week in the client's cache defeats exactly that.
        &[("cache-control", "public, max-age=3600")]
    } else if has_link {
        &[("cache-control", "public, max-age=604800, stale-while-revalidate=86400")]
    } else {
        &[("cache-control", "no-store")]
    };
    httputil::json(StatusCode::OK, &payload, extra)
}
