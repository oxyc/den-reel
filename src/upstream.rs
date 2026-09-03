//! ADDON discovery: imdb id → ordered YouTube trailer candidates, via TMDB (primary) and KinoCheck
//! (fallback). Behind a trait so tests can swap in a fake with no network.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use crate::config::Config;

/// Max upstream JSON body we'll buffer. TMDB /videos + KinoCheck responses are a few KB; 4 MB is a
/// generous ceiling that still stops a runaway/hostile body from ballooning memory.
const MAX_UPSTREAM_BODY: usize = 4 * 1024 * 1024;

/// The two upstream lookups the resolver needs. A miss/error is always an empty result (never an
/// error to the caller) — same as the Node version's try/catch-to-`[]`.
#[async_trait]
pub trait Upstream: Send + Sync {
    /// `tmdb_key` / `kinocheck_key` are the per-request BYOK credentials (from the URL config, or the
    /// env fallback during migration) — resolved by the caller so the upstream holds no key of its own.
    async fn tmdb_candidates(&self, tmdb_key: &str, imdb: &str, ty: &str, lang: &str) -> Vec<String>;
    async fn kinocheck_youtube_id(&self, kinocheck_key: Option<&str>, imdb: &str, ty: &str, lang: &str) -> Option<String>;
    /// imdb → the title (+ year, e.g. "Backrooms 2025") for a YouTube-search fallback query, or None on
    /// miss. Used only when TMDB/KinoCheck carry no trailer for the title.
    async fn tmdb_title(&self, tmdb_key: &str, imdb: &str, ty: &str) -> Option<String>;
    /// Consecutive hard upstream faults, for /health (ADDON-02). Non-HTTP upstreams report 0.
    fn recent_failures(&self) -> u32 {
        0
    }
    /// Monotonic count of hard faults, for callers that must tell "the lookup failed" from "the
    /// lookup found nothing". Compare it across a call: if it moved, the empty result is not an
    /// answer. Like `recent_failures` this speaks only for TMDB — KinoCheck is a fallback, and
    /// letting its outage mean "we got no answer" made every resolve refuse to cache, which turned
    /// one dead fallback into unbounded repeat lookups. Unlike `recent_failures` it never resets
    /// (a later success must not erase what this call saw) and it counts 401/403, which are not a
    /// health signal but do mean this request got no answer.
    fn hard_faults(&self) -> u64 {
        0
    }
}

/// Rank a TMDB /videos entry: official trailer first, then trailer, teaser, anything else.
fn rank(v: &Value) -> u8 {
    let ty = v["type"].as_str().unwrap_or("");
    let official = v["official"].as_bool().unwrap_or(false);
    match (ty, official) {
        ("Trailer", true) => 0,
        ("Trailer", _) => 1,
        ("Teaser", _) => 2,
        _ => 3,
    }
}

/// Rank + dedupe a TMDB /videos result into an ordered list of YouTube ids (official trailer first,
/// then trailer, teaser, anything else). Pure — unit-tested.
pub fn pick_trailer_candidates(results: &[Value]) -> Vec<String> {
    let mut yt: Vec<&Value> = results
        .iter()
        // An id from upstream is untrusted: it ends up as a cache filename and a yt-dlp -o path,
        // so `../../tmp/evil` would write outside the cache dir. The inbound imdb id is already
        // checked for the same reason; this is the other direction.
        .filter(|v| v["site"] == "YouTube" && v["key"].as_str().is_some_and(crate::is_valid_vid))
        .collect();
    yt.sort_by_key(|v| rank(v)); // stable → preserves TMDB order within a rank, like JS's sort
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for v in yt {
        if let Some(k) = v["key"].as_str() {
            if seen.insert(k.to_string()) {
                out.push(k.to_string());
            }
        }
    }
    out
}

pub struct HttpUpstream {
    cfg: Arc<Config>,
    http: reqwest::Client,
    /// Consecutive hard upstream faults (transport / 401 / 403 / 429 / 5xx) — surfaced as `degraded`
    /// on /health (ADDON-02). A 404 "not found" is a miss, not a fault, so it doesn't count.
    fails: AtomicU32,
    /// Never reset, and not restricted to the source /health speaks for. See `hard_faults`.
    faults: AtomicU64,
}

impl HttpUpstream {
    pub fn new(cfg: Arc<Config>, http: reqwest::Client) -> HttpUpstream {
        HttpUpstream { cfg, http, fails: AtomicU32::new(0), faults: AtomicU64::new(0) }
    }

    /// Is this the source /health speaks for? KinoCheck is a fallback — its outage does not mean
    /// trailers are broken, and counting it let `/health` say "TMDB/KinoCheck have been failing"
    /// while TMDB was answering every request.
    fn counts_toward_health(&self, url: &str) -> bool {
        url.starts_with(&self.cfg.tmdb_base)
    }

    async fn get_json(&self, url: &str, headers: &[(&str, &str)]) -> Option<Value> {
        let mut req = self.http.get(url);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let res = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                // A network/DNS/TLS fault is a HARD failure (vs a 200-with-no-results miss) — log it
                // (path only; the api_key lives in the query string and is dropped by redact()).
                eprintln!("upstream request failed: {} ({e})", redact(url));
                if self.counts_toward_health(url) {
                    self.faults.fetch_add(1, Ordering::Relaxed);
                    self.fails.fetch_add(1, Ordering::Relaxed);
                }
                return None;
            }
        };
        let status = res.status();
        if !status.is_success() {
            // Surface the faults that mean "misconfigured / throttled / upstream down" — but not 404
            // (a normal "not found" for KinoCheck), so a broken TMDB_KEY isn't a silent empty result.
            eprintln!("upstream {} -> {status}", redact(url));
            // A 404 is a real "this title is not there"; anything else means we did not get an answer.
            if status != 404 && self.counts_toward_health(url) {
                self.faults.fetch_add(1, Ordering::Relaxed);
            }
            // 401/403 is THIS install's key, not the upstream. The counter is process-wide while
            // keys are per-install, so counting them let one bad key report "TMDB has been failing"
            // for everyone — and, the other way round, a healthy install's traffic cleared the
            // counter so a persistently broken one never showed up at all.
            if (status == 429 || status.is_server_error()) && self.counts_toward_health(url) {
                self.fails.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        if self.counts_toward_health(url) {
            self.fails.store(0, Ordering::Relaxed); // a successful TMDB call clears the signal
        }
        // Cap the body (defense-in-depth beyond the 15s timeout): these JSON payloads are small, so a
        // multi-MB response is either broken or hostile — stop reading rather than buffer it all.
        let mut stream = res.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.ok()?;
            if buf.len() + chunk.len() > MAX_UPSTREAM_BODY {
                eprintln!("upstream body over {MAX_UPSTREAM_BODY} bytes: {}", redact(url));
                return None;
            }
            buf.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&buf).ok() // null (not throw) on a malformed body, like safeJson()
    }
}

/// Drop the query string (which carries `api_key=…`) so a logged URL never leaks the key.
fn redact(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

#[async_trait]
impl Upstream for HttpUpstream {
    /// imdb → TMDB id (via /find) → /videos → ordered YouTube trailer candidates ([] on miss).
    async fn tmdb_candidates(&self, tmdb_key: &str, imdb: &str, ty: &str, lang: &str) -> Vec<String> {
        if tmdb_key.is_empty() {
            return Vec::new();
        }
        let key = tmdb_key;
        let tmdb_type = if ty == "series" { "tv" } else { "movie" };
        let find_url = format!(
            "{}/find/{imdb}?external_source=imdb_id&api_key={key}",
            self.cfg.tmdb_base
        );
        let found = match self.get_json(&find_url, &[]).await {
            Some(v) => v,
            None => return Vec::new(),
        };
        let results = if tmdb_type == "movie" {
            &found["movie_results"]
        } else {
            &found["tv_results"]
        };
        let hit_id = match results.get(0).and_then(|h| h["id"].as_i64()) {
            Some(id) => id,
            None => return Vec::new(),
        };
        let videos_url = format!(
            "{}/{tmdb_type}/{hit_id}/videos?api_key={key}&language={lang}",
            self.cfg.tmdb_base
        );
        let data = match self.get_json(&videos_url, &[]).await {
            Some(v) => v,
            None => return Vec::new(),
        };
        let empty = Vec::new();
        let results = data["results"].as_array().unwrap_or(&empty);
        pick_trailer_candidates(results)
    }

    /// imdb → "Title Year" via TMDB /find, for the YouTube-search fallback query. None on miss.
    async fn tmdb_title(&self, tmdb_key: &str, imdb: &str, ty: &str) -> Option<String> {
        if tmdb_key.is_empty() {
            return None;
        }
        let tmdb_type = if ty == "series" { "tv" } else { "movie" };
        let find_url = format!(
            "{}/find/{imdb}?external_source=imdb_id&api_key={tmdb_key}",
            self.cfg.tmdb_base
        );
        let found = self.get_json(&find_url, &[]).await?;
        let hit = if tmdb_type == "movie" {
            found["movie_results"].get(0)?
        } else {
            found["tv_results"].get(0)?
        };
        // Movies carry `title` + `release_date`; TV carries `name` + `first_air_date`.
        let title = hit["title"].as_str().or_else(|| hit["name"].as_str())?.trim();
        if title.is_empty() {
            return None;
        }
        let year = hit["release_date"]
            .as_str()
            .or_else(|| hit["first_air_date"].as_str())
            .and_then(|d| d.get(0..4))
            .filter(|y| y.len() == 4);
        Some(match year {
            Some(y) => format!("{title} {y}"),
            None => title.to_string(),
        })
    }

    /// KinoCheck discovery fallback: imdb → official trailer's YouTube id (or None).
    async fn kinocheck_youtube_id(&self, kinocheck_key: Option<&str>, imdb: &str, ty: &str, lang: &str) -> Option<String> {
        let endpoint = if ty == "series" { "shows" } else { "movies" };
        let language = if lang.starts_with("de") { "de" } else { "en" };
        let url = format!(
            "{}/{endpoint}?imdb_id={imdb}&categories=Trailer&language={language}",
            self.cfg.kinocheck_base
        );
        let mut headers: Vec<(&str, &str)> = vec![("Accept", "application/json")];
        if let Some(k) = kinocheck_key {
            headers.push(("X-Api-Key", k));
            headers.push(("X-Api-Host", "api.kinocheck.com"));
        }
        let data = self.get_json(&url, &headers).await?;
        data["trailer"]["youtube_video_id"]
            .as_str()
            .filter(|id| crate::is_valid_vid(id))
            .map(|s| s.to_string())
    }

    fn recent_failures(&self) -> u32 {
        self.fails.load(Ordering::Relaxed)
    }
    fn hard_faults(&self) -> u64 {
        self.faults.load(Ordering::Relaxed)
    }
}
