//! ADDON discovery: imdb id → ordered YouTube trailer candidates, via TMDB (primary) and KinoCheck
//! (fallback). Behind a trait so tests can swap in a fake with no network.

use std::sync::atomic::{AtomicU32, Ordering};
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
    /// Consecutive hard upstream faults, for /health (ADDON-02). Non-HTTP upstreams report 0.
    fn recent_failures(&self) -> u32 {
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
        .filter(|v| v["site"] == "YouTube" && v["key"].as_str().is_some_and(|k| !k.is_empty()))
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
}

impl HttpUpstream {
    pub fn new(cfg: Arc<Config>, http: reqwest::Client) -> HttpUpstream {
        HttpUpstream { cfg, http, fails: AtomicU32::new(0) }
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
                self.fails.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let status = res.status();
        if !status.is_success() {
            // Surface the faults that mean "misconfigured / throttled / upstream down" — but not 404
            // (a normal "not found" for KinoCheck), so a broken TMDB_KEY isn't a silent empty result.
            if status == 401 || status == 403 || status == 429 || status.is_server_error() {
                eprintln!("upstream {} -> {status}", redact(url));
                self.fails.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        self.fails.store(0, Ordering::Relaxed); // a successful call clears the degraded state
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
        data["trailer"]["youtube_video_id"].as_str().map(|s| s.to_string())
    }

    fn recent_failures(&self) -> u32 {
        self.fails.load(Ordering::Relaxed)
    }
}
