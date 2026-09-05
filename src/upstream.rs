//! ADDON discovery: imdb id → ordered YouTube trailer candidates, via TMDB (primary) and KinoCheck
//! (fallback). Behind a trait so tests can swap in a fake with no network.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use crate::config::Config;

/// Max upstream JSON body we'll buffer.
///
/// 256 KB, down from 4 MB. Asking for `include_video_language={lang},en,null` made these responses
/// 10–30× larger — a title can now come back with 57 videos instead of two — so "these payloads are
/// a few KB" stopped being the whole story, and the cap is the only thing standing between a runaway
/// or hostile body and memory. It is not one buffer either: nothing gates concurrent resolves, so the
/// real ceiling is this times however many `/meta` misses are in flight, and each buffer becomes a
/// `serde_json::Value` tree several times its own size.
///
/// 256 KB still admits roughly a thousand videos for one title, which is orders of magnitude past
/// anything TMDB actually returns.
const MAX_UPSTREAM_BODY: usize = 256 * 1024;

/// A source could not be asked: transport error, a wrong key's 401, a 429, a 5xx, or a 200 whose
/// body never arrived. Distinct from a source that answered with nothing, which is a real result.
/// The distinction has to travel with the call — a shared counter compared across one cannot say
/// which lookup faulted, so an unrelated title's outage was read as this one's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoAnswer;

pub type Answered<T> = Result<T, NoAnswer>;

/// The two upstream lookups the resolver needs, plus the title lookup behind the search fallback.
#[async_trait]
pub trait Upstream: Send + Sync {
    /// `tmdb_key` / `kinocheck_key` are the per-request BYOK credentials (from the URL config, or the
    /// env fallback during migration) — resolved by the caller so the upstream holds no key of its own.
    /// An empty `tmdb_key` is `Ok(vec![])`: TMDB was not consulted, which is not a failure.
    async fn tmdb_candidates(&self, tmdb_key: &str, imdb: &str, ty: &str, lang: &str) -> Answered<Vec<String>>;
    async fn kinocheck_youtube_id(&self, kinocheck_key: Option<&str>, imdb: &str, ty: &str, lang: &str) -> Answered<Option<String>>;
    /// imdb → the title (+ year, e.g. "Backrooms 2025") for a YouTube-search fallback query, or None on
    /// miss. Used only when TMDB/KinoCheck carry no trailer for the title.
    async fn tmdb_title(&self, tmdb_key: &str, imdb: &str, ty: &str) -> Answered<Option<String>>;
    /// Consecutive hard upstream faults, for /health (ADDON-02). Non-HTTP upstreams report 0.
    fn recent_failures(&self) -> u32 {
        0
    }
}

/// Which language band a TMDB /videos entry falls in: the film's own language, English, or other.
///
/// An untagged video (`iso_639_1` absent or empty) lands in `other` rather than being guessed at. We
/// ask for `null` in `include_video_language` so those are not lost, but an untagged video is not
/// evidence of anything, so it sorts behind the two we can actually identify.
fn lang_band(v: &Value, original: &str) -> u8 {
    match v["iso_639_1"].as_str() {
        Some(l) if l.eq_ignore_ascii_case(original) => 0,
        Some(l) if l.eq_ignore_ascii_case("en") => 1,
        _ => 2,
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

/// Rank + dedupe a TMDB /videos result into an ordered list of YouTube ids. Pure — unit-tested.
///
/// Ordered by language band first, then by kind (official trailer, trailer, teaser, other). The
/// bands are: the FILM'S OWN language, then English, then anything else.
///
/// `original` is the film's `original_language`, not the viewer's. That is the whole point: a trailer
/// tagged with the viewer's language is a dub or a local-market cut, and the thing worth watching is
/// the film as it was made — with subtitles if the client wants them, which is the client's business.
/// English second because it is both the most common original language and the most likely to exist
/// at all; when the film IS English the two bands coincide and this is simply kind order.
pub fn pick_trailer_candidates(results: &[Value], original: &str) -> Vec<String> {
    let mut yt: Vec<&Value> = results
        .iter()
        // An id from upstream is untrusted: it ends up as a cache filename and a yt-dlp -o path,
        // so `../../tmp/evil` would write outside the cache dir. The inbound imdb id is already
        // checked for the same reason; this is the other direction.
        .filter(|v| v["site"] == "YouTube" && v["key"].as_str().is_some_and(crate::is_valid_vid))
        .collect();
    // stable → preserves TMDB order within a rank, like JS's sort
    yt.sort_by_key(|v| (lang_band(v, original), rank(v)));
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
        HttpUpstream {
            cfg,
            http,
            fails: AtomicU32::new(0),
        }
    }

    /// Is this the source /health speaks for? KinoCheck is a fallback — its outage does not mean
    /// trailers are broken, and counting it let `/health` say "TMDB/KinoCheck have been failing"
    /// while TMDB was answering every request.
    fn counts_toward_health(&self, url: &str) -> bool {
        url.starts_with(&self.cfg.tmdb_base)
    }

    /// `Ok(Some(v))` parsed; `Ok(None)` the upstream said "not there" (404); `Err(NoAnswer)` we did
    /// not get an answer at all.
    async fn get_json(&self, url: &str, headers: &[(&str, &str)]) -> Answered<Option<Value>> {
        let mut req = self.http.get(url);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let res = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                // A network/DNS/TLS fault is a HARD failure (vs a 200-with-no-results miss) — log it
                // with the path only. redact() strips the api_key from OUR url, but reqwest's own
                // Display re-appends the whole thing ("… for url (…?api_key=…)"), so redacting one
                // side and interpolating the error beside it published the BYOK key on every
                // outage. without_url() drops reqwest's copy; keep both, or neither works.
                eprintln!("{}", transport_fault_line(url, e));
                if self.counts_toward_health(url) {
                    self.fails.fetch_add(1, Ordering::Relaxed);
                }
                return Err(NoAnswer);
            }
        };
        let status = res.status();
        if !status.is_success() {
            // Surface the faults that mean "misconfigured / throttled / upstream down" — but not 404
            // (a normal "not found" for KinoCheck), so a broken TMDB_KEY isn't a silent empty result.
            eprintln!("upstream {} -> {status}", redact(url));
            // 401/403 is THIS install's key, not the upstream — and /health is process-wide while
            // keys are per-install, so counting them let one bad key report "TMDB has been failing"
            // for everyone, and a healthy install's traffic cleared the counter so a persistently
            // broken one never surfaced. The CALLER still hears about it: a wrong key means this
            // request got no answer, which is a different question from whether TMDB is up.
            if (status == 429 || status.is_server_error()) && self.counts_toward_health(url) {
                self.fails.fetch_add(1, Ordering::Relaxed);
            }
            // A 404 is a real "this title is not there". Everything else is an absent answer.
            if status == 404 {
                return Ok(None);
            }
            return Err(NoAnswer);
        }
        // Cap the body (defense-in-depth beyond the 15s timeout): these JSON payloads are small, so a
        // multi-MB response is either broken or hostile — stop reading rather than buffer it all.
        let mut stream = res.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                // A body that stalls or truncates after a 200 — the shape an overloaded upstream
                // takes. Everything below returns the same empty result as "no trailer", so a
                // status line alone must not count as an answer: it used to return here silently,
                // having ALREADY cleared the health signal, and the caller then pinned "no trailer"
                // for an hour for every install.
                Err(e) => return self.no_answer(url, &body_fault_why(e)),
            };
            if buf.len() + chunk.len() > MAX_UPSTREAM_BODY {
                return self.no_answer(url, &format!("body over {MAX_UPSTREAM_BODY} bytes"));
            }
            buf.extend_from_slice(&chunk);
        }
        let Ok(v) = serde_json::from_slice(&buf) else {
            return self.no_answer(url, "body was not JSON");
        };
        if self.counts_toward_health(url) {
            self.fails.store(0, Ordering::Relaxed); // a parsed TMDB response clears the signal
        }
        Ok(Some(v))
    }

    /// Log and return: we did not get an answer, whatever the status line said. A fault that lands
    /// after the status line is the same outage as one that lands before it — reqwest's timeout
    /// spans the body read, so which side of the line a wedged upstream falls on is arbitrary.
    fn no_answer(&self, url: &str, why: &str) -> Answered<Option<Value>> {
        eprintln!("upstream {}: {why}", redact(url));
        if self.counts_toward_health(url) {
            self.fails.fetch_add(1, Ordering::Relaxed);
        }
        Err(NoAnswer)
    }
}

/// The log line for a transport fault, built here so it can be tested for what it must NOT contain.
///
/// Both halves have to be redacted. `redact()` strips the api_key from our own URL, but reqwest's
/// Display re-appends the entire thing ("… for url (…?api_key=…)") whenever the error carries one,
/// so interpolating the error beside a redacted URL published the BYOK key on every transport fault
/// — throughout precisely the outage that generates the most log lines.
/// An error plus its source chain. Display alone is a bare category ("error sending request",
/// "error decoding response body") that renders connection-refused, DNS failure, TLS failure and
/// timeout identically — the cause is only in the chain. Debug carries it too, but re-includes the
/// url and with it the api_key, so walk source(): those are hyper/rustls/std errors, which never
/// carry a url.
fn cause_chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        out.push_str(&format!(": {c}"));
        cause = c.source();
    }
    out
}

/// Why a body read failed, with its cause. Same reason as `transport_fault_line`: "error decoding
/// response body" is identical for a truncation and for a timeout, and the difference — one is the
/// upstream dying mid-response, the other is it wedging — is the whole diagnostic.
pub(crate) fn body_fault_why(e: reqwest::Error) -> String {
    // Body errors carry no url today (reqwest sets one only on the send path), so this is belt and
    // braces — but the function is pub(crate), and a future caller handing it a send-path error
    // would leak the api_key immediately. Costs nothing to not depend on that.
    format!("body read failed ({})", cause_chain(&e.without_url()))
}

pub(crate) fn transport_fault_line(url: &str, e: reqwest::Error) -> String {
    // without_url() is what keeps the api_key out: reqwest's Display re-appends the whole url, and
    // it is the only error in the chain that carries one.
    let e = e.without_url();
    format!("upstream request failed: {} ({})", redact(url), cause_chain(&e))
}

/// Drop the query string (which carries `api_key=…`) so a logged URL never leaks the key.
fn redact(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

#[async_trait]
impl Upstream for HttpUpstream {
    /// imdb → TMDB id (via /find) → /videos → ordered YouTube trailer candidates ([] on miss).
    // `_lang` is the VIEWER's language, and TMDB video selection deliberately does not use it — see
    // the `include_video_language` reasoning below. KinoCheck still does.
    async fn tmdb_candidates(&self, tmdb_key: &str, imdb: &str, ty: &str, _lang: &str) -> Answered<Vec<String>> {
        if tmdb_key.is_empty() {
            return Ok(Vec::new()); // not consulted, which is not a failure
        }
        let key = tmdb_key;
        let tmdb_type = if ty == "series" { "tv" } else { "movie" };
        let find_url = format!(
            "{}/find/{imdb}?external_source=imdb_id&api_key={key}",
            self.cfg.tmdb_base
        );
        let Some(found) = self.get_json(&find_url, &[]).await? else {
            return Ok(Vec::new());
        };
        let results = if tmdb_type == "movie" {
            &found["movie_results"]
        } else {
            &found["tv_results"]
        };
        let Some(hit_id) = results.get(0).and_then(|h| h["id"].as_i64()) else {
            return Ok(Vec::new());
        };
        // The film's own language, which is what decides which trailer we want — NOT the viewer's.
        // Already in the /find hit, so it costs nothing to read.
        let original = results
            .get(0)
            .and_then(|h| h["original_language"].as_str())
            .filter(|l| l.len() == 2 && l.bytes().all(|b| b.is_ascii_lowercase()))
            .unwrap_or("en");
        // `include_video_language` is what makes this work at all, and it is doing two jobs.
        //
        // Without it, `/videos` returns only videos TAGGED with `language=` — and with no `language=`
        // at all TMDB defaults to en-US, which is the same thing. Measured against the live API:
        // Amélie returns 14 videos, all English, and NOT its two French trailers. So the original
        // trailer for a foreign-language film was simply unreachable.
        //
        // And it takes a list that `language=` need not contain, so the film's language can be asked
        // for regardless of the viewer's: `language=en&include_video_language=fr,en,null` returns the
        // French ones. Amélie 14 → 16 (fr=2), Spirited Away 6 → 7 (ja=1).
        //
        // The viewer's language is deliberately NOT in this list. A video tagged with it is a dub or
        // a local-market cut, not the film as made — asking for `fi` got Oppenheimer a Finnish
        // trailer ranked above the English original. Leaving it out drops that (52 → 51) as well as
        // adding what we wanted. Subtitles are the client's business, not ours.
        let videos_url = format!(
            "{}/{tmdb_type}/{hit_id}/videos?api_key={key}&language=en&include_video_language={original},en,null",
            self.cfg.tmdb_base
        );
        let Some(data) = self.get_json(&videos_url, &[]).await? else {
            return Ok(Vec::new());
        };
        let empty = Vec::new();
        let results = data["results"].as_array().unwrap_or(&empty);
        Ok(pick_trailer_candidates(results, original))
    }

    /// imdb → "Title Year" via TMDB /find, for the YouTube-search fallback query. None on miss.
    async fn tmdb_title(&self, tmdb_key: &str, imdb: &str, ty: &str) -> Answered<Option<String>> {
        if tmdb_key.is_empty() {
            return Ok(None); // not consulted, which is not a failure
        }
        let tmdb_type = if ty == "series" { "tv" } else { "movie" };
        let find_url = format!(
            "{}/find/{imdb}?external_source=imdb_id&api_key={tmdb_key}",
            self.cfg.tmdb_base
        );
        let Some(found) = self.get_json(&find_url, &[]).await? else {
            return Ok(None);
        };
        let hit = if tmdb_type == "movie" {
            found["movie_results"].get(0)
        } else {
            found["tv_results"].get(0)
        };
        let Some(hit) = hit else { return Ok(None) };
        // Movies carry `title` + `release_date`; TV carries `name` + `first_air_date`.
        let Some(title) = hit["title"].as_str().or_else(|| hit["name"].as_str()) else {
            return Ok(None);
        };
        let title = title.trim();
        if title.is_empty() {
            return Ok(None);
        }
        let year = hit["release_date"]
            .as_str()
            .or_else(|| hit["first_air_date"].as_str())
            .and_then(|d| d.get(0..4))
            .filter(|y| y.len() == 4);
        Ok(Some(match year {
            Some(y) => format!("{title} {y}"),
            None => title.to_string(),
        }))
    }

    /// KinoCheck discovery fallback: imdb → official trailer's YouTube id (or None).
    async fn kinocheck_youtube_id(&self, kinocheck_key: Option<&str>, imdb: &str, ty: &str, lang: &str) -> Answered<Option<String>> {
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
        let Some(data) = self.get_json(&url, &headers).await? else {
            return Ok(None);
        };
        Ok(data["trailer"]["youtube_video_id"]
            .as_str()
            .filter(|id| crate::is_valid_vid(id))
            .map(|s| s.to_string()))
    }

    fn recent_failures(&self) -> u32 {
        self.fails.load(Ordering::Relaxed)
    }
}
