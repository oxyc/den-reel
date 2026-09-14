//! HLS: YouTube's own master playlist, rewritten so a browser can fetch it through this server.
//!
//! `/direct` hands a browser googlevideo's URLs and gets out of the way, which is the cheapest thing
//! this service does — no download, no ffmpeg, no bytes through here. WebKit plays an HLS master from
//! a bare `<video>`, so on iOS that is the whole story.
//!
//! Everywhere else HLS needs MSE, and MSE fetches its segments with XHR: googlevideo answers those
//! with no `Access-Control-Allow-Origin` at all, so hls.js cannot read a byte of them. That left
//! Chrome and Firefox on `/play` — the download, the remux, the cache volume, and a cold trailer that
//! shows nothing for as long as the whole file takes.
//!
//! So proxy the playlists and their segments, and only those. The master is fetched here, every URI
//! in it is rewritten to `/seg`, and each of those fetches one googlevideo URL and streams it back
//! with this server's CORS on it. What the browser has then is ordinary HLS from one origin: it opens
//! on a low variant and climbs, so a cold trailer starts in a second or two rather than after a file.
//!
//! **`/seg` is not an open proxy.** The URL it fetches is signed with the same secret `/play` uses —
//! a different message, so neither tag opens the other — and must be a googlevideo host over https.
//! With no secret configured (the default, as play signing is) the host check stands on its own, and
//! what is left is a proxy for URLs Google itself signed and expires.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::header::{HeaderMap, RANGE};
use hyper::{Response, StatusCode};

use crate::httputil::{self, Body};
use crate::state::AppState;

/// A playlist is text and small. Something arriving here claiming to be one and running past this is
/// not a playlist we should be parsing, let alone holding in memory.
const MAX_PLAYLIST_BYTES: usize = 4 * 1024 * 1024;

/// Long enough for a segment on a slow line, short enough that a wedged fetch cannot pin a task.
/// Per-request, because the shared client's 15s is sized for JSON lookups.
const FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// What a proxied URL's tag covers. Prefixed, so a tag minted for a video id cannot open a URL and a
/// URL's tag cannot stand in for one on `/play`.
fn message(url: &str) -> String {
    format!("seg\0{url}")
}

/// YouTube's own media hosts over https, whatever else a URL says. Userinfo is dropped first: a URL
/// may spell `https://x.googlevideo.com@evil.example/…`, whose host is the part AFTER the `@`.
fn googlevideo(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("https://") else { return false };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or("");
    let host = host.split(':').next().unwrap_or("").to_ascii_lowercase();
    host == "googlevideo.com" || host.ends_with(".googlevideo.com")
}

/// The proxy URL for one upstream URL, RELATIVE to the playlist carrying it.
///
/// Relative because this server is reached at several addresses and, on the web app's own name, from
/// behind den-edge's `/reel` relay — so neither an absolute URL nor a root-relative path can be
/// written here and be right for all of them. Both `/hls/<vid>.m3u8` and `/hls/seg` sit in the same
/// directory, so a bare `seg?u=…` resolves correctly from the master AND from a variant fetched
/// through `seg` itself, at whatever depth the mount puts them.
fn proxied(secret: Option<&str>, url: &str) -> String {
    match secret {
        Some(secret) => {
            format!("seg?u={}&s={}", encode(url), crate::sign::tag(secret, &message(url)))
        }
        None => format!("seg?u={}", encode(url)),
    }
}

/// Percent-encode a URL so it survives as a query VALUE: RFC 3986 unreserved characters pass, and
/// everything else — `&`, `=`, `%`, `/`, `?` — is escaped, since what is being carried is itself a
/// URL with a query of its own.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + s.len() / 4);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Rewrite every URI in a playlist to go through `/seg`.
///
/// A line is either a tag or a URI. A tag can carry one too — renditions, the initialisation segment
/// and the encryption key all name theirs as `URI="…"` — so both halves are rewritten, and a
/// rendition left pointing at googlevideo is an audio track the browser silently cannot fetch.
fn rewrite(playlist: &str, base: &str, proxy: &dyn Fn(&str) -> String) -> String {
    let mut out = String::with_capacity(playlist.len() * 2);
    for line in playlist.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        let ending = &line[body.len()..];
        if body.is_empty() {
            out.push_str(line);
        } else if let Some(tag) = body.strip_prefix('#') {
            out.push('#');
            out.push_str(&rewrite_tag(tag, base, proxy));
            out.push_str(ending);
        } else {
            out.push_str(&proxy(&join(base, body)));
            out.push_str(ending);
        }
    }
    out
}

/// Rewrite each `URI="…"` in one tag line, leaving everything around them alone.
fn rewrite_tag(tag: &str, base: &str, proxy: &dyn Fn(&str) -> String) -> String {
    let mut out = String::with_capacity(tag.len());
    let mut rest = tag;
    while let Some(at) = rest.find("URI=\"") {
        let (before, from) = rest.split_at(at + "URI=\"".len());
        out.push_str(before);
        // An unterminated attribute is a malformed playlist; copy the remainder rather than guess.
        let Some(end) = from.find('"') else {
            out.push_str(from);
            return out;
        };
        out.push_str(&proxy(&join(base, &from[..end])));
        out.push('"');
        rest = &from[end + 1..];
    }
    out.push_str(rest);
    out
}

/// A playlist's reference resolved against the playlist's own URL: an absolute one as it is, a
/// root-relative one against the origin, anything else against the directory it was read from.
fn join(base: &str, reference: &str) -> String {
    if reference.starts_with("https://") || reference.starts_with("http://") {
        return reference.to_string();
    }
    let after_scheme = base.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &base[after_scheme..];
    let path_at = rest.find('/').map(|i| after_scheme + i).unwrap_or(base.len());
    let origin = &base[..path_at];
    if reference.starts_with('/') {
        return format!("{origin}{reference}");
    }
    let path = base[path_at..].split(['?', '#']).next().unwrap_or("");
    let dir = path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
    format!("{origin}{dir}/{reference}")
}

/// `/hls/<vid>.m3u8`: the trailer's master playlist, rewritten.
///
/// Resolving goes through `/direct`'s own path, so a warm title costs a hash lookup and a title
/// already resolving joins that resolve instead of starting a second one.
pub async fn handle_master(state: Arc<AppState>, vid: String) -> Response<Body> {
    let (answer, spent) = crate::direct::answer(&state, &vid).await;
    let direct = match answer {
        Ok(d) => d,
        Err(e) => return crate::play::play_error(&state, &vid, &e),
    };
    let Some(master) = direct.hls else {
        // A trailer with no HLS is not a failure to report as one: the page still has `/play`.
        return httputil::error(
            StatusCode::NOT_FOUND,
            "no_hls",
            "YouTube published no HLS master for this trailer.",
        );
    };
    let timing = match spent {
        Some(d) => httputil::timing("resolve", d),
        None => "cache;desc=hit".to_string(),
    };
    httputil::timed(through(&state, &master, &HeaderMap::new(), true).await, &timing)
}

/// `/hls/seg?u=…&s=…`: one upstream URL, checked and fetched. A playlist comes back rewritten like the
/// master did — a master names variants, and each variant names the segments — and anything else is
/// streamed out as it arrives.
pub async fn handle_segment(state: Arc<AppState>, query: &str, headers: &HeaderMap) -> Response<Body> {
    let Some(url) = httputil::query_param(query, "u") else {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_request", "Expected a u= parameter.");
    };
    if !googlevideo(&url) {
        return refused();
    }
    if let Some(secret) = state.cfg.play_secret.as_deref() {
        let presented = httputil::query_param(query, "s");
        let ok = crate::sign::verify_any(
            secret,
            &state.cfg.play_secrets_prev,
            &message(&url),
            presented.as_deref(),
        );
        if !ok {
            return refused();
        }
    }
    through(&state, &url, headers, false).await
}

/// A URL nothing here will fetch. The same answer for a host we do not proxy and for a tag that does
/// not verify: neither is worth telling a caller apart.
fn refused() -> Response<Body> {
    httputil::error(StatusCode::FORBIDDEN, "bad_signature", "This URL is not one this server serves.")
}

/// Fetch one upstream URL and answer with it: rewritten when it is a playlist, streamed when it is
/// not. `Range` travels in both directions, so a player can seek inside a segment.
async fn through(state: &AppState, url: &str, headers: &HeaderMap, expect_playlist: bool) -> Response<Body> {
    let mut req = state.http.get(url).timeout(FETCH_TIMEOUT);
    if let Some(range) = headers.get(RANGE).and_then(|v| v.to_str().ok()) {
        req = req.header("range", range);
    }
    let res = match req.send().await {
        Ok(res) => res,
        Err(e) => {
            crate::log_limited("hls transport", || {
                format!("hls upstream request failed ({})", crate::upstream::body_fault_why(e))
            });
            return httputil::error(
                StatusCode::BAD_GATEWAY,
                "upstream_unreachable",
                "Could not reach this trailer's stream.",
            );
        }
    };
    let status = StatusCode::from_u16(res.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if !status.is_success() {
        // Google's own refusal, which for an expired URL is a 403. Said plainly, and never cached:
        // the client's answer is to ask `/hls` again for a freshly resolved master.
        return httputil::error(
            StatusCode::BAD_GATEWAY,
            "upstream_refused",
            "This trailer's stream is no longer available.",
        );
    }
    let content_type = res
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    if expect_playlist || content_type.contains("mpegurl") {
        return playlist(state, url, res).await;
    }
    let mut out = Response::builder().status(status).header("cache-control", "private, max-age=3600");
    for name in ["content-type", "content-length", "content-range", "accept-ranges"] {
        if let Some(v) = res.headers().get(name) {
            out = out.header(name, v.clone());
        }
    }
    let stream = res.bytes_stream().map_ok(Frame::data).map_err(std::io::Error::other);
    // Spelled out: `StreamExt` defines a `boxed` of its own, and the one wanted here is the body's.
    out.body(BodyExt::boxed(StreamBody::new(stream))).unwrap_or_else(|_| {
        httputil::error(StatusCode::BAD_GATEWAY, "upstream_unreadable", "Could not read this stream.")
    })
}

/// Read a playlist (bounded) and answer with every URI in it pointing back here.
async fn playlist(state: &AppState, url: &str, res: reqwest::Response) -> Response<Body> {
    let mut stream = res.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return httputil::error(
                StatusCode::BAD_GATEWAY,
                "upstream_unreadable",
                "This trailer's playlist did not arrive.",
            );
        };
        if buf.len() + chunk.len() > MAX_PLAYLIST_BYTES {
            return httputil::error(
                StatusCode::BAD_GATEWAY,
                "upstream_unreadable",
                "This trailer's playlist is not a playlist.",
            );
        }
        buf.extend_from_slice(&chunk);
    }
    let text = String::from_utf8_lossy(&buf);
    let secret = state.cfg.play_secret.as_deref();
    let body = rewrite(&text, url, &|target: &str| proxied(secret, target));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/vnd.apple.mpegurl")
        .header("content-length", body.len())
        // Playlists name URLs that expire; a stale one is a trailer that stops mid-play. Minutes, so
        // a reload during a session costs nothing and a tab left open tomorrow resolves again.
        .header("cache-control", "private, max-age=300")
        .body(httputil::full(body))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MASTER: &str =
        "https://manifest.googlevideo.com/api/manifest/hls_variant/expire/1900/file/index.m3u8";

    fn proxy(url: &str) -> String {
        proxied(Some("s3cret"), url)
    }

    /// Every URI has to travel, whichever half of the playlist it is written in: a variant on its own
    /// line, and a rendition or an initialisation segment inside a tag.
    #[test]
    fn every_uri_in_a_playlist_comes_back_here() {
        let playlist = "#EXTM3U\n\
             #EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",URI=\"https://r1.googlevideo.com/audio.m3u8\"\n\
             #EXT-X-STREAM-INF:BANDWIDTH=1\n\
             https://r2.googlevideo.com/video.m3u8\n\
             /rooted.m3u8\n\
             relative.ts\n";
        let out = rewrite(playlist, MASTER, &proxy);
        assert!(out.starts_with("#EXTM3U\n"), "the tags themselves are left alone");
        assert_eq!(out.matches("seg?u=").count(), 4, "two tags' URIs and two lines: {out}");
        // Relative, so the same playlist is right at reel's own address and behind den-edge's relay.
        assert!(!out.contains("/seg?u="), "a rooted path would leave the mount behind: {out}");
        assert!(!out.contains("googlevideo.com/audio"), "a rendition left behind is a silent trailer");
        // Resolved against the playlist's own URL, not against nothing.
        assert!(out.contains(&encode("https://manifest.googlevideo.com/rooted.m3u8")), "{out}");
        assert!(
            out.contains(&encode(
                "https://manifest.googlevideo.com/api/manifest/hls_variant/expire/1900/file/relative.ts"
            )),
            "{out}"
        );
        assert!(out.ends_with('\n'), "line endings are preserved");
    }

    #[test]
    fn a_reference_resolves_like_a_browser_would() {
        assert_eq!(join(MASTER, "https://other.example/x.ts"), "https://other.example/x.ts");
        assert_eq!(join("https://h.example/a/b/i.m3u8", "/c.ts"), "https://h.example/c.ts");
        assert_eq!(join("https://h.example/a/b/i.m3u8?q=1", "c.ts"), "https://h.example/a/b/c.ts");
        assert_eq!(join("https://h.example", "c.ts"), "https://h.example/c.ts");
    }

    /// The whole point of the signature: `/seg` fetches what this server signed and nothing else.
    #[test]
    fn only_a_signed_googlevideo_url_is_fetched() {
        let url = "https://r1.googlevideo.com/videoplayback?itag=140";
        assert!(crate::sign::verify(
            "s3cret",
            &message(url),
            Some(&crate::sign::tag("s3cret", &message(url)))
        ));
        // A tag minted for a video id must not open a URL, and a URL's must not open `/play`.
        assert!(!crate::sign::verify("s3cret", &message(url), Some(&crate::sign::tag("s3cret", url))));

        assert!(googlevideo(url));
        assert!(googlevideo("https://manifest.googlevideo.com/api/manifest/hls_variant/x"));
        for refused in [
            "http://r1.googlevideo.com/x",               // plaintext
            "https://evil.example/x",                    // not YouTube at all
            "https://googlevideo.com.evil.example/x",    // a suffix, not the host
            "https://r1.googlevideo.com@evil.example/x", // the host is what follows the @
            "file:///etc/passwd",
        ] {
            assert!(!googlevideo(refused), "{refused}");
        }
    }

    /// A URL carries `&` and `=` of its own, which have to survive being carried inside a query.
    #[test]
    fn a_proxied_url_survives_the_query_it_travels_in() {
        let url = "https://r1.googlevideo.com/videoplayback?a=1&b=2%20x";
        let proxied = proxy(url);
        let query = proxied.strip_prefix("seg?").expect("a seg URL");
        assert_eq!(httputil::query_param(query, "u").as_deref(), Some(url));
        assert_eq!(httputil::query_param(query, "s").map(|s| s.len()), Some(24));
    }
}
