//! Tiny HTTP plumbing shared by the handlers: a unified response-body type, JSON/text/error
//! response builders, and hand-rolled query/percent-decode helpers (no url/regex dependency).

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::header::{HeaderMap, HeaderValue, CACHE_CONTROL, ETAG, IF_NONE_MATCH};
use hyper::{Method, Response, StatusCode};

/// The one body type every handler returns: bytes in, `io::Error` out (streamed file bodies can
/// fail mid-flight, full-buffer bodies never do).
pub type Body = BoxBody<Bytes, std::io::Error>;

/// A fully-buffered body from anything byte-ish.
pub fn full(data: impl Into<Bytes>) -> Body {
    Full::new(data.into()).map_err(|never| match never {}).boxed()
}

/// A strong, quoted ETag derived from the response body. A fast non-crypto hash (std
/// `DefaultHasher`) is plenty — an ETag only needs to change when the bytes change, not resist an
/// adversary. Length is folded in as a cheap extra guard against hash collisions.
pub fn etag_of(bytes: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    format!("\"{:016x}-{:x}\"", h.finish(), bytes.len())
}

/// The `Cache-Control` value from a handler's extra-header slice, if any (case-insensitive key).
fn cache_control_of<'a>(extra: &'a [(&str, &str)]) -> Option<&'a str> {
    extra
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("cache-control"))
        .map(|(_, v)| *v)
}

/// Whether a response is a cacheable success that should carry a validator (ETag): a 200 with a
/// caching directive that isn't `no-store`. Errors and `no-store` bodies never get an ETag.
fn cacheable(status: StatusCode, cache_control: Option<&str>) -> bool {
    status == StatusCode::OK
        && cache_control.is_some_and(|cc| !cc.is_empty() && !cc.contains("no-store"))
}

/// `sendJson` equivalent: JSON + permissive CORS + explicit Content-Length, plus any extra headers
/// (e.g. Cache-Control). Serialization can't fail for our own types, but stay total just in case.
pub fn json(
    status: StatusCode,
    value: &serde_json::Value,
    extra: &[(&str, &str)],
) -> Response<Body> {
    let s = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    let mut b = Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("access-control-allow-origin", "*")
        .header("content-length", s.len());
    let cc = cache_control_of(extra);
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    // Errors must never be cached (a transient 502/503/504 mustn't stick in a client/proxy cache).
    if !status.is_success() && cc.is_none() {
        b = b.header("cache-control", "no-store");
    }
    // Attach a strong validator to cacheable 200s so a conditional GET can collapse to a 304.
    if cacheable(status, cc) {
        b = b.header(ETAG, etag_of(&s));
    }
    b.body(full(s)).unwrap()
}

/// HTML response (the embedded /configure page), plus any extra headers (e.g. Cache-Control).
pub fn html(status: StatusCode, body: &'static str, extra: &[(&str, &str)]) -> Response<Body> {
    let mut b = Response::builder()
        .status(status)
        .header("content-type", "text/html; charset=utf-8")
        .header("content-length", body.len());
    let cc = cache_control_of(extra);
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    if cacheable(status, cc) {
        b = b.header(ETAG, etag_of(body.as_bytes()));
    }
    b.body(full(body)).unwrap()
}

/// A typed error body `{ "error": <code>, "message": <msg> }` (no-store applied via `json`).
pub fn error(status: StatusCode, code: &str, message: &str) -> Response<Body> {
    json(status, &serde_json::json!({ "error": code, "message": message }), &[])
}

/// Plain-text response (health, bad-request bodies). Non-2xx get `Cache-Control: no-store`.
pub fn text(status: StatusCode, msg: &'static str) -> Response<Body> {
    let mut b = Response::builder()
        .status(status)
        .header("content-length", msg.len());
    if !status.is_success() {
        b = b.header("cache-control", "no-store");
    }
    b.body(full(msg)).unwrap()
}

/// Honor a conditional GET/HEAD: if the request's `If-None-Match` matches the response's `ETag`,
/// collapse to a `304 Not Modified` that keeps the `ETag` + `Cache-Control` headers and drops the
/// body. A no-op for unsafe methods, responses without an ETag (errors, `no-store`), or a
/// non-matching request.
pub fn apply_conditional(
    method: &Method,
    req_headers: &HeaderMap,
    resp: Response<Body>,
) -> Response<Body> {
    if !matches!(*method, Method::GET | Method::HEAD) {
        return resp;
    }
    let Some(etag) = resp.headers().get(ETAG) else {
        return resp;
    };
    let matched = req_headers
        .get(IF_NONE_MATCH)
        .is_some_and(|inm| if_none_match_matches(inm, etag));
    if !matched {
        return resp;
    }
    let mut b = Response::builder().status(StatusCode::NOT_MODIFIED);
    let headers = b.headers_mut().expect("fresh builder has headers");
    if let Some(v) = resp.headers().get(ETAG) {
        headers.insert(ETAG, v.clone());
    }
    if let Some(v) = resp.headers().get(CACHE_CONTROL) {
        headers.insert(CACHE_CONTROL, v.clone());
    }
    b.body(full("")).unwrap()
}

/// RFC 9110 `If-None-Match`: `*` matches anything; otherwise any entry in the comma-separated list
/// that equals the ETag matches. Our ETags are strong, but we compare with the weak-validator
/// prefix (`W/`) stripped from both sides so a proxy that weakened it still gets its 304.
fn if_none_match_matches(inm: &HeaderValue, etag: &HeaderValue) -> bool {
    let (Ok(inm), Ok(etag)) = (inm.to_str(), etag.to_str()) else {
        return false;
    };
    let etag = etag.trim_start_matches("W/");
    inm.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate.trim_start_matches("W/") == etag
    })
}

/// Look up a query-string parameter without pulling in a URL parser. Returns the decoded value of
/// the first occurrence of `key`.
pub fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(k) == key).then(|| percent_decode(v))
    })
}

/// Minimal percent-decoding (`%XX` → byte, lossy UTF-8). Enough for Den's `tt…`/`tt…:S:E` ids and
/// our short query params; we never emit encoded paths ourselves.
pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// A parsed `Range: bytes=start-end` against a known file size. Mirrors the Node regex
/// `bytes=(\d+)-(\d*)`: a header that doesn't match is treated as no range at all.
pub enum RangeReq {
    /// Serve `[start, end]` inclusive (206).
    Satisfiable { start: u64, end: u64 },
    /// Range asks past EOF → 416.
    Unsatisfiable,
}

pub fn parse_range(header: Option<&str>, size: u64) -> Option<RangeReq> {
    let h = header?;
    let rest = h.trim().strip_prefix("bytes=")?;
    let (a, b) = rest.split_once('-')?;
    let start: u64 = a.trim().parse().ok()?; // `\d+` required
    if !b.trim().is_empty() && b.trim().parse::<u64>().is_err() {
        return None; // trailing junk that isn't `\d*` → not a match, fall back to full body
    }
    let end = match b.trim().parse::<u64>() {
        Ok(e) => e.min(size.saturating_sub(1)),
        Err(_) => size.saturating_sub(1),
    };
    if start >= size || start > end {
        return Some(RangeReq::Unsatisfiable);
    }
    Some(RangeReq::Satisfiable { start, end })
}
