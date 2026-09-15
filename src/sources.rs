//! SOURCES: which forms of a trailer a page should try, in order, as URLs this server mints.
//!
//! The web app used to build these itself: it swapped `/play/` in a play URL for `/direct/`, `/hls/` or
//! `/progressive/`, repeated `height` and `audio` on a `/meta` warm-up that only helped if they matched the
//! request that followed, and its relay had to know every media route by name. Here the page says what its
//! surface needs and which HLS player it has, and this server answers with an ordered list of URLs it signed.
//! Asking for the list is the warm-up: it waits for the resolve the first entry plays from, and for the index
//! when that entry is a progressive file.
//!
//! **Two surfaces**, told apart by whether sound can be asked for in place: `silent` never gets it (a
//! billboard slide), `audible` has it from the first frame or on demand without a new page (a detail hero).
//!
//! **Media URLs are `/m/<blob>`**: the variant — video, form, height, sound, the install it was minted for,
//! and when it expires — as base64url JSON, tagged with `PLAY_SECRET` over the blob. The one entry that is not
//! is Google's own video URL, offered to a Media Source player for a silent surface: Chrome starts it without
//! an index, and its bytes never cross this box.
//!
//! Safari's HLS entry keeps its segment URIs on googlevideo, as `/hls?native=1` does, so only the playlist
//! crosses the box. A proxied master's URIs are relative (`seg?u=…`), which from `/m/<blob>` resolves to
//! `/m/seg` under whatever prefix the relay mounts this at.

use std::collections::HashSet;
use std::sync::Arc;

use base64::Engine;
use hyper::header::HeaderMap;
use hyper::{Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::httputil::{self, query_param, Body};
use crate::state::AppState;

/// How long a minted media URL is honoured. Far longer than a page holds a list (den-edge caches one for five
/// minutes); the Google URLs behind it are resolved again whenever they expire.
const MEDIA_TTL_SECS: u64 = 24 * 60 * 60;

/// The height a silent surface is capped at, before the ladder rounds it.
const SILENT_HEIGHT: &str = "720";

/// What a surface needs of its trailer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Surface {
    /// Muted, and never given sound.
    Silent,
    /// Sound from the first frame, or asked for in place.
    Audible,
}

/// The page's HLS player: Safari's own, or hls.js on Media Source.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Player {
    Native,
    HlsJs,
}

/// One form a trailer can be played in.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Form {
    /// Google's own fragmented video file.
    Google { cap: Option<u32> },
    /// `/progressive`: an ordinary MP4 with its index first.
    Progressive { cap: Option<u32>, audio: bool },
    /// This server's HLS master; `native` keeps its segments on googlevideo.
    Hls { native: bool },
}

impl Form {
    /// The resolve this form plays from: capped, or the whole ladder's.
    fn cap(self) -> Option<u32> {
        match self {
            Form::Google { cap } | Form::Progressive { cap, .. } => cap,
            Form::Hls { .. } => None,
        }
    }
}

/// The forms to offer, best first, by what was measured on 2026-09-15 (macOS, same trailers).
pub(crate) fn plan(surface: Surface, player: Player, silent_cap: Option<u32>) -> Vec<Form> {
    match (surface, player) {
        // Safari's own progressive player reads every fragment of Google's file before it plays (4.9 s);
        // with the index first it played in 324 ms.
        (Surface::Silent, Player::Native) => {
            vec![Form::Progressive { cap: silent_cap, audio: false }, Form::Hls { native: true }]
        }
        // Chrome plays Google's fragmented file in 811 ms, and none of it crosses this box.
        (Surface::Silent, Player::HlsJs) => vec![
            Form::Google { cap: silent_cap },
            Form::Progressive { cap: silent_cap, audio: false },
            Form::Hls { native: false },
        ],
        // Safari's own HLS player reached metadata in 636 ms, against 934 ms for the progressive file.
        (Surface::Audible, Player::Native) => {
            vec![Form::Hls { native: true }, Form::Progressive { cap: None, audio: true }]
        }
        (Surface::Audible, Player::HlsJs) => {
            vec![Form::Hls { native: false }, Form::Progressive { cap: None, audio: true }]
        }
    }
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// What a media URL carries. Short field names, since all of it rides in the URL.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub(crate) struct Media {
    /// The video.
    pub v: String,
    /// `p` for `/progressive`, `h` for the HLS master.
    pub f: String,
    /// The height cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub h: Option<u32>,
    /// With sound.
    #[serde(default, skip_serializing_if = "is_false")]
    pub a: bool,
    /// A master whose segments stay on googlevideo.
    #[serde(default, skip_serializing_if = "is_false")]
    pub n: bool,
    /// The page's playable report, as it sent it, for a master to list only what it plays.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p: Option<String>,
    /// The install it was minted for, and that install's epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub i: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e: Option<u64>,
    /// When it stops being honoured, in epoch seconds.
    pub x: u64,
}

/// What a media URL's tag covers. Prefixed, so no tag minted for anything else opens one.
fn message(blob: &str) -> String {
    format!("m\0{blob}")
}

/// The path and query of the media URL for `media`, tagged when there is a signer.
pub(crate) fn seal(signer: Option<&crate::sign::Signer>, media: &Media) -> String {
    let json = serde_json::to_vec(media).unwrap_or_default();
    let blob = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json);
    match signer {
        Some(signer) => format!("m/{blob}?s={}", signer.tag(&message(&blob))),
        None => format!("m/{blob}"),
    }
}

/// The media a URL names, when it is one this server minted: its tag verifies where a secret is set, and it
/// decodes. Expiry and the install are the caller's to check.
pub(crate) fn unseal(
    secret: Option<&str>,
    prev: &[String],
    blob: &str,
    presented: Option<&str>,
) -> Option<Media> {
    if let Some(secret) = secret {
        if !crate::sign::verify_any(secret, prev, &message(blob), presented) {
            return None;
        }
    }
    let json = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(blob).ok()?;
    serde_json::from_slice(&json).ok()
}

/// `/sources/<vid>.json?surface=silent|audible&player=native|hls.js`: the forms to try, in order.
pub async fn handle_sources(
    state: Arc<AppState>,
    headers: &HeaderMap,
    vid: String,
    query: &str,
) -> Response<Body> {
    let surface = match query_param(query, "surface").as_deref() {
        Some("silent") => Surface::Silent,
        Some("audible") => Surface::Audible,
        _ => {
            return httputil::error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "Expected surface=silent or audible.",
            )
        }
    };
    let player = match query_param(query, "player").as_deref() {
        Some("native") => Player::Native,
        Some("hls.js") => Player::HlsJs,
        _ => {
            return httputil::error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "Expected player=native or hls.js.",
            )
        }
    };
    let mut forms = plan(surface, player, crate::direct::height_cap(&state.cfg, Some(SILENT_HEIGHT)));
    let first = forms[0];

    // The resolve the first form plays from is waited for: Google's own URL is that resolve, and the page is
    // about to ask for it anyway. The other forms are left to their own requests.
    let (answer, spent) = crate::direct::answer(&state, &vid, first.cap()).await;
    let mut timing = match spent {
        Some(d) => httputil::timing("resolve", d),
        None => "cache;desc=hit".to_string(),
    };
    let direct = match answer {
        Ok(d) => d,
        Err(e) => return httputil::timed(crate::play::play_error(&state, &vid, &e), &timing),
    };
    if let Form::Progressive { cap, audio } = first {
        let started = std::time::Instant::now();
        let built = crate::progressive::prepare(&state, &vid, cap, &direct, audio).await;
        timing.push_str(", ");
        timing.push_str(&httputil::timing("index", started.elapsed()));
        // A file that cannot be indexed is left off rather than offered to fail. A fetch that failed stays:
        // it may work when the page asks.
        if let Some(e) = built.err().filter(|e| !e.retry) {
            crate::log_limited("sources unindexable", || format!("[{vid}] progressive left off ({})", e.why));
            forms.remove(0);
        }
    }

    let now = (state.clock)();
    let base = crate::addon::self_base(state.cfg.public_base_url.as_deref(), headers, state.cfg.port);
    let signer = state.cfg.play_secret.as_deref().map(crate::sign::Signer::new);
    let iid = query_param(query, "i");
    let ep = query_param(query, "e").and_then(|e| e.parse().ok());
    let report = crate::client::raw_report(headers, query);
    let expires = now / 1000 + MEDIA_TTL_SECS;
    let media_url = |f: &str, h: Option<u32>, a: bool, n: bool, p: Option<String>| {
        let media = Media { v: vid.clone(), f: f.into(), h, a, n, p, i: iid.clone(), e: ep, x: expires };
        format!("{base}/{}", seal(signer.as_ref(), &media))
    };

    let mut seen = HashSet::new();
    let mut sources = Vec::new();
    let mut soonest = expires * 1000;
    for form in forms {
        let (kind, url, audio, height) = match form {
            Form::Google { .. } => {
                soonest = soonest.min(direct.expires);
                ("mp4", direct.video.clone(), false, direct.height)
            }
            Form::Progressive { cap, audio } => {
                let height = if cap == first.cap() { direct.height } else { None };
                ("mp4", media_url("p", cap, audio, false, None), audio, height)
            }
            Form::Hls { native } => ("hls", media_url("h", None, false, native, report.clone()), true, None),
        };
        // Never the same URL twice: a step to the URL already playing starts no load and fires no error.
        if seen.insert(url.clone()) {
            sources.push(json!({ "kind": kind, "url": url, "audio": audio, "height": height }));
        }
    }

    // The letterbox, when it is known. Nothing here waits for it: an unmeasured trailer is measured from its
    // keyframes in the background, and the next answer carries it.
    let crop =
        state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).get(&vid).and_then(|r| r.fractions());
    if crop.is_none() {
        crate::crop::measure_in_background(&state, &vid, first.cap());
    }

    let max_age = direct
        .expires
        .saturating_sub(now)
        .saturating_sub(crate::direct::EXPIRY_MARGIN_MS)
        .min(MEDIA_TTL_SECS * 1000)
        / 1000;
    let body = json!({ "id": vid, "sources": sources, "crop": crop, "expires": soonest / 1000 });
    let cache = format!("private, max-age={max_age}");
    let resp =
        httputil::json(StatusCode::OK, &body, &[("cache-control", &cache), ("vary", crate::client::HEADER)]);
    httputil::timed(resp, &timing)
}

/// `/m/<blob>`: a form `/sources` minted, served by the handler that plays it.
pub async fn handle_media(
    state: Arc<AppState>,
    headers: &HeaderMap,
    blob: &str,
    query: &str,
) -> Response<Body> {
    let presented = query_param(query, "s");
    let cfg = &state.cfg;
    let Some(media) = unseal(cfg.play_secret.as_deref(), &cfg.play_secrets_prev, blob, presented.as_deref())
    else {
        return httputil::error(
            StatusCode::FORBIDDEN,
            "bad_signature",
            "This URL is not one this server serves.",
        );
    };
    if !crate::is_valid_vid(&media.v) {
        return httputil::not_found();
    }
    if media.x.saturating_mul(1000) <= (state.clock)() {
        return httputil::error(
            StatusCode::GONE,
            "expired",
            "This trailer URL has expired; ask for its sources again.",
        );
    }
    if let Some(ep) = media.e {
        if let Err(why) = cfg.revocation.check_install(media.i.as_deref(), ep) {
            crate::log_limited("media_install_refused", || {
                format!("bad_signature: media for {} refused — {why}", media.v)
            });
            return httputil::error(
                StatusCode::FORBIDDEN,
                "bad_signature",
                "This URL is not one this server serves.",
            );
        }
    }
    let cap = crate::direct::height_cap(cfg, media.h.map(|h| h.to_string()).as_deref());
    match media.f.as_str() {
        "p" => crate::progressive::handle_progressive(state, headers, media.v, cap, media.a).await,
        "h" => {
            let uris = if media.n { crate::hls::Uris::Native } else { crate::hls::Uris::Proxy };
            let playable = media.p.as_deref().and_then(crate::client::parse);
            crate::hls::handle_master(state, media.v, uris, playable).await
        }
        _ => httputil::not_found(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The order is what was measured, per surface and player, and no plan repeats a form.
    #[test]
    fn each_surface_and_player_gets_its_measured_order() {
        let cap = Some(720);
        assert_eq!(
            plan(Surface::Silent, Player::Native, cap),
            [Form::Progressive { cap, audio: false }, Form::Hls { native: true }]
        );
        assert_eq!(
            plan(Surface::Silent, Player::HlsJs, cap),
            [Form::Google { cap }, Form::Progressive { cap, audio: false }, Form::Hls { native: false }]
        );
        assert_eq!(
            plan(Surface::Audible, Player::Native, cap),
            [Form::Hls { native: true }, Form::Progressive { cap: None, audio: true }]
        );
        assert_eq!(
            plan(Surface::Audible, Player::HlsJs, cap),
            [Form::Hls { native: false }, Form::Progressive { cap: None, audio: true }]
        );
    }

    fn media() -> Media {
        Media {
            v: "dQw4w9WgXcQ".into(),
            f: "p".into(),
            h: Some(720),
            a: true,
            n: false,
            p: Some(r#"{"h264":51}"#.into()),
            i: Some("iid".into()),
            e: Some(3),
            x: 4_000_000_000,
        }
    }

    fn split(path: &str) -> (&str, Option<&str>) {
        let rest = path.strip_prefix("m/").unwrap();
        match rest.split_once("?s=") {
            Some((blob, tag)) => (blob, Some(tag)),
            None => (rest, None),
        }
    }

    /// A media URL opens only as it was minted: a changed blob or a tag from another secret opens nothing.
    #[test]
    fn a_media_url_opens_only_as_minted() {
        let signer = crate::sign::Signer::new("s3cret");
        let path = seal(Some(&signer), &media());
        let (blob, tag) = split(&path);
        assert_eq!(unseal(Some("s3cret"), &[], blob, tag), Some(media()));
        assert_eq!(
            unseal(Some("other"), &["s3cret".into()], blob, tag),
            Some(media()),
            "a prior secret still opens it"
        );
        assert_eq!(unseal(Some("other"), &[], blob, tag), None);
        assert_eq!(unseal(Some("s3cret"), &[], blob, None), None, "no tag");
        let changed = seal(Some(&signer), &Media { v: "abc123DEF01".into(), ..media() });
        assert_eq!(unseal(Some("s3cret"), &[], split(&changed).0, tag), None, "a tag for another blob");

        let unsigned = seal(None, &media());
        assert_eq!(split(&unsigned).1, None);
        assert_eq!(
            unseal(None, &[], split(&unsigned).0, None),
            Some(media()),
            "unsigned where no secret is set"
        );
    }
}
