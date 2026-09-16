//! SOURCES: which forms of a trailer a page should try, in order, as URLs this server mints.
//!
//! The web app used to build these itself: it swapped `/play/` in a play URL for `/direct/`, `/hls/` or
//! `/progressive/`, repeated `height` and `audio` on a `/meta` warm-up that only helped if they matched the
//! request that followed, and its relay had to know every media route by name. Here the page says what its
//! surface needs and which HLS player it has, and this server answers with an ordered list of URLs it signed.
//! Asking for the list is the warm-up. For a silent surface it waits for the resolve the first entry plays
//! from, and for the index when that entry is a progressive file; an audible surface is answered at once and
//! its resolve started, since its first entry needs neither.
//!
//! **Two surfaces**, told apart by whether sound can be asked for in place: `silent` never gets it (a
//! billboard slide), `audible` has it from the first frame or on demand without a new page (a detail hero).
//!
//! **Media URLs are `/m/<n|s>/<blob>`**: the variant — video, form, height, sound, the install it was minted
//! for, and when it expires — as base64url JSON, tagged with `PLAY_SECRET` over the blob. The one entry that is
//! not is Google's own video URL, offered to a Media Source player for a silent surface: Chrome starts it
//! without an index, and its bytes never cross this box.
//!
//! The segment says how much crosses the box, for a relay that meters it: `n` is Safari's HLS entry, which
//! keeps its segment URIs on googlevideo as `/hls?native=1` does, so only the playlist crosses; `s` is what
//! this server carries. A proxied master's URIs are relative (`seg?u=…`), which from `/m/s/<blob>` resolves
//! to `/m/s/seg` under whatever prefix the relay mounts this at.

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

/// How long an answer given before its resolve may be cached.
const UNRESOLVED_MAX_AGE_SECS: u64 = 300;

/// The height a silent surface is capped at, before the ladder rounds it.
const SILENT_HEIGHT: &str = "720";

/// How long an audible answer waits for an index that is not built yet, before leading with the master.
///
/// Only ever waited where the resolve is already warm, which is exactly when the build is short: 93–241 ms
/// measured at this rung on 2026-09-16, against 1007–2873 ms at the full ladder. Bounded because a page is
/// opening while this runs.
///
/// A wait that runs out costs this one answer the wait and nothing else: `layout_for` drives every build on a
/// task of its own, so the index still finishes and the next ask leads with it. Set against what the wait buys
/// — a master measured in production at 1136–2704 ms, and 2850–4101 ms on iOS — a quarter second is cheap.
const INDEX_LEAD_WAIT: std::time::Duration = std::time::Duration::from_millis(250);

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
        // The master leads, and the fallback asks for the SILENT SURFACE'S RUNG rather than the whole ladder.
        //
        // A resolve is cached per (video, rung) and is the expensive half of a cold open: 1.2–3.6 s measured,
        // against 90–133 ms to index an ordinary trailer. On a rung of its own this fallback paid that every
        // time; on the rung the billboard already warms it pays neither. Measured 2026-09-16, resolve warm and
        // index cold: 249 ms on macOS Safari, 412 ms on iOS, 475 ms in Chrome — against 2295–3383 ms for the
        // same file at a rung nobody had asked for. It is also the rung this surface warms in the background
        // below, so the warm-up and the fallback now agree instead of building two indexes.
        //
        // Whether the master should still lead is NOT settled by those numbers, though they argue against it:
        // it loses every warm case on every browser and is sometimes unplayable on iOS (`hls_drm`). Moving the
        // progressive file first would either block this answer on an index build while a page is opening —
        // which is the one thing the audible path exists to avoid — or hand the page a URL that stalls at play
        // time with no error to fall back from. Doing it properly needs a non-blocking look at the index cache,
        // which `prepare`/`warm`/`indexed` do not offer: each of them builds or joins a build.
        (Surface::Audible, Player::Native) => {
            vec![Form::Hls { native: true }, Form::Progressive { cap: silent_cap, audio: true }]
        }
        (Surface::Audible, Player::HlsJs) => {
            vec![Form::Hls { native: false }, Form::Progressive { cap: silent_cap, audio: true }]
        }
    }
}

/// Move the progressive-with-sound entry to the front of `forms`, saying whether there was one to move.
///
/// Apart from the condition that calls it, so the two can be read and tested separately: whether a file is
/// ready to play is a question about caches, and which entry leads is a question about this list.
fn lead_with_sound(forms: &mut [Form]) -> bool {
    match forms.iter().position(|f| matches!(f, Form::Progressive { audio: true, .. })) {
        Some(at) => {
            forms[..=at].rotate_right(1);
            true
        }
        None => false,
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

/// The path segment a media URL is filed under: `n` for a native master, whose segments stay on googlevideo so
/// only its playlist crosses this box, and `s` for everything this server carries. A relay accounts for the two
/// differently, and can trust the segment, because `handle_media` refuses a blob filed under the other.
pub(crate) fn segment(media: &Media) -> &'static str {
    if media.f == "h" && media.n {
        "n"
    } else {
        "s"
    }
}

/// The path and query of the media URL for `media`, tagged when there is a signer.
pub(crate) fn seal(signer: Option<&crate::sign::Signer>, media: &Media) -> String {
    let json = serde_json::to_vec(media).unwrap_or_default();
    let blob = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json);
    let segment = segment(media);
    match signer {
        Some(signer) => format!("m/{segment}/{blob}?s={}", signer.tag(&message(&blob))),
        None => format!("m/{segment}/{blob}"),
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
    // `intent=warm`: asked because a viewer might open the trailer, not because a surface is about to play it.
    // The same question with the same answer, so the warm-up and the play cannot drift apart — only less in
    // front of the caller: it never waits, never leads with a file, and measures no letterbox.
    //
    // It DOES build the index the hero will need. That index costs Google about 45 range requests, and this ask
    // is a press on a title — a finger already on it, not a title glanced at. Measured 2026-09-16, a first hero
    // open with the resolve warm and this index missing costs 122 ms of waiting; built here, it costs nothing.
    // The wasted case is a press that never becomes a view, and that is the trade being made deliberately.
    let speculative = query_param(query, "intent").as_deref() == Some("warm");
    let mut forms = plan(surface, player, crate::direct::height_cap(&state.cfg, Some(SILENT_HEIGHT)));
    // The rung this surface's progressive-with-sound entry plays from: what an audible surface warms behind its
    // answer, and — where it is not speculative — what it may lead with.
    let fallback = forms.iter().find_map(|f| match f {
        Form::Progressive { cap, audio: true } => Some(*cap),
        _ => None,
    });

    // An audible surface leads with the progressive file when that file is ready to play NOW, and with the
    // master otherwise.
    //
    // Measured to a first painted frame on 2026-09-16, progressive-with-sound at this rung against the master:
    // 99 ms vs 1083 in Chrome, 98–179 vs 616–965 on macOS Safari, 66–166 vs 2850–4101 on iOS — and on iOS a
    // DRM-protected trailer's master produces no frame at all. The master wins nothing once the index it is
    // compared against exists.
    //
    // Ready to play is the whole condition. What must never happen is handing the page a file whose index is
    // still building: that URL stalls mid-load, and a stall fires no `error` event, so the page's ladder never
    // advances past it — worse than the master it replaced.
    //
    // But "already built" alone was too strict, and production said so. The billboard warms the 720 index
    // WITHOUT sound; a hero needs the one WITH sound, which is a different index, and its build only starts
    // when the hero asks. Measured on d.oxy.fi 2026-09-16: three of four first opens led with the master and
    // paid 1136–2704 ms for it, and only a return visit two minutes later led with the file. The build had
    // finished a fraction of a second after the answer went out.
    //
    // So where the resolve is already warm, this waits a bounded `INDEX_LEAD_WAIT` for the build rather than
    // giving up on it. Only there: a cold resolve is seconds, and nothing waits seconds for a page that is
    // opening. A wait that runs out simply leads with the master, and loses only the wait.
    // Never on a speculative ask: nobody is waiting on it, and the page asks again in earnest when it opens.
    let mut led = None;
    if surface == Surface::Audible && !speculative {
        if let Some(cap) = fallback {
            if let Some(Ok(d)) = crate::direct::peek(&state, &vid, cap) {
                let built = crate::progressive::ready(&state, &vid, cap, &d, true)
                    || tokio::time::timeout(
                        INDEX_LEAD_WAIT,
                        crate::progressive::prepare(&state, &vid, cap, &d, true),
                    )
                    .await
                    .is_ok_and(|built| built.is_ok());
                if built && lead_with_sound(&mut forms) {
                    led = Some(d);
                }
            }
        }
    }
    let first = forms[0];

    // The resolve the first form plays from. A silent surface waits for it: Google's own URL is that resolve, a
    // progressive entry needs its index, and a billboard asks seconds ahead. An audible surface does not: its
    // first entry is a master whose URL needs neither, and it is asked for as a page opens, where waiting here
    // would put a round trip in front of a player that waits on the same resolve anyway. That resolve is started
    // instead, and the master's request joins it. A video already known to be gone is still said at once.
    //
    // Behind the resolve, the index for the progressive file with sound is built too, which is this surface's
    // fallback. A cold index costs 0.7–3.9 s at the full ladder, 93–241 ms at this rung (measured 2026-09-16),
    // almost all of it Google's edge fetching regions of the file it has not served lately, so the only way to
    // spare a viewer that is to build it before they need it — and the build probably warms the edge for the
    // playback that follows. Once it is built, the next ask leads with it rather than the master.
    let (direct, mut timing) = if let Some(d) = led {
        // Already resolved and already indexed: nothing to wait for and nothing to start.
        (Some(d), "cache;desc=hit".to_string())
    } else if surface == Surface::Audible {
        match crate::direct::peek(&state, &vid, first.cap()) {
            Some(Err(e)) => {
                return httputil::timed(crate::play::play_error(&state, &vid, &e), "cache;desc=hit")
            }
            Some(Ok(d)) => {
                if let Some(cap) = fallback {
                    (state.direct_warm)(state.clone(), vid.clone(), cap, Some(true));
                }
                (Some(d), "cache;desc=hit".to_string())
            }
            None => {
                (state.direct_warm)(
                    state.clone(),
                    vid.clone(),
                    fallback.unwrap_or(first.cap()),
                    fallback.map(|_| true),
                );
                (None, "resolve;desc=background".to_string())
            }
        }
    } else {
        let (answer, spent) = crate::direct::answer(&state, &vid, first.cap()).await;
        let timing = match spent {
            Some(d) => httputil::timing("resolve", d),
            None => "cache;desc=hit".to_string(),
        };
        match answer {
            Ok(d) => (Some(d), timing),
            Err(e) => return httputil::timed(crate::play::play_error(&state, &vid, &e), &timing),
        }
    };
    if let (Form::Progressive { cap, audio }, Some(direct)) = (first, &direct) {
        let started = std::time::Instant::now();
        let built = crate::progressive::prepare(&state, &vid, cap, direct, audio).await;
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
    let signer = state.cfg.play_secret.as_deref().map(crate::sign::Signer::new);
    let iid = query_param(query, "i");
    let ep = query_param(query, "e").and_then(|e| e.parse().ok());
    let report = crate::client::raw_report(headers, query);
    let expires = now / 1000 + MEDIA_TTL_SECS;
    let media_url = |f: &str, h: Option<u32>, a: bool, n: bool, p: Option<String>| {
        let media = Media { v: vid.clone(), f: f.into(), h, a, n, p, i: iid.clone(), e: ep, x: expires };
        // Relative to this answer's own URL, `/sources/<id>.json`, for the reason a proxied playlist's URIs are: this
        // server is reached at several addresses and under a relay's prefix, and knows neither. A page resolves it
        // against the URL it asked, which is by definition one it can reach.
        format!("../{}", seal(signer.as_ref(), &media))
    };

    // The rung whose index this answer is building, for the letterbox below: the progressive form's, which is
    // NOT `first.cap()` on an audible surface — there the first form is the master and names no rung. Read
    // before the loop, which consumes `forms`.
    let indexed = forms
        .iter()
        .find_map(|f| match f {
            Form::Progressive { cap, .. } => Some(*cap),
            _ => None,
        })
        .unwrap_or(first.cap());

    let mut seen = HashSet::new();
    let mut sources = Vec::new();
    let mut soonest = expires * 1000;
    for form in forms {
        // The frame the resolve picked. Both are null until the resolve is in — which for an audible surface is
        // after this answer — and they describe the rendition, not the letterbox inside it, which is `crop`.
        let (kind, url, audio, (width, height)) = match form {
            Form::Google { .. } => {
                // Only a silent surface lists it, and that always has its resolve.
                let Some(d) = &direct else { continue };
                soonest = soonest.min(d.expires);
                ("mp4", d.video.clone(), false, (d.width, d.height))
            }
            Form::Progressive { cap, audio } => {
                // Only when this form plays from the resolve in hand. An audible surface's fallback asks for
                // the silent rung while the resolve here is the master's, so its frame stays null even with a
                // resolve cached — the full ladder's dimensions do not describe the 720 rendition, and a page
                // that needs them has `requestVideoFrameCallback`.
                let frame = direct.as_ref().filter(|_| cap == first.cap());
                let frame = (frame.and_then(|d| d.width), frame.and_then(|d| d.height));
                ("mp4", media_url("p", cap, audio, false, None), audio, frame)
            }
            Form::Hls { native } => {
                ("hls", media_url("h", None, false, native, report.clone()), true, (None, None))
            }
        };
        // Never the same URL twice: a step to the URL already playing starts no load and fires no error.
        if seen.insert(url.clone()) {
            sources
                .push(json!({ "kind": kind, "url": url, "audio": audio, "width": width, "height": height }));
        }
    }

    // The letterbox, when it is known. Nothing here waits for it: an unmeasured trailer is measured from its
    // keyframes in the background, and the next answer carries it.
    let crop =
        state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).get(&vid).and_then(|r| r.fractions());
    if crop.is_none() && !speculative {
        // From the index this surface is building anyway: the one with sound for an audible surface. Measuring
        // from `first.cap()` instead would build a SECOND index, at a rung nothing in this list plays, for a
        // letterbox the fallback's own index could have given.
        crate::crop::measure_in_background(&state, &vid, indexed, surface == Surface::Audible);
    }

    let max_age = match &direct {
        Some(d) => {
            d.expires
                .saturating_sub(now)
                .saturating_sub(crate::direct::EXPIRY_MARGIN_MS)
                .min(MEDIA_TTL_SECS * 1000)
                / 1000
        }
        // An answer given ahead of its resolve is good for as long as its URLs, but its letterbox and heights are
        // likely to be known minutes later.
        None => UNRESOLVED_MAX_AGE_SECS,
    };
    let body = json!({ "id": vid, "sources": sources, "crop": crop, "expires": soonest / 1000 });
    let cache = format!("private, max-age={max_age}");
    let resp =
        httputil::json(StatusCode::OK, &body, &[("cache-control", &cache), ("vary", crate::client::HEADER)]);
    httputil::timed(resp, &timing)
}

/// `/m/<segment>/<blob>`: a form `/sources` minted, served by the handler that plays it — and only under the
/// segment it was minted for (`segment`).
pub async fn handle_media(
    state: Arc<AppState>,
    headers: &HeaderMap,
    filed: &str,
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
    // Filed under the other segment, a URL would be accounted for as something it is not.
    if !crate::is_valid_vid(&media.v) || segment(&media) != filed {
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
        // The audible fallback asks for the SAME rung as the silent surface, and that is the point of it: a
        // resolve is cached per (video, rung), so a fallback on a rung of its own pays a 1.2-3.6 s resolve
        // that the billboard has already paid on 720. Measured warm-resolve/cold-index at this rung:
        // 249 ms macOS Safari, 412 ms iOS, 475 ms Chrome.
        assert_eq!(
            plan(Surface::Audible, Player::Native, cap),
            [Form::Hls { native: true }, Form::Progressive { cap, audio: true }]
        );
        assert_eq!(
            plan(Surface::Audible, Player::HlsJs, cap),
            [Form::Hls { native: false }, Form::Progressive { cap, audio: true }]
        );
        // Every progressive form in every plan names one rung, so one index serves all of them.
        for (surface, player) in [
            (Surface::Silent, Player::Native),
            (Surface::Silent, Player::HlsJs),
            (Surface::Audible, Player::Native),
            (Surface::Audible, Player::HlsJs),
        ] {
            for form in plan(surface, player, cap) {
                if let Form::Progressive { cap: asked, .. } = form {
                    assert_eq!(asked, cap, "{surface:?}/{player:?} asks for a rung of its own");
                }
            }
        }
    }

    /// Which entry leads, given that the file is ready — the other half of the decision, and the half that
    /// does not depend on a cache.
    #[test]
    fn a_ready_file_is_moved_in_front_of_the_master() {
        let cap = Some(720);
        let mut forms = plan(Surface::Audible, Player::Native, cap);
        assert!(lead_with_sound(&mut forms));
        assert_eq!(forms, [Form::Progressive { cap, audio: true }, Form::Hls { native: true }]);

        // Idempotent: an entry already leading stays where it is, and the list keeps every form.
        assert!(lead_with_sound(&mut forms));
        assert_eq!(forms, [Form::Progressive { cap, audio: true }, Form::Hls { native: true }]);

        let mut forms = plan(Surface::Audible, Player::HlsJs, cap);
        assert!(lead_with_sound(&mut forms));
        assert_eq!(forms, [Form::Progressive { cap, audio: true }, Form::Hls { native: false }]);

        // A silent plan has no entry with sound, so there is nothing to lead with and nothing moves.
        let mut forms = plan(Surface::Silent, Player::HlsJs, cap);
        let before = forms.clone();
        assert!(!lead_with_sound(&mut forms));
        assert_eq!(forms, before);
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
        let rest = path.strip_prefix("m/").unwrap().split_once('/').unwrap().1;
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
