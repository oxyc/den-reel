//! CROP DETECTION: report the real (non-black) content rectangle of a trailer so the app can
//! aspect-fill it — trimming baked-in letterbox bars — without any re-encode or quality loss.
//!
//! We run ffmpeg `cropdetect` over the *cached* MP4 (keyframe-sampled, so it's cheap) and parse the
//! bounding box of everything non-black. That makes it **logo-safe by construction**: a logo, laurel
//! or "in theaters" card sitting in the bar is non-black, so cropdetect keeps that region and the
//! bar is simply left in place for that trailer — we never crop a logo away.
//!
//! Exposed as `GET /crop/<id>.json`. Additive; the /play download+serve hot path is untouched — the
//! (small) cropdetect cost is only paid when the app actually asks, and the result is cached.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;

use hyper::{Response, StatusCode};
use serde::Serialize;
use serde_json::to_value;
use tokio::process::Command;

use crate::config::Config;
use crate::httputil::{self, Body};
use crate::play::fetch_trailer;
use crate::state::AppState;

#[derive(Clone, Serialize)]
pub struct Dim {
    pub w: u32,
    pub h: u32,
}

#[derive(Clone, Serialize)]
pub struct Rect {
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
}

/// What `/crop/<id>.json` returns. `letterboxed=false` (with `content` == source, or absent) means
/// "play normally". When `letterboxed=true`, the app should aspect-fill `content` within the frame.
#[derive(Clone, Serialize)]
pub struct CropReport {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Dim>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Rect>,
    pub letterboxed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aspect: Option<f64>,
}

impl CropReport {
    /// "Couldn't determine — just play it": returned when the file isn't available or ffmpeg fails.
    /// Not cached, so a later call retries.
    fn unknown(id: &str) -> CropReport {
        CropReport { id: id.to_string(), source: None, content: None, letterboxed: false, aspect: None }
    }
}

/// A `crop=W:H:X:Y` box parsed from cropdetect output.
#[derive(Debug, PartialEq)]
pub struct RawCrop {
    pub w: u32,
    pub h: u32,
    pub x: u32,
    pub y: u32,
}

/// Parse the LAST `crop=W:H:X:Y` cropdetect emits. With `reset=0` cropdetect accumulates, so the
/// final value is the union bounding box of all non-black pixels seen (the conservative choice).
pub fn parse_crop(stderr: &str) -> Option<RawCrop> {
    let mut last = None;
    for (pos, _) in stderr.match_indices("crop=") {
        let rest = &stderr[pos + 5..];
        let token: String = rest.chars().take_while(|c| c.is_ascii_digit() || *c == ':').collect();
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() == 4 {
            if let (Ok(w), Ok(h), Ok(x), Ok(y)) =
                (parts[0].parse(), parts[1].parse(), parts[2].parse(), parts[3].parse())
            {
                last = Some(RawCrop { w, h, x, y });
            }
        }
    }
    last
}

/// Pull the source `WxH` out of ffmpeg's `Stream #… Video:` line.
pub fn parse_source_dims(stderr: &str) -> Option<(u32, u32)> {
    stderr.lines().find(|l| l.contains("Video:")).and_then(find_dims)
}

fn find_dims(s: &str) -> Option<(u32, u32)> {
    let b = s.as_bytes();
    for i in 1..b.len() {
        if b[i] == b'x' && b[i - 1].is_ascii_digit() {
            let mut l = i;
            while l > 0 && b[l - 1].is_ascii_digit() {
                l -= 1;
            }
            let mut r = i + 1;
            while r < b.len() && b[r].is_ascii_digit() {
                r += 1;
            }
            if r > i + 1 {
                if let (Ok(w), Ok(h)) = (s[l..i].parse::<u32>(), s[i + 1..r].parse::<u32>()) {
                    if w >= 16 && h >= 16 {
                        return Some((w, h));
                    }
                }
            }
        }
    }
    None
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// Build a report from a raw crop + (optional) source dims. Treats bars ≤2% of a side as noise
/// (not letterboxed), so we don't ask the app to shave a few encoder-fuzz pixels.
pub fn report_from(id: &str, src: Option<(u32, u32)>, raw: RawCrop) -> CropReport {
    let (sw, sh) = src.unwrap_or((raw.w, raw.h));
    let bar_v = sh.saturating_sub(raw.h);
    let bar_h = sw.saturating_sub(raw.w);
    // >2% of the dimension counts as a real bar (bar*50 > total ⇔ bar/total > 1/50).
    let letterboxed = bar_v * 50 > sh || bar_h * 50 > sw;
    let aspect = (raw.h > 0).then(|| round2(raw.w as f64 / raw.h as f64));
    CropReport {
        id: id.to_string(),
        source: Some(Dim { w: sw, h: sh }),
        content: Some(Rect { x: raw.x, y: raw.y, w: raw.w, h: raw.h }),
        letterboxed,
        aspect,
    }
}

/// Run cropdetect over the cached file. Keyframe-sampled (`-skip_frame nokey`) so it's a light
/// decode-only pass, no encode. `None` if ffmpeg can't be run or emits no usable box.
pub async fn detect(cfg: &Config, id: &str, fp: &Path) -> Option<CropReport> {
    let out = Command::new(&cfg.ffmpeg)
        .args([
            "-hide_banner",
            "-nostdin",
            "-skip_frame",
            "nokey", // decode only keyframes → fast, still catches held logo cards
            "-i",
            &fp.to_string_lossy(),
            "-an",
            "-vf",
            "cropdetect=limit=24:round=2:reset=0",
            "-f",
            "null",
            "-",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await
        .ok()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    let raw = parse_crop(&stderr)?;
    Some(report_from(id, parse_source_dims(&stderr), raw))
}

pub async fn handle_crop(state: Arc<AppState>, id: String) -> Response<Body> {
    if let Some(cached) = state.crop_cache.lock().unwrap().get(&id).cloned() {
        return json(&cached);
    }
    // Ensure the file (de-dupes with a concurrent /play), then detect. If either fails, answer
    // "unknown" so the app just plays normally — and don't cache that, so it retries later.
    let report = match fetch_trailer(state.clone(), id.clone()).await {
        Ok(fp) => match detect(&state.cfg, &id, &fp).await {
            Some(r) => {
                state.crop_cache.lock().unwrap().insert(id.clone(), r.clone());
                r
            }
            None => CropReport::unknown(&id),
        },
        Err(_) => CropReport::unknown(&id),
    };
    json(&report)
}

fn json(report: &CropReport) -> Response<Body> {
    let value = to_value(report).unwrap_or_else(|_| serde_json::json!({ "letterboxed": false }));
    httputil::json(StatusCode::OK, &value, &[])
}
