//! CROP DETECTION: report the real content rectangle of a trailer so the app can aspect-fill it —
//! trimming baked-in letterbox bars — without any re-encode or quality loss.
//!
//! We run ffmpeg `cropdetect` over the *cached* MP4 (keyframe-sampled, so it's cheap) with `reset=1`,
//! so each keyframe yields its own crop box instead of one growing union. We then take the **typical
//! (median) box** and snap it to a standard cinematic aspect. That crops a *transient* logo / laurel /
//! "in theaters" card out of the bar: appearing on only a minority of keyframes, it can't hold the bar
//! open (a persistent, whole-trailer logo still can). A minimum-content floor guards against
//! over-cropping a dark trailer whose frames momentarily read as mostly black.
//!
//! **Full-frame guard.** A median alone would over-crop a *mixed-framing* trailer — one that's mostly
//! letterboxed but has genuine full-frame shots (common in animated trailers: e.g. Monsters vs Aliens
//! is ~2.35 with a few full-frame hero shots). The median picks the dominant letterbox and slices
//! those shots. So if more than a stray keyframe is essentially the full frame, we DON'T crop at all —
//! keeping bars beats shaving real content. A logo in a bar makes a frame *taller but not full*, so
//! this guard never suppresses logo cropping. Net: identical to the old union on such trailers, but it
//! additionally drops transient logos on genuinely-letterboxed ones.
//!
//! Exposed as `GET /crop/<id>.json`. Additive; the /play download+serve hot path is untouched — the
//! (small) cropdetect cost is only paid when the app actually asks, and the result is cached.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use hyper::{Response, StatusCode};
use serde::Serialize;
use serde_json::to_value;
use tokio::process::Command;

use crate::config::Config;
use crate::httputil::{self, Body};
use crate::play::fetch_trailer;
use crate::state::AppState;
use crate::CROP_CACHE_MAX;

const DETECT_TIMEOUT_SECS: u64 = 60; // cropdetect keyframe pass; backstop a hung ffmpeg
const BAKE_TIMEOUT_SECS: u64 = 30; // MP4Box clap write is ~instant; backstop a hang

/// Standard cinematic content aspects we snap a detected top/bottom letterbox to, so a logo/laurel
/// card that inflated a few keyframes' boxes doesn't leave the bar half-cropped — we clap the clean
/// scope crop instead. (2.39/2.35 scope, 2.0 univisium, 1.85 flat.)
const STD_ASPECTS: [f64; 4] = [2.39, 2.35, 2.0, 1.85];
/// Accept a snap only within this *relative* distance of a standard aspect.
const SNAP_TOL: f64 = 0.05;
/// If the snapped height is within this many px of the measured typical box, keep the measured box —
/// don't jitter an already-clean letterbox by a pixel or two.
const SNAP_KEEP_PX: u32 = 6;
/// Never crop a top/bottom letterbox below this fraction of the source height. A more aggressive,
/// non-standard vertical crop is treated as a dark-frame artifact and left uncropped (play full).
const MIN_CONTENT_FRAC: f64 = 0.6;

/// A keyframe whose box fills the frame (both bars ≤2%) counts as "full frame". If at least this many
/// keyframes — AND this percent of them — are full-frame, the trailer genuinely uses the full frame
/// (mixed framing), so we must not crop it. Two thresholds so a single stray full-frame flash on a
/// clean letterbox doesn't suppress the crop, but a handful of real full-frame shots does.
const FULL_FRAME_MIN: usize = 2;
const FULL_FRAME_PCT: usize = 3;

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
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RawCrop {
    pub w: u32,
    pub h: u32,
    pub x: u32,
    pub y: u32,
}

/// Every `crop=W:H:X:Y` cropdetect emits. With `reset=1` that's one box per analyzed keyframe, so the
/// set is a distribution we can take a robust typical value from (rather than the growing union).
pub fn parse_all_crops(stderr: &str) -> Vec<RawCrop> {
    let mut out = Vec::new();
    for (pos, _) in stderr.match_indices("crop=") {
        let rest = &stderr[pos + 5..];
        let token: String = rest.chars().take_while(|c| c.is_ascii_digit() || *c == ':').collect();
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() == 4 {
            if let (Ok(w), Ok(h), Ok(x), Ok(y)) =
                (parts[0].parse(), parts[1].parse(), parts[2].parse(), parts[3].parse())
            {
                out.push(RawCrop { w, h, x, y });
            }
        }
    }
    out
}

/// The typical box: the per-field median across all keyframe boxes. Median (not union) is what lets a
/// transient logo/laurel card — present on a minority of keyframes — fall out, while the dominant
/// letterbox wins. Each field is taken independently, which is exact for the common centred letterbox.
pub fn typical_crop(boxes: &[RawCrop]) -> Option<RawCrop> {
    if boxes.is_empty() {
        return None;
    }
    fn median(mut v: Vec<u32>) -> u32 {
        v.sort_unstable();
        v[v.len() / 2]
    }
    Some(RawCrop {
        w: median(boxes.iter().map(|c| c.w).collect()),
        h: median(boxes.iter().map(|c| c.h).collect()),
        x: median(boxes.iter().map(|c| c.x).collect()),
        y: median(boxes.iter().map(|c| c.y).collect()),
    })
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

/// Whether enough keyframes fill the frame (both bars ≤2%) that the trailer genuinely *uses* the full
/// frame — more than a stray flash. When true the caller must not crop (mixed-framing trailer): a
/// dominant letterbox with real full-frame shots. A logo in a bar makes a frame taller-but-not-full,
/// so it is not counted here — logo cropping is unaffected.
pub fn uses_full_frame(boxes: &[RawCrop], src: Option<(u32, u32)>) -> bool {
    let Some((sw, sh)) = src else {
        return false; // no source dims → can't judge; fall back to the crop path
    };
    let full = boxes
        .iter()
        .filter(|b| sh.saturating_sub(b.h) * 50 <= sh && sw.saturating_sub(b.w) * 50 <= sw)
        .count();
    full >= FULL_FRAME_MIN && full * 100 >= boxes.len() * FULL_FRAME_PCT
}

/// A "play the full frame" report for `src` — used when a mixed-framing trailer must not be cropped.
fn full_frame_report(id: &str, sw: u32, sh: u32) -> CropReport {
    CropReport {
        id: id.to_string(),
        source: Some(Dim { w: sw, h: sh }),
        content: Some(Rect { x: 0, y: 0, w: sw, h: sh }),
        letterboxed: false,
        aspect: Some(round2(sw as f64 / sh as f64)),
    }
}

/// Nearest standard cinematic aspect to `a`, or `None` if none is within `SNAP_TOL` (relative).
fn nearest_std_aspect(a: f64) -> Option<f64> {
    STD_ASPECTS
        .iter()
        .copied()
        .min_by(|x, y| (x - a).abs().partial_cmp(&(y - a).abs()).unwrap_or(std::cmp::Ordering::Equal))
        .filter(|best| (best - a).abs() / best <= SNAP_TOL)
}

/// Turn a typical-box report into what we actually serve. We only ever trim a top/bottom letterbox
/// from a full-width, landscape content region that keeps enough height — the one case that makes sense
/// for the landscape billboard. Everything else plays the full frame (no crop, no clap): a portrait
/// source (huge top/bottom padding isn't a cinematic letterbox — cropping it to a thin strip breaks the
/// billboard), a pillarbox or both-bar crop (not our job), or an over-aggressive vertical crop below
/// `MIN_CONTENT_FRAC` height (likely a dark-frame artifact — keeping bars beats shaving real content).
/// When we do keep a letterbox we snap the height to a standard cinematic aspect (dropping a transient
/// logo that inflated the box) and always emit a centred, full-width box, so the baked clap is
/// symmetric and geometrically valid every time.
pub fn refine_report(r: CropReport) -> CropReport {
    if !r.letterboxed {
        return r;
    }
    let (Some(src), Some(c)) = (r.source.clone(), r.content.clone()) else {
        return r;
    };
    let bar_v = src.h.saturating_sub(c.h);
    let bar_h = src.w.saturating_sub(c.w);
    let full_width = c.w * 20 >= src.w * 19; // content spans ≥95% width (no real side bars)
    let landscape = c.w >= c.h; // the resulting region is itself landscape
    let enough_height = c.h as f64 >= src.h as f64 * MIN_CONTENT_FRAC;
    if !(bar_v > bar_h && full_width && landscape && enough_height) {
        return full_frame_report(&r.id, src.w, src.h);
    }
    // Snap the height to a standard aspect when it's close and would move the box meaningfully (e.g. a
    // logo inflated it); otherwise keep the measured height.
    let mut h = c.h;
    if let Some(snapped) = nearest_std_aspect(src.w as f64 / c.h as f64) {
        let target_h = (src.w as f64 / snapped).round() as u32;
        if target_h <= src.h
            && target_h as f64 >= src.h as f64 * MIN_CONTENT_FRAC
            && target_h.abs_diff(c.h) > SNAP_KEEP_PX
        {
            h = target_h;
        }
    }
    let y = (src.h - h) / 2;
    CropReport {
        id: r.id,
        source: Some(src.clone()),
        content: Some(Rect { x: 0, y, w: src.w, h }),
        letterboxed: src.h.saturating_sub(h) * 50 > src.h,
        aspect: Some(round2(src.w as f64 / h as f64)),
    }
}

/// Run cropdetect over the cached file. Keyframe-sampled (`-skip_frame nokey`) so it's a light
/// decode-only pass, no encode. `None` if ffmpeg can't be run or emits no usable box.
pub async fn detect(cfg: &Config, id: &str, fp: &Path) -> Option<CropReport> {
    let mut cmd = Command::new(&cfg.ffmpeg);
    cmd.args([
        "-hide_banner",
        "-nostdin",
        "-skip_frame",
        "nokey", // decode only keyframes → fast
        "-i",
        &fp.to_string_lossy(),
        "-an",
        "-vf",
        // reset=1: a fresh box per keyframe (not the growing union), so a transient logo card can't
        // hold the bar open — we take the median box below. (ffmpeg documents reset for exactly this.)
        "cropdetect=limit=24:round=2:reset=1",
        "-f",
        "null",
        "-",
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    // Backstop timeout: on elapse the output future (owning the child) is dropped → kill_on_drop.
    let out = tokio::time::timeout(Duration::from_secs(DETECT_TIMEOUT_SECS), cmd.output())
        .await
        .ok()?
        .ok()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    let boxes = parse_all_crops(&stderr);
    let src = parse_source_dims(&stderr);
    // Mixed-framing trailer (real full-frame shots) → don't crop, keep bars (see module docs).
    if let (true, Some((sw, sh))) = (uses_full_frame(&boxes, src), src) {
        return Some(full_frame_report(id, sw, sh));
    }
    let typical = typical_crop(&boxes)?;
    Some(refine_report(report_from(id, src, typical)))
}

/// The `clap` box params for a letterboxed report: `(width, height, horizOffNum, vertOffNum)`, each
/// offset over denominator 2. Offsets are the content-centre relative to the frame centre — so a
/// symmetric letterbox is 0, and an off-centre crop (e.g. a logo kept in one bar) gets the right
/// nonzero value. `None` when there's nothing worth cropping.
pub fn clap_params(report: &CropReport) -> Option<(u32, u32, i64, i64)> {
    if !report.letterboxed {
        return None;
    }
    let src = report.source.as_ref()?;
    let c = report.content.as_ref()?;
    let ho = 2 * c.x as i64 + c.w as i64 - src.w as i64;
    let vo = 2 * c.y as i64 + c.h as i64 - src.h as i64;
    Some((c.w, c.h, ho, vo))
}

/// Bake a `clap` box into the cached MP4 in place (MP4Box, ~13 ms, +40 bytes, no re-encode,
/// faststart preserved) so the billboard AVPlayer crops the letterbox. Best-effort: any failure is
/// logged and ignored — the un-clap'd file still plays fine (clients that don't read clap just show
/// the full frame). No-op when disabled or not letterboxed.
pub async fn bake_clap(cfg: &Config, fp: &Path, report: &CropReport) -> bool {
    if !cfg.bake_clap {
        return false;
    }
    let Some((w, h, ho, vo)) = clap_params(report) else {
        return false;
    };
    let spec = format!("1={w},1,{h},1,{ho},2,{vo},2");
    let mut cmd = Command::new(&cfg.mp4box);
    cmd.args(["-clap", &spec, &fp.to_string_lossy()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    match tokio::time::timeout(Duration::from_secs(BAKE_TIMEOUT_SECS), cmd.status()).await {
        Ok(Ok(s)) if s.success() => true,
        other => {
            eprintln!("bake_clap {}: {other:?}", fp.display());
            false
        }
    }
}

/// Insert a crop report into the cache, bounding growth (crop has no TTL, so cap the size).
pub fn cache_report(state: &Arc<AppState>, id: &str, report: CropReport) {
    let mut c = state.crop_cache.lock().unwrap_or_else(|e| e.into_inner());
    if c.len() >= CROP_CACHE_MAX {
        c.clear(); // crude but bounded; entries are cheap to recompute on next request
    }
    c.insert(id.to_string(), report);
}

pub async fn handle_crop(state: Arc<AppState>, id: String) -> Response<Body> {
    if !crate::play::cache_available(&state.cfg).await {
        return httputil::error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cache_unavailable",
            "Trailer cache is unavailable.",
        );
    }
    if let Some(cached) = state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).get(&id).cloned() {
        return json(&cached);
    }
    // Ensure the file (de-dupes with a concurrent /play), then detect. If either fails, answer
    // "unknown" so the app just plays normally — and don't cache that, so it retries later.
    let report = match fetch_trailer(state.clone(), id.clone()).await {
        Ok(fp) => {
            // download_cached may have just cached the report — reuse it with a SINGLE lock (using the
            // Option directly, so a concurrent cache_report clear() can't wedge us on an unwrap).
            let cached = state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).get(&id).cloned();
            match cached {
                Some(r) => r,
                None => match detect(&state.cfg, &id, &fp).await {
                    Some(r) => {
                        cache_report(&state, &id, r.clone());
                        r
                    }
                    None => CropReport::unknown(&id),
                },
            }
        }
        Err(_) => CropReport::unknown(&id),
    };
    json(&report)
}

fn json(report: &CropReport) -> Response<Body> {
    let value = to_value(report).unwrap_or_else(|_| serde_json::json!({ "letterboxed": false }));
    // The detected crop rect is immutable per video (the cached MP4 never changes), so it can be
    // cached hard + `immutable`; the ETag `json` attaches lets a conditional GET still 304.
    httputil::json(StatusCode::OK, &value, &[("cache-control", "public, max-age=31536000, immutable")])
}
