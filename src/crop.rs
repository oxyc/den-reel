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
//! Exposed as `GET /crop/<id>.json`. Detection and the `clap` bake run once per cold download,
//! inside the download's concurrency slot and before the file is published — so a first play pays
//! for them, not the first `/crop` call.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    /// Never cached as an ANSWER (no client stores it, and `crop_cache` only ever holds real rects);
    /// the id is parked in `crop_unknown` for a few minutes so the ffmpeg pass isn't re-run per call.
    fn unknown(id: &str) -> CropReport {
        CropReport { id: id.to_string(), source: None, content: None, letterboxed: false, aspect: None }
    }

    /// Did detection actually produce a rect? An `unknown` must not be cached by anyone.
    fn is_known(&self) -> bool {
        self.source.is_some()
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
    // In its own process group and registered live, so a SIGTERM kills it too. Without that it was
    // orphaned on shutdown — and this timeout lives in den-reel's timer, so an orphan has none.
    let out = tokio::time::timeout(
        Duration::from_secs(DETECT_TIMEOUT_SECS),
        crate::ytdlp::output_in_group(&mut cmd),
    )
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

/// How many keyframes a detection from keyframes reads: every one in a trailer (two dozen to a hundred),
/// evenly thinned past this for a longer video.
///
/// Every one, not one a fragment: on 2026-09-15 a trailer read from the first keyframe of each fragment came
/// out full frame where the whole file measured a 2.40 letterbox, and read from all of them it measured 2.41.
const KEYFRAMES_MAX: usize = 64;

/// The rung `/crop?detect=keyframes` reads keyframes from: the one Den Web's billboard plays, so its index is
/// usually built already.
const KEYFRAME_HEIGHT: &str = "720";

/// The content rectangle, from the keyframes of YouTube's own stream at `cap` rather than a downloaded file.
///
/// `/progressive`'s index already says where every keyframe sits, so this fetches only those — half a
/// megabyte to a few for a trailer — and hands them to the same `detect` pass as one H.264 stream of still
/// pictures. No yt-dlp download, no cache slot. Measured against the whole-file pass on eight cached trailers
/// it agreed on seven; the eighth's downloaded file carries twice the keyframes of Google's stream, two of
/// them full frame, which is enough to hold the mixed-framing guard there and not here.
pub async fn detect_from_keyframes(
    state: &Arc<AppState>,
    id: &str,
    cap: Option<u32>,
) -> Result<CropReport, String> {
    let (layout, url) = crate::progressive::indexed(state, id, cap).await?;
    let stream = crate::progressive::keyframe_stream(&state.http, &url, &layout, KEYFRAMES_MAX).await?;
    let n = state.dl_gen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // A dotfile at the top of the cache: eviction leaves it alone, and the partial sweep reclaims it
    // should the removal below ever not happen.
    let path = state.cfg.cache_dir.join(format!(".crop-{id}-{n}.h264"));
    tokio::fs::write(&path, &stream).await.map_err(|e| format!("write the keyframes: {e}"))?;
    let detected = {
        let _permit = state.probe_sem.acquire().await;
        detect(&state.cfg, id, &path).await
    };
    if let Err(e) = tokio::fs::remove_file(&path).await {
        crate::log_limited("crop keyframes cleanup", || format!("[{id}] keyframes left behind: {e}"));
    }
    detected.ok_or_else(|| "cropdetect found no box in the keyframes".into())
}

/// `/crop/<id>.json?detect=keyframes`: detect now, from keyframes, whatever is cached.
pub async fn handle_keyframe_crop(state: Arc<AppState>, id: String) -> Response<Body> {
    let started = Instant::now();
    let cap = crate::direct::height_cap(&state.cfg, Some(KEYFRAME_HEIGHT));
    let detected = detect_from_keyframes(&state, &id, cap).await;
    let timing = httputil::timing("keyframes", started.elapsed());
    match detected {
        Ok(report) => httputil::timed(json(&report), &timing),
        Err(why) => {
            crate::log_limited("crop keyframes", || format!("[{id}] no crop from keyframes ({why})"));
            httputil::timed(json(&CropReport::unknown(&id)), &timing)
        }
    }
}

/// Measure `id`'s letterbox from its keyframes at `cap`, in the background, unless it is known, already being
/// measured, or recently found unmeasurable. A result never replaces one a whole-file pass cached.
pub fn measure_in_background(state: &Arc<AppState>, id: &str, cap: Option<u32>) {
    if unknown_is_fresh(state, id) {
        return;
    }
    if !state.crop_inflight.lock().unwrap_or_else(|e| e.into_inner()).insert(id.to_string()) {
        return;
    }
    let (state, id) = (state.clone(), id.to_string());
    tokio::spawn(async move {
        match detect_from_keyframes(&state, &id, cap).await {
            Ok(report) => {
                if !state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&id) {
                    cache_report(&state, &id, report);
                }
            }
            Err(why) => {
                crate::log_limited("crop keyframes", || format!("[{id}] no crop from keyframes ({why})"));
                record_unknown(&state, &id);
            }
        }
        state.crop_inflight.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
    });
}

impl CropReport {
    /// The content rectangle as fractions of the frame — `[x, y, width, height]` — which hold at whatever height
    /// a page plays, with the aspect and whether it is letterboxed at all. `None` when nothing was measured.
    pub fn fractions(&self) -> Option<serde_json::Value> {
        let (s, c) = (self.source.as_ref()?, self.content.as_ref()?);
        let f = |n: u32, of: u32| (n as f64 / of.max(1) as f64 * 10_000.0).round() / 10_000.0;
        Some(serde_json::json!({
            "letterboxed": self.letterboxed,
            "aspect": self.aspect,
            "rect": [f(c.x, s.w), f(c.y, s.h), f(c.w, s.w), f(c.h, s.h)],
        }))
    }
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

/// What a bake did to the FILE, which is not the same question as whether it worked.
///
/// MP4Box rewrites in place — verified against GPAC 26.02, same inode before and after, the content
/// change landing as a burst near the end of a 1.5s run on a 200 MB file. So a bake killed part-way
/// leaves a half-rewritten trailer, which the caller must not rename into the cache: it is served for
/// as long as it stays cached and never re-fetched. A bake that refused, or never ran, leaves the file
/// exactly as it was, and losing that trailer over a cosmetic step would be the worse bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bake {
    /// The file is untouched: disabled, not letterboxed, MP4Box missing, or it refused.
    Skipped,
    Baked,
    /// It was written to and not finished. Do not publish.
    Damaged,
}

/// Bake a `clap` box into the cached MP4 in place (MP4Box, ~13 ms, +40 bytes, no re-encode,
/// faststart preserved) so the billboard AVPlayer crops the letterbox. Best-effort for everything
/// except a half-written file — clients that don't read clap just show the full frame, so a trailer
/// without the box is worth far more than no trailer. No-op when disabled or not letterboxed.
pub async fn bake_clap(cfg: &Config, fp: &Path, report: &CropReport) -> Bake {
    if !cfg.bake_clap {
        return Bake::Skipped;
    }
    let Some((w, h, ho, vo)) = clap_params(report) else {
        return Bake::Skipped;
    };
    let spec = format!("1={w},1,{h},1,{ho},2,{vo},2");
    let mut cmd = Command::new(&cfg.mp4box);
    // -tmp keeps MP4Box's working copy on the cache volume. It rewrites via a temp file the size
    // of the trailer, and by default that lands in the container's own /tmp — a filesystem nothing
    // here sizes or evicts, so a bake could ENOSPC on a box with plenty of cache room. stderr is
    // kept for the same reason ytdlp pipes it: a silent best-effort failure is undiagnosable.
    let tmp_dir = cfg.cache_dir.to_string_lossy().into_owned();
    cmd.args(["-tmp", &tmp_dir, "-clap", &spec, &fp.to_string_lossy()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let before = file_stamp(fp).await;
    // output(), not status(): a piped stderr nobody reads can wedge the child on a full pipe.
    // Grouped and registered for the same reason as detect: a bake orphaned by a redeploy keeps
    // rewriting the trailer with no timeout behind it.
    match tokio::time::timeout(
        Duration::from_secs(BAKE_TIMEOUT_SECS),
        crate::ytdlp::output_in_group(&mut cmd),
    )
    .await
    {
        Ok(Ok(o)) if o.status.success() => bake_outcome(BakeRun::Ok),
        Ok(Ok(o)) => {
            let touched = file_stamp(fp).await != before;
            eprintln!(
                "bake_clap {}: exit {:?}{} — {}",
                fp.display(),
                o.status.code(),
                if touched { " AFTER writing" } else { " without touching the file" },
                crate::ytdlp::stderr_tail(&o.stderr)
            );
            bake_outcome(BakeRun::Refused { touched })
        }
        Ok(Err(e)) => {
            eprintln!("bake_clap {}: could not start MP4Box — {e}", fp.display());
            bake_outcome(BakeRun::NeverStarted)
        }
        Err(_) => {
            // Ask the file here too. The timeout covers MP4Box's read/parse phase as well as the
            // rewrite, so a bake killed while still reading a large trailer never touched the
            // target — condemning it on the exit alone is exactly the premise this stopped using.
            // The SIGKILL from the guard's drop is asynchronous, so settle first.
            tokio::time::sleep(KILL_SETTLE).await;
            let touched = file_stamp(fp).await != before;
            eprintln!(
                "bake_clap {}: exceeded {BAKE_TIMEOUT_SECS}s and was killed{}",
                fp.display(),
                if touched { " mid-rewrite" } else { " before it wrote anything" }
            );
            bake_outcome(if touched { BakeRun::Killed } else { BakeRun::Refused { touched: false } })
        }
    }
}

/// How a bake ended, before deciding what that means for the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BakeRun {
    /// Exited cleanly.
    Ok,
    /// Exited non-zero. `touched` says whether the file changed while it ran — which is the actual
    /// question, and not one an exit code answers: MP4Box validates its arguments and the input
    /// before opening the file for writing, so every refusal mode (unknown flag, no such track,
    /// read-only target, unparseable MP4) leaves it byte-identical. Assuming otherwise deleted
    /// perfectly good trailers. But a write that fails PART WAY — ENOSPC, EIO — also exits
    /// non-zero, and that one really is damage, so the file itself has to be asked.
    Refused { touched: bool },
    /// MP4Box could not be spawned at all.
    NeverStarted,
    /// Timed out and was SIGKILLed after it had already written — a half-rewritten file. A timeout
    /// that killed it before it wrote is a `Refused { touched: false }`, not this.
    Killed,
}

/// Only a file that was actually written to and not finished is unpublishable.
pub(crate) fn bake_outcome(run: BakeRun) -> Bake {
    match run {
        BakeRun::Ok => Bake::Baked,
        BakeRun::NeverStarted | BakeRun::Refused { touched: false } => Bake::Skipped,
        BakeRun::Refused { touched: true } | BakeRun::Killed => Bake::Damaged,
    }
}

/// Long enough for an asynchronous SIGKILL to land before the file is stamped, short enough to be
/// invisible — only ever paid on a bake that already ran into its 30s timeout.
const KILL_SETTLE: Duration = Duration::from_millis(200);

/// Size + mtime, the cheap evidence of whether something wrote to the file.
async fn file_stamp(p: &Path) -> Option<(u64, std::time::SystemTime)> {
    let md = tokio::fs::metadata(p).await.ok()?;
    Some((md.len(), md.modified().ok()?))
}

/// Insert a crop report into the cache, bounding growth (crop has no TTL, so cap the size).
pub fn cache_report(state: &Arc<AppState>, id: &str, report: CropReport) {
    let mut c = state.crop_cache.lock().unwrap_or_else(|e| e.into_inner());
    if c.len() >= CROP_CACHE_MAX {
        c.clear(); // crude but bounded; entries are cheap to recompute on next request
    }
    c.insert(id.to_string(), report);
}

/// Did a recent cropdetect over this id already come back with nothing parsable?
fn unknown_is_fresh(state: &Arc<AppState>, id: &str) -> bool {
    let now = (state.clock)();
    let m = state.crop_unknown.lock().unwrap_or_else(|e| e.into_inner());
    m.get(id).is_some_and(|exp| *exp > now)
}

/// Remember that it did, so the next call answers "just play it" without another whole-file pass.
/// Bounded and cleared wholesale like `cache_report` above, and for the same reason: every entry is
/// an optimisation, and losing one costs a single recomputation.
fn record_unknown(state: &Arc<AppState>, id: &str) {
    let now = (state.clock)();
    let mut m = state.crop_unknown.lock().unwrap_or_else(|e| e.into_inner());
    if m.len() >= crate::CROP_UNKNOWN_MAX {
        m.retain(|_, exp| *exp > now);
        if m.len() >= crate::CROP_UNKNOWN_MAX {
            m.clear();
        }
    }
    m.insert(id.to_string(), now + crate::CROP_UNKNOWN_TTL_MS);
}

/// The answer for a caller that did not present a signature: "just play the full frame".
///
/// Not a 403, deliberately. `/crop` is a HINT — every failing path in `handle_crop` already answers
/// `unknown` and the app plays normally — so refusing it outright would turn a missing tag into
/// silently lost de-letterboxing, with no error the app could report and nothing in `/health`. What
/// the gate is actually for is the download behind this endpoint, and returning here reaches none of
/// it: no fetch, no ffmpeg, no cache write. An unsigned caller gets a constant.
///
/// The trailers that matter most are unaffected either way: a letterbox detected at download time is
/// baked into the file as a `clap` box, which AVPlayer honours with no `/crop` call at all.
pub fn unsigned_response(id: &str) -> Response<Body> {
    json(&CropReport::unknown(id))
}

pub async fn handle_crop(state: Arc<AppState>, id: String) -> Response<Body> {
    if !crate::play::cache_available(&state.cfg).await {
        return crate::play::cache_unavailable();
    }
    if let Some(cached) = state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).get(&id).cloned() {
        return httputil::timed(json(&cached), "cache;desc=hit");
    }
    // A recent pass over this file produced nothing parsable, so the answer is already decided:
    // `unknown`, identical to what every other failing path here returns.
    //
    // Checked BEFORE the fetch, not after. Behind it sits a whole trailer download — and this is
    // reachable with the file gone, because `record_unknown` only proves the file existed ten
    // minutes ago and eviction is ordinary behaviour on a full volume. Fetching to produce a value
    // that does not depend on what was fetched is the expensive half of the problem the permit
    // below does not solve. /play still downloads what it needs; nothing here has to.
    if unknown_is_fresh(&state, &id) {
        return httputil::timed(json(&CropReport::unknown(&id)), "cache;desc=hit");
    }
    // Ensure the file (de-dupes with a concurrent /play), then detect. If either fails, answer
    // "unknown" so the app just plays normally — and don't cache that, so it retries later.
    let mut timing = String::new();
    let report = match fetch_trailer(state.clone(), id.clone()).await {
        Ok(fetched) => {
            // A download this call waited on is part of what it cost.
            timing = fetched.timing();
            let fp = fetched.path;
            // download_cached may have just cached the report — reuse it with a SINGLE lock (using the
            // Option directly, so a concurrent cache_report clear() can't wedge us on an unwrap).
            let cached = state.crop_cache.lock().unwrap_or_else(|e| e.into_inner()).get(&id).cloned();
            match cached {
                Some(r) => r,
                // The one subprocess spawn that took no permit. Every other one — downloads, probes,
                // searches — is capped, and this is a whole-file ffmpeg pass per request with no
                // negative cache behind it, so a trailer that yields no parsable box re-runs it on
                // every call, at any concurrency. Shares the probe budget: same weight, same purpose.
                None => {
                    let detected = {
                        let _permit = state.probe_sem.acquire().await;
                        let started = Instant::now();
                        let detected = detect(&state.cfg, &id, &fp).await;
                        if !timing.is_empty() {
                            timing.push_str(", ");
                        }
                        timing.push_str(&httputil::timing("cropdetect", started.elapsed()));
                        detected
                    };
                    match detected {
                        Some(r) => {
                            cache_report(&state, &id, r.clone());
                            r
                        }
                        None => {
                            record_unknown(&state, &id);
                            CropReport::unknown(&id)
                        }
                    }
                }
            }
        }
        Err(_) => CropReport::unknown(&id),
    };
    httputil::timed(json(&report), &timing)
}

pub(crate) fn json(report: &CropReport) -> Response<Body> {
    let value = to_value(report).unwrap_or_else(|_| serde_json::json!({ "letterboxed": false }));
    // A real rect describes the cached MP4, which does not change while it stays cached — but after
    // eviction the same id is a new download, so it caches for a week rather than a year and is not
    // `immutable`; the ETag lets a revalidation 304. An `unknown` is the opposite — it means ffmpeg failed
    // or the file was not there — and it was going out with the same year-long `immutable`, so one
    // hiccup cost that trailer its de-letterboxing until the client's own cache was cleared. Its
    // own doc says "not cached, so a later call retries"; that was true server-side only.
    if report.is_known() {
        httputil::json(StatusCode::OK, &value, &[("cache-control", "public, max-age=604800")])
    } else {
        // "Play the full frame" stands in for a rect nobody could (or, unsigned, would) measure,
        // and the app is told so rather than left to read it as a verdict about the video.
        httputil::json(
            StatusCode::OK,
            &value,
            &[("cache-control", "no-store"), ("x-den-degraded", "crop_unavailable")],
        )
    }
}
