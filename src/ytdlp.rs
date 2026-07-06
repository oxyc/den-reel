//! The yt-dlp/ffmpeg subprocess layer: probe a candidate (fast, no download), download+mux a
//! faststart MP4, and map yt-dlp's stderr to an HTTP status so `/play` can say *why* it failed.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::config::Config;

const PROBE_TIMEOUT_SECS: u64 = 30; // yt-dlp --simulate should be quick; backstop a hang
const DOWNLOAD_TIMEOUT_SECS: u64 = 240; // download+mux backstop (yt-dlp also gets --socket-timeout)

/// A typed `/play` failure: an HTTP status + a short machine reason + a user-facing message.
/// Clone-able because the in-flight de-dupe shares one download future across waiters.
#[derive(Clone, Debug)]
pub struct PlayError {
    pub status: u16,
    pub reason: String,
    pub message: String,
    /// Diagnostic detail (last of stderr / spawn error) — logged, never sent to the client.
    pub detail: String,
}

impl PlayError {
    pub fn spawn(e: std::io::Error) -> PlayError {
        PlayError {
            status: 502,
            reason: "extraction_failed".into(),
            message: "Could not fetch this trailer.".into(),
            detail: format!("spawn subprocess: {e}"),
        }
    }
    pub fn timed_out() -> PlayError {
        PlayError {
            status: 504,
            reason: "timeout".into(),
            message: "This trailer took too long to fetch.".into(),
            detail: format!("yt-dlp exceeded {DOWNLOAD_TIMEOUT_SECS}s"),
        }
    }
}

/// Map a yt-dlp failure to an HTTP status + short reason (the cause is in stderr; match the common
/// YouTube ones). Anything unrecognized is a blanket 502.
pub fn classify(code: Option<i32>, stderr: &str) -> PlayError {
    let s = stderr.to_lowercase();
    let (status, reason, message) = if s.contains("available in your country")
        || s.contains("available in your location")
        || s.contains("blocked it in your country")
        || s.contains("not available from your location")
    {
        (451, "geo_blocked", "This trailer is not available in your region.")
    } else if s.contains("private video")
        || s.contains("sign in to confirm your age")
        || s.contains("age-restricted")
        || s.contains("members-only")
    {
        (403, "restricted", "This trailer is private or age-restricted.")
    } else if s.contains("video unavailable")
        || s.contains("has been removed")
        || s.contains("no longer available")
        || s.contains("does not exist")
        || s.contains("removed by the uploader")
    {
        (404, "unavailable", "This trailer is no longer available.")
    } else {
        (502, "extraction_failed", "Could not fetch this trailer.")
    };
    let tail: String = stderr.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
    PlayError {
        status,
        reason: reason.into(),
        message: message.into(),
        detail: format!("yt-dlp exit {code:?}: {tail}"),
    }
}

fn watch_url(vid: &str) -> String {
    format!("https://www.youtube.com/watch?v={vid}")
}

/// Append the configured YouTube `--extractor-args` (the player-client override) to a yt-dlp command
/// when one is set. Applied to every command that actually extracts a video (probe + download) so the
/// probe validates exactly what playback fetches. yt-dlp accepts options after the URL, so this can be
/// appended to an already-built command.
fn apply_extractor_args(cmd: &mut Command, cfg: &Config) {
    if let Some(ea) = &cfg.ytdlp_extractor_args {
        cmd.args(["--extractor-args", ea]);
    }
}

/// Does yt-dlp think this id is extractable HERE (right region, decodable formats)? Fast:
/// Outcome of probing a candidate without downloading: whether yt-dlp can extract it here, and (when
/// known) whether it's landscape. So the resolver can prefer a landscape trailer over a portrait one —
/// a portrait trailer plays as a tall sliver on the landscape billboard. Unknown dimensions default to
/// landscape, so we never skip a good trailer over missing metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    Unplayable,
    Playable { landscape: bool },
}

/// `--print` the selected format's dimensions (implies `--simulate`, so no download) — this both
/// validates extractability (exit 0 ⇔ the format resolves here, same as the old `--simulate`) and
/// yields orientation. Any spawn/exec error, non-zero exit, or timeout → `Unplayable`; exit 0 with
/// unparsable/missing dims → `Playable { landscape: true }` (don't penalise a good trailer).
pub async fn probe(cfg: &Config, vid: &str) -> Probe {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--no-warnings",
        "--socket-timeout",
        "15",
        "--cache-dir",
        &cache,
        "-f",
        &cfg.ytdlp_format,
        "--print",
        "%(width)s %(height)s",
        &watch_url(vid),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped()) // capture, never swallow — a silent probe error hid a total-outage regression
    .kill_on_drop(true); // cancelled/timed-out probe kills its yt-dlp too
    apply_extractor_args(&mut cmd, cfg);
    // Timeout backstop: on elapse the output future is dropped → kill_on_drop reaps.
    match tokio::time::timeout(Duration::from_secs(PROBE_TIMEOUT_SECS), cmd.output()).await {
        Ok(Ok(o)) if o.status.success() => {
            Probe::Playable { landscape: parse_landscape(&String::from_utf8_lossy(&o.stdout)) }
        }
        Ok(Ok(o)) => {
            // Non-zero exit. Surface WHY (was swallowed), then fall back to the proven plain
            // `--simulate` extractability gate: if yt-dlp can still extract the title, serve it (unknown
            // orientation → landscape) rather than dropping a good trailer over a `--print`/format quirk.
            // Only a genuine failure (geo-block, removed, bot-check) fails both → Unplayable.
            eprintln!("probe {vid}: --print exit {:?} — {}", o.status.code(), stderr_tail(&o.stderr));
            if probe_extractable(cfg, vid).await {
                eprintln!("probe {vid}: extractable via --simulate → serving (orientation unknown → landscape)");
                Probe::Playable { landscape: true }
            } else {
                Probe::Unplayable
            }
        }
        Ok(Err(e)) => {
            eprintln!("probe {vid}: yt-dlp spawn error — {e}");
            Probe::Unplayable
        }
        Err(_) => {
            eprintln!("probe {vid}: timed out after {PROBE_TIMEOUT_SECS}s");
            Probe::Unplayable
        }
    }
}

/// The proven extractability gate (pre-0.3.2): `--simulate`, exit 0 ⇔ yt-dlp can extract the selected
/// format here. Used as the safety net when the richer `--print` probe fails, so a `--print`/dimension
/// quirk can't drop an otherwise-playable trailer.
async fn probe_extractable(cfg: &Config, vid: &str) -> bool {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--simulate",
        "--no-warnings",
        "--socket-timeout",
        "15",
        "--cache-dir",
        &cache,
        "-f",
        &cfg.ytdlp_format,
        &watch_url(vid),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .kill_on_drop(true);
    apply_extractor_args(&mut cmd, cfg);
    matches!(
        tokio::time::timeout(Duration::from_secs(PROBE_TIMEOUT_SECS), cmd.status()).await,
        Ok(Ok(s)) if s.success()
    )
}

/// YouTube-search fallback: `yt-dlp "ytsearchN:<query>"` → up to `n` video ids (flat, no per-video
/// extraction, no download). Used when TMDB/KinoCheck carry no trailer for a title; the ids are then
/// probed like any other candidate. Empty on any error (logged, never swallowed).
pub async fn search(cfg: &Config, query: &str, n: usize) -> Vec<String> {
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--no-warnings",
        "--flat-playlist",
        "--socket-timeout",
        "15",
        "--print",
        "id",
        &format!("ytsearch{n}:{query}"),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    match tokio::time::timeout(Duration::from_secs(PROBE_TIMEOUT_SECS), cmd.output()).await {
        Ok(Ok(o)) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect(),
        Ok(Ok(o)) => {
            eprintln!("search {query:?}: yt-dlp exit {:?} — {}", o.status.code(), stderr_tail(&o.stderr));
            Vec::new()
        }
        Ok(Err(e)) => {
            eprintln!("search {query:?}: yt-dlp spawn error — {e}");
            Vec::new()
        }
        Err(_) => {
            eprintln!("search {query:?}: timed out after {PROBE_TIMEOUT_SECS}s");
            Vec::new()
        }
    }
}

/// Last ~200 chars of stderr on one line, for a compact diagnostic log.
fn stderr_tail(stderr: &[u8]) -> String {
    let s = String::from_utf8_lossy(stderr);
    let tail: String = s.chars().rev().take(200).collect::<Vec<_>>().into_iter().rev().collect();
    tail.replace('\n', " ").trim().to_string()
}

/// Parse yt-dlp's `"W H"` print → is it landscape (`w >= h`)? Missing/unparsable dims → `true`.
pub fn parse_landscape(s: &str) -> bool {
    let mut it = s.split_whitespace();
    match (
        it.next().and_then(|w| w.parse::<u32>().ok()),
        it.next().and_then(|h| h.parse::<u32>().ok()),
    ) {
        (Some(w), Some(h)) => w >= h,
        _ => true,
    }
}

/// Keep only the http(s) lines from a `--get-url` dump (yt-dlp may print a stray blank line). One line
/// = a progressive/muxed format (single input); two = separate DASH video+audio streams (two inputs).
pub fn parse_get_url(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("http://") || l.starts_with("https://"))
        .map(str::to_string)
        .collect()
}

/// EXTRACT-ONLY (no download): resolve the direct googlevideo URL(s) for the selected format, for the
/// HLS stream-remux path. Returns 1 URL (a progressive/muxed format) or 2 (DASH video+audio) — the
/// same `-f` selector `/play` downloads, so we get identical quality. These URLs are IP-locked to THIS
/// server and expire (~6h), so the caller caches them briefly and den-reel (not the device) fetches
/// them. Classified [`PlayError`] on failure, same taxonomy as `download_to`.
pub async fn extract_urls(cfg: &Config, vid: &str) -> Result<Vec<String>, PlayError> {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let mut cmd = Command::new(&cfg.ytdlp);
    cmd.args([
        "-q",
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout",
        "15",
        "--cache-dir",
        &cache, // reuse the nsig/player-JS work
        "-f",
        &cfg.ytdlp_format,
        "--get-url",
        &watch_url(vid),
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    apply_extractor_args(&mut cmd, cfg);
    let out = match tokio::time::timeout(Duration::from_secs(PROBE_TIMEOUT_SECS), cmd.output()).await {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return Err(PlayError::spawn(e)),
        Err(_) => return Err(PlayError::timed_out()),
    };
    let urls = parse_get_url(&String::from_utf8_lossy(&out.stdout));
    if out.status.success() && !urls.is_empty() {
        Ok(urls)
    } else {
        Err(classify(out.status.code(), &String::from_utf8_lossy(&out.stderr)))
    }
}

/// Run yt-dlp+ffmpeg to produce a faststart MP4 at `tmp`. Returns `Ok` iff the process exited 0 and
/// wrote a non-empty file; otherwise a classified [`PlayError`]. Caller owns `tmp`'s lifecycle
/// (rename on success, unlink on failure).
pub async fn download_to(cfg: &Config, vid: &str, tmp: &Path) -> Result<(), PlayError> {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let tmp_s = tmp.to_string_lossy().into_owned();
    let work = async {
        let mut cmd = Command::new(&cfg.ytdlp);
        cmd.args([
            "-q",
            "--no-playlist",
            "--no-warnings",
            "--socket-timeout",
            "15", // yt-dlp aborts stalled sockets itself; the outer timeout is a backstop
            "--cache-dir",
            &cache, // reuse the nsig/player-JS work the probe already did
            "-N",
            "4", // parallel DASH fragments → faster download
            // AVPlayer hardware-decodable: H.264 (avc1) + AAC (mp4a) — same string the probe validates.
            "-f",
            &cfg.ytdlp_format,
            "--merge-output-format",
            "mp4",
            // faststart during the merge's ffmpeg (one pass), not a separate whole-file rewrite.
            "--postprocessor-args",
            "Merger+ffmpeg:-movflags +faststart",
            "-o",
            &tmp_s,
            &watch_url(vid),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true); // don't leave an orphaned yt-dlp/ffmpeg if the task is dropped
        apply_extractor_args(&mut cmd, cfg); // same player-client override the probe validated with
        let mut child = cmd.spawn().map_err(PlayError::spawn)?;

        // Drain stderr concurrently with wait() so a chatty yt-dlp can't deadlock on a full pipe.
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");
        let drain = tokio::spawn(async move {
            let mut buf = String::new();
            let _ = stderr_pipe.read_to_string(&mut buf).await;
            buf
        });
        let status = child.wait().await.map_err(PlayError::spawn)?;
        let stderr = drain.await.unwrap_or_default();

        let wrote = tokio::fs::metadata(tmp).await.map(|m| m.len() > 0).unwrap_or(false);
        if status.success() && wrote {
            Ok(())
        } else {
            Err(classify(status.code(), &stderr))
        }
    };
    // On timeout the work future (owning `child`) is dropped → kill_on_drop reaps yt-dlp/ffmpeg.
    match tokio::time::timeout(Duration::from_secs(DOWNLOAD_TIMEOUT_SECS), work).await {
        Ok(r) => r,
        Err(_) => Err(PlayError::timed_out()),
    }
}
