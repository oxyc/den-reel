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
/// How long to wait for yt-dlp's stderr pipe to close after it exits, before assuming a descendant
/// is holding it open and killing the group. Long enough for a normal exit, short enough that a
/// stuck one costs a download slot for seconds rather than the full download timeout.
const STDERR_DRAIN_GRACE: Duration = Duration::from_secs(5);
/// Long enough to see a writer make progress, short enough to be invisible. Only ever paid when
/// something is still holding stderr after yt-dlp exits.
const WRITE_PROBE: Duration = Duration::from_millis(200);
const _: () = assert!(
    STDERR_DRAIN_GRACE.as_secs() * 4 < DOWNLOAD_TIMEOUT_SECS,
    "the stderr grace must stay well inside the download timeout, or it is unreachable"
);

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
    fn spawn(e: std::io::Error) -> PlayError {
        PlayError {
            status: 502,
            reason: "extraction_failed".into(),
            message: "Could not fetch this trailer.".into(),
            detail: format!("spawn yt-dlp: {e}"),
        }
    }
    /// yt-dlp exited 0 but we do not trust what is on disk — no file, or something was still
    /// writing it when the grace expired. A LOCAL failure, deliberately not `extraction_failed`:
    /// that reason feeds /health's systemic signal, and telling the operator to bump yt-dlp for a
    /// grace kill or a full disk points at the wrong thing entirely.
    fn incomplete(detail: String) -> PlayError {
        PlayError {
            status: 502,
            reason: "incomplete_download".into(),
            message: "Could not fetch this trailer.".into(),
            detail,
        }
    }
    fn timed_out() -> PlayError {
        PlayError {
            status: 504,
            reason: "timeout".into(),
            message: "This trailer took too long to fetch.".into(),
            detail: format!("yt-dlp exceeded {DOWNLOAD_TIMEOUT_SECS}s"),
        }
    }
}

async fn file_len(p: &Path) -> u64 {
    tokio::fs::metadata(p).await.map(|m| m.len()).unwrap_or(0)
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
/// `None` means the search could not be run (spawn error, non-zero exit, timeout) — distinct from
/// `Some(vec![])`, "YouTube has nothing". The caller negative-caches an empty answer, so collapsing
/// the two pinned "this title has no trailer" for an hour every time yt-dlp was broken.
pub async fn search(cfg: &Config, query: &str, n: usize) -> Option<Vec<String>> {
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
            // Same gate the TMDB and KinoCheck candidates get: this is the third source of ids and
            // they all end up as filenames.
            .filter(|l| crate::is_valid_vid(l))
            .collect::<Vec<_>>()
            .into(),
        Ok(Ok(o)) => {
            eprintln!("search {query:?}: yt-dlp exit {:?} — {}", o.status.code(), stderr_tail(&o.stderr));
            None
        }
        Ok(Err(e)) => {
            eprintln!("search {query:?}: yt-dlp spawn error — {e}");
            None
        }
        Err(_) => {
            eprintln!("search {query:?}: timed out after {PROBE_TIMEOUT_SECS}s");
            None
        }
    }
}

/// Last ~200 chars of stderr on one line, for a compact diagnostic log.
pub(crate) fn stderr_tail(stderr: &[u8]) -> String {
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

/// Run yt-dlp+ffmpeg to produce a faststart MP4 at `tmp`. Returns `Ok` iff the process exited 0 and
/// wrote a non-empty file; otherwise a classified [`PlayError`]. Caller owns `tmp`'s lifecycle
/// (rename on success, unlink on failure).
pub async fn download_to(cfg: &Config, vid: &str, tmp: &Path) -> Result<(), PlayError> {
    let cache = cfg.ytdlp_cache.to_string_lossy().into_owned();
    let tmp_s = tmp.to_string_lossy().into_owned();
    // Shared so the guard below can reap the group even when the future is dropped mid-flight.
    let group = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let group_for_work = group.clone();
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
        // Its own process group, so a timeout can reap the whole tree. yt-dlp forks ffmpeg to do
        // the merge, and kill_on_drop signals only the direct child — the ffmpeg survived, kept
        // writing, and could recreate the temp file we had just deleted.
        .process_group(0)
        .kill_on_drop(true);
        apply_extractor_args(&mut cmd, cfg); // same player-client override the probe validated with
        let mut child = cmd.spawn().map_err(PlayError::spawn)?;
        let pgid = child.id();
        group_for_work.store(pgid.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
        register_group(pgid);

        // Drain stderr concurrently with wait() so a chatty yt-dlp can't deadlock on a full pipe.
        // read_to_end, not read_to_string: read_to_string discards the WHOLE buffer on one non-UTF-8
        // byte, so a single accented character in an upstream error message left the log blank and
        // the failure misclassified as a generic extraction error.
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");
        let mut drain = tokio::spawn(async move {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf).await;
            String::from_utf8_lossy(&buf).into_owned()
        });
        let status = child.wait().await.map_err(PlayError::spawn)?;
        // The stderr pipe is inherited by every descendant, so EOF means "the last of them exited",
        // not "yt-dlp exited". Waiting on it unbounded let a lingering descendant pin a download
        // that had already finished — complete file, exit 0 — until the 240s timeout turned it into
        // a 504 whose cleanup deleted the file. Killing the group first fixed that and broke
        // something worse: a descendant still writing the output got SIGKILLed mid-write and a
        // truncated MP4 was published to the cache, and an ERROR line written after exit was lost,
        // misrouting classify() and the /health signal.
        //
        // So: give the pipe a moment to close on its own — which is what stock yt-dlp does, and
        // keeps stderr intact — and only force it if something is genuinely holding on.
        let mut killed_mid_write = false;
        let stderr = match tokio::time::timeout(STDERR_DRAIN_GRACE, &mut drain).await {
            Ok(r) => r.unwrap_or_default(),
            Err(_) => {
                // Something is holding the inherited pipe. Whether that matters depends on what it
                // is doing: a descendant still producing the OUTPUT must not be killed and its
                // half-file published, while one merely holding the pipe is harmless and its
                // download is complete. Sampling the size across a moment answers exactly that,
                // and only costs anything on this abnormal path.
                let before = file_len(tmp).await;
                tokio::time::sleep(WRITE_PROBE).await;
                killed_mid_write = file_len(tmp).await != before;
                eprintln!(
                    "yt-dlp {vid}: stderr still held after exit; killing the group                      (output {})",
                    if killed_mid_write { "still growing" } else { "settled" }
                );
                kill_group(pgid);
                drain.await.unwrap_or_default()
            }
        };

        let wrote = tokio::fs::metadata(tmp).await.map(|m| m.len() > 0).unwrap_or(false);
        if !status.success() {
            return Err(classify(status.code(), &stderr));
        }
        // Exit 0 is yt-dlp's verdict on the EXTRACTION; whether we have a usable file is ours, and
        // `len > 0` was the only thing between a SIGKILLed writer and a truncated MP4 renamed into
        // the cache — where it is served `immutable` for a year and never re-fetched, because a
        // cached file of any size counts as a hit. A forced kill means something was still writing
        // when the grace expired, so the file cannot be trusted: fail, and let the next request
        // fetch it properly. Neither case is an extractor failure, so neither moves /health.
        if killed_mid_write {
            return Err(PlayError::incomplete(format!(
                "output still growing {}s after exit; killed mid-write",
                STDERR_DRAIN_GRACE.as_secs()
            )));
        }
        if !wrote {
            return Err(PlayError::incomplete("yt-dlp exited 0 with no output file".into()));
        }
        Ok(())
    };
    // Dropping the future kills yt-dlp; `guard` kills whatever it forked.
    let guard = GroupGuard(group.clone());
    let out = tokio::time::timeout(Duration::from_secs(DOWNLOAD_TIMEOUT_SECS), work).await;
    drop(guard);
    match out {
        Ok(r) => r,
        Err(_) => Err(PlayError::timed_out()),
    }
}

/// Kills the download's process group on the way out, however the future ended.
struct GroupGuard(std::sync::Arc<std::sync::atomic::AtomicU32>);

impl Drop for GroupGuard {
    fn drop(&mut self) {
        let pgid = Some(self.0.load(std::sync::atomic::Ordering::Relaxed)).filter(|&p| p != 0);
        kill_group(pgid);
        unregister_group(pgid);
    }
}

/// Every live download's process group.
///
/// The guard above only fires when the download future is DROPPED, and on shutdown it is not:
/// `in_flight` holds a `Shared` clone of a future that itself captures the `Arc<AppState>` the map
/// lives in, so the cycle keeps the child alive past runtime teardown. Registering the groups gives
/// shutdown something it can act on directly, without depending on drop order.
static LIVE_GROUPS: std::sync::Mutex<Option<std::collections::HashSet<u32>>> =
    std::sync::Mutex::new(None);

pub(crate) fn register_group(pgid: Option<u32>) {
    if let Some(p) = pgid.filter(|&p| p > 1 && p <= i32::MAX as u32) {
        let mut g = LIVE_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
        g.get_or_insert_with(Default::default).insert(p);
    }
}

pub(crate) fn unregister_group(pgid: Option<u32>) {
    if let Some(p) = pgid.filter(|&p| p != 0) {
        let mut g = LIVE_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(set) = g.as_mut() {
            set.remove(&p);
        }
    }
}

/// Run `cmd` in its own process group, registered as live for the duration, so shutdown can kill
/// the whole tree. `Command::output()` owns the child, so `kill_on_drop` covers a timeout or a
/// cancelled task — but not the process exiting out from under it, which is what shutdown is.
pub(crate) async fn output_in_group(cmd: &mut Command) -> std::io::Result<std::process::Output> {
    #[cfg(unix)]
    cmd.process_group(0);
    let child = cmd.spawn()?;
    let pgid = child.id();
    register_group(pgid);
    // A guard, not statements after the await: the await is a cancellation point, and the routine
    // way to reach it is a client hanging up mid-/crop, which main.rs correctly treats as normal.
    // Cleaning up only on the happy path left the pgid registered forever — nothing else prunes it
    // — so shutdown signalled a group that had been dead for hours, and pid reuse makes that
    // somebody else's group. Same reason download_to uses GroupGuard.
    let _entry = GroupEntry(pgid);
    child.wait_with_output().await
}

/// Kills the group and deregisters it however the future ended — return, error, or cancellation.
struct GroupEntry(Option<u32>);

impl Drop for GroupEntry {
    fn drop(&mut self) {
        kill_group(self.0); // whatever it forked
        unregister_group(self.0);
    }
}

/// Is this group still registered as live? Tests only — the registry is process-wide, so asserting
/// on its size races other tests' subprocesses; asking about one id does not.
#[cfg(test)]
pub(crate) fn is_group_live(pgid: u32) -> bool {
    let g = LIVE_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
    g.as_ref().is_some_and(|set| set.contains(&pgid))
}

/// SIGKILL every download still running. Returns how many groups it signalled.
pub(crate) fn kill_live_groups() -> usize {
    let taken = {
        let mut g = LIVE_GROUPS.lock().unwrap_or_else(|e| e.into_inner());
        g.take().unwrap_or_default()
    };
    for p in &taken {
        kill_group(Some(*p));
    }
    taken.len()
}

#[cfg(unix)]
pub(crate) fn kill_group(pgid: Option<u32>) {
    // A pgid above i32::MAX negates into a POSITIVE number — i.e. a single unrelated pid, not a
    // group. `-(u32::MAX - 7) as i32` is 8. Nothing should produce such a value, which is exactly
    // why it must not be a SIGKILL aimed at whatever pid 8 happens to be.
    // 0 is our own group and 1 makes kill(-1) "every process we may signal" — the whole container.
    // Neither is reachable from child.id(), which is exactly why neither should be a live SIGKILL.
    if let Some(pgid) = pgid.filter(|&p| p > 1 && p <= i32::MAX as u32) {
        // Negative pid = the whole group. Already-exited is ESRCH, which we don't care about.
        unsafe { libc::kill(-(pgid as i32), libc::SIGKILL) };
    }
}

#[cfg(not(unix))]
pub(crate) fn kill_group(_pgid: Option<u32>) {}
