//! A resident yt-dlp, so a resolve costs the extraction and not the interpreter.
//!
//! Spawning the standalone binary spends about 840ms before it has looked at anything — Python
//! starting and yt-dlp importing itself — measured on the box against a 2162ms resolve. That is two
//! fifths of every one, and it is paid again for every trailer. This keeps one process alive and
//! talks to it over a pipe: one JSON object per line each way (`worker/resolve.py`).
//!
//! **Everything here is optional.** No worker configured, one that will not start, one that dies
//! mid-answer, one that takes too long — every path returns `None`, and the caller spawns the binary
//! exactly as it always did. That is the point: the fast path may fail in any way it likes and the
//! service only gets slower, never broken.
//!
//! **One at a time, deliberately.** A resolve is about a second and a half of waiting on YouTube, and
//! the requests that arrive together are usually for the SAME video — which `direct::answer` already
//! collapses into one. What is left is a handful of distinct ids a browse kicks off, and serialising
//! those behind one interpreter is a better trade than supervising a pool. If that ever stops being
//! true, this is where a pool would go.
//!
//! **It does not stay forever.** The box has a 1 GiB cap for this container and a resident Python is
//! ~40 MB of it, so a worker that has not been asked anything in `IDLE_TIMEOUT` is shut down and the
//! next resolve starts a new one, paying the 840ms once more. Trailers are bursty — a browse, then
//! nothing for hours — and that is exactly the shape this suits.

use std::process::Stdio;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::config::Config;

/// How long one answer may take before the worker is presumed wedged. Generously past a real resolve
/// (~1.3s once warm) and inside `direct`'s own patience, so a stuck worker costs one slow request and
/// then goes away rather than holding the lock for everyone behind it.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a worker may sit unasked before it is shut down.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// What the worker answers with; the caller turns it into a `Direct`.
#[derive(Debug, Deserialize)]
pub struct Answer {
    pub width: Option<u32>,
    pub height: Option<u32>,
    #[serde(default)]
    pub urls: Vec<String>,
    pub hls: Option<String>,
}

/// The wire reply: an answer, or yt-dlp's own words about why there isn't one.
#[derive(Deserialize)]
struct Reply {
    ok: bool,
    error: Option<String>,
    #[serde(flatten)]
    answer: Option<Answer>,
}

struct Resident {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    /// When it last answered, so `reap_idle` can tell whether anyone still wants it.
    used: Instant,
}

/// The resident process, when there is one.
#[derive(Default)]
pub struct Worker(Mutex<Option<Resident>>);

impl Worker {
    /// Resolve `vid` through the resident process.
    ///
    /// `None` means "this path is not available" — not configured, could not start, died, or took
    /// too long — and the caller should spawn the binary. `Some(Err)` is yt-dlp's own failure for
    /// this video, which the caller classifies exactly as it classifies stderr from the binary.
    pub async fn resolve(&self, cfg: &Config, vid: &str, format: &str) -> Option<Result<Answer, String>> {
        let path = cfg.ytdlp_worker.as_deref()?;
        let mut held = self.0.lock().await;
        if held.is_none() {
            *held = start(cfg, path);
        }
        let resident = held.as_mut()?;

        let request = serde_json::json!({
            "id": vid,
            "format": format,
            "cache": cfg.ytdlp_cache.to_string_lossy(),
            "extractor_args": cfg.ytdlp_extractor_args,
        });
        match tokio::time::timeout(ANSWER_TIMEOUT, exchange(resident, &request.to_string())).await {
            Ok(Ok(outcome)) => {
                resident.used = Instant::now();
                Some(outcome)
            }
            // The pipe broke, or it answered with something unreadable: it is not a worker any more.
            Ok(Err(why)) => {
                crate::log_limited("worker lost", || format!("resident yt-dlp: {why}"));
                stop(held.take());
                None
            }
            Err(_) => {
                eprintln!("resident yt-dlp: no answer for {vid} in {ANSWER_TIMEOUT:?}; restarting it");
                stop(held.take());
                None
            }
        }
    }

    /// Shut the worker down if nothing has asked it anything for `IDLE_TIMEOUT`.
    pub async fn reap_idle(&self) {
        let mut held = self.0.lock().await;
        if held.as_ref().is_some_and(|r| r.used.elapsed() >= IDLE_TIMEOUT) {
            eprintln!("resident yt-dlp: idle for {IDLE_TIMEOUT:?}; shutting it down");
            stop(held.take());
        }
    }

    /// Shut it down on the way out, so a redeploy does not leave an interpreter behind.
    pub async fn shutdown(&self) {
        stop(self.0.lock().await.take());
    }
}

/// Start one, or `None` (said once) if it cannot be started — the caller then uses the binary.
fn start(cfg: &Config, path: &str) -> Option<Resident> {
    let mut child = Command::new(&cfg.python)
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Inherited, not piped: a traceback belongs in the log, and nothing here drains a third pipe.
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| {
            crate::log_limited("worker spawn", || {
                format!("resident yt-dlp: cannot start {} {path}: {e}", cfg.python)
            })
        })
        .ok()?;
    let stdin = child.stdin.take()?;
    let stdout = BufReader::new(child.stdout.take()?);
    Some(Resident { child, stdin, stdout, used: Instant::now() })
}

/// One reply line as the caller wants it: an answer, or what yt-dlp said about this video. The OUTER
/// error is different in kind — the line itself was unreadable, which means whatever is on the far
/// end has stopped being a worker and should be replaced rather than believed.
pub(crate) fn read_reply(line: &str) -> Result<Result<Answer, String>, String> {
    let reply: Reply = serde_json::from_str(line).map_err(|e| format!("unreadable reply: {e}"))?;
    Ok(match (reply.ok, reply.answer, reply.error) {
        (true, Some(answer), _) => Ok(answer),
        (_, _, said) => Err(said.unwrap_or_else(|| "no answer".into())),
    })
}

/// One request in, one reply out. Any I/O failure means the process is gone as far as we care.
async fn exchange(resident: &mut Resident, request: &str) -> Result<Result<Answer, String>, String> {
    resident.stdin.write_all(request.as_bytes()).await.map_err(|e| e.to_string())?;
    resident.stdin.write_all(b"\n").await.map_err(|e| e.to_string())?;
    resident.stdin.flush().await.map_err(|e| e.to_string())?;
    let mut line = String::new();
    match resident.stdout.read_line(&mut line).await {
        Ok(0) => Err("it closed its output".into()),
        Ok(_) => read_reply(&line),
        Err(e) => Err(e.to_string()),
    }
}

/// Kill it and reap it. `kill_on_drop` covers the drop, but asking first means the process is gone
/// before the next resolve tries to start its replacement.
fn stop(resident: Option<Resident>) {
    if let Some(mut resident) = resident {
        let _ = resident.child.start_kill();
    }
}
