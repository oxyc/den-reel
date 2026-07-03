//! Shared application state and the injectable seams (prober / prewarm / clock / upstream) that let
//! the tests run the addon and serve paths with no network and no yt-dlp binary — the Rust
//! equivalent of the Node service's `_setProber` / `_setPrewarm` / `_setClock` hooks.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::future::Shared;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::seal::Keyring;
use crate::upstream::{HttpUpstream, Upstream};
use crate::ytdlp::{self, PlayError};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ProbeFn = Box<dyn Fn(String) -> BoxFuture<bool> + Send + Sync>;
pub type PrewarmFn = Box<dyn Fn(Arc<AppState>, String) + Send + Sync>;
pub type ClockFn = Box<dyn Fn() -> u64 + Send + Sync>;
/// One in-flight download shared across every waiter for the same id (de-dupe).
pub type SharedDownload = Shared<BoxFuture<Result<PathBuf, PlayError>>>;

/// A resolved (or negatively-cached) ytId with its expiry, in ms since epoch.
pub struct YtEntry {
    pub id: String,
    pub exp: u64,
}

pub struct AppState {
    pub cfg: Arc<Config>,
    /// Decrypts a sealed config path segment (den-scout/docs/SEALED-CONFIG.md). `None` = sealed URLs
    /// disabled (legacy plaintext still works); the current key's public half is served at `/config-key`.
    pub config_keyring: Option<Keyring>,
    /// Cache the STABLE ytId (the expensive lookup); playback is just our /play proxy for it.
    /// In-memory (24h TTL) — cheap to rebuild on restart, no external store needed. "" = "no trailer".
    pub yt_cache: Mutex<HashMap<String, YtEntry>>,
    /// vid -> (generation, shared download future), so concurrent /play (and prewarm) share one
    /// yt-dlp run. The generation lets the creator clear its own entry without clobbering a newer one.
    pub in_flight: Mutex<HashMap<String, (u64, SharedDownload)>>,
    /// Monotonic id handed to each created download (map tag + unique temp-file suffix).
    pub dl_gen: AtomicU64,
    /// vid -> detected content rectangle (from ffmpeg cropdetect), so /crop is computed once.
    pub crop_cache: Mutex<HashMap<String, crate::crop::CropReport>>,
    pub upstream: Box<dyn Upstream>,
    pub prober: ProbeFn,
    pub prewarm: PrewarmFn,
    pub clock: ClockFn,
    /// Global caps on concurrent subprocess trees, so a burst of distinct ids can't fork-bomb the
    /// box: downloads (yt-dlp+ffmpeg) and probes (yt-dlp --simulate).
    pub download_sem: Arc<Semaphore>,
    pub probe_sem: Arc<Semaphore>,
}

impl AppState {
    /// Production state: real HTTP upstream, real yt-dlp prober, real fetch-on-prewarm.
    pub fn new(cfg: Config) -> Arc<AppState> {
        let cfg = Arc::new(cfg);
        // rustls client with a modest timeout so a wedged upstream can't pin a request forever.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("reqwest client");
        let upstream = Box::new(HttpUpstream::new(cfg.clone(), http));
        let probe_sem = Arc::new(Semaphore::new(crate::PROBE_CONCURRENCY));
        // A malformed key disables sealed URLs (legacy plaintext keeps working) rather than crashing.
        let config_keyring = match Keyring::from_env(&cfg.config_key, &cfg.config_keys_prev) {
            Ok(kr) => kr,
            Err(e) => {
                eprintln!("warning: REEL_CONFIG_KEY invalid ({e}) — sealed configs disabled");
                None
            }
        };
        Arc::new(AppState {
            cfg: cfg.clone(),
            config_keyring,
            yt_cache: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            dl_gen: AtomicU64::new(0),
            crop_cache: Mutex::new(HashMap::new()),
            upstream,
            prober: default_prober(cfg, probe_sem.clone()),
            prewarm: default_prewarm(),
            clock: Box::new(default_clock),
            download_sem: Arc::new(Semaphore::new(crate::DOWNLOAD_CONCURRENCY)),
            probe_sem,
        })
    }
}

/// Real prober: ask yt-dlp (simulate) whether the id is extractable here, holding a probe permit so
/// a `/meta` fan-out (and concurrent `/meta`s) can't spawn unbounded yt-dlp processes.
pub fn default_prober(cfg: Arc<Config>, sem: Arc<Semaphore>) -> ProbeFn {
    Box::new(move |vid: String| {
        let cfg = cfg.clone();
        let sem = sem.clone();
        Box::pin(async move {
            let _permit = sem.acquire().await;
            ytdlp::probe_extractable(&cfg, &vid).await
        })
    })
}

/// Real prewarm: fire-and-forget a download so the following /play is warm. Bounded by in-flight
/// size to survive a /meta burst; the later real /play de-dupes onto the same download.
pub fn default_prewarm() -> PrewarmFn {
    Box::new(|state: Arc<AppState>, id: String| {
        if id.is_empty() {
            return;
        }
        let busy = state.in_flight.lock().unwrap_or_else(|e| e.into_inner()).len();
        if busy < crate::PREWARM_MAX {
            tokio::spawn(async move {
                let _ = crate::play::fetch_trailer(state, id).await;
            });
        }
    })
}

pub fn default_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
