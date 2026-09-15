//! Shared application state and the injectable seams (prober / prewarm / clock / upstream) that let
//! the tests run the addon and serve paths with no network and no yt-dlp binary — the Rust
//! equivalent of the Node service's `_setProber` / `_setPrewarm` / `_setClock` hooks.

use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::future::Shared;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::seal::Keyring;
use crate::upstream::{HttpUpstream, Upstream};
use crate::ytdlp::{self, PlayError};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type ProbeFn = Box<dyn Fn(String) -> BoxFuture<crate::ytdlp::Probe> + Send + Sync>;
/// YouTube-search fallback: query → candidate video ids. Injectable so tests stay hermetic.
pub type SearchFn = Box<dyn Fn(String) -> BoxFuture<Option<Vec<String>>> + Send + Sync>;
pub type PrewarmFn = Box<dyn Fn(Arc<AppState>, String) + Send + Sync>;
/// A direct warm-up: the id, the height cap the request that follows will ask for, and whether to build
/// `/progressive`'s index too (`Some`, with its sound when `true`).
pub type DirectWarmFn = Box<dyn Fn(Arc<AppState>, String, Option<u32>, Option<bool>) + Send + Sync>;
pub type ClockFn = Box<dyn Fn() -> u64 + Send + Sync>;
/// One in-flight download shared across every waiter for the same id (de-dupe).
pub type SharedDownload = Shared<BoxFuture<Result<crate::play::Fetched, PlayError>>>;
/// The same, for a direct resolve: `/meta` warms one and the page asks for it a moment later, and
/// without this they each spawn their own yt-dlp for the same video.
pub type SharedResolve = Shared<BoxFuture<Result<crate::direct::Direct, PlayError>>>;

/// Resolved (or negatively-cached) trailer ytIds — best-playable first, then unprobed alternates the
/// client falls back to on a playback failure. Empty = "no trailer". `exp` is ms since epoch.
///
/// Serializable so the map survives a redeploy (`load_resolve_cache` / `save_resolve_cache`). Both
/// timestamps are epoch-milliseconds, so they still mean something in the next process.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct YtEntry {
    pub ids: Vec<String>,
    pub exp: u64,
    /// When these ids were last actually confirmed by an upstream that answered — not when the
    /// entry was last written. A failed lookup re-serves the last known answer and rewrites `exp`
    /// to the retry cooldown, so `exp` alone cannot say how old the ANSWER is, and a trailer that
    /// was removed upstream would be served for as long as anything kept faulting.
    pub confirmed: u64,
}

pub struct AppState {
    pub cfg: Arc<Config>,
    /// Decrypts a sealed config path segment (den-scout/docs/SEALED-CONFIG.md). `None` = sealed URLs
    /// disabled (legacy plaintext still works); the current key's public half is served at `/config-key`.
    pub config_keyring: Option<Keyring>,
    /// Cache the STABLE ytId (the expensive lookup); playback is just our /play proxy for it.
    /// In-memory (24h TTL), parked to `cfg.resolve_cache` at shutdown and read back at boot so a
    /// redeploy does not make the next browse re-ask TMDB for every title on screen. Empty ids =
    /// "no trailer".
    pub yt_cache: Mutex<HashMap<String, YtEntry>>,
    /// The same title's imdb and tmdb ids, mapped both ways, as clients tell us them. A client browsing
    /// from TMDB usually holds both already, and saying so here is worth more than it looks: KinoCheck
    /// takes either id, TMDB wants its own, and a search or an Apple lookup wants neither — so knowing
    /// the pair means whichever id arrives can reach every source without a lookup to convert it. It
    /// also lets one title hold ONE resolve entry rather than one per id form.
    ///
    /// In memory only. A pair never changes, so it would park happily enough, but re-learning one costs
    /// nothing when the client is carrying both anyway.
    pub ids: Mutex<HashMap<String, String>>,
    /// vid -> (generation, shared download future), so concurrent /play (and prewarm) share one
    /// yt-dlp run. The generation lets the creator clear its own entry without clobbering a newer one.
    pub in_flight: Mutex<HashMap<String, (u64, SharedDownload)>>,
    /// Monotonic id handed to each created download (map tag + unique temp-file suffix).
    pub dl_gen: AtomicU64,
    /// vid -> detected content rectangle (from ffmpeg cropdetect), so /crop is computed once.
    pub crop_cache: Mutex<HashMap<String, crate::crop::CropReport>>,
    /// vid -> when to run cropdetect again after it produced nothing parsable. Only the SUCCESSFUL
    /// side was cached above, and the comment where the pass is spawned says what that costs: a
    /// whole-file ffmpeg read on every call, forever, for exactly the trailers it cannot read.
    pub crop_unknown: Mutex<HashMap<String, u64>>,
    /// vids whose letterbox is being measured from keyframes right now, so a second ask does not start another.
    pub crop_inflight: Mutex<std::collections::HashSet<String>>,
    /// vid -> (why it failed, when to try again). A `/play` verdict was the one thing this service
    /// learned and then threw away: the in-flight entry is cleared however a download ends, so the
    /// next request for a video YouTube has REMOVED spent another of three download permits, and
    /// another yt-dlp process, discovering the same thing. The TTL is reason-aware
    /// (`play::fail_ttl_ms`) — "removed" is a fact, "timed out" is a mood.
    pub play_fails: Mutex<HashMap<String, (PlayError, u64)>>,
    /// vid -> (the direct googlevideo URLs, or why they could not be resolved; when to ask again).
    /// Unlike every other cache here the TTL is not ours to choose: the URLs carry their own expiry,
    /// so an entry stands until shortly before they stop working (`direct::ttl_ms`).
    pub direct_cache: Mutex<HashMap<String, crate::direct::CachedDirect>>,
    /// vid -> the resolve already running for it. The probe budget is six, so a warm-up and the
    /// request chasing it never queued behind one another — they simply both ran.
    pub direct_inflight: Mutex<HashMap<String, SharedResolve>>,
    /// vid (and height step) -> the `/progressive` index built for that stream's URL, finished or
    /// still building, kept until the URL is close to expiring.
    pub progressive: Mutex<HashMap<String, crate::progressive::Entry>>,
    /// The resident yt-dlp, when one is configured and running (`worker.rs`). Every failure of it
    /// falls back to spawning the binary, so this is only ever an optimisation.
    pub worker: crate::worker::Worker,
    pub upstream: Box<dyn Upstream>,
    /// The same HTTP client the upstream lookups use, for the HLS proxy's own fetches (`hls.rs`).
    /// Shared rather than a second one: a client is a connection pool, and googlevideo and TMDB are
    /// both plain https with the same TLS stack behind them.
    pub http: reqwest::Client,
    pub prober: ProbeFn,
    /// YouTube-search fallback (fires only when TMDB/KinoCheck carry no trailer).
    pub searcher: SearchFn,
    pub prewarm: PrewarmFn,
    /// Resolve the direct URLs ahead of the request, as `prewarm` does the download. Injectable for
    /// the same reason it is: a test has to be able to keep yt-dlp out of the `/meta` path.
    pub direct_warm: DirectWarmFn,
    pub clock: ClockFn,
    /// Global caps on concurrent subprocess trees, so a burst of distinct ids can't fork-bomb the
    /// box: downloads (yt-dlp+ffmpeg) and probes (yt-dlp --simulate).
    pub download_sem: Arc<Semaphore>,
    /// Real cap on speculative downloads. Counting `in_flight` instead was check-then-act: the
    /// entry is only inserted after `fetch_trailer`'s first await, so a burst of /meta all read the
    /// same stale count and all spawned.
    pub prewarm_sem: Arc<Semaphore>,
    pub probe_sem: Arc<Semaphore>,
    /// Consecutive resolves that had real trailer candidates but yt-dlp could extract **none** of them
    /// — the signature of a systemic extraction outage (YouTube BotGuard / a broken nsig-JS runtime),
    /// which is otherwise invisible to /health (upstream TMDB/KinoCheck still answer fine). Reset to 0
    /// on any resolve that DOES yield a playable trailer, so only a real run of failures accumulates.
    /// Surfaced as `degraded: extractor_unavailable` past the threshold (ADDON-02).
    /// What the cache volume held when eviction last walked it, for `/metrics`. Published from the
    /// pass that already does the walk — after each download and once an hour — so an operations
    /// endpoint never turns into a directory scan on the request path. `cache_measured_at` is 0
    /// until the first pass has run.
    pub cache_trailer_bytes: AtomicU64,
    pub cache_trailer_count: AtomicU64,
    pub cache_scratch_bytes: AtomicU64,
    pub cache_measured_at: AtomicU64,
    pub extract_fails: AtomicU32,
    /// Consecutive downloads that failed for a LOCAL reason — exit 0 with no file, a bake killed
    /// mid-rewrite. Separate from `extract_fails` because the fix is different: nothing about
    /// yt-dlp or the player clients will help. Without it these were invisible, and an instance
    /// failing every single download reported `ok`.
    pub local_fails: AtomicU32,
    /// Paused while YouTube is throttling this box. Every path that would start an extraction —
    /// download, direct resolve, search — asks it first; a throttled extraction trips it, and one
    /// that works clears it. Surfaced as `degraded: youtube_throttled`.
    pub youtube: crate::backoff::Backoff,
    /// The /health reason last logged (`None` = ok), so `note_health` writes a line only when the
    /// verdict changes.
    pub health_logged: Mutex<Option<&'static str>>,
}

impl AppState {
    /// Production state: real HTTP upstream, real yt-dlp prober, real fetch-on-prewarm.
    pub fn new(cfg: Config) -> Arc<AppState> {
        let cfg = Arc::new(cfg);
        // rustls client with a modest timeout so a wedged upstream can't pin a request forever.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            // Named, so an upstream operator reading their logs can tell who is calling.
            .user_agent(concat!("den-reel/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client");
        let upstream = Box::new(HttpUpstream::new(cfg.clone(), http.clone()));
        let probe_sem = Arc::new(Semaphore::new(crate::PROBE_CONCURRENCY));
        let prewarm_sem = Arc::new(Semaphore::new(crate::PREWARM_MAX));
        // A malformed key disables sealed URLs (legacy plaintext keeps working) rather than crashing.
        let config_keyring = match Keyring::from_env(&cfg.config_key, &cfg.config_keys_prev) {
            Ok(kr) => kr,
            Err(e) => {
                eprintln!("warning: CONFIG_KEY invalid ({e}) — sealed configs disabled");
                None
            }
        };
        Arc::new(AppState {
            cfg: cfg.clone(),
            config_keyring,
            yt_cache: Mutex::new(HashMap::new()),
            ids: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            dl_gen: AtomicU64::new(0),
            crop_cache: Mutex::new(HashMap::new()),
            crop_unknown: Mutex::new(HashMap::new()),
            crop_inflight: Mutex::new(Default::default()),
            play_fails: Mutex::new(HashMap::new()),
            direct_cache: Mutex::new(HashMap::new()),
            direct_inflight: Mutex::new(HashMap::new()),
            progressive: Mutex::new(HashMap::new()),
            worker: Default::default(),
            upstream,
            http,
            prober: default_prober(cfg.clone(), probe_sem.clone()),
            searcher: default_searcher(cfg, probe_sem.clone()),
            prewarm: default_prewarm(),
            direct_warm: default_direct_warm(),
            clock: Box::new(default_clock),
            download_sem: Arc::new(Semaphore::new(crate::DOWNLOAD_CONCURRENCY)),
            prewarm_sem,
            probe_sem,
            cache_trailer_bytes: AtomicU64::new(0),
            cache_trailer_count: AtomicU64::new(0),
            cache_scratch_bytes: AtomicU64::new(0),
            cache_measured_at: AtomicU64::new(0),
            extract_fails: AtomicU32::new(0),
            local_fails: AtomicU32::new(0),
            youtube: youtube_backoff(),
            health_logged: Mutex::new(None),
        })
    }

    /// Publish what the eviction pass just measured, so `/metrics` can read it instead of walking the
    /// volume. Relaxed throughout: these four are a report, not a decision anything is made on.
    pub fn record_cache_usage(&self, u: crate::play::CacheUsage) {
        use std::sync::atomic::Ordering::Relaxed;
        self.cache_trailer_bytes.store(u.trailer_bytes, Relaxed);
        self.cache_trailer_count.store(u.trailer_count, Relaxed);
        self.cache_scratch_bytes.store(u.scratch_bytes, Relaxed);
        self.cache_measured_at.store((self.clock)(), Relaxed);
    }

    /// What /health decides on: whether any install can supply a discovery key, the three
    /// consecutive-failure counters, and what is left of a YouTube throttle pause.
    pub fn health_inputs(&self) -> (bool, u32, u32, u32, Option<u64>) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.cfg.tmdb_key.is_some() || self.config_keyring.is_some(),
            self.upstream.recent_failures(),
            self.extract_fails.load(Relaxed),
            self.local_fails.load(Relaxed),
            self.youtube.remaining_ms((self.clock)()),
        )
    }

    /// Log the /health verdict when it changes — once on the way into `degraded`, with its reason,
    /// and once on the way back — instead of once per failure behind it. Called where the counters
    /// it reads move (after a resolve's upstream calls, after a download) and once at boot; nothing
    /// polls. Returns the line it wrote, so a test can see it.
    pub fn note_health(&self) -> Option<String> {
        let (key, upstream, extract, local, paused) = self.health_inputs();
        let verdict = crate::health_verdict(key, upstream, extract, local, paused.is_some());
        let reason = verdict.map(|(reason, _)| reason);
        {
            let mut last = self.health_logged.lock().unwrap_or_else(|e| e.into_inner());
            if *last == reason {
                return None;
            }
            *last = reason;
        }
        let line = match verdict {
            Some((reason, detail)) => format!("health: degraded ({reason}) — {detail}"),
            None => "health: ok".to_string(),
        };
        eprintln!("{line}");
        Some(line)
    }
}

/// The YouTube throttle pause, with its production window.
pub fn youtube_backoff() -> crate::backoff::Backoff {
    crate::backoff::Backoff::new("youtube", crate::YOUTUBE_PAUSE_BASE_MS, crate::YOUTUBE_PAUSE_CAP_MS)
}

/// Real prober: ask yt-dlp whether the id is extractable here and (for the resolver's landscape
/// preference) its orientation, holding a probe permit so a `/meta` fan-out (and concurrent `/meta`s)
/// can't spawn unbounded yt-dlp processes.
pub fn default_prober(cfg: Arc<Config>, sem: Arc<Semaphore>) -> ProbeFn {
    Box::new(move |vid: String| {
        let cfg = cfg.clone();
        let sem = sem.clone();
        Box::pin(async move {
            let _permit = sem.acquire().await;
            ytdlp::probe(&cfg, &vid).await
        })
    })
}

/// Real searcher: `yt-dlp ytsearch` for a query, holding a probe permit (searches are as heavy as
/// probes) so a fallback can't spawn unbounded yt-dlp processes.
pub fn default_searcher(cfg: Arc<Config>, sem: Arc<Semaphore>) -> SearchFn {
    Box::new(move |query: String| {
        let cfg = cfg.clone();
        let sem = sem.clone();
        Box::pin(async move {
            let _permit = sem.acquire().await;
            ytdlp::search(&cfg, &query, crate::SEARCH_MAX).await
        })
    })
}

/// Real prewarm: fire-and-forget a download so the following /play is warm. The permit is held for
/// the whole task, so a browse burst cannot queue speculative downloads ahead of the /play the
/// viewer is actually waiting for.
pub fn default_prewarm() -> PrewarmFn {
    Box::new(|state: Arc<AppState>, id: String| {
        if id.is_empty() {
            return;
        }
        // Already downloading this one? Then there is nothing to prewarm, and taking a permit for
        // it would spend the cap on a duplicate: three repeat /meta calls for one title used to
        // exhaust it and lock every other title out until that download finished.
        if state.in_flight.lock().unwrap_or_else(|e| e.into_inner()).contains_key(&id) {
            return;
        }
        let Ok(permit) = state.prewarm_sem.clone().try_acquire_owned() else {
            return; // already prewarming our fill; the real /play will fetch it if it is wanted
        };
        tokio::spawn(async move {
            let _permit = permit;
            let _ = crate::play::fetch_trailer(state, id).await;
        });
    })
}

/// Real direct warm-up: resolve into the cache so the `/direct` that follows is a hash lookup.
pub fn default_direct_warm() -> DirectWarmFn {
    Box::new(|state: Arc<AppState>, id: String, cap: Option<u32>, index: Option<bool>| {
        crate::direct::warm(state, id, cap, index)
    })
}

/// Read the resolve cache left by the previous process, dropping whatever no longer holds.
///
/// The two kinds of entry are validated by DIFFERENT clocks, and getting that wrong is worse than
/// not persisting at all. A populated entry means "an upstream vouched for these ids at
/// `confirmed`", and `exp` cannot speak for it: the stale-substitution path rewrites `exp` to a
/// retry cooldown while leaving the ids and `confirmed` alone, so a perfectly good 24h answer can be
/// carrying a 60-second expiry. An EMPTY entry is the opposite — `confirmed` is set to `now` on
/// every write including the failures, so honouring it there would promote a 60-second
/// `YT_FAIL_TTL_MS` cooldown into a 24-hour "this title has no trailer", which is precisely what the
/// `YT_FAIL_TTL_MS < YT_NEG_TTL_MS` assertion exists to prevent.
///
/// So: populated entries live by `confirmed`, empty ones by their own `exp`.
pub fn load_resolve_cache(cfg: &Config, now: u64) -> HashMap<String, YtEntry> {
    // Bound the FILE before opening it, because the entry cap below cannot: the map is materialised
    // in full before anything can be counted, so by then the memory has already been spent.
    //
    // Sized against what this process can actually write, because a cap below that ceiling does not
    // protect anything — it just makes every boot discard the whole parked cache, permanently, since
    // each shutdown rewrites the same oversized file.
    //
    // Worst case at YT_CACHE_MAX: a bounded imdb id plus the longest namespace suffix
    // (`:en:nokey:nokc`) is a ~27-byte key, and MAX_PROBE ids at 11 characters plus two timestamps
    // is ~140 bytes of body — ~169 a piece, so ~1.61 MB when completely full. Two megabytes clears
    // that by a fifth.
    //
    // Not more than that. The cap is the ONLY bound on what boot will materialise, and a file this
    // process did not write can be all minimum-size entries, which JSON expands severalfold into the
    // map. So every byte of slack here is several bytes of peak RSS at startup, on a box measured in
    // single-digit megabytes. Bounding the imdb id is what made a tight cap safe; spending that
    // safety on headroom nothing needs would be a poor trade.
    const MAX_RESOLVE_FILE: u64 = 2 * 1024 * 1024;
    match std::fs::metadata(&cfg.resolve_cache) {
        Ok(md) if md.len() > MAX_RESOLVE_FILE => {
            eprintln!(
                "resolve cache at {} is {} bytes, over the {MAX_RESOLVE_FILE} limit; starting empty",
                cfg.resolve_cache.display(),
                md.len()
            );
            return HashMap::new();
        }
        Ok(_) => {}
        Err(_) => return HashMap::new(), // no parked cache, which is the normal first boot
    }
    // Streamed, and then filtered IN PLACE. The obvious shape — read the file into a Vec, parse that
    // into one map, drain it into a second — has three full copies of the data alive at the worst
    // moment: the byte buffer (which `from_slice` borrows, so it cannot be dropped before the parse
    // completes), the parsed table, and the destination table growing to match by doubling. At boot,
    // which is when a redeploy still has the outgoing process resident.
    //
    // This is the same mistake `save_resolve_cache` was fixed for one function down, in the same
    // direction: build the whole document in memory rather than stream it. The read side is the
    // larger half, because what it materialises is a HashMap and not a byte vector.
    let Ok(file) = std::fs::File::open(&cfg.resolve_cache) else { return HashMap::new() };
    let mut parsed: HashMap<String, YtEntry> = match serde_json::from_reader(std::io::BufReader::new(file)) {
        Ok(m) => m,
        Err(_) => {
            eprintln!("resolve cache at {} is not readable; starting empty", cfg.resolve_cache.display());
            return HashMap::new();
        }
    };
    let mut kept = 0usize;
    parsed.retain(|_, e| {
        if kept >= crate::YT_CACHE_MAX {
            return false;
        }
        // Everything downstream ADDS to `confirmed` without checking: `now >= confirmed + YT_TTL_MS`
        // decides staleness on the /meta path, and the substitution path adds `STALE_GRACE_MS` on
        // top of that. All of it was safe for as long as `confirmed` could only ever be a reading of
        // our own clock — which is exactly the invariant this file breaks, since it is the one place
        // the value arrives from outside the process.
        //
        // So restore the invariant HERE, once, rather than hardening three arithmetic sites: an
        // entry claiming it was confirmed in the future did not come from our clock, and is not an
        // entry worth keeping. (A backwards clock jump drops the cache, which costs a re-resolve.)
        // Checked against both kinds of entry, because the empty ones are read by that same /meta
        // staleness test.
        if e.confirmed > now {
            return false;
        }
        let keep = if e.ids.is_empty() {
            e.exp > now
        } else {
            // Belt and braces on top of the guard above, which already rules the overflow out.
            match e.confirmed.checked_add(crate::YT_TTL_MS) {
                Some(earned) if now < earned => {
                    // Restore the expiry a confirmed answer is entitled to, rather than whatever
                    // cooldown the last failing lookup happened to leave behind.
                    e.exp = earned;
                    true
                }
                _ => false,
            }
        };
        if keep {
            kept += 1;
        }
        keep
    });
    // `retain` removes entries but keeps the table, so what survives here is sized to the FILE's
    // entry count and never shrinks again — `addon.rs`'s own trimming is also a `retain`, and the
    // map lives for the process. Filtering in place lowered the peak by dropping a byte buffer and a
    // second table; without this it would raise the floor by the same order, which is the worse of
    // the two on a box measured in single-digit megabytes. Costs one realloc, at boot, of something
    // strictly smaller than what was just freed.
    parsed.shrink_to_fit();
    if kept > 0 {
        eprintln!("resolve cache: {kept} entr{} still good", if kept == 1 { "y" } else { "ies" });
    }
    parsed
}

/// Park the resolve cache on the way out, so a redeploy does not make the next browse re-ask TMDB
/// for every title on screen. Best-effort by design: this is a cache, and failing to write it is not
/// worth delaying a shutdown over — but say so, because a volume that cannot be written is worth
/// knowing about for other reasons.
pub fn save_resolve_cache(state: &AppState) {
    let path = &state.cfg.resolve_cache;
    let Some(dir) = path.parent() else { return };
    if let Err(e) = std::fs::create_dir_all(dir) {
        eprintln!("resolve cache: {} is not writable ({e})", dir.display());
        return;
    }
    // Only what is still live — an entry already dead is bytes to write now, bytes to read at boot,
    // and an entry to parse and immediately discard.
    //
    // "Live" has to be asked the same way the loader asks it, and `exp` alone is not that question.
    // A populated entry is admitted on `confirmed`, because the stale-substitution path rewrites
    // `exp` to a 60-second retry cooldown while leaving perfectly good ids and their original
    // `confirmed` in place. Filtering on `exp` here dropped exactly those entries — so a TMDB blip
    // followed by a redeploy more than a minute later wiped the answers the blip had been leaning
    // on, which is the failure this whole file exists to prevent, arriving during an outage.
    let now = (state.clock)();
    let cache = state.yt_cache.lock().unwrap_or_else(|e| e.into_inner());
    let live: HashMap<&String, &YtEntry> = cache
        .iter()
        .filter(|(_, e)| {
            if e.ids.is_empty() {
                e.exp > now
            } else {
                now < e.confirmed.saturating_add(crate::YT_TTL_MS)
            }
        })
        .collect();
    let n = live.len();
    // Write-then-rename, so a kill mid-write cannot leave a half-file that the next boot has to
    // parse. The temp lives in the same directory, which is what makes the rename atomic.
    let tmp = path.with_extension("json.tmp");
    // Streamed through a BufWriter rather than serialised into a Vec and handed to `fs::write`.
    // That Vec was the whole document in memory — ~1.6 MB at capacity, and more during its final
    // doubling, with the old and new buffers briefly both alive — arriving at exactly the moment a
    // redeploy has the incoming process booting alongside this one. This costs a buffer.
    let written = (|| -> std::io::Result<()> {
        let file = std::fs::File::create(&tmp)?;
        let mut w = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut w, &live).map_err(std::io::Error::other)?;
        w.flush()
    })();
    drop(live);
    drop(cache);
    if let Err(e) = written {
        eprintln!("resolve cache: {e}");
        let _ = std::fs::remove_file(&tmp); // never leave a half-written temp for the next boot
        return;
    }
    match std::fs::rename(&tmp, path) {
        Ok(()) => eprintln!("shutdown: parked {n} resolve cache entr{}", if n == 1 { "y" } else { "ies" }),
        Err(e) => {
            eprintln!("resolve cache: {e}");
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

pub fn default_clock() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
