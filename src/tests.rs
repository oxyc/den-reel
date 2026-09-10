//! Ports meta.test.js: the pure functions, the resolve/cache logic (with a fake upstream + a
//! stubbed prober, so no network and no yt-dlp), and the HTTP /meta + /play serve contract.

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::state::{default_clock, AppState, PrewarmFn, ProbeFn};
use crate::upstream::NoAnswer;
use crate::upstream::{pick_trailer_candidates, Upstream};
use crate::ytdlp::classify;

// --- fakes / builders -------------------------------------------------------

struct FakeInner {
    tmdb: Mutex<Vec<String>>,
    kc: Mutex<Option<String>>,
    title: Mutex<Option<String>>,
    calls: AtomicUsize,
    fail: std::sync::atomic::AtomicBool,
    fail_kc: std::sync::atomic::AtomicBool,
    fail_title: std::sync::atomic::AtomicBool,
    /// Holds a lookup inside `tmdb_candidates` until the test releases it, so a second resolve can
    /// run to completion in between. The live-entry branch is only reachable that way.
    gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
}

#[derive(Clone)]
struct FakeUpstream(Arc<FakeInner>);

impl FakeUpstream {
    fn new(tmdb: &[&str], kc: Option<&str>) -> FakeUpstream {
        FakeUpstream(Arc::new(FakeInner {
            tmdb: Mutex::new(tmdb.iter().map(|s| s.to_string()).collect()),
            kc: Mutex::new(kc.map(|s| s.to_string())),
            title: Mutex::new(None),
            calls: AtomicUsize::new(0),
            fail: std::sync::atomic::AtomicBool::new(false),
            fail_kc: std::sync::atomic::AtomicBool::new(false),
            fail_title: std::sync::atomic::AtomicBool::new(false),
            gate: Mutex::new(None),
        }))
    }
    fn set_tmdb(&self, tmdb: &[&str]) {
        *self.0.tmdb.lock().unwrap() = tmdb.iter().map(|s| s.to_string()).collect();
    }
    fn set_kc(&self, kc: Option<&str>) {
        *self.0.kc.lock().unwrap() = kc.map(|s| s.to_string());
    }
    fn set_title(&self, title: &str) {
        *self.0.title.lock().unwrap() = Some(title.to_string());
    }
    fn calls(&self) -> usize {
        self.0.calls.load(Ordering::SeqCst)
    }
    /// Make the next lookup look like a transport error / 401 / 429 / 5xx: an empty result that is
    /// NOT an answer. The fault must land DURING the call, which is the only thing that
    /// distinguishes it from a title that genuinely has no trailer.
    fn fail_next(&self) {
        self.0.fail.store(true, Ordering::SeqCst);
    }
    /// Make the next lookup block inside `tmdb_candidates` until the returned semaphore is given a
    /// permit. Lets a test interleave two resolves on the one runtime thread.
    fn gate_next(&self) -> Arc<tokio::sync::Semaphore> {
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        *self.0.gate.lock().unwrap() = Some(sem.clone());
        sem
    }
    /// The title lookup could not be made. It gates the search fallback, so its failure means no
    /// search ran and the empty result is not an answer.
    fn fail_title(&self) {
        self.0.fail_title.store(true, Ordering::SeqCst);
    }
    /// A hard fault on the FALLBACK source, as the real upstream records it.
    fn fail_fallback(&self) {
        self.0.fail_kc.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl Upstream for FakeUpstream {
    async fn tmdb_candidates(
        &self,
        _tmdb_key: &str,
        _imdb: &str,
        _ty: &str,
        _lang: &str,
    ) -> crate::upstream::Answered<Vec<String>> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        // Claim this call's outcome BEFORE parking, or the resolve that runs while we are parked
        // consumes the flag that was armed for us.
        let failed = self.0.fail.swap(false, Ordering::SeqCst);
        let gate = self.0.gate.lock().unwrap().take();
        if let Some(sem) = gate {
            let _ = sem.acquire().await;
        }
        if failed {
            return Err(crate::upstream::NoAnswer);
        }
        Ok(self.0.tmdb.lock().unwrap().clone())
    }
    async fn kinocheck_youtube_id(
        &self,
        _kinocheck_key: Option<&str>,
        _imdb: &str,
        _ty: &str,
        _lang: &str,
    ) -> crate::upstream::Answered<Option<String>> {
        if self.0.fail_kc.load(Ordering::SeqCst) {
            return Err(crate::upstream::NoAnswer);
        }
        Ok(self.0.kc.lock().unwrap().clone())
    }
    async fn tmdb_title(
        &self,
        _tmdb_key: &str,
        _imdb: &str,
        _ty: &str,
    ) -> crate::upstream::Answered<Option<String>> {
        if self.0.fail_title.load(Ordering::SeqCst) {
            return Err(crate::upstream::NoAnswer);
        }
        Ok(self.0.title.lock().unwrap().clone())
    }
}

/// A clock the test drives, so a TTL can be asserted by advancing time rather than sleeping.
#[derive(Clone, Default)]
struct TestClock(Arc<AtomicU64>);
impl TestClock {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
    fn as_fn(&self) -> crate::state::ClockFn {
        let c = self.0.clone();
        Box::new(move || c.load(Ordering::SeqCst))
    }
}

/// `kill(pid, 0)`: ESRCH means gone, anything else means it exists (EPERM included — not ours, but
/// alive, so still not ours to delete).
fn pid_is_alive(pid: u32) -> bool {
    if unsafe { libc::kill(pid as i32, 0) } == 0 {
        return true;
    }
    // last_os_error rather than errno directly: the symbol differs per platform (__error on macOS,
    // __errno_location on Linux) and this has to build on both.
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

static TMP_CNT: AtomicUsize = AtomicUsize::new(0);
fn temp_dir() -> PathBuf {
    // Sweep what DEAD runs left. Nothing removes its own directory — a test that fails mid-way
    // should leave its files to look at — but a suite run creates ~100 and they had accumulated
    // into tens of thousands.
    //
    // Liveness matters: "not my pid" also matches a second test binary running right now, and
    // deleting its directories takes its fake yt-dlp/ffmpeg scripts out from under it. That
    // reproduced every time two runs overlapped, and failed the other run's tests with errors
    // pointing at the code rather than at this.
    static SWEPT: std::sync::Once = std::sync::Once::new();
    SWEPT.call_once(|| {
        let me = std::process::id();
        let Ok(rd) = std::fs::read_dir(std::env::temp_dir()) else { return };
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy();
            let Some(rest) = name.strip_prefix("den-reel-test-") else { continue };
            let Some(pid) = rest.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
                continue;
            };
            if pid != me && !pid_is_alive(pid) {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    });
    let n = TMP_CNT.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("den-reel-test-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn test_cfg(cache_dir: PathBuf) -> Config {
    Config {
        port: 8092,
        ytdlp_cache: cache_dir.join("yt-dlp"),
        resolve_cache: cache_dir.join("state").join("resolve.json"),
        cache_dir,
        ytdlp: "yt-dlp".into(),
        ffmpeg: "ffmpeg".into(),
        mp4box: "MP4Box".into(),
        bake_clap: true,
        max_height: "1080".into(),
        cache_max_bytes: 8 * 1024 * 1024 * 1024,
        cache_ttl: std::time::Duration::from_secs(365 * 24 * 60 * 60), // effectively off for the size-cap tests
        tmdb_key: Some("test-key".into()),
        kinocheck_key: None,
        config_key: String::new(),
        config_keys_prev: String::new(),
        play_secret: None,
        play_secrets_prev: Vec::new(),
        metrics_token: None,
        public_base_url: None,
        ytdlp_format: "fmt".into(),
        ytdlp_extractor_args: Some("youtube:player_client=visionos".into()),
        tmdb_base: "http://unused".into(),
        kinocheck_base: "http://unused".into(),
        cache_ok_until: std::sync::atomic::AtomicU64::new(0),
        cache_epoch: std::sync::atomic::AtomicU64::new(0),
    }
}

fn always_playable() -> ProbeFn {
    Box::new(|_id| Box::pin(async { crate::ytdlp::Probe::Playable { landscape: true } }))
}
fn noop_prewarm() -> PrewarmFn {
    Box::new(|_state, _id| {})
}

/// The search fallback never fires in most tests (mock `tmdb_title` is None); a no-op keeps them hermetic.
fn noop_searcher() -> crate::state::SearchFn {
    Box::new(|_q| Box::pin(async { Some(Vec::<String>::new()) }))
}

fn build_state(
    cache_dir: PathBuf,
    upstream: Box<dyn Upstream>,
    prober: ProbeFn,
    prewarm: PrewarmFn,
) -> Arc<AppState> {
    build_state_cfg(test_cfg(cache_dir), upstream, prober, prewarm)
}

/// Like `build_state` but with an explicit `Config` — lets a test enable the sealed-config keyring
/// (via `config_key`) exactly the way production does.
fn build_state_cfg(
    cfg: Config,
    upstream: Box<dyn Upstream>,
    prober: ProbeFn,
    prewarm: PrewarmFn,
) -> Arc<AppState> {
    build_state_full(cfg, upstream, prober, prewarm, noop_searcher())
}

/// Full builder with an injectable searcher (only the search-fallback test needs a non-noop one).
fn build_state_full(
    cfg: Config,
    upstream: Box<dyn Upstream>,
    prober: ProbeFn,
    prewarm: PrewarmFn,
    searcher: crate::state::SearchFn,
) -> Arc<AppState> {
    let config_keyring = crate::seal::Keyring::from_env(&cfg.config_key, &cfg.config_keys_prev).unwrap();
    Arc::new(AppState {
        cfg: Arc::new(cfg),
        config_keyring,
        yt_cache: Mutex::new(HashMap::new()),
        in_flight: Mutex::new(HashMap::new()),
        dl_gen: std::sync::atomic::AtomicU64::new(0),
        crop_cache: Mutex::new(HashMap::new()),
        crop_unknown: Mutex::new(HashMap::new()),
        play_fails: Mutex::new(HashMap::new()),
        upstream,
        prober,
        searcher,
        prewarm,
        clock: Box::new(default_clock),
        download_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::DOWNLOAD_CONCURRENCY)),
        prewarm_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::PREWARM_MAX)),
        probe_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::PROBE_CONCURRENCY)),
        cache_trailer_bytes: std::sync::atomic::AtomicU64::new(0),
        cache_trailer_count: std::sync::atomic::AtomicU64::new(0),
        cache_scratch_bytes: std::sync::atomic::AtomicU64::new(0),
        cache_measured_at: std::sync::atomic::AtomicU64::new(0),
        extract_fails: std::sync::atomic::AtomicU32::new(0),
        local_fails: std::sync::atomic::AtomicU32::new(0),
    })
}

fn build_state_clock(
    cache_dir: PathBuf,
    upstream: Box<dyn Upstream>,
    clock: crate::state::ClockFn,
) -> Arc<AppState> {
    let state = build_state(cache_dir, upstream, always_playable(), noop_prewarm());
    let mut state = Arc::try_unwrap(state).ok().expect("sole owner");
    state.clock = clock;
    Arc::new(state)
}

/// Both at once: a real `Config` (so a test can point `ytdlp` at a fake) and a driven clock (so a
/// TTL can be asserted by advancing time rather than sleeping).
fn build_state_cfg_clock(
    cfg: Config,
    upstream: Box<dyn Upstream>,
    clock: crate::state::ClockFn,
) -> Arc<AppState> {
    let state = build_state_cfg(cfg, upstream, always_playable(), noop_prewarm());
    let mut state = Arc::try_unwrap(state).ok().expect("sole owner");
    state.clock = clock;
    Arc::new(state)
}

/// How many times the fake yt-dlp was actually run (it appends a line per invocation).
fn spawn_count(p: &std::path::Path) -> usize {
    std::fs::read_to_string(p).map(|s| s.lines().count()).unwrap_or(0)
}

/// Start the real router on an ephemeral port; returns the base URL.
async fn spawn_server(state: Arc<AppState>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let state = state.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| {
                    let state = state.clone();
                    async move { Ok::<_, Infallible>(crate::handle_request(state, req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    format!("http://{addr}")
}

// --- pure functions ---------------------------------------------------------

#[test]
fn pick_candidates_orders_and_dedupes() {
    let results = vec![
        json!({ "site": "YouTube", "type": "Teaser", "key": "teaser00000" }),
        json!({ "site": "Vimeo", "type": "Trailer", "key": "ignored0000" }),
        json!({ "site": "YouTube", "type": "Trailer", "official": true, "key": "official111" }),
        json!({ "site": "YouTube", "type": "Trailer", "key": "plain222222" }),
        json!({ "site": "YouTube", "type": "Trailer", "official": true, "key": "official111" }),
    ];
    assert_eq!(pick_trailer_candidates(&results, "en"), vec!["official111", "plain222222", "teaser00000"]);
}

/// The ordering prefers the FILM'S language, then English, then anything else — never the viewer's.
///
/// A video tagged with the viewer's language is a dub or a local-market cut; the thing worth watching
/// is the film as it was made, subtitled by the client if it wants. An earlier version of this ranked
/// the viewer's language first, which handed a Finnish viewer a Finnish-dubbed trailer for an English
/// film in preference to the original.
#[test]
fn pick_candidates_prefers_the_films_own_language_then_english() {
    // The Finnish entry is deliberately the OFFICIAL trailer: language has to outrank kind, or a
    // locally-marketed cut wins on being marked official.
    let results = vec![
        json!({ "site": "YouTube", "type": "Teaser", "iso_639_1": "en", "key": "engTeaser01" }),
        json!({ "site": "YouTube", "type": "Trailer", "official": true, "iso_639_1": "fr", "key": "frenchOff01" }),
        json!({ "site": "YouTube", "type": "Trailer", "iso_639_1": "en", "key": "engTrailer1" }),
        json!({ "site": "YouTube", "type": "Trailer", "official": true, "iso_639_1": "fi", "key": "finnishDub1" }),
        json!({ "site": "YouTube", "type": "Trailer", "key": "untagged001" }),
    ];

    // A French film: its own official trailer leads, then English by kind, then the rest. The
    // Finnish cut is behind both English videos despite being the other "official" one.
    assert_eq!(
        pick_trailer_candidates(&results, "fr"),
        vec!["frenchOff01", "engTrailer1", "engTeaser01", "finnishDub1", "untagged001"]
    );
    // An English film: English leads by kind, and the French and Finnish officials both fall behind
    // an English video that is not marked official at all.
    assert_eq!(
        pick_trailer_candidates(&results, "en"),
        vec!["engTrailer1", "engTeaser01", "frenchOff01", "finnishDub1", "untagged001"]
    );
    // A film whose own language has no video here falls back to English — and behaves exactly like
    // an English film, because nothing occupies the first band. Nothing is dropped: a dub is still a
    // playable last resort if everything ahead of it fails.
    assert_eq!(
        pick_trailer_candidates(&results, "ja"),
        pick_trailer_candidates(&results, "en"),
        "falling back to English should order identically to an English film"
    );
}

#[test]
fn build_meta_produces_same_host_play_url() {
    let out = crate::addon::build_meta(
        "movie",
        "tt0111161",
        "https://trailers.example.com/",
        &["abc123DEF01".to_string()],
        None,
    );
    assert_eq!(out["meta"]["links"][0]["trailers"], "https://trailers.example.com/play/abc123DEF01.mp4");
}

/// With a secret configured the play URL carries the tag /play and /crop will demand — and without
/// one it is byte-for-byte the URL it has always been, because every install in the field is
/// holding unsigned URLs with a week of `max-age` on them.
#[test]
fn a_signed_install_hands_out_signed_play_urls() {
    let signed = crate::addon::build_meta(
        "movie",
        "tt0111161",
        "https://trailers.example.com",
        &["abc123DEF01".to_string()],
        Some("s3cret"),
    );
    let url = signed["meta"]["links"][0]["trailers"].as_str().unwrap();
    let expected = format!(
        "https://trailers.example.com/play/abc123DEF01.mp4?s={}",
        crate::sign::tag("s3cret", "abc123DEF01")
    );
    assert_eq!(url, expected);
}

/// The only thing standing in front of /play and /crop was "is this eleven characters", and both
/// spend a download permit, a yt-dlp process and cache space on whatever they are handed. An
/// instance reachable beyond the LAN was a YouTube extraction service for anyone who found it.
#[tokio::test]
async fn a_signed_install_refuses_unsigned_play_and_crop() {
    let dir = temp_dir();
    seed_cache(&dir, "cachedVid07", 100);
    let mut cfg = test_cfg(dir);
    cfg.play_secret = Some("s3cret".into());
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let client = reqwest::Client::new();

    let r = client.get(format!("{base}/play/cachedVid07.mp4")).send().await.unwrap();
    assert_eq!(r.status(), 403, "an unsigned /play was served");

    let r =
        client.get(format!("{base}/play/cachedVid07.mp4?s=deadbeefdeadbeefdeadbeef")).send().await.unwrap();
    assert_eq!(r.status(), 403, "a wrong tag was accepted");

    let tag = crate::sign::tag("s3cret", "cachedVid07");
    let r = client.get(format!("{base}/play/cachedVid07.mp4?s={tag}")).send().await.unwrap();
    assert_eq!(r.status(), 200, "the URL /meta hands out must actually play");

    // /crop is a second door to the same download, so it is gated too — by the same tag, since the
    // signature covers the id rather than the path. But it DEGRADES rather than refusing: nothing
    // this server emits is a signed crop URL (the tag rides on the play URL and the client has to
    // carry it across), so a 403 here would turn a client that does not into de-letterboxing that
    // silently vanishes. The answer is the same "just play it" every other failing path returns —
    // and it reaches none of the download the gate exists to protect.
    let r = client.get(format!("{base}/crop/cachedVid07.json")).send().await.unwrap();
    assert_eq!(r.status(), 200, "an unsigned /crop should degrade, not break de-letterboxing");
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["letterboxed"], false, "an unsigned caller gets the play-it-whole answer");
    assert!(body.get("content").is_none(), "an unsigned caller must not get a detected rect");

    let r = client.get(format!("{base}/crop/cachedVid07.json?s={tag}")).send().await.unwrap();
    assert_eq!(r.status(), 200, "the play URL's tag must open /crop for the same id");
}

/// `serve_until` on a loopback port, stopped by the returned sender instead of a signal.
async fn start_serve(
    grace: std::time::Duration,
) -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state =
        build_state(temp_dir(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let stop = async move {
        let _ = rx.await;
    };
    (addr, tx, tokio::spawn(crate::serve_until(listener, state, stop, grace)))
}

#[tokio::test]
async fn an_idle_server_stops_at_once() {
    let (_, stop, server) = start_serve(std::time::Duration::from_secs(5)).await;
    stop.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("an idle server waited out the grace instead of stopping")
        .unwrap();
}

/// A graceful shutdown waits for every connection, so without a deadline a client that sends half a
/// request head and goes quiet would hold the stop open for as long as it liked.
#[tokio::test]
async fn a_half_sent_request_cannot_hold_the_stop_open() {
    use tokio::io::AsyncWriteExt;
    let (addr, stop, server) = start_serve(std::time::Duration::from_millis(300)).await;
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n").await.unwrap(); // no terminating blank line
    tokio::time::sleep(std::time::Duration::from_millis(50)).await; // let the server accept it first
    stop.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), server)
        .await
        .expect("the drain is unbounded")
        .unwrap();
}

/// ...and with no secret configured, nothing changes: every install in the field is holding unsigned
/// play URLs that /meta told it to cache for a week.
#[tokio::test]
async fn an_unsigned_install_is_completely_unchanged() {
    let dir = temp_dir();
    seed_cache(&dir, "cachedVid08", 100);
    let state = build_state(dir, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;

    let r = reqwest::get(format!("{base}/play/cachedVid08.mp4")).await.unwrap();
    assert_eq!(r.status(), 200, "signing is opt-in and this install did not opt in");
}

/// /health answers one question in one word. Everything behind that verdict — how full the volume
/// is, how many downloads are in flight, how much of the resolve cache is standing — was visible
/// only by reading logs. The figures come from the eviction pass, which already walks the directory,
/// because an ops endpoint that stats a few thousand files per request makes a busy box busier.
#[tokio::test]
async fn metrics_reports_the_measured_cache_rather_than_walking_it() {
    let dir = temp_dir();
    seed_cache(&dir, "statsVid001", 4096);
    let mut cfg = test_cfg(dir);
    cfg.metrics_token = Some("scrape-me".into());
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state.clone()).await;

    let r = scrape_metrics(&base, Some("scrape-me")).await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["content-type"], "text/plain; version=0.0.4; charset=utf-8");
    let body = r.text().await.unwrap();
    let has = |body: &str, line: &str| body.lines().any(|l| l == line);
    assert!(has(&body, &format!("reel_build_info{{version=\"{}\"}} 1", env!("CARGO_PKG_VERSION"))));
    // Nothing has measured the volume yet — and /metrics must not be the thing that does, even
    // though there is a trailer sitting right there.
    assert!(has(&body, "reel_cache_measured_at_seconds 0"), "/metrics walked the cache directory itself");
    assert!(has(&body, "reel_cache_trailers 0"), "/metrics walked the cache directory itself");

    // The eviction pass measures; /metrics reports what it found.
    let u = crate::play::evict_if_needed(&state.cfg).expect("the cache dir is readable");
    state.record_cache_usage(u);

    let body = scrape_metrics(&base, Some("scrape-me")).await.text().await.unwrap();
    assert!(has(&body, "reel_cache_trailers 1"));
    assert!(has(&body, "reel_cache_trailer_bytes 4096"));
    assert!(!has(&body, "reel_cache_measured_at_seconds 0"), "the measurement was not timestamped");
    assert!(
        has(&body, &format!("reel_cache_free_bytes {}", state.cfg.cache_max_bytes - 4096)),
        "free space must account for what trailers already hold"
    );
    assert!(has(&body, &format!("reel_downloads_concurrency_limit {}", crate::DOWNLOAD_CONCURRENCY)));
    assert!(has(&body, "reel_consecutive_failures{kind=\"extract\"} 0"));
}

async fn scrape_metrics(base: &str, token: Option<&str>) -> reqwest::Response {
    let mut req = reqwest::Client::new().get(format!("{base}/metrics"));
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    req.send().await.unwrap()
}

/// With no METRICS_TOKEN the endpoint does not exist — whatever the caller sends.
#[tokio::test]
async fn metrics_is_off_without_a_token() {
    let state =
        build_state(temp_dir(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    assert_eq!(scrape_metrics(&base, None).await.status(), 404);
    assert_eq!(scrape_metrics(&base, Some("anything")).await.status(), 404);
}

/// A wrong token is refused exactly as an unknown path is, so a refusal says nothing about whether a
/// token is configured.
#[tokio::test]
async fn metrics_refuses_a_wrong_token_like_a_missing_route() {
    let mut cfg = test_cfg(temp_dir());
    cfg.metrics_token = Some("scrape-me".into());
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;

    let wrong = scrape_metrics(&base, Some("scrape-me-not")).await;
    assert_eq!(wrong.status(), 404);
    let missing = reqwest::get(format!("{base}/no-such-route")).await.unwrap();
    assert_eq!(missing.status(), 404);
    assert_eq!(wrong.text().await.unwrap(), missing.text().await.unwrap());
    assert_eq!(scrape_metrics(&base, None).await.status(), 404);
}

#[test]
fn classify_maps_geoblock_to_451() {
    let e = classify(
        Some(1),
        "ERROR: [youtube] X: The uploader has not made this video available in your country",
    );
    assert_eq!(e.status, 451);
    assert_eq!(e.reason, "geo_blocked");
}

#[test]
fn classify_defaults_to_502() {
    assert_eq!(classify(Some(1), "some other failure").status, 502);
}

// --- /health (ADDON-02) ------------------------------------------------------

#[test]
fn health_reports_degraded_and_ok_states() {
    // No TMDB key AND no sealed-config keyring → trailers can't work → degraded.
    assert_eq!(
        crate::health_body(false, 0, 0, 0),
        json!({"status": "degraded", "reason": "tmdb_key_missing", "detail": "set CONFIG_KEY (per-install BYOK) or TMDB_KEY"})
    );
    // A missing key wins even if upstreams / the extractor are also failing.
    assert_eq!(crate::health_body(false, 99, 99, 0)["reason"], "tmdb_key_missing");

    // Key present but upstreams have been failing (>= threshold) → degraded (wins over the extractor).
    assert_eq!(
        crate::health_body(true, 3, 99, 0),
        json!({"status": "degraded", "reason": "upstream_unavailable", "detail": "TMDB has been failing"})
    );
    assert_eq!(crate::health_body(true, 4, 0, 0)["reason"], "upstream_unavailable");

    // Upstreams fine but yt-dlp can't extract anything (>= threshold) → degraded (the silent-outage gap).
    assert_eq!(crate::health_body(true, 0, 3, 0)["reason"], json!("extractor_unavailable"));
    assert_eq!(crate::health_body(true, 0, 2, 0), json!({"status": "ok"})); // below threshold → ok

    // Key present, everything below the threshold → ok.
    assert_eq!(crate::health_body(true, 0, 0, 0), json!({"status": "ok"}));
    assert_eq!(crate::health_body(true, 2, 2, 0), json!({"status": "ok"}));
}

// --- resolve logic ----------------------------------------------------------

#[tokio::test]
async fn resolve_returns_first_playable_and_caches() {
    let fake = FakeUpstream::new(&["firstGood11"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());

    assert_eq!(
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
            .await
            .ids
            .first()
            .map(String::as_str),
        Some("firstGood11")
    );
    let after = fake.calls();
    assert_eq!(
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
            .await
            .ids
            .first()
            .map(String::as_str),
        Some("firstGood11")
    );
    assert_eq!(fake.calls(), after, "second lookup is a cache hit (no new upstream calls)");
}

#[tokio::test]
async fn resolve_returns_alternates_after_the_primary_for_fallback() {
    // Best-playable pick first, then the other candidates as unprobed fallbacks (#5 — the client tries
    // the next one on a playback failure). No extra probing beyond first_playable.
    let fake = FakeUpstream::new(&["playable1", "playable2"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids, vec!["playable1".to_string(), "playable2".to_string()]);
}

#[tokio::test]
async fn resolve_returns_candidates_in_rank_order_without_probing() {
    // No resolve-time probe: the TMDB/KinoCheck candidates come back in rank order, unvalidated. A dead /
    // geo-blocked / portrait pick is the client's problem (it advances to the next), and playability is
    // validated lazily on /play — so /meta never spawns yt-dlp.
    let fake = FakeUpstream::new(&["blockedUS01", "worldwide22"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids, vec!["blockedUS01".to_string(), "worldwide22".to_string()]);
}

#[tokio::test]
async fn resolve_empty_only_when_no_candidates_at_all() {
    // Empty ONLY when TMDB/KinoCheck carry no trailer (and search finds nothing) — never because a
    // candidate looked unplayable (that check moved to /play).
    let fake = FakeUpstream::new(&["someCandidate"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids, vec!["someCandidate".to_string()]);
}

#[tokio::test]
async fn resolve_falls_back_to_youtube_search_when_no_candidates() {
    // TMDB + KinoCheck carry no trailer, but the title is known → search YouTube and return the results
    // (in search order; no probe — the client picks the first that plays + is landscape).
    let fake = FakeUpstream::new(&[], None);
    fake.set_title("Backrooms 2025");
    let searcher: crate::state::SearchFn =
        Box::new(|_q| Box::pin(async { Some(vec!["searchOne".into(), "searchTwo".into()]) }));
    let state =
        build_state_full(test_cfg(temp_dir()), Box::new(fake), always_playable(), noop_prewarm(), searcher);
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt99999999", "movie", "en").await.ids;
    assert_eq!(ids, vec!["searchOne".to_string(), "searchTwo".to_string()]);
}

#[tokio::test]
async fn resolve_no_search_when_title_unknown() {
    // No candidates AND no title → the search fallback can't build a query → empty, no panic.
    let fake = FakeUpstream::new(&[], None); // title left None
    let prober: ProbeFn =
        Box::new(|_id| Box::pin(async { crate::ytdlp::Probe::Playable { landscape: true } }));
    let searcher: crate::state::SearchFn =
        Box::new(|_q| Box::pin(async { Some(vec!["shouldNotBeUsed".into()]) }));
    let state = build_state_full(test_cfg(temp_dir()), Box::new(fake), prober, noop_prewarm(), searcher);
    assert_eq!(
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0", "movie", "en").await.ids,
        Vec::<String>::new()
    );
}

// (Landscape preference moved off the server: den-reel returns candidates in rank order and the CLIENT
// advances past a portrait/dead pick — so the old `resolve_prefers_landscape` / `falls_back_to_portrait`
// probe tests are gone. `parse_landscape` is still exercised below since the prober helper retains it.)

#[test]
fn parse_landscape_reads_dims_and_defaults_safely() {
    use crate::ytdlp::parse_landscape;
    assert!(parse_landscape("1920 1080"), "wide → landscape");
    assert!(parse_landscape("1080 1080"), "square counts as landscape (not a sliver)");
    assert!(!parse_landscape("1080 1920"), "tall → portrait");
    assert!(parse_landscape("NA NA"), "unknown dims default to landscape (don't skip)");
    assert!(parse_landscape(""), "empty output defaults to landscape");
}

// --- HTTP contract ----------------------------------------------------------

#[tokio::test]
async fn get_manifest_returns_addon_manifest() {
    let state =
        build_state(temp_dir(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let body: Value = reqwest::get(format!("{base}/manifest.json")).await.unwrap().json().await.unwrap();
    assert_eq!(body["id"], "com.den.reel");
    assert_eq!(body["resources"][0], "meta");
}

#[tokio::test]
async fn get_meta_rejects_non_imdb_with_no_upstream_call() {
    let fake = FakeUpstream::new(&["should-not-be-used"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let body: Value =
        reqwest::get(format!("{base}/meta/movie/not-an-id.json")).await.unwrap().json().await.unwrap();
    assert_eq!(body["meta"]["links"].as_array().unwrap().len(), 0);
    assert_eq!(fake.calls(), 0, "no upstream call for a non-imdb id");
}

#[tokio::test]
async fn get_meta_resolves_imdb_to_play_url_on_request_host() {
    let fake = FakeUpstream::new(&["vidKey12345"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let body: Value = reqwest::Client::new()
        .get(format!("{base}/meta/movie/tt0111161.json"))
        .header("x-forwarded-host", "trailers.example.com")
        .header("x-forwarded-proto", "https")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["meta"]["links"][0]["trailers"], "https://trailers.example.com/play/vidKey12345.mp4");
}

#[tokio::test]
async fn get_meta_caches_success_not_empty() {
    let fake = FakeUpstream::new(&["vidKey12345"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());
    let base = spawn_server(state.clone()).await;
    let client = reqwest::Client::new();

    let ok = client.get(format!("{base}/meta/movie/tt0111161.json")).send().await.unwrap();
    assert!(ok.headers().get("cache-control").unwrap().to_str().unwrap().contains("max-age=604800"));

    state.yt_cache.lock().unwrap().clear();
    fake.set_tmdb(&[]); // no trailer → empty links → no-store (client re-checks, doesn't cache a miss)
    let empty = client.get(format!("{base}/meta/movie/tt0111161.json")).send().await.unwrap();
    assert_eq!(empty.headers().get("cache-control").unwrap(), "no-store");
    let body: Value = empty.json().await.unwrap();
    assert_eq!(body["meta"]["links"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn prewarm_default_but_not_when_opted_out() {
    let warmed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let rec = warmed.clone();
    let prewarm: PrewarmFn = Box::new(move |_state, id| rec.lock().unwrap().push(id));
    let fake = FakeUpstream::new(&["vidKey12345"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), prewarm);
    let base = spawn_server(state).await;
    let client = reqwest::Client::new();

    client.get(format!("{base}/meta/movie/tt0111161.json?prewarm=0")).send().await.unwrap();
    assert!(warmed.lock().unwrap().is_empty(), "prewarm should be skipped");

    client.get(format!("{base}/meta/movie/tt0111161.json")).send().await.unwrap();
    assert_eq!(*warmed.lock().unwrap(), vec!["vidKey12345".to_string()], "default should prewarm");
}

// --- sealed config-in-URL (den-scout/docs/SEALED-CONFIG.md) -----------------

// The fixed vector key + a PyNaCl-sealed {tmdbKey,kinocheckKey} segment (same key the seal/userconfig
// unit tests use), driven through the real router so the config-scoped routes are proven end-to-end.
const VEC_PRIV: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
const VEC_PUB: &str = "j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8=";
const SEALED_SEG: &str = "Abo-qmntVxuOmeVa0Q5pPWju0VrZDS4aRoAP-0JHNtk7nmMcduhttWlvldwvUdXPafUGUegc4ul5J3gFVo8nEGOd8htc7he_3BihPsWtiuA5_2Du-FL5NpaNzfvqhDAHM_LAjw";

/// Build a state with the sealed-config keyring enabled and NO env TMDB key — so a resolved trailer
/// can only come from the per-install (sealed) config path.
fn sealed_state(fake: FakeUpstream) -> Arc<AppState> {
    let mut cfg = test_cfg(temp_dir());
    cfg.tmdb_key = None; // prove the URL config supplies the key, not the env
    cfg.config_key = VEC_PRIV.into();
    build_state_cfg(cfg, Box::new(fake), always_playable(), noop_prewarm())
}

#[tokio::test]
async fn config_key_serves_pubkey_when_keyring_set() {
    let base = spawn_server(sealed_state(FakeUpstream::new(&[], None))).await;
    let r = reqwest::get(format!("{base}/config-key")).await.unwrap();
    assert_eq!(r.status(), 200);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body["key"], VEC_PUB);
}

#[tokio::test]
async fn config_key_404s_when_sealing_disabled() {
    // Default test state has no CONFIG_KEY → sealing disabled.
    let state =
        build_state(temp_dir(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    assert_eq!(reqwest::get(format!("{base}/config-key")).await.unwrap().status(), 404);
}

#[tokio::test]
async fn sealed_config_url_resolves_manifest_and_meta() {
    let fake = FakeUpstream::new(&["vidKey12345"], None);
    let base = spawn_server(sealed_state(fake)).await;
    let client = reqwest::Client::new();

    // The pasted install URL.
    let manifest = client.get(format!("{base}/{SEALED_SEG}/manifest.json")).send().await.unwrap();
    assert_eq!(manifest.status(), 200);

    // Stremio then derives /<config>/meta/... — resolves the trailer using the sealed BYOK TMDB key.
    let body: Value = client
        .get(format!("{base}/{SEALED_SEG}/meta/movie/tt0111161.json"))
        .header("x-forwarded-host", "trailers.example.com")
        .header("x-forwarded-proto", "https")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["meta"]["links"][0]["trailers"], "https://trailers.example.com/play/vidKey12345.mp4");
}

#[tokio::test]
async fn a_bad_config_segment_fails_closed() {
    let base = spawn_server(sealed_state(FakeUpstream::new(&["x"], None))).await;
    let client = reqwest::Client::new();
    // Garbage where a config belongs → 400, never a silent env-key fallback under a config-shaped URL.
    let bad = client.get(format!("{base}/not-a-valid-config/manifest.json")).send().await.unwrap();
    assert_eq!(bad.status(), 400);
    let body: Value = bad.json().await.unwrap();
    assert_eq!(body["error"], "bad_config");
}

#[tokio::test]
async fn legacy_plaintext_config_resolves_with_a_keyring_present() {
    use base64::Engine;
    let fake = FakeUpstream::new(&["vidKey12345"], None);
    let base = spawn_server(sealed_state(fake)).await;
    let seg = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"tmdbKey":"legacy"}"#);
    let body: Value =
        reqwest::get(format!("{base}/{seg}/meta/movie/tt0111161.json")).await.unwrap().json().await.unwrap();
    assert_eq!(
        body["meta"]["links"].as_array().unwrap().len(),
        1,
        "legacy plaintext config must still resolve"
    );
}

// --- /play serve contract (seed a cached file so fetch_trailer never spawns yt-dlp) ---

fn seed_cache(dir: &std::path::Path, vid: &str, size: usize) -> usize {
    std::fs::write(dir.join(format!("{vid}.mp4")), vec![7u8; size]).unwrap();
    size
}

#[tokio::test]
async fn play_cached_no_range_is_200_with_length_and_ranges() {
    let dir = temp_dir();
    let size = seed_cache(&dir, "cachedVid01", 4096);
    let state = build_state(dir, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let r = reqwest::get(format!("{base}/play/cachedVid01.mp4")).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("content-length").unwrap(), &size.to_string());
    assert_eq!(r.headers().get("accept-ranges").unwrap(), "bytes");
    assert_eq!(r.headers().get("content-type").unwrap(), "video/mp4");
}

#[tokio::test]
async fn play_with_range_is_206() {
    let dir = temp_dir();
    let size = seed_cache(&dir, "cachedVid02", 4096);
    let state = build_state(dir, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let r = reqwest::Client::new()
        .get(format!("{base}/play/cachedVid02.mp4"))
        .header("range", "bytes=0-99")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 206);
    assert_eq!(r.headers().get("content-range").unwrap(), &format!("bytes 0-99/{size}"));
    assert_eq!(r.headers().get("content-length").unwrap(), "100");
    assert_eq!(r.headers().get("accept-ranges").unwrap(), "bytes");
}

// --- cropdetect parsing ---

#[test]
fn typical_crop_takes_the_modal_box_not_the_union() {
    // With reset=1 cropdetect prints one box per keyframe. Most keyframes are a clean 1920x816
    // letterbox; two "logo card" frames read taller. The UNION (old behaviour) would keep the taller
    // box and leave the bar in; the median keeps the letterbox → the transient logo is cropped away.
    let stderr = "\
[cropdetect] crop=1920:816:0:132\n\
[cropdetect] crop=1920:816:0:132\n\
[cropdetect] crop=1920:1060:0:20\n\
[cropdetect] crop=1920:816:0:132\n\
[cropdetect] crop=1920:1060:0:20\n\
[cropdetect] crop=1920:816:0:132\n";
    let boxes = crate::crop::parse_all_crops(stderr);
    assert_eq!(boxes.len(), 6);
    assert_eq!(
        crate::crop::typical_crop(&boxes),
        Some(crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 })
    );
}

#[test]
fn refine_snaps_transient_logo_and_guards_dark_frames() {
    use crate::crop::{refine_report, report_from, RawCrop};
    let src = Some((1920, 1080));

    // Already-clean 2.35 letterbox (816): snapping to 2.35 lands on 817 — within the keep-px slop, so
    // the measured box is left untouched (no 1px jitter).
    let clean = refine_report(report_from("x", src, RawCrop { w: 1920, h: 816, x: 0, y: 132 }));
    assert_eq!(clean.content.as_ref().map(|c| (c.w, c.h, c.x, c.y)), Some((1920, 816, 0, 132)));
    assert!(clean.letterboxed);

    // A logo-inflated box (840, ~2.29:1) is within tolerance of 2.35 and >keep-px off → snapped to a
    // clean, centred scope crop (1920x817), cropping the logo strip out of the bar.
    let inflated = refine_report(report_from("x", src, RawCrop { w: 1920, h: 840, x: 0, y: 120 }));
    assert_eq!(inflated.content.as_ref().map(|c| (c.w, c.h, c.x, c.y)), Some((1920, 817, 0, 131)));

    // A pathological dark-frame box (500px, ~3.84:1 — no standard match, below the 60% floor) is
    // treated as unsure → not cropped (play the full frame) rather than shave real content.
    let dark = refine_report(report_from("x", src, RawCrop { w: 1920, h: 500, x: 0, y: 290 }));
    assert!(!dark.letterboxed);
    assert_eq!(dark.content.as_ref().map(|c| c.h), Some(1080));

    // A mild non-standard letterbox (738px, ~2.6:1 — no snap, but above the floor) is kept as measured.
    let mild = refine_report(report_from("x", src, RawCrop { w: 1920, h: 738, x: 0, y: 171 }));
    assert_eq!(mild.content.as_ref().map(|c| c.h), Some(738));
    assert!(mild.letterboxed);
}

#[test]
fn full_frame_guard_spares_mixed_framing_trailers() {
    use crate::crop::{uses_full_frame, RawCrop};
    let src = Some((640u32, 360u32));
    let lb = RawCrop { w: 640, h: 272, x: 0, y: 44 }; // 2.35 letterbox
    let full = RawCrop { w: 640, h: 360, x: 0, y: 0 }; // full frame

    // Monsters-vs-Aliens shape: dominant 2.35 letterbox + a few genuine full-frame shots → guard fires,
    // so the caller keeps the full frame instead of slicing those shots (the v0.3.0 regression).
    let mut mixed = vec![lb; 60];
    mixed.extend([full; 3]);
    mixed.extend([RawCrop { w: 578, h: 272, x: 0, y: 44 }; 2]);
    assert!(uses_full_frame(&mixed, src), "3 full-frame shots among a letterbox → don't crop");

    // A cleanly letterboxed trailer (no full-frame keyframes) is not spared → it still gets cropped.
    assert!(!uses_full_frame(&vec![lb; 60], src));

    // A single stray full-frame flash on an otherwise-clean letterbox is below the floor → still crops.
    let mut flash = vec![lb; 60];
    flash.push(full);
    assert!(!uses_full_frame(&flash, src), "one flash shouldn't suppress the crop");
}

#[test]
fn refine_plays_full_frame_for_portrait_and_pillarbox() {
    use crate::crop::{refine_report, report_from, RawCrop};

    // Portrait source (landscape clip padded into a 720x1280 frame): the huge top/bottom padding is NOT
    // a cinematic letterbox — cropping it to a thin strip is what broke the billboard. Must play full.
    let portrait =
        refine_report(report_from("x", Some((720, 1280)), RawCrop { w: 640, h: 404, x: 40, y: 438 }));
    assert!(!portrait.letterboxed, "a portrait source must not be letterbox-cropped");
    assert_eq!(portrait.content.as_ref().map(|c| (c.w, c.h)), Some((720, 1280)));

    // Pillarbox (side bars, not top/bottom) → not our job → full frame.
    let pillar =
        refine_report(report_from("x", Some((1920, 1080)), RawCrop { w: 1200, h: 1080, x: 360, y: 0 }));
    assert!(!pillar.letterboxed);
    assert_eq!(pillar.content.as_ref().map(|c| c.w), Some(1920));

    // Kept letterboxes are always emitted centred + full-width, so the baked clap is symmetric/valid.
    let kept = refine_report(report_from("x", Some((1920, 1080)), RawCrop { w: 1918, h: 804, x: 1, y: 138 }));
    assert_eq!(kept.content.as_ref().map(|c| (c.x, c.w)), Some((0, 1920)), "normalised to full width");
    assert_eq!(kept.content.as_ref().map(|c| c.y), Some((1080 - 804) / 2), "centred vertically");
}

#[test]
fn parse_source_dims_reads_the_video_stream_line() {
    let stderr = "  Stream #0:0(und): Video: h264 (High) (avc1 / 0x31637661), yuv420p, 1920x1080 [SAR 1:1 DAR 16:9], 24 fps";
    assert_eq!(crate::crop::parse_source_dims(stderr), Some((1920, 1080)));
}

#[test]
fn report_flags_letterbox_but_not_pixel_noise() {
    // 1080 → 816 content = 264px bars (~24%) → letterboxed, ~2.35 aspect.
    let boxed = crate::crop::report_from(
        "x",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 },
    );
    assert!(boxed.letterboxed);
    assert_eq!(boxed.aspect, Some(2.35));
    // 1080 → 1072 content = 8px (<2%) → treated as noise, not letterboxed.
    let noise = crate::crop::report_from(
        "x",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 1072, x: 0, y: 4 },
    );
    assert!(!noise.letterboxed);
}

// Exercises the real detect()+bake_clap() path against ffmpeg + MP4Box. Kept out of CI (which has
// neither). Run locally with: cargo test -- --ignored
#[tokio::test]
#[ignore]
async fn clap_pipeline_bakes_box_end_to_end() {
    let dir = temp_dir();
    let fp = dir.join("clapvid0001.mp4");
    // 1920x1080 with a 1920x816 testsrc content region and 132px black bars top/bottom.
    let ok = std::process::Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=1920x816:rate=24:d=2",
            "-vf",
            "pad=1920:1080:0:132:color=black",
            "-c:v",
            "libx264",
            "-g",
            "6",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
        ])
        .arg(&fp)
        .status()
        .unwrap()
        .success();
    assert!(ok, "ffmpeg failed to build the letterbox fixture");

    let cfg = test_cfg(dir);
    let report = crate::crop::detect(&cfg, "clapvid0001", &fp).await.expect("detect returned a rect");
    assert!(report.letterboxed, "132px bars should read as letterboxed");
    assert_eq!(report.content.as_ref().unwrap().h, 816);
    assert_eq!(
        crate::crop::bake_clap(&cfg, &fp, &report).await,
        crate::crop::Bake::Baked,
        "MP4Box should write the clap box"
    );

    // ffprobe reads the clap back as frame cropping — 132px top & bottom.
    let out = std::process::Command::new("ffprobe")
        .args(["-hide_banner", "-v", "error", "-show_streams"])
        .arg(&fp)
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("crop_top=132") && s.contains("crop_bottom=132"), "clap not read back: {s}");
}

// Proves the modal detection crops a TRANSIENT logo card out of the bar — not just a clean letterbox.
// A union over all frames would keep the bar; the median shouldn't. Needs ffmpeg + MP4Box; run locally
// with: cargo test -- --ignored
#[tokio::test]
#[ignore]
async fn clap_pipeline_crops_transient_logo_end_to_end() {
    let dir = temp_dir();
    let fp = dir.join("logovid0001.mp4");
    // 4s of a 1920x816 letterbox padded to 1080, with a bright "logo" box drawn in the TOP black bar
    // for the last second only (a minority of keyframes).
    let ok = std::process::Command::new("ffmpeg")
        .args([
            "-y", "-f", "lavfi", "-i", "testsrc=size=1920x816:rate=24:d=4",
            "-vf",
            "pad=1920:1080:0:132:color=black,drawbox=x=40:y=20:w=420:h=90:color=white:t=fill:enable='between(t,3,4)'",
            "-c:v", "libx264", "-g", "6", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ])
        .arg(&fp)
        .status().unwrap().success();
    assert!(ok, "ffmpeg failed to build the transient-logo fixture");

    let cfg = test_cfg(dir);
    let report = crate::crop::detect(&cfg, "logovid0001", &fp).await.expect("detect returned a rect");
    assert!(report.letterboxed, "the dominant frame is a 132px letterbox");
    // The logo appears in a minority of keyframes, so the typical box is still the 816 letterbox and
    // the logo is cropped away — a union would have reported a taller box here and kept the bar.
    assert_eq!(report.content.as_ref().unwrap().h, 816, "a transient logo must not hold the bar open");
    assert_eq!(
        crate::crop::bake_clap(&cfg, &fp, &report).await,
        crate::crop::Bake::Baked,
        "MP4Box should write the clap box"
    );

    let out = std::process::Command::new("ffprobe")
        .args(["-hide_banner", "-v", "error", "-show_streams"])
        .arg(&fp)
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("crop_top=132") && s.contains("crop_bottom=132"), "clap not read back: {s}");
}

// A mixed-framing trailer (mostly letterboxed + genuine full-frame shots) must NOT be cropped — the
// full-frame guard keeps the whole frame rather than slicing those shots. Reproduces the real
// Monsters-vs-Aliens regression. Needs ffmpeg; run locally with: cargo test -- --ignored
#[tokio::test]
#[ignore]
async fn detect_does_not_crop_mixed_framing_end_to_end() {
    let dir = temp_dir();
    let fp = dir.join("mixedvid0001.mp4");
    // 6s of a full-frame testsrc with 131px black bars painted top+bottom (a 2.35 letterbox) — EXCEPT
    // the last ~1.2s, which is left full-frame (genuine full-frame shots).
    let ok = std::process::Command::new("ffmpeg")
        .args([
            "-y", "-f", "lavfi", "-i", "testsrc=size=1920x1080:rate=24:d=6",
            "-vf",
            "drawbox=x=0:y=0:w=1920:h=131:color=black:t=fill:enable='lt(t,4.8)',drawbox=x=0:y=949:w=1920:h=131:color=black:t=fill:enable='lt(t,4.8)'",
            "-c:v", "libx264", "-g", "6", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ])
        .arg(&fp)
        .status().unwrap().success();
    assert!(ok, "ffmpeg failed to build the mixed-framing fixture");

    let cfg = test_cfg(dir);
    let report = crate::crop::detect(&cfg, "mixedvid0001", &fp).await.expect("detect returned a report");
    // The dominant framing is the 2.35 letterbox, but real full-frame shots are present → play full.
    assert!(!report.letterboxed, "a trailer with genuine full-frame shots must not be cropped");
    assert_eq!(report.content.as_ref().unwrap().h, 1080, "full frame kept, not sliced to the letterbox");
    // And nothing is baked, so an AVPlayer sees the full frame.
    assert_eq!(
        crate::crop::bake_clap(&cfg, &fp, &report).await,
        crate::crop::Bake::Skipped,
        "no clap baked for a full-frame report"
    );
}

// A PORTRAIT trailer (landscape clip padded into a tall frame) must NOT be letterbox-cropped — that
// baked a clap that broke the billboard. detect() should report full-frame and bake nothing. Needs
// ffmpeg + MP4Box; run locally with: cargo test -- --ignored
#[tokio::test]
#[ignore]
async fn detect_does_not_crop_portrait_end_to_end() {
    let dir = temp_dir();
    let fp = dir.join("portrait0001.mp4");
    // A 720x404 landscape testsrc padded into a 720x1280 portrait frame (huge top/bottom padding).
    let ok = std::process::Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc=size=720x404:rate=24:d=3",
            "-vf",
            "pad=720:1280:0:438:color=black",
            "-c:v",
            "libx264",
            "-g",
            "6",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
        ])
        .arg(&fp)
        .status()
        .unwrap()
        .success();
    assert!(ok, "ffmpeg failed to build the portrait fixture");

    let cfg = test_cfg(dir);
    let report = crate::crop::detect(&cfg, "portrait0001", &fp).await.expect("detect returned a report");
    assert!(!report.letterboxed, "a portrait source must not be letterbox-cropped");
    assert_eq!(report.content.as_ref().unwrap().h, 1280, "full portrait frame kept, not a thin strip");
    assert_eq!(
        crate::crop::bake_clap(&cfg, &fp, &report).await,
        crate::crop::Bake::Skipped,
        "no clap baked for a portrait trailer"
    );
}

#[tokio::test]
async fn cache_available_reflects_dir_usability() {
    let dir = temp_dir();
    let cfg_ok = test_cfg(dir.clone());
    assert!(crate::play::cache_available(&cfg_ok).await, "a normal temp dir is usable");

    // Point cache_dir under a regular file so create_dir_all fails (ENOTDIR) → unavailable.
    let file = dir.join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let mut cfg_bad = test_cfg(dir);
    cfg_bad.cache_dir = file.join("cache");
    cfg_bad.ytdlp_cache = cfg_bad.cache_dir.join("yt-dlp");
    assert!(!crate::play::cache_available(&cfg_bad).await, "cache under a file is unusable");

    // A positive answer is memoised for a few seconds — `tokio::fs` is spawn_blocking underneath, so
    // asking per request cost two dispatches to the blocking pool ahead of every cache hit. A
    // FAILURE is never memoised, or a volume coming back would have to wait out a TTL first.
    assert!(!crate::play::cache_available(&cfg_bad).await, "a failure was remembered as an answer");
    assert!(crate::play::cache_available(&cfg_ok).await, "a working volume must still be usable");
}

#[test]
fn error_responses_are_no_store() {
    let e = crate::httputil::error(hyper::StatusCode::SERVICE_UNAVAILABLE, "cache_unavailable", "x");
    assert_eq!(e.headers().get("cache-control").unwrap(), "no-store");
    // 404 text path too.
    let t = crate::httputil::text(hyper::StatusCode::NOT_FOUND, "not found");
    assert_eq!(t.headers().get("cache-control").unwrap(), "no-store");
    // ...but a 2xx isn't forced to no-store.
    let ok = crate::httputil::text(hyper::StatusCode::OK, "ok");
    assert!(ok.headers().get("cache-control").is_none());
}

#[test]
fn clap_params_are_center_relative() {
    // Symmetric 2.35 letterbox → offsets 0 (content centre == frame centre).
    let centered = crate::crop::report_from(
        "x",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 },
    );
    assert_eq!(crate::crop::clap_params(&centered), Some((1920, 816, 0, 0)));

    // Logo kept in the bottom bar → content off-centre downward → positive vertOff (num over 2).
    let off = crate::crop::report_from(
        "x",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 922, x: 0, y: 132 },
    );
    assert_eq!(crate::crop::clap_params(&off), Some((1920, 922, 0, 106))); // 106/2 = 53px

    // Not letterboxed → nothing to bake.
    let full = crate::crop::report_from(
        "x",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 1080, x: 0, y: 0 },
    );
    assert_eq!(crate::crop::clap_params(&full), None);
}

#[test]
fn eviction_evicts_real_files_but_skips_partial_dotfiles() {
    let dir = temp_dir();
    std::fs::write(dir.join("aaaaaaaaaa1.mp4"), vec![0u8; 100]).unwrap();
    std::fs::write(dir.join(".bbbbbb.123.0.partial.mp4"), vec![0u8; 100]).unwrap();
    let mut cfg = test_cfg(dir.clone());
    // Scratch counts toward the cap but is not evictable, so leave a budget above it — a cap at or
    // below the scratch means evicting trailers cannot help, which is the case below.
    cfg.cache_max_bytes = 150;
    crate::play::evict_if_needed(&cfg);
    assert!(!dir.join("aaaaaaaaaa1.mp4").exists(), "completed file should be evicted");
    assert!(
        dir.join(".bbbbbb.123.0.partial.mp4").exists(),
        "in-progress .partial temp must be skipped by eviction"
    );
}

/// `valid_lang` accepts either case by design, but everything downstream is case-sensitive: the
/// resolve cache keys on the raw string, and KinoCheck's language pick is a `starts_with("de")`.
/// So "DE" got its own cache entry AND silently fell back to English trailers.
#[tokio::test]
async fn an_uppercase_language_is_the_same_language() {
    let fake = FakeUpstream::new(&["dQw4w9WgXcQ"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());
    let headers = hyper::header::HeaderMap::new();

    let _ = crate::addon::handle_meta(&state, &headers, None, "movie", "tt0111161", "lang=de").await;
    let after_lower = fake.calls();
    let _ = crate::addon::handle_meta(&state, &headers, None, "movie", "tt0111161", "lang=DE").await;
    assert_eq!(
        fake.calls(),
        after_lower,
        "\"DE\" resolved separately from \"de\" instead of hitting the same cache entry"
    );
}

/// Repeat /meta for ONE title is the common burst — a re-rendered detail screen, a retry, two
/// installs on the same film. Those all de-dupe onto a single download, so spending a permit each
/// let three of them exhaust the cap and lock every other title out until that download finished.
#[tokio::test]
async fn duplicates_of_one_title_do_not_spend_the_prewarm_cap() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_dir();
    let slow = dir.join("slow-ytdlp");
    std::fs::write(&slow, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = slow.to_string_lossy().into_owned();
    let state = build_state_cfg(
        cfg,
        Box::new(FakeUpstream::new(&["dQw4w9WgXcQ"], None)),
        always_playable(),
        crate::state::default_prewarm(),
    );

    // The same title, over and over.
    for _ in 0..10 {
        (state.prewarm)(state.clone(), "sameVid0001".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // A different title must still be able to start.
    (state.prewarm)(state.clone(), "otherVid002".to_string());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let running: Vec<String> = state.in_flight.lock().unwrap().keys().cloned().collect();
    assert!(
        running.iter().any(|k| k == "otherVid002"),
        "duplicates of one title locked the cap; in flight: {running:?}"
    );
}

/// PREWARM_MAX was a check-then-act count of `in_flight`, but that map is only written after
/// `fetch_trailer`'s first await — so a browse burst all read the same stale count and all spawned,
/// queueing speculative downloads ahead of the /play the viewer is actually waiting for.
#[tokio::test]
async fn a_browse_burst_cannot_outrun_the_prewarm_cap() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_dir();
    // A yt-dlp that just blocks, so a started prewarm stays started and the count is observable.
    let slow = dir.join("slow-ytdlp");
    std::fs::write(&slow, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = slow.to_string_lossy().into_owned();
    let state = build_state_cfg(
        cfg,
        Box::new(FakeUpstream::new(&["dQw4w9WgXcQ"], None)),
        always_playable(),
        crate::state::default_prewarm(),
    );

    // Fire far more than the cap in one go, exactly as a shelf of /meta calls would.
    for i in 0..20 {
        (state.prewarm)(state.clone(), format!("vid{i:0>8}"));
    }
    // Let every spawned task get past its first await, which is where it registers itself.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let started = state.in_flight.lock().unwrap().len();
    assert!(
        started <= crate::PREWARM_MAX,
        "{started} prewarms running against a cap of {}",
        crate::PREWARM_MAX
    );
}

/// An `unknown` crop means ffmpeg failed or the file was not there — a transient condition its own
/// doc says "a later call retries". It was served with the same year-long `immutable` as a real
/// rect, so one hiccup cost that trailer its de-letterboxing until the client cleared its cache.
#[tokio::test]
async fn an_unknown_crop_is_not_cached_by_the_client() {
    let dir = temp_dir();
    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = "/nonexistent/yt-dlp".into(); // nothing cached, and the download cannot start
    let state = build_state_cfg(
        cfg,
        Box::new(FakeUpstream::new(&["dQw4w9WgXcQ"], None)),
        always_playable(),
        noop_prewarm(),
    );

    let cc_of = |r: hyper::Response<crate::httputil::Body>| {
        r.headers().get("cache-control").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
    };

    let resp = crate::crop::handle_crop(state.clone(), "dQw4w9WgXcQ".into()).await;
    assert_eq!(resp.status(), hyper::StatusCode::OK, "an unknown crop still answers 200");
    let cc = cc_of(resp);
    assert!(!cc.contains("immutable"), "an unknown crop was cached as if it were a real rect: {cc}");

    // A detected rect is immutable per video and must still cache hard.
    let known = crate::crop::refine_report(crate::crop::report_from(
        "dQw4w9WgXcQ",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 },
    ));
    let cc = cc_of(crate::crop::json(&known));
    assert!(cc.contains("immutable"), "a detected rect stopped caching: {cc}");
}

/// The format ladder degrades in quality order, but the lower rungs were a fixed 720/480 — so a
/// cap below 720 was matched by a rung LOOSER than itself, and MAX_HEIGHT=480 could fetch and cache
/// a 720p file whenever the ≤480 avc1 rendition was missing.
#[test]
fn the_format_ladder_never_exceeds_the_configured_cap() {
    let heights_in = |fmt: &str| -> Vec<u32> {
        fmt.split("height<=").skip(1).filter_map(|t| t.split(']').next()?.parse::<u32>().ok()).collect()
    };
    for (cap, expect_rungs) in [
        ("1080", vec![1080, 1080, 720, 720, 480, 480]),
        ("720", vec![720, 720, 480, 480]),
        ("480", vec![480, 480]),
        ("360", vec![360, 360]),
    ] {
        std::env::set_var("MAX_HEIGHT", cap);
        let cfg = crate::config::Config::from_env();
        std::env::remove_var("MAX_HEIGHT");
        let cap_n: u32 = cap.parse().unwrap();
        let got = heights_in(&cfg.ytdlp_format);
        assert!(got.iter().all(|h| *h <= cap_n), "cap {cap}: ladder reaches above it: {got:?}");
        assert_eq!(got, expect_rungs, "cap {cap}");
    }
    // A MAX_HEIGHT the ladder cannot use must cost the setting, not the service. yt-dlp rejects a
    // malformed filter while BUILDING the selector, so `height<=abc` in the first rung killed the
    // whole chain — terminal fallback included — and every trailer 502'd until the env was fixed.
    // "0" and "12" parse but are the opposite failure: no rung can match, so selection falls
    // through to the uncapped terminal fallback and the cap becomes no cap at all.
    for bad in ["abc", "-5", "1e3", "1080p", "", "  ", "0", "12"] {
        std::env::set_var("MAX_HEIGHT", bad);
        let cfg = crate::config::Config::from_env();
        std::env::remove_var("MAX_HEIGHT");
        assert_eq!(cfg.max_height, "1080", "MAX_HEIGHT={bad:?} was not normalised");
        // Every `height<=` in the selector must be followed by a number. If one isn't, yt-dlp
        // rejects the whole chain while building it.
        let total = cfg.ytdlp_format.matches("height<=").count();
        assert_eq!(
            heights_in(&cfg.ytdlp_format).len(),
            total,
            "MAX_HEIGHT={bad:?} put a non-numeric filter in the selector: {}",
            cfg.ytdlp_format
        );
    }

    // The terminal fallback must still pin the hardware-decode codecs.
    std::env::set_var("MAX_HEIGHT", "1080");
    let cfg = crate::config::Config::from_env();
    std::env::remove_var("MAX_HEIGHT");
    assert!(cfg.ytdlp_format.ends_with("18/b[ext=mp4][vcodec^=avc1][acodec^=mp4a]"), "{}", cfg.ytdlp_format);
}

/// The search fallback is the third source of YouTube ids, after TMDB and KinoCheck, and they all
/// become filenames. It was the one left ungated when the other two were fixed.
#[cfg(unix)]
#[tokio::test]
async fn search_ids_are_gated_like_every_other_source() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_dir();
    let fake = dir.join("fake-ytdlp");
    // yt-dlp prints one id per line; these are what a hostile or broken source could emit.
    std::fs::write(
        &fake,
        "#!/bin/sh\nprintf '../../../../tmp/evil\\n/etc/passwd\\nhas/slash\\nsh\\ndQw4w9WgXcQ\\n'\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = fake.to_string_lossy().into_owned();
    assert_eq!(
        crate::ytdlp::search(&cfg, "anything", 5).await,
        Some(vec!["dQw4w9WgXcQ".to_string()]),
        "an id that is not a YouTube id must not reach a filename"
    );
}

/// `/health`'s upstream counter is process-wide, but TMDB keys are per-install. A 401 means THIS
/// install's key is wrong — counting it let one bad key report "TMDB has been failing" for
/// everyone, and let a healthy install's traffic clear a broken one's failures so they never
/// surfaced. Only faults that are actually about the upstream count.
#[tokio::test]
async fn a_bad_install_key_does_not_mark_the_upstream_down() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn server(status: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = "{}";
                let resp = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        format!("http://{addr}")
    }

    let fails_after = |status: &'static str| async move {
        let mut cfg = test_cfg(temp_dir());
        cfg.tmdb_base = server(status).await;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());
        let _ = up.tmdb_candidates("bad-key", "tt0111161", "movie", "en").await;
        crate::upstream::Upstream::recent_failures(&up)
    };

    // KinoCheck is a fallback; its outage does not mean trailers are broken.
    {
        let mut cfg = test_cfg(temp_dir());
        cfg.kinocheck_base = server("503 Service Unavailable").await;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());
        let _ = up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await;
        assert_eq!(
            crate::upstream::Upstream::recent_failures(&up),
            0,
            "a KinoCheck outage reported TMDB as down"
        );
    }

    assert_eq!(fails_after("401 Unauthorized").await, 0, "one install's bad key marked TMDB down");
    assert_eq!(fails_after("403 Forbidden").await, 0, "one install's bad key marked TMDB down");
    assert!(fails_after("503 Service Unavailable").await > 0, "a real upstream fault must count");
    assert!(fails_after("429 Too Many Requests").await > 0, "throttling must count");
}

/// The scheme in the play URL comes from a client-supplied header. Reflected unchecked it produced
/// `javascript://host/...` — the same spoofing the Host filter beside it was written to stop.
#[tokio::test]
async fn a_forwarded_proto_is_a_scheme_or_it_is_http() {
    use hyper::header::{HeaderMap, HeaderValue};
    let base = |proto: &str| {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-proto", HeaderValue::from_str(proto).unwrap());
        h.insert("host", HeaderValue::from_static("reel.local:8092"));
        crate::addon::self_base(None, &h, 8092)
    };
    assert_eq!(base("https"), "https://reel.local:8092");
    assert_eq!(base("http"), "http://reel.local:8092");
    // A proxy that title-cases the header still means https; downgrading it to http would hand
    // every play URL back as plaintext on a TLS-fronted install.
    assert_eq!(base("HTTPS"), "https://reel.local:8092");
    assert_eq!(base("Https"), "https://reel.local:8092");
    for hostile in ["javascript", "https://attacker.evil", "file", ""] {
        assert_eq!(base(hostile), "http://reel.local:8092", "accepted scheme {hostile:?}");
    }
}

/// yt-dlp forks ffmpeg to do the merge, so killing the direct child left the grandchild running —
/// and it kept writing to the temp path we had just deleted. The download runs in its own process
/// group so the whole tree can be reaped; this pins that the group kill reaches a grandchild.
#[cfg(unix)]
#[tokio::test]
async fn killing_the_group_reaches_a_grandchild() {
    use std::os::unix::process::CommandExt;
    let dir = temp_dir();
    let marker = dir.join("grandchild-alive");
    // A parent that forks a long-lived child, exactly like yt-dlp spawning ffmpeg.
    let script = dir.join("parent.sh");
    std::fs::write(&script, format!("#!/bin/sh\nsh -c 'sleep 30; : > {}' &\nsleep 30\n", marker.display()))
        .unwrap();
    std::fs::set_permissions(
        &script,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755),
    )
    .unwrap();

    let mut cmd = std::process::Command::new(&script);
    cmd.process_group(0);
    let mut child = cmd.spawn().unwrap();
    let pgid = child.id();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    crate::ytdlp::kill_group(Some(pgid));
    let _ = child.wait();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Still alive? Then it is holding CPU and will finish writing to a path nobody supervises.
    let alive = std::process::Command::new("pgrep")
        .args(["-g", &pgid.to_string()])
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    assert!(!alive, "a grandchild outlived the group kill");
}

/// Ids from TMDB/KinoCheck become a cache filename and a yt-dlp `-o` path, and `/meta` prewarms on
/// them with no client involvement — so a traversal in upstream data wrote outside the cache dir.
/// The inbound imdb id was already checked for exactly this reason; this is the other direction.
#[test]
fn a_traversing_id_from_upstream_is_not_a_candidate() {
    use serde_json::json;
    let results = vec![
        json!({"site": "YouTube", "type": "Trailer", "official": true, "key": "../../../../tmp/evil"}),
        json!({"site": "YouTube", "type": "Trailer", "official": true, "key": "/etc/cron.d/evil"}),
        json!({"site": "YouTube", "type": "Trailer", "official": true, "key": "has/slash"}),
        json!({"site": "YouTube", "type": "Trailer", "official": true, "key": "sh"}),
        json!({"site": "YouTube", "type": "Trailer", "official": true, "key": "dQw4w9WgXcQ"}),
    ];
    assert_eq!(
        crate::upstream::pick_trailer_candidates(&results, "en"),
        vec!["dQw4w9WgXcQ".to_string()],
        "an id that is not a YouTube id must not reach a filename"
    );
}

/// The imdb id becomes an upstream request path AND part of a resolve cache key that is written to
/// disk at shutdown. Unbounded, a caller could park a 60-digit id in that key and push the file past
/// the size the loader will read — which does not fail loudly, it just discards the whole parked
/// cache on every boot from then on, because each shutdown rewrites the same oversized file. /meta
/// has no gate in front of it.
#[tokio::test]
async fn an_imdb_id_is_bounded_before_it_reaches_a_cache_key() {
    let fake = FakeUpstream::new(&["vidKey12345"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());
    let base = spawn_server(state.clone()).await;

    let huge = format!("tt{}", "1".repeat(60));
    let body: serde_json::Value =
        reqwest::get(format!("{base}/meta/movie/{huge}.json")).await.unwrap().json().await.unwrap();

    assert!(
        body["meta"]["links"].as_array().is_some_and(|a| a.is_empty()),
        "an unbounded id was resolved rather than rejected"
    );
    assert_eq!(fake.calls(), 0, "an unbounded id reached the upstream");
    assert!(
        state.yt_cache.lock().unwrap().is_empty(),
        "an unbounded id reached the resolve cache, and from there the file parked at shutdown"
    );

    // A real one, and a plausibly longer future one, still work.
    for ok in ["tt0111161", "tt10000000"] {
        let body: serde_json::Value =
            reqwest::get(format!("{base}/meta/movie/{ok}.json")).await.unwrap().json().await.unwrap();
        assert!(
            body["meta"]["links"].as_array().is_some_and(|a| !a.is_empty()),
            "{ok} was rejected, but it is a real shape"
        );
    }
}

/// A YouTube video id is a base64url-encoded 64-bit value: exactly 11 characters, and it has been
/// for the life of the service. This gate is the only check standing in front of /play and /crop,
/// both of which spend a download permit and a yt-dlp process on whatever they are handed, and it is
/// also what the cache sweep uses to tell a published trailer from abandoned scratch.
#[test]
fn a_video_id_is_exactly_eleven_characters() {
    assert!(crate::is_valid_vid("dSdWpY2Bxsc"), "a real id must still pass");
    assert!(crate::is_valid_vid("_-Aa09Zz123"), "the full base64url alphabet is legal");
    for bad in [
        "",
        "short",
        "dSdWpY2Bxs",   // 10
        "dSdWpY2Bxscc", // 12
        "dSdWpY2Bxs/",  // right length, path separator
        "dSdWpY2Bxs.",  // right length, extension games
        "../../etc",
    ] {
        assert!(!crate::is_valid_vid(bad), "{bad:?} was accepted as a YouTube id");
    }
}

/// And the sink refuses it too, so a future caller cannot reintroduce the same hole.
#[tokio::test]
async fn fetch_trailer_refuses_an_id_that_is_not_a_youtube_id() {
    let dir = temp_dir();
    let state = build_state(
        dir.clone(),
        Box::new(FakeUpstream::new(&["dQw4w9WgXcQ"], None)),
        always_playable(),
        noop_prewarm(),
    );
    let err = crate::play::fetch_trailer(state, "../../../../tmp/evil".into())
        .await
        .expect_err("a traversing id must be refused");
    assert_eq!(err.status, 400);
    let escaped = dir.join("../../../../tmp/evil.mp4");
    assert!(!escaped.exists(), "a file was written outside the cache dir");
}

#[test]
fn stale_partials_are_reclaimed_but_live_ones_are_left_alone() {
    use std::time::{Duration, SystemTime};
    let dir = temp_dir();
    // Dot-prefixed so eviction skips them — which is why nothing counted them toward the cap and
    // nothing ever removed them. A crash or redeploy mid-download left one on disk forever.
    // The names yt-dlp actually leaves behind mid-download, not just the finished temp name:
    // `<tmp>.part` while fetching, and a per-format `.f<id>.<ext>.part` for each stream it merges.
    let abandoned = [
        ".aaaaaa.1.0.partial.mp4",
        ".aaaaaa.1.0.partial.mp4.part",
        ".aaaaaa.1.0.partial.f137.mp4.part",
        ".aaaaaa.1.0.partial.f140.m4a.part",
        // MP4Box's own working copy, from `-tmp <cache_dir>`: no dot, no extension. A SIGKILL mid
        // bake leaves it, and it matched neither cleanup filter.
        "_libgpac_64884_0x133704950_4587_352815186",
    ];
    for name in abandoned {
        std::fs::write(dir.join(name), vec![0u8; 100]).unwrap();
    }
    std::fs::write(dir.join(".bbbbbb.2.0.partial.mp4.part"), vec![0u8; 100]).unwrap();
    std::fs::write(dir.join("cccccc.mp4"), vec![0u8; 100]).unwrap();
    // Older than any download can still be running: the download timeout is 240s.
    let old = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
    for name in abandoned {
        let f = std::fs::File::open(dir.join(name)).unwrap();
        f.set_times(std::fs::FileTimes::new().set_modified(old).set_accessed(old)).unwrap();
    }

    crate::play::sweep_partials(&test_cfg(dir.clone()));
    for name in abandoned {
        assert!(!dir.join(name).exists(), "{name} was left on disk");
    }
    assert!(
        dir.join(".bbbbbb.2.0.partial.mp4.part").exists(),
        "a live download was deleted under its writer"
    );
    assert!(dir.join("cccccc.mp4").exists(), "the sweep touched a finished trailer");

    // yt-dlp keeps its player-JS cache in a subdirectory here; the sweep must not touch it.
    let ytdlp_cache = dir.join("yt-dlp");
    std::fs::create_dir_all(&ytdlp_cache).unwrap();
    std::fs::write(ytdlp_cache.join("player.json"), b"{}").unwrap();
    let old_dir = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
    let f = std::fs::File::open(&ytdlp_cache).unwrap();
    let _ = f.set_times(std::fs::FileTimes::new().set_modified(old_dir).set_accessed(old_dir));
    crate::play::sweep_partials(&test_cfg(dir.clone()));
    assert!(ytdlp_cache.join("player.json").exists(), "the sweep removed yt-dlp's own cache");
}

#[test]
fn eviction_ttl_drops_stale_but_keeps_fresh() {
    use std::time::{Duration, SystemTime};
    let dir = temp_dir();
    // Real `<vid>.mp4` names: anything else on the volume is scratch, which the TTL pass leaves to
    // sweep_partials rather than deleting under a live download.
    std::fs::write(dir.join("freshVid001.mp4"), vec![0u8; 100]).unwrap();
    std::fs::write(dir.join("staleVid001.mp4"), vec![0u8; 100]).unwrap();
    // Age the stale one's last-access to 20 days ago (past a 14-day TTL); the other stays at "now".
    let old = SystemTime::now() - Duration::from_secs(20 * 24 * 60 * 60);
    let f = std::fs::File::open(dir.join("staleVid001.mp4")).unwrap();
    f.set_times(std::fs::FileTimes::new().set_accessed(old)).unwrap();
    let mut cfg = test_cfg(dir.clone());
    cfg.cache_ttl = Duration::from_secs(14 * 24 * 60 * 60); // 14-day TTL
    cfg.cache_max_bytes = u64::MAX; // isolate the TTL: the size cap must not interfere
    crate::play::evict_if_needed(&cfg);
    assert!(!dir.join("staleVid001.mp4").exists(), "trailer past the last-access TTL should be evicted");
    assert!(dir.join("freshVid001.mp4").exists(), "recently-served trailer must be kept");
}

/// Serving a warm file must reach the bytes without consulting the extractor at all, and must stamp
/// atime on the way — that stamp is the only thing making an in-use trailer sort as recently used,
/// and `evict_if_needed` removes oldest-atime first after every download.
///
/// Both halves are now done by the single blocking call that opens the file, which also makes the
/// stamp deterministic: it completes before the response is built, where the old fire-and-forget
/// touch could still be in flight while eviction was already reading the directory.
#[tokio::test]
async fn a_warm_serve_stamps_atime_and_never_reaches_yt_dlp() {
    use std::time::{Duration, SystemTime};
    let dir = temp_dir();
    seed_cache(&dir, "warmVid0001", 100);

    // Age the stamp well past anything a test could take, so a bump is unmistakable.
    let old = SystemTime::now() - Duration::from_secs(20 * 24 * 60 * 60);
    let f = std::fs::File::open(dir.join("warmVid0001.mp4")).unwrap();
    f.set_times(std::fs::FileTimes::new().set_accessed(old)).unwrap();
    drop(f);

    let mut cfg = test_cfg(dir.clone());
    // If the warm path tries to download, this fails loudly rather than quietly succeeding.
    cfg.ytdlp = "/nonexistent/yt-dlp".into();
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    let resp = crate::play::handle_play(state, &hyper::HeaderMap::new(), "warmVid0001".into()).await;
    assert_eq!(resp.status(), 200, "a cached trailer was not served from cache");

    let atime = std::fs::metadata(dir.join("warmVid0001.mp4")).unwrap().accessed().unwrap();
    assert!(
        atime.elapsed().unwrap() < Duration::from_secs(60),
        "the serve did not stamp atime, so eviction cannot tell this file is in use"
    );
}

#[tokio::test]
async fn play_unsatisfiable_range_is_416() {
    let dir = temp_dir();
    let size = seed_cache(&dir, "cachedVid03", 100);
    let state = build_state(dir, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let r = reqwest::Client::new()
        .get(format!("{base}/play/cachedVid03.mp4"))
        .header("range", format!("bytes={}-", size + 10))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 416);
}

/// A failed lookup and a title with no trailer both arrive as an empty Vec, and the negative cache
/// pinned either for an hour under a key that excludes the credential — so one install's 401, or one
/// TMDB blip, blanked that title for every install while /health stayed green. A failure now gets a
/// short cooldown instead: long enough not to stampede a sick upstream, short enough that a recovery
/// shows up in about a minute rather than an hour.
#[tokio::test]
async fn a_failed_lookup_cools_down_instead_of_caching_no_trailer() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    fake.fail_next();
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());

    // Within the cooldown the failure is not re-asked — that is what bounds the stampede.
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS / 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    assert_eq!(fake.calls(), after, "a failed lookup is not rate-limited at all");

    // Past it — and long before a real negative would have expired — the recovery is visible.
    fake.set_tmdb(&["realTrailer"]);
    clock.advance(crate::YT_FAIL_TTL_MS);
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(
        ids.first().map(String::as_str),
        Some("realTrailer"),
        "a failed lookup was cached as 'this title has no trailer'"
    );
}

/// A KinoCheck fault must NOT move the fault counter: it is a fallback, and treating its outage as
/// "we got no answer" made every resolve refuse to cache — turning one dead fallback into unbounded
/// repeat lookups, which then fed the very 429s that kept it dead.
///
/// Exercised against the real HttpUpstream, because the thing under test is which URLs count, and a
/// fake upstream cannot get that wrong. Both bases point at a closed port, so each call is a
/// transport error with no server needed.
#[tokio::test]
async fn only_the_primary_source_moves_the_fault_counter() {
    let mut cfg = test_cfg(temp_dir());
    cfg.tmdb_base = "http://127.0.0.1:1/tmdb".to_string();
    cfg.kinocheck_base = "http://127.0.0.1:1/kinocheck".to_string();
    let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

    // Both report NoAnswer for themselves — the caller decides which one matters, per request.
    assert_eq!(up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await, Err(NoAnswer));
    assert_eq!(up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await, Err(NoAnswer));
    // ...and neither marks the UPSTREAM down, which is a separate question from this call's outcome.
    assert_eq!(up.recent_failures(), 1, "only the primary source speaks for /health");
}

/// A keyless lookup asks a narrower question — only KinoCheck runs — so its answer lives under its
/// own cache key. Sharing it let a config-less /meta blank titles for installs that DO have a key.
#[tokio::test]
async fn a_keyless_answer_does_not_blank_the_title_for_keyed_installs() {
    let fake = FakeUpstream::new(&[], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());

    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());

    fake.set_tmdb(&["realTrailer"]);
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(
        ids.first().map(String::as_str),
        Some("realTrailer"),
        "a keyless lookup was cached as the keyed answer"
    );
}

/// The same asymmetry on the OTHER credential. An install with no KinoCheck key can ask a narrower
/// question too — KinoCheck answers keyless requests until it rate-limits or rejects one — and its
/// thinner candidate list was published under the shared key, where an install that DOES have a key
/// then read it for a full YT_TTL_MS. It costs an alternate rather than a primary, which is exactly
/// why it went unnoticed: the trailer still plays, there is just nothing to fall back to.
#[tokio::test]
async fn a_kinocheck_keyless_answer_does_not_blank_the_alternate_for_keyed_installs() {
    let fake = FakeUpstream::new(&["primaryVid1"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());

    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids, vec!["primaryVid1"], "no KinoCheck key, so no fallback candidate");

    // Now the source that install could not ask has something to say.
    fake.set_kc(Some("kcAltVid111"));
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", Some("kc-key"), "tt0111161", "movie", "en")
            .await
            .ids;
    assert_eq!(
        ids,
        vec!["primaryVid1", "kcAltVid111"],
        "a KinoCheck-keyless answer was cached as the keyed one, losing the fallback trailer"
    );
}

/// ...and the keyless answer is still cached in its own right, at the FULL negative TTL. Treating a
/// missing key as a transient failure meant re-asking KinoCheck every 60s, forever, per title.
#[tokio::test]
async fn a_keyless_answer_is_cached_for_a_full_negative_ttl() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS * 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    assert_eq!(fake.calls(), after, "a missing key was priced as a transient blip and re-asked");
}

/// A genuine "no trailer" must still be negative-cached for the FULL hour, or every browse re-hits
/// TMDB. Nothing pinned this arm: the failure tests all advance the clock past the 60s cooldown, so
/// collapsing every negative onto the cooldown — a 60x load increase — passed the whole suite.
#[tokio::test]
async fn a_real_empty_answer_is_cached_for_the_full_negative_ttl() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    let after = fake.calls();

    // Well past the failure cooldown — a real answer must not be re-asked on that schedule.
    clock.advance(crate::YT_FAIL_TTL_MS * 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    assert_eq!(fake.calls(), after, "a real 'no trailer' was re-asked at the failure cooldown");

    // ...and it does expire eventually, so a geo-block or a late-added trailer is picked up.
    fake.set_tmdb(&["realTrailer"]);
    clock.advance(crate::YT_NEG_TTL_MS);
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids.first().map(String::as_str), Some("realTrailer"), "the negative cache never expired");
}

/// A cache cap smaller than one trailer parses cleanly and inverts the setting: eviction runs right
/// after the rename and sees the file it just published, so every /play downloads, deletes its own
/// output, retries once, and 500s — forever, on every request.
#[test]
fn a_cache_cap_too_small_to_hold_a_trailer_falls_back() {
    for bad in ["0", "4", "1048576"] {
        std::env::set_var("CACHE_MAX_BYTES", bad);
        let cfg = crate::config::Config::from_env();
        std::env::remove_var("CACHE_MAX_BYTES");
        assert_eq!(
            cfg.cache_max_bytes,
            4 * 1024 * 1024 * 1024,
            "CACHE_MAX_BYTES={bad} was accepted; eviction would delete each trailer as it is written"
        );
    }
    // A real, usable cap must still be honoured.
    std::env::set_var("CACHE_MAX_BYTES", "536870912");
    let cfg = crate::config::Config::from_env();
    std::env::remove_var("CACHE_MAX_BYTES");
    assert_eq!(cfg.cache_max_bytes, 536_870_912, "a usable cap was overridden");
}

/// CLAP is the documented escape hatch for a mis-cropped trailer. Recognising only "0" meant the
/// three other obvious spellings silently left baking enabled.
#[test]
fn the_clap_escape_hatch_answers_to_more_than_one_spelling() {
    for off in ["0", "false", "off", "no", "FALSE", " off ", ""] {
        std::env::set_var("CLAP", off);
        let cfg = crate::config::Config::from_env();
        std::env::remove_var("CLAP");
        assert!(!cfg.bake_clap, "CLAP={off:?} left clap baking enabled");
    }
    for on in ["1", "true", "yes"] {
        std::env::set_var("CLAP", on);
        let cfg = crate::config::Config::from_env();
        std::env::remove_var("CLAP");
        assert!(cfg.bake_clap, "CLAP={on:?} disabled clap baking");
    }
}

/// A failed download used to unlink only the final temp, leaving yt-dlp's sibling scratch on disk.
/// Those are dot-prefixed, so the size cap neither counts nor evicts them — real usage exceeded the
/// cap by every failed download until the hourly sweep's 30-minute grace expired.
#[tokio::test]
async fn a_failed_download_leaves_none_of_its_scratch_behind() {
    let dir = temp_dir();
    let cfg = test_cfg(dir.clone());
    let tmp = dir.join(".vidvidvid11.42.0.partial.mp4");
    for name in [
        ".vidvidvid11.42.0.partial.mp4",
        ".vidvidvid11.42.0.partial.mp4.part",
        // yt-dlp puts `.f<id>` on EITHER side of the extension: inserted when the stream's ext
        // matches the output's (video, always mp4 under this ladder), appended when it differs
        // (audio, m4a). Matching the full filename caught only the small audio one and left the
        // multi-hundred-MB video partial — which is the whole leak.
        ".vidvidvid11.42.0.partial.f137.mp4",
        ".vidvidvid11.42.0.partial.f137.mp4.part",
        ".vidvidvid11.42.0.partial.f137.mp4.part-Frag3",
        ".vidvidvid11.42.0.partial.mp4.f140.m4a.part",
    ] {
        std::fs::write(dir.join(name), b"x").unwrap();
    }
    // Another download's scratch, and a published trailer: neither is ours to remove.
    std::fs::write(dir.join(".vidvidvid11.99.0.partial.mp4.part"), b"x").unwrap();
    std::fs::write(dir.join("cccccccccc1.mp4"), b"x").unwrap();

    crate::play::remove_temp_set(&cfg, &tmp).await;

    let left: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".vidvidvid11.42.0."))
        .collect();
    assert!(left.is_empty(), "the failed download left scratch the size cap cannot see: {left:?}");
    assert!(dir.join(".vidvidvid11.99.0.partial.mp4.part").exists(), "another download's temp was removed");
    assert!(dir.join("cccccccccc1.mp4").exists(), "a published trailer was removed");
}

/// /meta and the manifest embed play URLs built from the forwarded host and scheme, and go out
/// `public, max-age=604800`. Without naming those inputs, a shared cache may hand one requester's
/// body — pointing at an authority they chose — to everyone else.
#[test]
fn a_cacheable_body_names_the_headers_its_urls_came_from() {
    let res = crate::httputil::json(
        hyper::StatusCode::OK,
        &serde_json::json!({"ok": true}),
        &[("cache-control", "public, max-age=604800")],
    );
    let vary = res.headers().get("vary").map(|v| v.to_str().unwrap().to_ascii_lowercase());
    let vary = vary.unwrap_or_default();
    assert!(
        vary.contains("x-forwarded-host"),
        "cacheable body did not vary on the host it embedded: {vary:?}"
    );
    assert!(
        vary.contains("x-forwarded-proto"),
        "cacheable body did not vary on the scheme it embedded: {vary:?}"
    );
}

/// A broken yt-dlp made the search fallback return the same empty list as "YouTube has nothing",
/// and that got negative-cached for an hour — the same two-failures-one-value bug as the other two
/// sources, on the one path that only runs for titles TMDB has no video for.
#[tokio::test]
async fn a_failed_search_is_not_cached_as_no_trailer() {
    let fake = FakeUpstream::new(&[], None);
    fake.set_title("Backrooms 2025");
    let broken: crate::state::SearchFn = Box::new(|_q| Box::pin(async { None }));
    let clock = TestClock::default();
    let mut state = build_state_full(
        test_cfg(temp_dir()),
        Box::new(fake.clone()),
        always_playable(),
        noop_prewarm(),
        broken,
    );
    {
        let st = Arc::get_mut(&mut state).expect("sole owner");
        st.clock = clock.as_fn();
    }

    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt99999999", "movie", "en")
        .await
        .ids
        .is_empty());

    fake.set_tmdb(&["realTrailer"]);
    clock.advance(crate::YT_FAIL_TTL_MS + 1);
    let ids =
        crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt99999999", "movie", "en").await.ids;
    assert_eq!(
        ids.first().map(String::as_str),
        Some("realTrailer"),
        "a broken search was cached as 'this title has no trailer'"
    );
}

/// Serve one fixed HTTP response on an ephemeral port, then close. `body` may be shorter than the
/// declared `content_length`, which is how a truncated response is simulated.
async fn serve_once(status_line: &str, content_length: usize, body: &'static str) -> String {
    serve_once_bytes(status_line, content_length, body.as_bytes().to_vec()).await
}

/// Send headers, then hold the socket open without sending the body — so a client timeout is what
/// ends the request. `serve_once` closes immediately, which is a connection reset, not a stall.
async fn serve_once_stalling(content_length: usize) -> String {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {content_length}\r\n\r\n"
    );
    tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            let _ = sock.write_all(head.as_bytes()).await;
            let _ = sock.write_all(b"{\"re").await;
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    });
    format!("http://{addr}")
}

async fn serve_once_bytes(status_line: &str, content_length: usize, body: Vec<u8>) -> String {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let head = format!(
        "{status_line}\r\ncontent-type: application/json\r\ncontent-length: {content_length}\r\n\r\n"
    );
    tokio::spawn(async move {
        if let Ok((mut sock, _)) = listener.accept().await {
            let _ = sock.write_all(head.as_bytes()).await;
            let _ = sock.write_all(&body).await;
            let _ = sock.shutdown().await;
        }
    });
    format!("http://{addr}")
}

/// A 200 whose body then fails — truncated, oversized, or not JSON — returns the same empty result
/// as "this title has no trailer". Counting only the status line meant an overloaded TMDB (which
/// accepts, replies 200, then stalls) got that empty answer pinned for an hour for every install,
/// with /health still green because the status line had already cleared the signal.
#[tokio::test]
async fn a_200_that_fails_after_the_status_line_is_not_an_answer() {
    for (label, len, body) in [("truncated", 500usize, "{\"re"), ("not JSON", 5usize, "hello")] {
        let base = serve_once("HTTP/1.1 200 OK", len, body).await;
        let mut cfg = test_cfg(temp_dir());
        cfg.tmdb_base = base;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

        assert_eq!(
            up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await,
            Err(NoAnswer),
            "a {label} body counted as a real 'no trailer' answer"
        );
    }
}

/// The BYOK TMDB key is a bearer secret the app keeps in the Keychain and never logs. redact()
/// strips it from our own URL, but reqwest's Display re-appends the whole thing ("… for url
/// (…?api_key=…)"), so interpolating the error beside a redacted URL published the key on every
/// transport fault — i.e. throughout exactly the outage that produces the most log lines.
#[tokio::test]
async fn a_transport_fault_does_not_log_the_api_key() {
    let url = "http://127.0.0.1:1/tmdb/3/find/tt0111161?external_source=imdb_id&api_key=SUPERSECRETKEY";
    let err = reqwest::Client::new().get(url).send().await.expect_err("a closed port must fail");

    let logged = crate::upstream::transport_fault_line(url, err);
    assert!(!logged.contains("SUPERSECRETKEY"), "the api_key reached a log line: {logged}");
    assert!(!logged.contains("api_key"), "the query string reached a log line: {logged}");
    // It still has to say which upstream failed AND why, or the redaction has eaten the diagnostic:
    // Display alone renders connection-refused, DNS failure and TLS failure byte-identically.
    assert!(logged.contains("/3/find/tt0111161"), "the log line lost the path: {logged}");
    // Pin the property, not an errno string: the line must distinguish failure shapes. Asserting
    // "connection refused" couples the test to an OS message and to where reqwest nests the cause.
    let timed_out = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(1))
        .build()
        .unwrap()
        .get("http://10.255.255.1/tmdb?api_key=SUPERSECRETKEY")
        .send()
        .await
        .expect_err("an unroutable address must fail");
    let other = crate::upstream::transport_fault_line(url, timed_out);
    assert!(!other.contains("SUPERSECRETKEY"), "the api_key reached a log line: {other}");
    assert_ne!(
        logged.split(" (").nth(1),
        other.split(" (").nth(1),
        "two different transport failures log an identical cause: {logged}"
    );
}

/// A fault that lands AFTER the status line is the same outage as one that lands before it —
/// reqwest's timeout spans the body read, so which side a wedged upstream falls on is arbitrary.
/// Bumping only the caching counter left /health reporting ok while every resolve came back empty.
#[tokio::test]
async fn a_200_that_fails_late_also_degrades_health() {
    let base = serve_once("HTTP/1.1 200 OK", 500, "{\"re").await;
    let mut cfg = test_cfg(temp_dir());
    cfg.tmdb_base = base;
    let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

    assert_eq!(up.recent_failures(), 0);
    assert_eq!(up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await, Err(NoAnswer));
    assert!(up.recent_failures() > 0, "/health stayed green through an outage that empties every resolve");
}

/// A body over the cap is the third late-failure shape. The body must stay VALID JSON past the cap:
/// an invalid one falls into the not-JSON arm, which bumps the same counter, so the test passed
/// with the cap removed entirely — pinning nothing, while claiming to pin the one guard against
/// buffering a runaway upstream.
#[tokio::test]
async fn an_oversize_body_is_not_an_answer() {
    let mut body = Vec::with_capacity(6 * 1024 * 1024);
    body.push(b'[');
    while body.len() < 6 * 1024 * 1024 {
        body.extend_from_slice(b"0,");
    }
    body.extend_from_slice(b"0]");
    let len = body.len();
    let base = serve_once_bytes("HTTP/1.1 200 OK", len, body).await;
    let mut cfg = test_cfg(temp_dir());
    cfg.tmdb_base = base;
    let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

    assert_eq!(
        up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await,
        Err(NoAnswer),
        "an oversize body was buffered and accepted as an answer"
    );
}

/// With no TMDB key, KinoCheck is the only source consulted — so its outage is a total failure to
/// get an answer, not the ignorable fallback blip it is for a keyed install.
#[tokio::test]
async fn a_keyless_lookup_treats_a_fallback_outage_as_no_answer() {
    let mut cfg = test_cfg(temp_dir());
    cfg.kinocheck_base = "http://127.0.0.1:1/kinocheck".to_string();
    let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

    assert_eq!(
        up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await,
        Err(NoAnswer),
        "a keyless install's only source failed and nothing recorded it"
    );
    // ...and it still must not move the TMDB-facing signal.
    assert_eq!(up.recent_failures(), 0, "a fallback outage degraded /health");
}

/// A keyless install consults ONLY KinoCheck, so its outage there is a total failure to get an
/// answer — it must take the short cooldown, not pin "no trailer" for an hour on the one path where
/// no other signal (health, the TMDB fault counter, the log line) can see it.
#[tokio::test]
async fn a_keyless_lookup_does_not_pin_a_fallback_outage_for_an_hour() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    fake.fail_fallback();
    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());

    // Past the cooldown but far short of a real negative: the outage must be re-asked.
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS + 1);
    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    assert!(
        fake.calls() > after,
        "a keyless install pinned its only source's outage as 'no trailer' for a full hour"
    );
}

/// ...while for a KEYED install a KinoCheck outage stays ignorable: TMDB answered, so the negative
/// is real and must keep its full TTL rather than being re-asked every minute.
#[tokio::test]
async fn a_keyed_lookup_still_ignores_a_fallback_outage() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    fake.fail_fallback();
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());

    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS * 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    assert_eq!(fake.calls(), after, "a fallback outage shortened a real answer's TTL");
}

/// Every way a source can fail to answer must move its counter — the bad-status path and the
/// post-200 body paths as well as the transport one. Only the transport writer was pinned, and the
/// two unpinned ones are exactly what fires when KinoCheck 5xx's or truncates: the decisive signal
/// for a keyless install, where it is the only source consulted.
#[tokio::test]
async fn every_no_answer_shape_moves_its_counter() {
    // (status line, content-length, body, is_tmdb)
    let cases: [(&str, usize, &'static str); 3] = [
        ("HTTP/1.1 503 Service Unavailable", 2, "{}"), // bad status
        ("HTTP/1.1 200 OK", 500, "{\"re"),             // truncated body
        ("HTTP/1.1 200 OK", 5, "hello"),               // not JSON
    ];
    for (status_line, len, body) in cases {
        // The fallback source...
        let base = serve_once(status_line, len, body).await;
        let mut cfg = test_cfg(temp_dir());
        cfg.kinocheck_base = base;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());
        assert_eq!(
            up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await,
            Err(NoAnswer),
            "{status_line:?} on the fallback source read as an answer; a keyless install would cache it"
        );

        // ...and the primary.
        let base = serve_once(status_line, len, body).await;
        let mut cfg = test_cfg(temp_dir());
        cfg.tmdb_base = base;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());
        assert_eq!(
            up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await,
            Err(NoAnswer),
            "{status_line:?} on TMDB read as an answer"
        );
    }
}

/// A wrong key means THIS request got no answer, so it must count — otherwise the empty result is
/// cached as a real "no trailer" for an hour, under a key that excludes the credential, and one
/// install's typo blanks the title for every install. That is the bug the counter exists for, and
/// it is easy to "tidy away" by mirroring the health counter, which excludes 401/403 for a
/// different and correct reason.
#[tokio::test]
async fn a_wrong_key_counts_as_no_answer_even_though_health_ignores_it() {
    for status_line in ["HTTP/1.1 401 Unauthorized", "HTTP/1.1 403 Forbidden"] {
        let base = serve_once(status_line, 2, "{}").await;
        let mut cfg = test_cfg(temp_dir());
        cfg.tmdb_base = base;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

        assert_eq!(
            up.tmdb_candidates("wrong-key", "tt0111161", "movie", "en").await,
            Err(NoAnswer),
            "{status_line:?} was read as a real 'no trailer'"
        );
        assert_eq!(up.recent_failures(), 0, "{status_line:?} marked the upstream itself down");
    }
}

/// The resolve cache key is credential-free and shared by every install, so an install whose key is
/// wrong must not replace a working install's trailer list with an empty one — that is a cache HIT
/// for the whole window, so the title shows no trailer and no upstream call happens to correct it.
#[tokio::test]
async fn a_failing_install_does_not_blank_a_cached_trailer_for_everyone() {
    let fake = FakeUpstream::new(&["goodTrailer"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    // A healthy install caches a real answer, which then expires.
    let ids =
        crate::addon::resolve_youtube_ids(&state, "good-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids.first().map(String::as_str), Some("goodTrailer"));
    clock.advance(crate::YT_TTL_MS + 1);

    // An install with a wrong key resolves the same title and gets nothing.
    fake.set_tmdb(&[]);
    fake.fail_next();
    let broken =
        crate::addon::resolve_youtube_ids(&state, "wrong-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(
        broken.first().map(String::as_str),
        Some("goodTrailer"),
        "a failed lookup discarded the answer we already had"
    );

    // The healthy install must still see its trailer, and the failure must not be serving as a hit.
    fake.set_tmdb(&["goodTrailer"]);
    let ids =
        crate::addon::resolve_youtube_ids(&state, "good-key", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(
        ids.first().map(String::as_str),
        Some("goodTrailer"),
        "one install's bad key blanked the trailer for every install"
    );
}

/// "error decoding response body" is what reqwest Displays for a truncation and for a timeout
/// alike — one is the upstream dying mid-response, the other is it wedging, and that difference is
/// the whole diagnostic during the outage this arm exists for. The stalling server has to actually
/// stall: closing the socket is a reset, which is the truncation case wearing a timeout's name.
#[tokio::test]
async fn a_body_fault_says_which_kind_it_was() {
    let base = serve_once("HTTP/1.1 200 OK", 500, "{\"re").await;
    let truncated = reqwest::Client::new()
        .get(format!("{base}/x?api_key=SUPERSECRETKEY"))
        .send()
        .await
        .expect("headers arrive")
        .bytes()
        .await
        .expect_err("a truncated body must fail");

    let slow = serve_once_stalling(500).await;
    let stalled = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(300))
        .build()
        .unwrap()
        .get(format!("{slow}/x?api_key=SUPERSECRETKEY"))
        .send()
        .await
        .expect("headers arrive")
        .bytes()
        .await
        .expect_err("a stalled body must time out");

    let a = crate::upstream::body_fault_why(truncated);
    let b = crate::upstream::body_fault_why(stalled);
    assert!(!a.contains("SUPERSECRETKEY") && !b.contains("SUPERSECRETKEY"), "{a} / {b}");
    assert!(b.to_lowercase().contains("time"), "a stalled body did not report a timeout: {b}");
    assert_ne!(a, b, "a truncated body and a stalled one log the same line: {a}");
}

/// One title's outage must not touch another's. The old signal was a process-wide counter sampled
/// across the join, so a fault raised while resolving ANY title read as this title's lookup having
/// failed — and Den resolves a whole row at once, so overlapping resolves are the steady state.
#[tokio::test]
async fn one_titles_outage_does_not_touch_another_title() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    fake.fail_next();
    assert!(crate::addon::resolve_youtube_ids(&state, "k", None, "tt9999999", "movie", "en")
        .await
        .ids
        .is_empty());

    // A different title, resolved successfully right after, must be cached at the FULL TTL.
    fake.set_tmdb(&["goodTrailer"]);
    assert_eq!(
        crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids,
        vec!["goodTrailer".to_string()]
    );
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS * 3);
    assert_eq!(
        crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids,
        vec!["goodTrailer".to_string()]
    );
    assert_eq!(fake.calls(), after, "another title's outage downgraded this title's entry");
}

/// Serving the last-known-good must still rate-limit the outage. Returning early skipped the
/// insert, so every browse during a fault paid a full upstream round.
#[tokio::test]
async fn serving_a_stale_answer_still_rate_limits_the_outage() {
    let fake = FakeUpstream::new(&["goodTrailer"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    assert!(!crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    clock.advance(crate::YT_TTL_MS + 1);

    // A persistent outage, browsed repeatedly.
    fake.set_tmdb(&[]);
    let mut calls = Vec::new();
    for _ in 0..5 {
        fake.fail_next();
        let ids = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids;
        assert_eq!(ids, vec!["goodTrailer".to_string()], "the last known answer was dropped");
        calls.push(fake.calls());
        clock.advance(crate::YT_FAIL_TTL_MS / 4);
    }
    assert!(calls[4] - calls[0] <= 1, "each browse during the outage paid a full upstream round: {calls:?}");
}

/// Serving the last known answer must expire. Re-serving rewrites the entry's expiry, so without an
/// independent "when was this confirmed" clock a trailer that was REMOVED upstream is handed out
/// for as long as anything in the process keeps faulting — and /meta ships it with a 7-day max-age.
#[tokio::test]
async fn a_stale_answer_stops_being_served_eventually() {
    let fake = FakeUpstream::new(&["goodTrailer"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    assert!(!crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    clock.advance(crate::YT_TTL_MS + 1);

    // The trailer is gone upstream, and the lookups keep failing. Browse repeatedly, well past the
    // grace, so each failure gets the chance to refresh the entry it is serving.
    fake.set_tmdb(&[]);
    let mut last = vec!["goodTrailer".to_string()];
    for _ in 0..40 {
        fake.fail_next();
        last = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids;
        clock.advance(crate::STALE_GRACE_MS / 10);
    }
    assert!(
        last.is_empty(),
        "a removed trailer was still being served after {} days of failures",
        (crate::YT_TTL_MS + 4 * crate::STALE_GRACE_MS) / (24 * 60 * 60 * 1000)
    );
}

/// ...but it is still served for a good while: an outage must not blank the catalogue immediately.
#[tokio::test]
async fn a_stale_answer_survives_a_long_outage() {
    let fake = FakeUpstream::new(&["goodTrailer"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    assert!(!crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    clock.advance(crate::YT_TTL_MS + 1);

    fake.set_tmdb(&[]);
    fake.fail_next();
    let ids = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids, vec!["goodTrailer".to_string()], "an outage blanked the title immediately");
}

/// A slow failing resolve must not downgrade a fast good one's full-TTL entry to the cooldown.
///
/// This needs the two resolves genuinely interleaved. Seeding the cache and calling again does not
/// work — a live entry short-circuits at the top of the function, so the branch is never reached
/// and the test passes with the code deleted. The failing resolve is held inside its upstream call
/// while the good one runs to completion and inserts.
#[tokio::test]
async fn a_failing_resolve_does_not_downgrade_a_live_entry() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    // A: starts first, will fail, and parks inside tmdb_candidates.
    let gate = fake.gate_next();
    fake.fail_next();
    let s_a = state.clone();
    let a = tokio::spawn(async move {
        crate::addon::resolve_youtube_ids(&s_a, "k", None, "tt0111161", "movie", "en").await.ids
    });
    tokio::task::yield_now().await;
    assert_eq!(fake.calls(), 1, "A did not reach the upstream");

    // B: runs to completion while A is parked, inserting a live 24h entry.
    fake.set_tmdb(&["goodTrailer"]);
    let ids = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids;
    assert_eq!(ids, vec!["goodTrailer".to_string()]);

    // A resumes and finds B's live entry.
    gate.add_permits(1);
    assert_eq!(a.await.unwrap(), vec!["goodTrailer".to_string()], "A published its own empty result");

    // B's entry must still be live well past the failure cooldown.
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS * 3);
    assert_eq!(
        crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids,
        vec!["goodTrailer".to_string()]
    );
    assert_eq!(fake.calls(), after, "a failing resolve downgraded a live 24h entry to the cooldown");
}

/// A redeploy made the next browse re-ask TMDB for every title on screen. Parking the map fixes
/// that, but the two kinds of entry have to be validated by DIFFERENT clocks, and getting it
/// backwards is worse than not persisting at all.
#[test]
fn a_parked_resolve_cache_keeps_answers_and_drops_cooldowns() {
    let dir = temp_dir();
    let cfg = test_cfg(dir);
    let now = 1_000_000_000_000u64;
    let mut map: HashMap<String, crate::state::YtEntry> = HashMap::new();

    // A good answer whose `exp` was rewritten to a retry cooldown by a failing lookup that
    // substituted it. The ids are still confirmed, so it must come back — and come back with the
    // expiry a confirmed answer earns, not the cooldown.
    map.insert(
        "tt1:en".into(),
        crate::state::YtEntry { ids: vec!["goodTrailer".into()], exp: now - 1, confirmed: now - 1000 },
    );
    // A confirmed answer that is simply too old to trust.
    map.insert(
        "tt2:en".into(),
        crate::state::YtEntry {
            ids: vec!["staleTrailr".into()],
            exp: now + 999,
            confirmed: now - crate::YT_TTL_MS - 1,
        },
    );
    // A 60-second failure cooldown that has already passed. `confirmed` is set to now on EVERY
    // write, empties included, so validating this one by `confirmed` would promote it into a
    // 24-hour "this title has no trailer" — the exact inversion YT_FAIL_TTL_MS < YT_NEG_TTL_MS
    // exists to prevent.
    map.insert("tt3:en".into(), crate::state::YtEntry { ids: vec![], exp: now - 1, confirmed: now - 1 });
    // A live "this title really has no trailer", which is worth keeping.
    map.insert("tt4:en".into(), crate::state::YtEntry { ids: vec![], exp: now + 60_000, confirmed: now - 1 });
    // A `confirmed` that did not come from our clock. Everything downstream ADDS to this value
    // without checking — the /meta staleness test, and the substitution path on top of that — which
    // was only ever safe because it could not come from anywhere but a clock reading. Both kinds of
    // entry, because the empty ones are read by that same staleness test.
    map.insert(
        "tt5:en".into(),
        crate::state::YtEntry { ids: vec!["overflowVid".into()], exp: now + 999, confirmed: u64::MAX - 10 },
    );
    map.insert(
        "tt6:en".into(),
        crate::state::YtEntry { ids: vec![], exp: now + 60_000, confirmed: u64::MAX - 10 },
    );

    std::fs::create_dir_all(cfg.resolve_cache.parent().unwrap()).unwrap();
    std::fs::write(&cfg.resolve_cache, serde_json::to_vec(&map).unwrap()).unwrap();
    let back = crate::state::load_resolve_cache(&cfg, now);

    assert_eq!(
        back.get("tt1:en").map(|e| e.ids.clone()),
        Some(vec!["goodTrailer".to_string()]),
        "a confirmed answer was dropped because a failing lookup had left a cooldown on it"
    );
    assert_eq!(
        back["tt1:en"].exp,
        back["tt1:en"].confirmed + crate::YT_TTL_MS,
        "the restored answer kept the cooldown instead of the expiry it had earned"
    );
    assert!(!back.contains_key("tt2:en"), "an answer past its confirmation window came back");
    assert!(!back.contains_key("tt3:en"), "a 60-second cooldown came back as a day of 'no trailer'");
    assert!(back.contains_key("tt4:en"), "a live negative answer was dropped, so every browse re-asks");
    assert!(!back.contains_key("tt5:en"), "a populated entry confirmed in the future was restored");
    assert!(!back.contains_key("tt6:en"), "an empty entry confirmed in the future was restored");
}

/// ...and the round trip actually works, through the paths production uses.
#[test]
fn the_resolve_cache_survives_a_restart() {
    let dir = temp_dir();
    let state = build_state(dir, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let now = (state.clock)();
    state.yt_cache.lock().unwrap().insert(
        "tt0111161:en".into(),
        crate::state::YtEntry { ids: vec!["goodTrailer".into()], exp: now + 1000, confirmed: now },
    );

    // Already dead when we shut down: bytes to write, bytes to read back, and an entry to parse and
    // immediately discard. It should not reach the file at all.
    state.yt_cache.lock().unwrap().insert(
        "tt0000001:en".into(),
        crate::state::YtEntry { ids: vec![], exp: now.saturating_sub(1), confirmed: now.saturating_sub(1) },
    );

    crate::state::save_resolve_cache(&state);
    let raw = std::fs::read_to_string(&state.cfg.resolve_cache).unwrap();
    assert!(!raw.contains("tt0000001:en"), "an already-expired entry was parked to disk");

    let back = crate::state::load_resolve_cache(&state.cfg, now);
    assert_eq!(
        back.get("tt0111161:en").map(|e| e.ids.clone()),
        Some(vec!["goodTrailer".to_string()]),
        "the parked cache did not come back"
    );
}

/// The saver has to ask "is this still live?" the same way the loader does, and `exp` alone is not
/// that question for a populated entry. The stale-substitution path rewrites `exp` to a 60-second
/// retry cooldown while leaving good ids and their original `confirmed` in place — so a save
/// filtered on `exp` dropped exactly the entries the loader goes out of its way to rescue. A TMDB
/// blip followed by a redeploy a minute later wiped the answers the blip had been leaning on, which
/// is this file's whole purpose, failing during an outage.
///
/// The other two persistence tests cannot see this seam: one writes its JSON straight to disk and
/// never calls the saver, the other saves an entry whose `exp` is fresh.
#[test]
fn a_stand_in_answer_survives_being_parked() {
    let dir = temp_dir();
    let state = build_state(dir, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let now = (state.clock)();

    // Exactly what addon.rs writes when a failing lookup serves the last known answer: good ids, an
    // old `confirmed`, and an `exp` that is only the failure cooldown — already elapsed.
    state.yt_cache.lock().unwrap().insert(
        "tt0111161:en".into(),
        crate::state::YtEntry {
            ids: vec!["goodTrailer".into()],
            exp: now.saturating_sub(1),
            confirmed: now.saturating_sub(60_000),
        },
    );

    crate::state::save_resolve_cache(&state);
    let back = crate::state::load_resolve_cache(&state.cfg, now);

    assert_eq!(
        back.get("tt0111161:en").map(|e| e.ids.clone()),
        Some(vec!["goodTrailer".to_string()]),
        "a stand-in answer was dropped at save, so a redeploy after an outage re-asks TMDB for everything"
    );
}

/// The parked file lives in a SUBDIRECTORY of the cache, and that is not cosmetic: the sweep deletes
/// every top-level file that is not `<vid>.mp4` as abandoned scratch.
#[test]
fn the_parked_resolve_cache_is_not_swept_away() {
    let dir = temp_dir();
    let state =
        build_state(dir.clone(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let now = (state.clock)();
    state.yt_cache.lock().unwrap().insert(
        "tt0111161:en".into(),
        crate::state::YtEntry { ids: vec!["goodTrailer".into()], exp: now + 1000, confirmed: now },
    );
    crate::state::save_resolve_cache(&state);
    assert!(state.cfg.resolve_cache.exists(), "nothing was written, so this proves nothing");

    crate::play::sweep_partials(&state.cfg);
    crate::play::evict_if_needed(&state.cfg);

    assert!(state.cfg.resolve_cache.exists(), "the sweep reaped the resolve cache it is meant to ignore");
}

/// The size sweep drops entries by expiry. Keying it off any other field wipes the whole cache on
/// every insert past the bound — which no test noticed.
#[tokio::test]
async fn the_cache_sweep_drops_only_expired_entries() {
    let fake = FakeUpstream::new(&["keepMe00001"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    // A live answer, then enough expired junk to trip the sweep on the next insert.
    assert!(!crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());
    {
        let mut cache = state.yt_cache.lock().unwrap();
        for i in 0..crate::YT_CACHE_MAX {
            cache.insert(
                format!("tt{i:08}:en"),
                crate::state::YtEntry { ids: vec!["x".into()], exp: 0, confirmed: 0 },
            );
        }
    }
    let _ = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0000001", "movie", "en").await.ids;

    let cache = state.yt_cache.lock().unwrap();
    assert!(cache.len() < crate::YT_CACHE_MAX, "the sweep did not run: {}", cache.len());
    // `:nokc` because the resolve above passes no KinoCheck key — the key names which sources the
    // request could ask. This test is about the sweep; the suffix is just what the key looks like.
    assert_eq!(
        cache.get("tt0111161:en:nokc").map(|e| e.ids.clone()),
        Some(vec!["keepMe00001".to_string()]),
        "the sweep dropped a live entry"
    );
}

/// The title lookup gates the search fallback: if IT could not be made, no search ran, so the empty
/// result is "we could not ask", not "this title has no trailer". Treating it as an answer pinned
/// the title empty for a full hour on the path that exists for titles TMDB has no video for.
#[tokio::test]
async fn a_failed_title_lookup_is_not_an_answer() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    fake.fail_title();
    assert!(crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en")
        .await
        .ids
        .is_empty());

    // Past the failure cooldown but well short of a negative TTL: it must be re-asked.
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS + 1);
    let _ = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await.ids;
    assert!(fake.calls() > after, "a failed title lookup was cached as 'this title has no trailer'");
}

/// A stand-in answer must not be pinned in every client for a week. The server stops trusting it
/// after a day, so a 7-day max-age outlives the server's own bound by six.
#[tokio::test]
async fn a_stale_answer_is_not_cached_in_the_client_for_a_week() {
    let fake = FakeUpstream::new(&["goodTrailer"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    // A fresh answer is cacheable for the full week.
    let fresh = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await;
    assert!(!fresh.stale, "a fresh answer was marked stale");

    // ...and the same answer, once it is only standing in for a failed lookup, is not.
    clock.advance(crate::YT_TTL_MS + 1);
    fake.set_tmdb(&[]);
    fake.fail_next();
    let stood_in = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await;
    assert_eq!(stood_in.ids, vec!["goodTrailer".to_string()]);
    assert!(stood_in.stale, "a stand-in answer was offered as a fresh one");

    // A cache HIT on that stand-in must stay marked too — the client sees the same body either way.
    let hit = crate::addon::resolve_youtube_ids(&state, "k", None, "tt0111161", "movie", "en").await;
    assert_eq!(hit.ids, vec!["goodTrailer".to_string()]);
    assert!(hit.stale, "a cache hit on a stand-in was offered as a fresh answer");
}

/// YT_CACHE_MAX must be a cap, not a threshold. Sweeping only expired entries means that once that
/// many are live the map grows anyway, and every later insert pays a full scan under the mutex.
#[tokio::test]
async fn the_resolve_cache_is_actually_bounded() {
    let fake = FakeUpstream::new(&["keepMe00001"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    // Fill past the bound with entries that are all LIVE, so the expiry sweep frees nothing.
    {
        let mut cache = state.yt_cache.lock().unwrap();
        let far = crate::YT_TTL_MS * 10;
        for i in 0..crate::YT_CACHE_MAX + 50 {
            cache.insert(
                format!("tt{i:08}:en"),
                crate::state::YtEntry { ids: vec!["x".into()], exp: far + i as u64, confirmed: 0 },
            );
        }
    }
    let _ = crate::addon::resolve_youtube_ids(&state, "k", None, "tt7777777", "movie", "en").await;

    let len = state.yt_cache.lock().unwrap().len();
    assert!(len < crate::YT_CACHE_MAX, "the map grew past its bound with all entries live: {len}");
}

/// ...and the response actually says so. The flag only matters if it reaches the header — /meta ships
/// a trailer link with a 7-day max-age, which for a stand-in outlives the server's own 48h bound.
#[tokio::test]
async fn meta_shortens_max_age_for_a_stand_in_answer() {
    let fake = FakeUpstream::new(&["goodTrailer"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());
    let base = spawn_server(state.clone()).await;

    let cc = |r: &reqwest::Response| r.headers().get("cache-control").unwrap().to_str().unwrap().to_string();

    let fresh = reqwest::get(format!("{base}/meta/movie/tt0111161.json")).await.unwrap();
    assert!(cc(&fresh).contains("604800"), "a fresh answer lost its long cache: {}", cc(&fresh));

    // Age it out and make the next lookup fail, so the answer becomes a stand-in.
    clock.advance(crate::YT_TTL_MS + 1);
    fake.set_tmdb(&[]);
    fake.fail_next();
    let stale = reqwest::get(format!("{base}/meta/movie/tt0111161.json")).await.unwrap();
    let header = cc(&stale);
    assert!(!header.contains("604800"), "a stand-in answer was pinned in the client for a week: {header}");
    assert!(header.contains("max-age"), "a stand-in answer lost caching entirely: {header}");
}

/// A redeploy (SIGTERM from `podman auto-update`) must not strand the partials it was writing.
/// They carry this process's pid, so after exit nothing can tell them from another instance's live
/// work — sweep_partials has to wait out its 30-minute grace, and until then they sit on the volume.
#[test]
fn shutdown_reclaims_this_processes_partials() {
    let dir = temp_dir();
    let pid = std::process::id();
    let mine = [
        format!(".vidvidvid11.{pid}.0.partial.mp4"),
        format!(".vidvidvid11.{pid}.0.partial.mp4.part"),
        format!(".othervid001.{pid}.3.partial.f137.mp4.part"),
    ];
    for n in &mine {
        std::fs::write(dir.join(n), b"x").unwrap();
    }
    // Another instance's live work, and a published trailer: neither is ours to remove.
    let other = format!(".vidvidvid11.{}.0.partial.mp4.part", pid + 1);
    std::fs::write(dir.join(&other), b"x").unwrap();
    std::fs::write(dir.join("cccccccccc1.mp4"), b"x").unwrap();

    crate::play::sweep_own_temps(&test_cfg(dir.clone()));

    for n in &mine {
        assert!(!dir.join(n).exists(), "{n} survived shutdown and is unreclaimable until the sweep");
    }
    assert!(dir.join(&other).exists(), "another instance's live download was deleted");
    assert!(dir.join("cccccccccc1.mp4").exists(), "a published trailer was deleted");
}

/// body_fault_why is pub(crate) and drops the url as belt-and-braces: body errors carry none today,
/// but a future caller handing it a SEND-path error — which does carry one, with the api_key in the
/// query string — would leak immediately. Feed it exactly that.
#[tokio::test]
async fn body_fault_why_redacts_even_a_send_path_error() {
    let url = "http://127.0.0.1:1/tmdb/3/find/tt0111161?external_source=imdb_id&api_key=SUPERSECRETKEY";
    let send_err = reqwest::Client::new().get(url).send().await.expect_err("a closed port must fail");
    assert!(
        send_err.url().is_some(),
        "this test is pointless unless the error carries the url it must not print"
    );

    let logged = crate::upstream::body_fault_why(send_err);
    assert!(!logged.contains("SUPERSECRETKEY"), "the api_key reached a log line: {logged}");
    assert!(!logged.contains("api_key"), "the query string reached a log line: {logged}");
}

/// ...and the check has to sit BEFORE the fetch. `record_unknown` only proves the file existed when
/// detection ran, and eviction is ordinary on a full volume — so with the check after the fetch, a
/// /crop inside the ten-minute window could take a download permit and pull a whole trailer down to
/// return a constant that does not depend on it.
#[tokio::test]
async fn an_unreadable_crop_does_not_re_download_the_trailer() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let downloads = dir.join("dl-count");
    let yt = dir.join("yt-counting");
    std::fs::write(&yt, format!("#!/bin/sh\necho x >> {}\nexit 1\n", downloads.display())).unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();
    let ff = dir.join("failing-ffmpeg");
    std::fs::write(&ff, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&ff, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    cfg.ffmpeg = ff.to_string_lossy().into_owned();
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    seed_cache(&dir, "unreadable2", 100);
    let _ = crate::crop::handle_crop(state.clone(), "unreadable2".into()).await;
    assert_eq!(spawn_count(&downloads), 0, "the file was cached; nothing should have downloaded");

    // Eviction takes the file while the "unreadable" verdict is still standing.
    std::fs::remove_file(dir.join("unreadable2.mp4")).unwrap();
    let resp = crate::crop::handle_crop(state.clone(), "unreadable2".into()).await;

    assert_eq!(resp.status(), 200);
    assert_eq!(
        spawn_count(&downloads),
        0,
        "/crop spent a download permit and a yt-dlp run to return a constant"
    );
}

/// The probe permit bounded how many cropdetect passes run at once. It did nothing about the same
/// unreadable trailer paying for a whole-file ffmpeg pass on every single call — which is the most
/// expensive thing this service does per request, and only the SUCCESSFUL side was ever cached.
#[tokio::test]
async fn an_unreadable_crop_is_not_re_detected_on_every_call() {
    let dir = temp_dir();
    let runs = dir.join("ffmpeg-runs");
    let fake = dir.join("failing-ffmpeg");
    std::fs::write(&fake, format!("#!/bin/sh\necho x >> {}\nexit 1\n", runs.display())).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ffmpeg = fake.to_string_lossy().into_owned();
    let clock = TestClock::default();
    let state = build_state_cfg_clock(cfg, Box::new(FakeUpstream::new(&[], None)), clock.as_fn());
    seed_cache(&dir, "unreadable1", 100);

    let _ = crate::crop::handle_crop(state.clone(), "unreadable1".into()).await;
    // However many spawns one detection costs internally — this test is about repetition, not that.
    let first = spawn_count(&runs);
    assert!(first > 0, "the fake ffmpeg never ran, so this test proves nothing");

    let _ = crate::crop::handle_crop(state.clone(), "unreadable1".into()).await;
    assert_eq!(spawn_count(&runs), first, "an unreadable trailer re-ran the whole-file ffmpeg pass");

    // Short-lived on purpose: the other way to land here is a cropdetect that timed out under load.
    clock.advance(crate::CROP_UNKNOWN_TTL_MS + 1);
    let _ = crate::crop::handle_crop(state.clone(), "unreadable1".into()).await;
    assert!(spawn_count(&runs) > first, "the unknown never expired");
}

/// /crop's ffmpeg pass takes a probe permit. It was the only subprocess spawn without one, and it
/// is a whole-file decode with no negative cache behind it, so an undetectable trailer re-runs it on
/// every request at any concurrency.
#[tokio::test]
async fn crop_detection_is_bounded_by_the_probe_budget() {
    let dir = temp_dir();
    // A fake ffmpeg that sleeps, so overlapping /crop calls are observable.
    let fake = dir.join("slow-ffmpeg");
    std::fs::write(&fake, "#!/bin/sh\nsleep 30\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ffmpeg = fake.to_string_lossy().into_owned();
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    // More concurrent /crop calls than the probe budget allows.
    let over = crate::PROBE_CONCURRENCY + 4;
    for i in 0..over {
        let id = format!("cropvid{i:04}");
        seed_cache(&dir, &id, 100);
        let st = state.clone();
        tokio::spawn(async move { crate::crop::handle_crop(st, id).await });
    }
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let running = std::process::Command::new("pgrep")
        .args(["-f", &fake.to_string_lossy()])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().count())
        .unwrap_or(0);
    let _ = std::process::Command::new("pkill").args(["-f", &fake.to_string_lossy()]).status();

    assert!(running > 0, "the fake ffmpeg never ran, so this test proves nothing");
    assert!(
        running <= crate::PROBE_CONCURRENCY,
        "{running} concurrent ffmpeg passes for {over} requests, over a budget of {}",
        crate::PROBE_CONCURRENCY
    );
}

/// Scratch counts toward the cap, so when it fills the cap on its own the eviction loop could never
/// satisfy the target — and deleted every trailer, on every call, including the one the download
/// that triggered it had just published. That is the death spiral the cap's floor exists to prevent,
/// arrived at from the other side.
#[test]
fn scratch_filling_the_cap_does_not_wipe_the_cache() {
    let dir = temp_dir();
    std::fs::write(dir.join("aaaaaaaaaa1.mp4"), vec![0u8; 100]).unwrap();
    std::fs::write(dir.join("bbbbbbbbbb2.mp4"), vec![0u8; 100]).unwrap();
    // An in-flight download bigger than the whole cap.
    std::fs::write(dir.join(".ccccccccc11.123.0.partial.mp4.part"), vec![0u8; 500]).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.cache_max_bytes = 400;
    crate::play::evict_if_needed(&cfg);

    assert!(dir.join("aaaaaaaaaa1.mp4").exists(), "a trailer was evicted to make room for scratch");
    assert!(dir.join("bbbbbbbbbb2.mp4").exists(), "the cache was wiped by unevictable scratch");
}

/// The process-group registry has real kill power, so a pgid left in it after the process died
/// would make shutdown SIGKILL whatever the OS reused that number for. A no-op'd `unregister_group`
/// is invisible to every other test, and that is exactly the dangerous mutation.
///
/// Asserted per-id rather than on the registry's size: it is process-wide, so a concurrent test's
/// subprocesses are in it too, and `kill_live_groups()` here would kill them.
#[test]
fn a_finished_download_leaves_nothing_in_the_kill_registry() {
    // In range, but far above any real pid — pid_max is orders of magnitude below this.
    let unused_pgid = i32::MAX as u32 - 1;
    assert!(!crate::ytdlp::is_group_live(unused_pgid));

    crate::ytdlp::register_group(Some(unused_pgid));
    assert!(crate::ytdlp::is_group_live(unused_pgid), "a live download was not registered");

    crate::ytdlp::unregister_group(Some(unused_pgid));
    assert!(
        !crate::ytdlp::is_group_live(unused_pgid),
        "a finished download stayed in the registry; shutdown would signal a reused pgid"
    );

    // pgid 0 is "the caller's own group" — registering it would make shutdown kill den-reel itself.
    crate::ytdlp::register_group(Some(0));
    assert!(!crate::ytdlp::is_group_live(0), "pgid 0 was registered; shutdown would kill our own group");

    // A pgid above i32::MAX negates into a POSITIVE pid: `-(u32::MAX - 7) as i32` is 8. It must
    // never reach the registry, because kill_group would then SIGKILL whatever pid 8 is.
    crate::ytdlp::register_group(Some(u32::MAX - 7));
    assert!(
        !crate::ytdlp::is_group_live(u32::MAX - 7),
        "a pgid that negates into a plain pid was registered"
    );
}

/// A cancelled subprocess must leave nothing registered. The await in `output_in_group` is a
/// cancellation point, and the routine way to reach it is a client hanging up mid-/crop — which the
/// server treats as normal. Cleaning up only after the await left the pgid registered forever
/// (nothing else prunes it), so shutdown SIGKILLed groups that had been dead for hours, and pid
/// reuse makes that somebody else's group.
#[tokio::test]
async fn a_cancelled_subprocess_leaves_nothing_in_the_kill_registry() {
    let dir = temp_dir();
    let slow = dir.join("slow-cmd");
    std::fs::write(&slow, "#!/bin/sh\nsleep 30\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o755)).unwrap();

    let pgid = {
        let mut cmd = tokio::process::Command::new(&slow);
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            // Every production caller sets this; without it the drop path under test is not theirs,
            // and tokio leaves the child unreaped as a zombie for the life of the test binary.
            .kill_on_drop(true);
        let fut = crate::ytdlp::output_in_group(&mut cmd);
        tokio::pin!(fut);

        tokio::select! {
            _ = &mut fut => panic!("the fake exited immediately; this test proves nothing"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(300)) => {}
        }
        // The child is ours and still running; its pid is its pgid (process_group(0)).
        let out = std::process::Command::new("pgrep")
            .arg("-f")
            .arg(slow.to_string_lossy().as_ref())
            .output()
            .unwrap();
        let pid: u32 = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .expect("the fake is running")
            .trim()
            .parse()
            .unwrap();
        assert!(crate::ytdlp::is_group_live(pid), "a running subprocess was not registered");
        pid
        // `fut` is dropped here — the cancellation the client hangup causes.
    };

    assert!(
        !crate::ytdlp::is_group_live(pgid),
        "a cancelled subprocess stayed in the registry; shutdown would signal a reused pgid"
    );
}

/// yt-dlp's stderr pipe is inherited by everything it forks, so draining to EOF waits on the
/// longest-lived descendant rather than on yt-dlp. A download that had already finished — complete
/// file on disk, exit 0 — blocked for as long as that descendant lived, holding a download slot,
/// and past the timeout became a 504 whose cleanup deleted the file it had just downloaded.
#[tokio::test]
async fn a_lingering_descendant_does_not_pin_a_finished_download() {
    let dir = temp_dir();
    let fake = dir.join("ytdlp-leaves-a-child");
    // Fork a grandchild that holds the inherited stderr open, then exit 0 having written the file.
    std::fs::write(
        &fake,
        "#!/bin/sh\nout=\"\"; prev=\"\"\nfor a in \"$@\"; do [ \"$prev\" = \"-o\" ] && out=\"$a\"; prev=\"$a\"; done\n\
         (sleep 30) &\nhead -c 1000 /dev/zero > \"$out\"\nexit 0\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = fake.to_string_lossy().into_owned();
    let out = dir.join("out.mp4");

    let started = std::time::Instant::now();
    let r = crate::ytdlp::download_to(&cfg, "abcdefghij1", &out).await;
    let took = started.elapsed();

    assert!(r.is_ok(), "the download failed: {r:?}");
    assert!(
        took < std::time::Duration::from_secs(10),
        "a finished download was pinned by a lingering descendant for {took:?}"
    );
}

/// yt-dlp's stderr decides how a failure is classified, which drives /play's status, the /health
/// extraction signal and autoPickRank. read_to_string discards the WHOLE buffer on one non-UTF-8
/// byte, so a single accented character in an upstream message blanked the log and turned a
/// geo-block into a generic extraction failure.
#[tokio::test]
async fn a_non_utf8_byte_does_not_discard_the_whole_error() {
    let dir = temp_dir();
    let fake = dir.join("ytdlp-latin1");
    std::fs::write(
        &fake,
        "#!/bin/sh\nprintf 'ERROR: [youtube] Caf\\351: The uploader has not made this video available in your country.\\n' >&2\nexit 1\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = fake.to_string_lossy().into_owned();
    let err = crate::ytdlp::download_to(&cfg, "abcdefghij1", &dir.join("o.mp4"))
        .await
        .expect_err("the fake exits 1");

    assert_eq!(err.status, 451, "a geo-block was misclassified: {err:?}");
}

/// An ERROR line written after yt-dlp exits — by something holding the inherited pipe — must still
/// be captured. Killing the group the instant the child exited threw it away, and with it the
/// difference between "this video is geo-blocked" and "our extractor is broken".
#[tokio::test]
async fn an_error_written_after_exit_is_still_captured() {
    let dir = temp_dir();
    let fake = dir.join("ytdlp-late-error");
    // A child that outlives the parent briefly and writes the diagnostic on the inherited stderr.
    std::fs::write(
        &fake,
        "#!/bin/sh\n(sleep 0.3; echo 'ERROR: [youtube] Video unavailable. This video is private' >&2) &\nexit 1\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = fake.to_string_lossy().into_owned();
    let err = crate::ytdlp::download_to(&cfg, "abcdefghij1", &dir.join("o.mp4"))
        .await
        .expect_err("the fake exits 1");

    assert_ne!(
        err.reason, "extraction_failed",
        "a late ERROR line was lost, so a private video read as a broken extractor: {err:?}"
    );
}

/// MP4Box rewrites the trailer IN PLACE, on the same inode, so a bake that was killed part-way
/// leaves a half-rewritten file — which then gets renamed into the cache and served immutable for a
/// year, never re-fetched. Whether the bake ran at all is therefore the load-bearing distinction,
/// and it was being thrown away with a bool: "disabled" and "killed mid-rewrite" were both `false`.
#[tokio::test]
async fn a_refusal_that_never_wrote_is_not_damage() {
    let dir = temp_dir();
    let fp = dir.join("bakevid0001.mp4");
    std::fs::write(&fp, vec![0u8; 2048]).unwrap();
    let report = crate::crop::report_from(
        "bakevid0001",
        Some((1920, 1080)),
        crate::crop::RawCrop { w: 1920, h: 800, x: 0, y: 140 },
    );

    use std::os::unix::fs::PermissionsExt;
    // Refused without touching the file — what a real MP4Box does for a bad flag, a missing track,
    // a read-only target or an unparseable MP4. The trailer is fine.
    let refusing = dir.join("mp4box-refuses");
    std::fs::write(&refusing, "#!/bin/sh\necho 'boom' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&refusing, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.mp4box = refusing.to_string_lossy().into_owned();
    assert_eq!(
        crate::crop::bake_clap(&cfg, &fp, &report).await,
        crate::crop::Bake::Skipped,
        "a refusal that never wrote cost us the trailer"
    );

    // Failed PART WAY through the rewrite — ENOSPC, EIO. This one really did damage it.
    let mangling = dir.join("mp4box-mangles");
    std::fs::write(
        &mangling,
        // POSIX: `for` leaves $f holding the LAST argument, which is MP4Box's target. `${@: -1}` is a
        // bashism — under dash, which is /bin/sh on Linux CI, it is a Bad Substitution, so the fake
        // exited without writing and the branch this test exists for was never reached there.
        "#!/bin/sh\nfor f in \"$@\"; do :; done\nhead -c 64 /dev/zero >> \"$f\"\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&mangling, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.mp4box = mangling.to_string_lossy().into_owned();
    let before = std::fs::metadata(&fp).unwrap().len();
    let verdict = crate::crop::bake_clap(&cfg, &fp, &report).await;
    // Check the FAKE did its job before trusting the verdict. A fake that silently no-ops makes
    // this test pass for the wrong reason — which is exactly what a bashism did under dash, leaving
    // the branch below unexercised everywhere except one developer's machine.
    assert_ne!(
        std::fs::metadata(&fp).unwrap().len(),
        before,
        "the fake MP4Box never wrote to the file, so this proves nothing about a damaged bake"
    );
    assert_eq!(
        verdict,
        crate::crop::Bake::Damaged,
        "a bake that wrote and then failed left a half-rewritten file"
    );

    // A binary that cannot start never touched the file, so the download is still publishable.
    let mut cfg = test_cfg(dir.clone());
    cfg.mp4box = dir.join("no-such-mp4box").to_string_lossy().into_owned();
    assert_eq!(
        crate::crop::bake_clap(&cfg, &fp, &report).await,
        crate::crop::Bake::Skipped,
        "a missing MP4Box must not condemn a perfectly good download"
    );

    // ...and so does baking being switched off.
    let mut cfg = test_cfg(dir.clone());
    cfg.bake_clap = false;
    assert_eq!(crate::crop::bake_clap(&cfg, &fp, &report).await, crate::crop::Bake::Skipped);
}

/// yt-dlp exiting 0 with no file is a LOCAL failure — a grace kill, a full disk, a bad output path
/// — never the extractor's verdict. Routing it through classify() produced a bare
/// `extraction_failed`, and three of those flip /health to `extractor_unavailable`, telling the
/// operator to bump yt-dlp for something yt-dlp did not do.
#[tokio::test]
async fn exit_zero_with_no_file_is_not_blamed_on_the_extractor() {
    let dir = temp_dir();
    let fake = dir.join("ytdlp-writes-nothing");
    std::fs::write(&fake, "#!/bin/sh\nexit 0\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = fake.to_string_lossy().into_owned();
    let err = crate::ytdlp::download_to(&cfg, "abcdefghij1", &dir.join("o.mp4"))
        .await
        .expect_err("no output file is a failure");

    assert_ne!(
        err.reason, "extraction_failed",
        "a local failure was charged to the extractor's health signal: {err:?}"
    );
}

/// A total outage has to show up somewhere. Local failures — no output file, a bake killed
/// mid-rewrite — are not the extractor's fault, so they must not say "bump yt-dlp"; but they used
/// to move nothing at all, and an instance failing every single download reported `ok`.
#[test]
fn downloads_failing_locally_degrade_health_under_their_own_reason() {
    let t = crate::HEALTH_FAIL_THRESHOLD;
    let ok = crate::health_body(true, 0, 0, 0);
    assert_eq!(ok["status"], "ok");

    let local = crate::health_body(true, 0, 0, t);
    assert_eq!(local["status"], "degraded", "every download failing still reported ok");
    assert_eq!(local["reason"], "downloads_failing");
    let detail = local["detail"].as_str().unwrap_or_default();
    assert!(!detail.contains("yt-dlp can't extract"), "local failures blamed the extractor: {detail}");

    // An extractor outage still wins: it is the more specific diagnosis.
    let both = crate::health_body(true, 0, t, t);
    assert_eq!(both["reason"], "extractor_unavailable");
}

/// An exit code does not say whether the file was written. MP4Box validates its arguments and its
/// input before opening the target, so every refusal mode — unknown flag, no such track, read-only
/// target, unparseable MP4 — leaves it byte-identical; treating those as damage deleted perfectly
/// good trailers. Only a run that actually touched the file and did not finish is unpublishable.
#[test]
fn only_a_bake_that_actually_wrote_condemns_the_file() {
    use crate::crop::{bake_outcome, Bake, BakeRun};
    assert_eq!(bake_outcome(BakeRun::Ok), Bake::Baked);
    assert_eq!(bake_outcome(BakeRun::NeverStarted), Bake::Skipped);
    assert_eq!(
        bake_outcome(BakeRun::Refused { touched: false }),
        Bake::Skipped,
        "MP4Box refusing left the file byte-identical; losing the trailer over that is the worse bug"
    );
    assert_eq!(
        bake_outcome(BakeRun::Refused { touched: true }),
        Bake::Damaged,
        "it exited non-zero AFTER writing — that file is half-rewritten"
    );
    assert_eq!(
        bake_outcome(BakeRun::Killed),
        Bake::Damaged,
        "a bake killed mid-rewrite was treated as if it had never touched the file"
    );
}

/// The gate itself: a bake that may have half-rewritten the file must not be renamed into the cache.
/// Once published it is served `immutable` for a year and never re-fetched, because any cached file
/// with len > 0 counts as a hit — so a single interrupted bake is permanent.
#[tokio::test]
async fn a_damaged_bake_is_not_renamed_into_the_cache() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let sh = |p: &std::path::Path, body: &str| {
        std::fs::write(p, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
    };
    // yt-dlp: write the output and exit 0.
    let yt = dir.join("yt");
    sh(&yt, "out=\"\"; prev=\"\"\nfor a in \"$@\"; do [ \"$prev\" = \"-o\" ] && out=\"$a\"; prev=\"$a\"; done\nhead -c 4096 /dev/zero > \"$out\"");
    // ffmpeg: report a letterboxed rect so a clap is worth baking.
    let ff = dir.join("ff");
    sh(&ff, "echo '  Stream #0:0: Video: h264, yuv420p, 1920x1080 [SAR 1:1]' >&2\ni=0; while [ $i -lt 12 ]; do echo 'crop=1920:800:0:140' >&2; i=$((i+1)); done");
    // MP4Box: ran, and failed — so the trailer may be mid-rewrite.
    let mp = dir.join("mp");
    let ran = dir.join("mp-ran");
    // Quoted: an unquoted path word-splits under a TMPDIR containing a space, the marker is never
    // written, and the test fails for a reason that has nothing to do with the code.
    sh(&mp, &format!("for f in \"$@\"; do :; done; head -c 64 /dev/zero >> \"$f\" && : > \"{}\"; echo boom >&2; exit 1", ran.display()));

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    cfg.ffmpeg = ff.to_string_lossy().into_owned();
    cfg.mp4box = mp.to_string_lossy().into_owned();
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    let err = crate::play::fetch_trailer(state.clone(), "bakevid0002".into())
        .await
        .expect_err("a possibly-corrupt trailer must not be published");
    // The fake MP4Box has to have actually written, or this asserts the wrong failure.
    assert!(
        dir.join("mp-ran").exists(),
        "the fake MP4Box never ran or never wrote; this proves nothing about a damaged bake"
    );
    assert_eq!(err.reason, "incomplete_download", "{err:?}");
    // Name the failure, or this passes just as well when yt-dlp wrote nothing and the bake never ran.
    assert!(
        err.detail.contains("bake"),
        "this asserts a blocked bake, but the failure was something else: {err:?}"
    );
    assert!(!dir.join("bakevid0002.mp4").exists(), "a half-rewritten trailer was published to the cache");
    // ...and it has to be visible. An instance failing every download this way reported `ok`.
    assert!(
        state.local_fails.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "a local download failure moved no health signal at all"
    );
    assert_eq!(
        state.extract_fails.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a local failure was charged to the extractor"
    );
}

/// The headline local failure — yt-dlp exits 0 and no file appears (a full or read-only cache
/// volume) — must move the health counter. It had no coverage at all: only the bake path did.
#[tokio::test]
async fn a_download_that_produces_no_file_degrades_health_locally() {
    let dir = temp_dir();
    let fake = dir.join("yt-writes-nothing");
    std::fs::write(&fake, "#!/bin/sh\nexit 0\n").unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = fake.to_string_lossy().into_owned();
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    let err = crate::play::fetch_trailer(state.clone(), "nofilevid01".into())
        .await
        .expect_err("no output file is a failure");
    assert_eq!(err.reason, "incomplete_download", "{err:?}");
    assert!(
        state.local_fails.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "the full-disk case moved no health signal"
    );
}

/// ...and it has to clear once a trailer is actually produced, or /health stays degraded forever
/// after the volume is fixed.
#[tokio::test]
async fn a_produced_trailer_clears_the_local_failure_signal() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let yt = dir.join("yt-writes");
    std::fs::write(
        &yt,
        "#!/bin/sh\nout=\"\"; prev=\"\"\nfor a in \"$@\"; do [ \"$prev\" = \"-o\" ] && out=\"$a\"; prev=\"$a\"; done\nhead -c 2048 /dev/zero > \"$out\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    cfg.bake_clap = false;
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    state.local_fails.store(5, std::sync::atomic::Ordering::Relaxed);

    crate::play::fetch_trailer(state.clone(), "goodvid0001".into())
        .await
        .expect("the download should succeed");
    assert_eq!(
        state.local_fails.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "/health stayed degraded after downloads recovered"
    );
}

/// A rename failure is ours — a full or read-only volume, or the destination already there as a
/// directory — not the extractor's. It was reported as `extraction_failed`, and because both
/// counters are cleared just before it and the error is built here rather than in download_to, an
/// instance failing EVERY download at the rename moved no counter and reported ok.
#[tokio::test]
async fn a_failed_publish_is_local_and_visible() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let yt = dir.join("yt-writes");
    std::fs::write(
        &yt,
        "#!/bin/sh\nout=\"\"; prev=\"\"\nfor a in \"$@\"; do [ \"$prev\" = \"-o\" ] && out=\"$a\"; prev=\"$a\"; done\nhead -c 2048 /dev/zero > \"$out\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();

    // The destination is a non-empty directory, so the rename cannot succeed.
    let blocked = dir.join("blockedvid1.mp4");
    std::fs::create_dir_all(blocked.join("in-the-way")).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    cfg.bake_clap = false;
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    let err = crate::play::fetch_trailer(state.clone(), "blockedvid1".into())
        .await
        .expect_err("the rename cannot succeed");
    assert_eq!(err.reason, "incomplete_download", "a full volume was blamed on the extractor: {err:?}");
    assert!(
        state.local_fails.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "every download failing at the rename still reported ok"
    );
}

/// The retry for "the file was evicted between fetch and open" could not do anything. The driver
/// task clears the in-flight entry only after its own await returns, and every waiter wakes on that
/// same completion — so the retry re-entered, joined the still-present entry, got the same
/// already-resolved future, was handed the same vanished path, and returned 500. Dead code in
/// exactly the case it exists for.
///
/// Driven by planting a resolved entry pointing at a path that does not exist, which is precisely
/// what the losing side of that race observes.
#[tokio::test]
async fn an_eviction_between_fetch_and_serve_is_actually_retried() {
    use futures_util::FutureExt;

    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let yt = dir.join("yt-writes");
    std::fs::write(
        &yt,
        "#!/bin/sh\nout=\"\"; prev=\"\"\nfor a in \"$@\"; do [ \"$prev\" = \"-o\" ] && out=\"$a\"; prev=\"$a\"; done\nhead -c 2048 /dev/zero > \"$out\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    cfg.bake_clap = false;
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    // A finished download whose file is gone: exactly what the retry is supposed to recover from.
    let gone = dir.join("evictedvid1.mp4");
    assert!(!gone.exists(), "the point is that this file is not there");
    let fut: crate::state::BoxFuture<Result<PathBuf, crate::ytdlp::PlayError>> =
        Box::pin(async move { Ok(gone) });
    let shared = fut.shared();
    let _ = shared.clone().await; // resolve it, so it is a COMPLETED entry
    state.in_flight.lock().unwrap().insert("evictedvid1".to_string(), (0, shared));

    let resp = crate::play::handle_play(state.clone(), &hyper::HeaderMap::new(), "evictedvid1".into()).await;
    assert_eq!(
        resp.status(),
        200,
        "the retry joined the finished download again instead of starting a fresh one"
    );
    assert!(dir.join("evictedvid1.mp4").exists(), "the retry never actually re-downloaded");
}

/// `download_sem` bounds how many downloads RUN at once, but the permit is taken inside
/// `download_cached` — so every distinct well-formed id got an `in_flight` entry and a spawned
/// driver that could sit queued for up to DOWNLOAD_TIMEOUT_SECS. On an instance without
/// PLAY_SECRET that is request-driven growth, reachable by anyone who can hit /play.
///
/// Joining a download already in flight must still be free: that is what the de-duplication is for,
/// and a viewer waiting on a trailer someone else triggered must never be turned away by the cap.
#[tokio::test]
async fn a_flood_of_distinct_ids_cannot_grow_the_in_flight_map_without_bound() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    // A yt-dlp that does not return, so downloads stay outstanding for the whole test. Only
    // DOWNLOAD_CONCURRENCY of these ever exist — the permit is taken before the spawn, so the rest
    // of the flood is tasks parked on the semaphore — and they are reaped when this test's runtime
    // drops. Emphatically NOT cleaned up with `kill_live_groups`: that registry is process-wide, and
    // the suite runs in parallel, so calling it here killed seven other tests' fake yt-dlps.
    let yt = dir.join("yt-hangs");
    std::fs::write(&yt, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    let state =
        build_state_cfg(cfg, Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());

    // Well past the cap, all distinct and all well-formed.
    for i in 0..(crate::IN_FLIGHT_MAX + 20) {
        let st = state.clone();
        let id = format!("floodvid{i:03}");
        tokio::spawn(async move { crate::play::fetch_trailer(st, id).await });
    }
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    let outstanding = state.in_flight.lock().unwrap().len();
    assert!(outstanding > 0, "nothing was queued, so this test proves nothing");
    assert!(
        outstanding <= crate::IN_FLIGHT_MAX,
        "{outstanding} outstanding downloads against a cap of {}",
        crate::IN_FLIGHT_MAX
    );

    // An id already in flight is still joined, not refused.
    let joined = state.in_flight.lock().unwrap().keys().next().cloned().expect("something is queued");
    let st = state.clone();
    let waiter = tokio::spawn(async move { crate::play::fetch_trailer(st, joined).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!waiter.is_finished(), "a request joining a live download was refused by the cap");
    waiter.abort();
}

/// A `/play` verdict was the one thing this service learned and then immediately forgot: the
/// in-flight entry is cleared however a download ends, so the next request for a video YouTube has
/// REMOVED spent another of three download permits, and another yt-dlp process, to rediscover it.
#[tokio::test]
async fn a_removed_video_is_not_re_extracted_on_every_request() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let spawns = dir.join("spawns");
    let yt = dir.join("yt-dead");
    std::fs::write(
        &yt,
        format!("#!/bin/sh\necho x >> {}\necho 'ERROR: Video unavailable' >&2\nexit 1\n", spawns.display()),
    )
    .unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir.clone());
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    cfg.bake_clap = false;
    let clock = TestClock::default();
    let state = build_state_cfg_clock(cfg, Box::new(FakeUpstream::new(&[], None)), clock.as_fn());

    let err =
        crate::play::fetch_trailer(state.clone(), "deadvideo01".into()).await.expect_err("the video is gone");
    assert_eq!(err.reason, "unavailable");
    assert_eq!(spawn_count(&spawns), 1, "the first request extracts");

    // Same answer, and it must cost nothing.
    let err = crate::play::fetch_trailer(state.clone(), "deadvideo01".into()).await.expect_err("still gone");
    assert_eq!(err.reason, "unavailable", "the cached failure lost its reason");
    assert_eq!(spawn_count(&spawns), 1, "a removed video was re-extracted on the next request");

    // ...but it is a cache, not a tombstone.
    clock.advance(crate::play::fail_ttl_ms("unavailable") + 1);
    let _ = crate::play::fetch_trailer(state.clone(), "deadvideo01".into()).await;
    assert_eq!(spawn_count(&spawns), 2, "the failure never expired");
}

/// A /play failure says when it is worth asking again, and the number is not a guess: it is exactly
/// how long the failure cache will answer this id from memory, so a client that retries sooner gets
/// the same response with no extraction behind it.
#[tokio::test]
async fn a_play_failure_says_when_to_come_back() {
    let dir = temp_dir();
    use std::os::unix::fs::PermissionsExt;
    let yt = dir.join("yt-gone");
    std::fs::write(&yt, "#!/bin/sh\necho 'ERROR: Video unavailable' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&yt, std::fs::Permissions::from_mode(0o755)).unwrap();

    let mut cfg = test_cfg(dir);
    cfg.ytdlp = yt.to_string_lossy().into_owned();
    let clock = TestClock::default();
    let state = build_state_cfg_clock(cfg, Box::new(FakeUpstream::new(&[], None)), clock.as_fn());
    let base = spawn_server(state).await;
    let ttl = crate::play::fail_ttl_ms("unavailable");

    let r = reqwest::get(format!("{base}/play/goneVideo01.mp4")).await.unwrap();
    assert_eq!(r.status(), 404);
    let retry_after = |r: &reqwest::Response| -> u64 {
        r.headers().get("retry-after").unwrap().to_str().unwrap().parse().unwrap()
    };
    assert_eq!(retry_after(&r), ttl / 1000, "the first answer is the whole window");

    // Ten minutes later the window is ten minutes shorter, and the header has to say so. Quoting
    // the full TTL again would tell a client asking near the end to wait another whole window —
    // up to twice the real cooldown, and unbounded if it keeps polling.
    clock.advance(600_000);
    let r = reqwest::get(format!("{base}/play/goneVideo01.mp4")).await.unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(
        retry_after(&r),
        (ttl - 600_000) / 1000,
        "a cached failure quoted the full TTL again instead of what is left of it"
    );
}

/// The TTL has to follow the REASON. One uniform value is wrong in both directions: short enough
/// not to pin a slow network as a dead trailer means re-extracting a removed video all afternoon;
/// long enough to stop that means a timeout during one bad minute costs the trailer for hours.
#[test]
fn a_failure_ttl_follows_the_reason() {
    let ttl = crate::play::fail_ttl_ms;
    assert!(
        ttl("unavailable") > ttl("geo_blocked"),
        "a removal is a fact about the world; a region block can lift"
    );
    assert!(
        ttl("geo_blocked") > ttl("timeout"),
        "a timeout is the reason most likely to be our network rather than the video"
    );
    assert!(ttl("timeout") > 0, "a stuck id must not be free to take a download permit every request");
    assert_eq!(ttl("something-new"), ttl("timeout"), "an unrecognised reason assumes the least");
}

/// Discovery does not probe, so a candidate that is geo-blocked or removed keeps its upstream rank
/// forever and every client rediscovers it. /play is the only thing that finds out; /meta should
/// listen — and should not spend a speculative download on an id it already knows is dead.
#[tokio::test]
async fn meta_demotes_and_stops_prewarming_a_candidate_play_found_dead() {
    let warmed: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let rec = warmed.clone();
    let prewarm: PrewarmFn = Box::new(move |_state, id| rec.lock().unwrap().push(id));
    let fake = FakeUpstream::new(&["deadFirst01", "liveSecond1"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), prewarm);

    // Exactly what a failed /play leaves behind.
    let gone = crate::ytdlp::PlayError {
        status: 404,
        reason: "unavailable".into(),
        message: "This trailer is no longer available.".into(),
        detail: "test".into(),
    };
    crate::play::record_failure(&state, "deadFirst01", &gone);

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/meta/movie/tt0111161.json")).await.unwrap();

    // An order shaped by a /play failure must not be pinned in every client for a week. The signal
    // behind it lives 60 seconds at the short end, after which this server has forgotten it.
    assert_eq!(
        resp.headers().get("cache-control").and_then(|v| v.to_str().ok()),
        Some("public, max-age=3600"),
        "a demoted ordering went out with the full 7-day max-age"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    let links = body["meta"]["links"].as_array().unwrap();
    assert!(
        links[0]["trailers"].as_str().unwrap().ends_with("/play/liveSecond1.mp4"),
        "the dead candidate was still handed out first: {links:?}"
    );
    assert!(
        links[1]["trailers"].as_str().unwrap().ends_with("/play/deadFirst01.mp4"),
        "the dead candidate was dropped rather than demoted — it may come back"
    );
    assert_eq!(
        *warmed.lock().unwrap(),
        vec!["liveSecond1".to_string()],
        "a prewarm permit was spent on a candidate already known to be dead"
    );
}

/// ...but only when the demotion actually moved something. A list already in the right order — a
/// live candidate ahead of a dead one — produces the same body the untouched path would, and
/// shortening its life to an hour makes every client re-ask 168 times more often for an answer that
/// cannot have changed.
#[tokio::test]
async fn meta_keeps_its_week_when_the_demotion_changed_nothing() {
    let fake = FakeUpstream::new(&["liveFirst01", "deadSecond1"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let gone = crate::ytdlp::PlayError {
        status: 404,
        reason: "unavailable".into(),
        message: "This trailer is no longer available.".into(),
        detail: "test".into(),
    };
    crate::play::record_failure(&state, "deadSecond1", &gone);

    let base = spawn_server(state).await;
    let resp = reqwest::get(format!("{base}/meta/movie/tt0111161.json")).await.unwrap();

    assert_eq!(
        resp.headers().get("cache-control").and_then(|v| v.to_str().ok()),
        Some("public, max-age=604800, stale-while-revalidate=86400"),
        "a response the demotion never touched lost six days of cacheability"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let links = body["meta"]["links"].as_array().unwrap();
    assert!(links[0]["trailers"].as_str().unwrap().ends_with("/play/liveFirst01.mp4"));
    assert!(links[1]["trailers"].as_str().unwrap().ends_with("/play/deadSecond1.mp4"));
}

/// A directory at a published trailer's path is served by nothing (the cache-hit check rejects it)
/// and removed by nothing — `is_published_trailer` matches on name, and both cleanup passes skip
/// non-files. So every request for that id re-downloads, fails at the rename, and bumps the health
/// counter, permanently.
#[test]
fn a_directory_masquerading_as_a_trailer_is_cleared() {
    let dir = temp_dir();
    let impostor = dir.join("impostorvid.mp4");
    std::fs::create_dir_all(impostor.join("in-the-way")).unwrap();
    std::fs::write(dir.join("realvideo01.mp4"), b"x").unwrap();
    std::fs::create_dir_all(dir.join("yt-dlp")).unwrap();
    std::fs::write(dir.join("yt-dlp").join("player.json"), b"{}").unwrap();

    crate::play::sweep_partials(&test_cfg(dir.clone()));

    assert!(!impostor.exists(), "a directory at a trailer's path survived the sweep forever");
    assert!(dir.join("realvideo01.mp4").exists(), "a real trailer was removed");
    assert!(dir.join("yt-dlp").join("player.json").exists(), "yt-dlp's own cache was removed");
}

/// The sweep and the serve path must agree on what a trailer is. `DirEntry::metadata` does not
/// follow symlinks and `fs::metadata` does, so a symlinked trailer that plays perfectly well read
/// as "not a file" to the sweep and was unlinked.
#[test]
fn a_symlinked_trailer_is_not_swept_away() {
    let dir = temp_dir();
    let real = dir.join("payload.bin");
    std::fs::write(&real, b"a real trailer").unwrap();
    let link = dir.join("linkedvid01.mp4");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    crate::play::sweep_partials(&test_cfg(dir.clone()));

    assert!(link.exists(), "a symlinked trailer the serve path would happily open was removed");
    assert!(real.exists(), "the symlink's target was removed");
}
