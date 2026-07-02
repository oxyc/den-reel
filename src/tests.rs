//! Ports meta.test.js: the pure functions, the resolve/cache logic (with a fake upstream + a
//! stubbed prober, so no network and no yt-dlp), and the HTTP /meta + /play serve contract.

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::state::{default_clock, AppState, PrewarmFn, ProbeFn};
use crate::upstream::{pick_trailer_candidates, Upstream};
use crate::ytdlp::classify;

// --- fakes / builders -------------------------------------------------------

struct FakeInner {
    tmdb: Mutex<Vec<String>>,
    kc: Mutex<Option<String>>,
    calls: AtomicUsize,
}

#[derive(Clone)]
struct FakeUpstream(Arc<FakeInner>);

impl FakeUpstream {
    fn new(tmdb: &[&str], kc: Option<&str>) -> FakeUpstream {
        FakeUpstream(Arc::new(FakeInner {
            tmdb: Mutex::new(tmdb.iter().map(|s| s.to_string()).collect()),
            kc: Mutex::new(kc.map(|s| s.to_string())),
            calls: AtomicUsize::new(0),
        }))
    }
    fn set_tmdb(&self, tmdb: &[&str]) {
        *self.0.tmdb.lock().unwrap() = tmdb.iter().map(|s| s.to_string()).collect();
    }
    fn calls(&self) -> usize {
        self.0.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Upstream for FakeUpstream {
    async fn tmdb_candidates(&self, _imdb: &str, _ty: &str, _lang: &str) -> Vec<String> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.tmdb.lock().unwrap().clone()
    }
    async fn kinocheck_youtube_id(&self, _imdb: &str, _ty: &str, _lang: &str) -> Option<String> {
        self.0.kc.lock().unwrap().clone()
    }
}

static TMP_CNT: AtomicUsize = AtomicUsize::new(0);
fn temp_dir() -> PathBuf {
    let n = TMP_CNT.fetch_add(1, Ordering::SeqCst);
    let p = std::env::temp_dir().join(format!("den-reel-test-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn test_cfg(cache_dir: PathBuf) -> Config {
    Config {
        port: 8092,
        ytdlp_cache: cache_dir.join("yt-dlp"),
        cache_dir,
        ytdlp: "yt-dlp".into(),
        ffmpeg: "ffmpeg".into(),
        max_height: "1080".into(),
        cache_max_bytes: 8 * 1024 * 1024 * 1024,
        tmdb_key: Some("test-key".into()),
        kinocheck_key: None,
        public_base_url: None,
        ytdlp_format: "fmt".into(),
        tmdb_base: "http://unused".into(),
        kinocheck_base: "http://unused".into(),
    }
}

fn always_playable() -> ProbeFn {
    Box::new(|_id| Box::pin(async { true }))
}
fn noop_prewarm() -> PrewarmFn {
    Box::new(|_state, _id| {})
}

fn build_state(cache_dir: PathBuf, upstream: Box<dyn Upstream>, prober: ProbeFn, prewarm: PrewarmFn) -> Arc<AppState> {
    Arc::new(AppState {
        cfg: Arc::new(test_cfg(cache_dir)),
        yt_cache: Mutex::new(HashMap::new()),
        in_flight: Mutex::new(HashMap::new()),
        dl_gen: std::sync::atomic::AtomicU64::new(0),
        crop_cache: Mutex::new(HashMap::new()),
        upstream,
        prober,
        prewarm,
        clock: Box::new(default_clock),
    })
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
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
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
    assert_eq!(
        pick_trailer_candidates(&results),
        vec!["official111", "plain222222", "teaser00000"]
    );
}

#[test]
fn build_meta_produces_same_host_play_url() {
    let out = crate::addon::build_meta("movie", "tt0111161", "https://trailers.example.com/", "abc123DEF");
    assert_eq!(
        out["meta"]["links"][0]["trailers"],
        "https://trailers.example.com/play/abc123DEF.mp4"
    );
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

// --- resolve logic ----------------------------------------------------------

#[tokio::test]
async fn resolve_returns_first_playable_and_caches() {
    let fake = FakeUpstream::new(&["firstGood11"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());

    assert_eq!(crate::addon::resolve_youtube_id(&state, "tt0111161", "movie", "en").await, "firstGood11");
    let after = fake.calls();
    assert_eq!(crate::addon::resolve_youtube_id(&state, "tt0111161", "movie", "en").await, "firstGood11");
    assert_eq!(fake.calls(), after, "second lookup is a cache hit (no new upstream calls)");
}

#[tokio::test]
async fn resolve_skips_geoblocked_and_falls_back() {
    let fake = FakeUpstream::new(&["blockedUS01", "worldwide22"], None);
    let prober: ProbeFn = Box::new(|id| Box::pin(async move { id != "blockedUS01" }));
    let state = build_state(temp_dir(), Box::new(fake), prober, noop_prewarm());
    assert_eq!(crate::addon::resolve_youtube_id(&state, "tt0111161", "movie", "en").await, "worldwide22");
}

#[tokio::test]
async fn resolve_returns_empty_when_none_playable() {
    let fake = FakeUpstream::new(&["blockedUS01"], None);
    let prober: ProbeFn = Box::new(|_id| Box::pin(async { false }));
    let state = build_state(temp_dir(), Box::new(fake), prober, noop_prewarm());
    assert_eq!(crate::addon::resolve_youtube_id(&state, "tt0111161", "movie", "en").await, "");
}

// --- HTTP contract ----------------------------------------------------------

#[tokio::test]
async fn get_manifest_returns_addon_manifest() {
    let state = build_state(temp_dir(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let body: Value = reqwest::get(format!("{base}/manifest.json")).await.unwrap().json().await.unwrap();
    assert_eq!(body["resources"][0], "meta");
}

#[tokio::test]
async fn get_meta_rejects_non_imdb_with_no_upstream_call() {
    let fake = FakeUpstream::new(&["should-not-be-used"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());
    let base = spawn_server(state).await;
    let body: Value = reqwest::get(format!("{base}/meta/movie/not-an-id.json")).await.unwrap().json().await.unwrap();
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
    assert_eq!(
        body["meta"]["links"][0]["trailers"],
        "https://trailers.example.com/play/vidKey12345.mp4"
    );
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
    fake.set_tmdb(&[]); // no trailer → empty links → must NOT be cached
    let empty = client.get(format!("{base}/meta/movie/tt0111161.json")).send().await.unwrap();
    assert!(empty.headers().get("cache-control").is_none());
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
fn parse_crop_takes_the_last_accumulated_box() {
    // cropdetect (reset=0) prints progressively wider boxes; the last is the conservative union.
    let stderr = "\
[Parsed_cropdetect_0 @ 0x1] x1:0 x2:1919 y1:140 y2:939 crop=1920:800:0:140\n\
[Parsed_cropdetect_0 @ 0x1] x1:0 x2:1919 y1:132 y2:947 crop=1920:816:0:132\n";
    assert_eq!(
        crate::crop::parse_crop(stderr),
        Some(crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 })
    );
}

#[test]
fn parse_source_dims_reads_the_video_stream_line() {
    let stderr = "  Stream #0:0(und): Video: h264 (High) (avc1 / 0x31637661), yuv420p, 1920x1080 [SAR 1:1 DAR 16:9], 24 fps";
    assert_eq!(crate::crop::parse_source_dims(stderr), Some((1920, 1080)));
}

#[test]
fn report_flags_letterbox_but_not_pixel_noise() {
    // 1080 → 816 content = 264px bars (~24%) → letterboxed, ~2.35 aspect.
    let boxed = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 });
    assert!(boxed.letterboxed);
    assert_eq!(boxed.aspect, Some(2.35));
    // 1080 → 1072 content = 8px (<2%) → treated as noise, not letterboxed.
    let noise = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 1072, x: 0, y: 4 });
    assert!(!noise.letterboxed);
}

#[test]
fn eviction_evicts_real_files_but_skips_partial_dotfiles() {
    let dir = temp_dir();
    std::fs::write(dir.join("aaaaaa.mp4"), vec![0u8; 100]).unwrap();
    std::fs::write(dir.join(".bbbbbb.123.0.partial.mp4"), vec![0u8; 100]).unwrap();
    let mut cfg = test_cfg(dir.clone());
    cfg.cache_max_bytes = 1; // force eviction of everything eligible
    crate::play::evict_if_needed(&cfg);
    assert!(!dir.join("aaaaaa.mp4").exists(), "completed file should be evicted");
    assert!(
        dir.join(".bbbbbb.123.0.partial.mp4").exists(),
        "in-progress .partial temp must be skipped by eviction"
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
