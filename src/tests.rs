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
    title: Mutex<Option<String>>,
    calls: AtomicUsize,
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
        }))
    }
    fn set_tmdb(&self, tmdb: &[&str]) {
        *self.0.tmdb.lock().unwrap() = tmdb.iter().map(|s| s.to_string()).collect();
    }
    fn set_title(&self, title: &str) {
        *self.0.title.lock().unwrap() = Some(title.to_string());
    }
    fn calls(&self) -> usize {
        self.0.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Upstream for FakeUpstream {
    async fn tmdb_candidates(&self, _tmdb_key: &str, _imdb: &str, _ty: &str, _lang: &str) -> Vec<String> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.tmdb.lock().unwrap().clone()
    }
    async fn kinocheck_youtube_id(&self, _kinocheck_key: Option<&str>, _imdb: &str, _ty: &str, _lang: &str) -> Option<String> {
        self.0.kc.lock().unwrap().clone()
    }
    async fn tmdb_title(&self, _tmdb_key: &str, _imdb: &str, _ty: &str) -> Option<String> {
        self.0.title.lock().unwrap().clone()
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
        mp4box: "MP4Box".into(),
        bake_clap: true,
        max_height: "1080".into(),
        cache_max_bytes: 8 * 1024 * 1024 * 1024,
        tmdb_key: Some("test-key".into()),
        kinocheck_key: None,
        config_key: String::new(),
        config_keys_prev: String::new(),
        public_base_url: None,
        ytdlp_format: "fmt".into(),
        ytdlp_extractor_args: Some("youtube:player_client=tv_embedded".into()),
        tmdb_base: "http://unused".into(),
        kinocheck_base: "http://unused".into(),
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
    Box::new(|_q| Box::pin(async { Vec::<String>::new() }))
}

fn build_state(cache_dir: PathBuf, upstream: Box<dyn Upstream>, prober: ProbeFn, prewarm: PrewarmFn) -> Arc<AppState> {
    build_state_cfg(test_cfg(cache_dir), upstream, prober, prewarm)
}

/// Like `build_state` but with an explicit `Config` — lets a test enable the sealed-config keyring
/// (via `config_key`) exactly the way production does.
fn build_state_cfg(cfg: Config, upstream: Box<dyn Upstream>, prober: ProbeFn, prewarm: PrewarmFn) -> Arc<AppState> {
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
        upstream,
        prober,
        searcher,
        prewarm,
        clock: Box::new(default_clock),
        download_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::DOWNLOAD_CONCURRENCY)),
        probe_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::PROBE_CONCURRENCY)),
        extract_fails: std::sync::atomic::AtomicU32::new(0),
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
    let out = crate::addon::build_meta("movie", "tt0111161", "https://trailers.example.com/", &["abc123DEF".to_string()]);
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

// --- /health (ADDON-02) ------------------------------------------------------

#[test]
fn health_reports_degraded_and_ok_states() {
    // No TMDB key AND no sealed-config keyring → trailers can't work → degraded.
    assert_eq!(
        crate::health_body(false, 0, 0),
        json!({"status": "degraded", "reason": "tmdb_key_missing", "detail": "set REEL_CONFIG_KEY (per-install BYOK) or TMDB_KEY"})
    );
    // A missing key wins even if upstreams / the extractor are also failing.
    assert_eq!(crate::health_body(false, 99, 99)["reason"], "tmdb_key_missing");

    // Key present but upstreams have been failing (>= threshold) → degraded (wins over the extractor).
    assert_eq!(
        crate::health_body(true, 3, 99),
        json!({"status": "degraded", "reason": "upstream_unavailable", "detail": "TMDB/KinoCheck have been failing"})
    );
    assert_eq!(crate::health_body(true, 4, 0)["reason"], "upstream_unavailable");

    // Upstreams fine but yt-dlp can't extract anything (>= threshold) → degraded (the silent-outage gap).
    assert_eq!(
        crate::health_body(true, 0, 3)["reason"],
        json!("extractor_unavailable")
    );
    assert_eq!(crate::health_body(true, 0, 2), json!({"status": "ok"})); // below threshold → ok

    // Key present, everything below the threshold → ok.
    assert_eq!(crate::health_body(true, 0, 0), json!({"status": "ok"}));
    assert_eq!(crate::health_body(true, 2, 2), json!({"status": "ok"}));
}

// --- resolve logic ----------------------------------------------------------

#[tokio::test]
async fn resolve_returns_first_playable_and_caches() {
    let fake = FakeUpstream::new(&["firstGood11"], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());

    assert_eq!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.first().map(String::as_str), Some("firstGood11"));
    let after = fake.calls();
    assert_eq!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.first().map(String::as_str), Some("firstGood11"));
    assert_eq!(fake.calls(), after, "second lookup is a cache hit (no new upstream calls)");
}

#[tokio::test]
async fn resolve_returns_alternates_after_the_primary_for_fallback() {
    // Best-playable pick first, then the other candidates as unprobed fallbacks (#5 — the client tries
    // the next one on a playback failure). No extra probing beyond first_playable.
    let fake = FakeUpstream::new(&["playable1", "playable2"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(ids, vec!["playable1".to_string(), "playable2".to_string()]);
}

#[tokio::test]
async fn resolve_returns_candidates_in_rank_order_without_probing() {
    // No resolve-time probe: the TMDB/KinoCheck candidates come back in rank order, unvalidated. A dead /
    // geo-blocked / portrait pick is the client's problem (it advances to the next), and playability is
    // validated lazily on /play — so /meta never spawns yt-dlp.
    let fake = FakeUpstream::new(&["blockedUS01", "worldwide22"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(ids, vec!["blockedUS01".to_string(), "worldwide22".to_string()]);
}

#[tokio::test]
async fn resolve_empty_only_when_no_candidates_at_all() {
    // Empty ONLY when TMDB/KinoCheck carry no trailer (and search finds nothing) — never because a
    // candidate looked unplayable (that check moved to /play).
    let fake = FakeUpstream::new(&["someCandidate"], None);
    let state = build_state(temp_dir(), Box::new(fake), always_playable(), noop_prewarm());
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(ids, vec!["someCandidate".to_string()]);
}

#[tokio::test]
async fn resolve_falls_back_to_youtube_search_when_no_candidates() {
    // TMDB + KinoCheck carry no trailer, but the title is known → search YouTube and return the results
    // (in search order; no probe — the client picks the first that plays + is landscape).
    let fake = FakeUpstream::new(&[], None);
    fake.set_title("Backrooms 2025");
    let searcher: crate::state::SearchFn =
        Box::new(|_q| Box::pin(async { vec!["searchOne".into(), "searchTwo".into()] }));
    let state = build_state_full(test_cfg(temp_dir()), Box::new(fake), always_playable(), noop_prewarm(), searcher);
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt99999999", "movie", "en").await;
    assert_eq!(ids, vec!["searchOne".to_string(), "searchTwo".to_string()]);
}

#[tokio::test]
async fn resolve_no_search_when_title_unknown() {
    // No candidates AND no title → the search fallback can't build a query → empty, no panic.
    let fake = FakeUpstream::new(&[], None); // title left None
    let prober: ProbeFn = Box::new(|_id| Box::pin(async { crate::ytdlp::Probe::Playable { landscape: true } }));
    let searcher: crate::state::SearchFn =
        Box::new(|_q| Box::pin(async { vec!["shouldNotBeUsed".into()] }));
    let state = build_state_full(test_cfg(temp_dir()), Box::new(fake), prober, noop_prewarm(), searcher);
    assert_eq!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0", "movie", "en").await, Vec::<String>::new());
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
    // Default test state has no REEL_CONFIG_KEY → sealing disabled.
    let state = build_state(temp_dir(), Box::new(FakeUpstream::new(&[], None)), always_playable(), noop_prewarm());
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
        .send().await.unwrap().json().await.unwrap();
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
    let body: Value = reqwest::get(format!("{base}/{seg}/meta/movie/tt0111161.json"))
        .await.unwrap().json().await.unwrap();
    assert_eq!(body["meta"]["links"].as_array().unwrap().len(), 1, "legacy plaintext config must still resolve");
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
    let portrait = refine_report(report_from("x", Some((720, 1280)), RawCrop { w: 640, h: 404, x: 40, y: 438 }));
    assert!(!portrait.letterboxed, "a portrait source must not be letterbox-cropped");
    assert_eq!(portrait.content.as_ref().map(|c| (c.w, c.h)), Some((720, 1280)));

    // Pillarbox (side bars, not top/bottom) → not our job → full frame.
    let pillar = refine_report(report_from("x", Some((1920, 1080)), RawCrop { w: 1200, h: 1080, x: 360, y: 0 }));
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
    let boxed = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 });
    assert!(boxed.letterboxed);
    assert_eq!(boxed.aspect, Some(2.35));
    // 1080 → 1072 content = 8px (<2%) → treated as noise, not letterboxed.
    let noise = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 1072, x: 0, y: 4 });
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
        .args(["-y", "-f", "lavfi", "-i", "testsrc=size=1920x816:rate=24:d=2",
               "-vf", "pad=1920:1080:0:132:color=black", "-c:v", "libx264",
               "-g", "6", "-pix_fmt", "yuv420p", "-movflags", "+faststart"])
        .arg(&fp)
        .status().unwrap().success();
    assert!(ok, "ffmpeg failed to build the letterbox fixture");

    let cfg = test_cfg(dir);
    let report = crate::crop::detect(&cfg, "clapvid0001", &fp).await.expect("detect returned a rect");
    assert!(report.letterboxed, "132px bars should read as letterboxed");
    assert_eq!(report.content.as_ref().unwrap().h, 816);
    assert!(crate::crop::bake_clap(&cfg, &fp, &report).await, "MP4Box should write the clap box");

    // ffprobe reads the clap back as frame cropping — 132px top & bottom.
    let out = std::process::Command::new("ffprobe")
        .args(["-hide_banner", "-v", "error", "-show_streams"]).arg(&fp)
        .output().unwrap();
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
    assert!(crate::crop::bake_clap(&cfg, &fp, &report).await, "MP4Box should write the clap box");

    let out = std::process::Command::new("ffprobe")
        .args(["-hide_banner", "-v", "error", "-show_streams"]).arg(&fp)
        .output().unwrap();
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
    assert!(!crate::crop::bake_clap(&cfg, &fp, &report).await, "no clap baked for a full-frame report");
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
            "-y", "-f", "lavfi", "-i", "testsrc=size=720x404:rate=24:d=3",
            "-vf", "pad=720:1280:0:438:color=black",
            "-c:v", "libx264", "-g", "6", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ])
        .arg(&fp)
        .status().unwrap().success();
    assert!(ok, "ffmpeg failed to build the portrait fixture");

    let cfg = test_cfg(dir);
    let report = crate::crop::detect(&cfg, "portrait0001", &fp).await.expect("detect returned a report");
    assert!(!report.letterboxed, "a portrait source must not be letterbox-cropped");
    assert_eq!(report.content.as_ref().unwrap().h, 1280, "full portrait frame kept, not a thin strip");
    assert!(!crate::crop::bake_clap(&cfg, &fp, &report).await, "no clap baked for a portrait trailer");
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
    let centered = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 816, x: 0, y: 132 });
    assert_eq!(crate::crop::clap_params(&centered), Some((1920, 816, 0, 0)));

    // Logo kept in the bottom bar → content off-centre downward → positive vertOff (num over 2).
    let off = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 922, x: 0, y: 132 });
    assert_eq!(crate::crop::clap_params(&off), Some((1920, 922, 0, 106))); // 106/2 = 53px

    // Not letterboxed → nothing to bake.
    let full = crate::crop::report_from("x", Some((1920, 1080)), crate::crop::RawCrop { w: 1920, h: 1080, x: 0, y: 0 });
    assert_eq!(crate::crop::clap_params(&full), None);
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
