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
use crate::upstream::{pick_trailer_candidates, Upstream};
use crate::ytdlp::classify;

// --- fakes / builders -------------------------------------------------------

struct FakeInner {
    tmdb: Mutex<Vec<String>>,
    kc: Mutex<Option<String>>,
    title: Mutex<Option<String>>,
    calls: AtomicUsize,
    faults: AtomicU64,
    fail: std::sync::atomic::AtomicBool,
    fail_kc: std::sync::atomic::AtomicBool,
    kc_faults: AtomicU64,
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
            faults: AtomicU64::new(0),
            fail: std::sync::atomic::AtomicBool::new(false),
            fail_kc: std::sync::atomic::AtomicBool::new(false),
            kc_faults: AtomicU64::new(0),
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
    /// Make the next lookup look like a transport error / 401 / 429 / 5xx: an empty result that is
    /// NOT an answer. The fault must land DURING the call, which is the only thing that
    /// distinguishes it from a title that genuinely has no trailer.
    fn fail_next(&self) {
        self.0.fail.store(true, Ordering::SeqCst);
    }
    /// A hard fault on the FALLBACK source, as the real upstream records it.
    fn fail_fallback(&self) {
        self.0.fail_kc.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl Upstream for FakeUpstream {
    async fn tmdb_candidates(&self, _tmdb_key: &str, _imdb: &str, _ty: &str, _lang: &str) -> Vec<String> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        if self.0.fail.swap(false, Ordering::SeqCst) {
            self.0.faults.fetch_add(1, Ordering::SeqCst);
            return Vec::new();
        }
        self.0.tmdb.lock().unwrap().clone()
    }
    async fn kinocheck_youtube_id(&self, _kinocheck_key: Option<&str>, _imdb: &str, _ty: &str, _lang: &str) -> Option<String> {
        if self.0.fail_kc.load(Ordering::SeqCst) {
            self.0.kc_faults.fetch_add(1, Ordering::SeqCst);
            return None;
        }
        self.0.kc.lock().unwrap().clone()
    }
    async fn tmdb_title(&self, _tmdb_key: &str, _imdb: &str, _ty: &str) -> Option<String> {
        self.0.title.lock().unwrap().clone()
    }
    fn hard_faults(&self) -> u64 {
        self.0.faults.load(Ordering::SeqCst)
    }
    fn fallback_faults(&self) -> u64 {
        self.0.kc_faults.load(Ordering::SeqCst)
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
        cache_ttl: std::time::Duration::from_secs(365 * 24 * 60 * 60), // effectively off for the size-cap tests
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
    Box::new(|_q| Box::pin(async { Some(Vec::<String>::new()) }))
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
        prewarm_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::PREWARM_MAX)),
        probe_sem: std::sync::Arc::new(tokio::sync::Semaphore::new(crate::PROBE_CONCURRENCY)),
        extract_fails: std::sync::atomic::AtomicU32::new(0),
    })
}

fn build_state_clock(cache_dir: PathBuf, upstream: Box<dyn Upstream>, clock: crate::state::ClockFn) -> Arc<AppState> {
    let state = build_state(cache_dir, upstream, always_playable(), noop_prewarm());
    let mut state = Arc::try_unwrap(state).ok().expect("sole owner");
    state.clock = clock;
    Arc::new(state)
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
        json!({"status": "degraded", "reason": "upstream_unavailable", "detail": "TMDB has been failing"})
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
        Box::new(|_q| Box::pin(async { Some(vec!["searchOne".into(), "searchTwo".into()]) }));
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
        Box::new(|_q| Box::pin(async { Some(vec!["shouldNotBeUsed".into()]) }));
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
        fmt.split("height<=")
            .skip(1)
            .filter_map(|t| t.split(']').next()?.parse::<u32>().ok())
            .collect()
    };
    for (cap, expect_rungs) in [("1080", vec![1080, 1080, 720, 720, 480, 480]), ("720", vec![720, 720, 480, 480]), ("480", vec![480, 480]), ("360", vec![360, 360])] {
        std::env::set_var("MAX_HEIGHT", cap);
        let cfg = crate::config::Config::from_env();
        std::env::remove_var("MAX_HEIGHT");
        let cap_n: u32 = cap.parse().unwrap();
        let got = heights_in(&cfg.ytdlp_format);
        assert!(
            got.iter().all(|h| *h <= cap_n),
            "cap {cap}: ladder reaches above it: {got:?}"
        );
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
    std::fs::write(
        &script,
        format!("#!/bin/sh\nsh -c 'sleep 30; : > {}' &\nsleep 30\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&script, <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755)).unwrap();

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
        crate::upstream::pick_trailer_candidates(&results),
        vec!["dQw4w9WgXcQ".to_string()],
        "an id that is not a YouTube id must not reach a filename"
    );
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
    assert!(dir.join(".bbbbbb.2.0.partial.mp4.part").exists(), "a live download was deleted under its writer");
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
    std::fs::write(dir.join("fresh.mp4"), vec![0u8; 100]).unwrap();
    std::fs::write(dir.join("stale.mp4"), vec![0u8; 100]).unwrap();
    // Age stale.mp4's last-access to 20 days ago (past a 14-day TTL); fresh.mp4 stays at "now".
    let old = SystemTime::now() - Duration::from_secs(20 * 24 * 60 * 60);
    let f = std::fs::File::open(dir.join("stale.mp4")).unwrap();
    f.set_times(std::fs::FileTimes::new().set_accessed(old)).unwrap();
    let mut cfg = test_cfg(dir.clone());
    cfg.cache_ttl = Duration::from_secs(14 * 24 * 60 * 60); // 14-day TTL
    cfg.cache_max_bytes = u64::MAX; // isolate the TTL: the size cap must not interfere
    crate::play::evict_if_needed(&cfg);
    assert!(!dir.join("stale.mp4").exists(), "trailer past the last-access TTL should be evicted");
    assert!(dir.join("fresh.mp4").exists(), "recently-served trailer must be kept");
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
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.is_empty());

    // Within the cooldown the failure is not re-asked — that is what bounds the stampede.
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS / 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.is_empty());
    assert_eq!(fake.calls(), after, "a failed lookup is not rate-limited at all");

    // Past it — and long before a real negative would have expired — the recovery is visible.
    fake.set_tmdb(&["realTrailer"]);
    clock.advance(crate::YT_FAIL_TTL_MS);
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await;
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

    let before = up.hard_faults();
    up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await;
    assert_eq!(
        up.hard_faults(),
        before,
        "a dead fallback counted as 'no answer', which disables the negative cache service-wide"
    );

    up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await;
    assert!(up.hard_faults() > before, "a dead TMDB did not register as 'no answer'");
}

/// A keyless lookup asks a narrower question — only KinoCheck runs — so its answer lives under its
/// own cache key. Sharing it let a config-less /meta blank titles for installs that DO have a key.
#[tokio::test]
async fn a_keyless_answer_does_not_blank_the_title_for_keyed_installs() {
    let fake = FakeUpstream::new(&[], None);
    let state = build_state(temp_dir(), Box::new(fake.clone()), always_playable(), noop_prewarm());

    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en").await.is_empty());

    fake.set_tmdb(&["realTrailer"]);
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(
        ids.first().map(String::as_str),
        Some("realTrailer"),
        "a keyless lookup was cached as the keyed answer"
    );
}

/// ...and the keyless answer is still cached in its own right, at the FULL negative TTL. Treating a
/// missing key as a transient failure meant re-asking KinoCheck every 60s, forever, per title.
#[tokio::test]
async fn a_keyless_answer_is_cached_for_a_full_negative_ttl() {
    let fake = FakeUpstream::new(&[], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en").await.is_empty());
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS * 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en").await.is_empty());
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

    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.is_empty());
    let after = fake.calls();

    // Well past the failure cooldown — a real answer must not be re-asked on that schedule.
    clock.advance(crate::YT_FAIL_TTL_MS * 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.is_empty());
    assert_eq!(fake.calls(), after, "a real 'no trailer' was re-asked at the failure cooldown");

    // ...and it does expire eventually, so a geo-block or a late-added trailer is picked up.
    fake.set_tmdb(&["realTrailer"]);
    clock.advance(crate::YT_NEG_TTL_MS);
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await;
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
    assert!(vary.contains("x-forwarded-host"), "cacheable body did not vary on the host it embedded: {vary:?}");
    assert!(vary.contains("x-forwarded-proto"), "cacheable body did not vary on the scheme it embedded: {vary:?}");
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

    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt99999999", "movie", "en").await.is_empty());

    fake.set_tmdb(&["realTrailer"]);
    clock.advance(crate::YT_FAIL_TTL_MS + 1);
    let ids = crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt99999999", "movie", "en").await;
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

async fn serve_once_bytes(status_line: &str, content_length: usize, body: Vec<u8>) -> String {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let head = format!("{status_line}\r\ncontent-type: application/json\r\ncontent-length: {content_length}\r\n\r\n");
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

        let before = up.hard_faults();
        up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await;
        assert!(
            up.hard_faults() > before,
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
    up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await;
    assert!(
        up.recent_failures() > 0,
        "/health stayed green through an outage that empties every resolve"
    );
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

    let before = up.hard_faults();
    up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await;
    assert!(up.hard_faults() > before, "an oversize body was buffered and accepted as an answer");
}

/// With no TMDB key, KinoCheck is the only source consulted — so its outage is a total failure to
/// get an answer, not the ignorable fallback blip it is for a keyed install.
#[tokio::test]
async fn a_keyless_lookup_treats_a_fallback_outage_as_no_answer() {
    let mut cfg = test_cfg(temp_dir());
    cfg.kinocheck_base = "http://127.0.0.1:1/kinocheck".to_string();
    let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());

    let before = up.fallback_faults();
    up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await;
    assert!(
        up.fallback_faults() > before,
        "a keyless install's only source failed and nothing recorded it"
    );
    // ...and it still must not move the TMDB-facing signals.
    assert_eq!(up.recent_failures(), 0, "a fallback outage degraded /health");
    assert_eq!(up.hard_faults(), 0, "a fallback outage disabled the negative cache for keyed installs");
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
    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en").await.is_empty());

    // Past the cooldown but far short of a real negative: the outage must be re-asked.
    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS + 1);
    assert!(crate::addon::resolve_youtube_ids(&state, "", None, "tt0111161", "movie", "en").await.is_empty());
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
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.is_empty());

    let after = fake.calls();
    clock.advance(crate::YT_FAIL_TTL_MS * 2);
    assert!(crate::addon::resolve_youtube_ids(&state, "test-key", None, "tt0111161", "movie", "en").await.is_empty());
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
        up.kinocheck_youtube_id(None, "tt0111161", "movie", "en").await;
        assert!(
            up.fallback_faults() > 0,
            "{status_line:?} on the fallback source recorded nothing; a keyless install would cache it as an answer"
        );
        assert_eq!(up.hard_faults(), 0, "a fallback fault reached the TMDB counter");

        // ...and the primary.
        let base = serve_once(status_line, len, body).await;
        let mut cfg = test_cfg(temp_dir());
        cfg.tmdb_base = base;
        let up = crate::upstream::HttpUpstream::new(Arc::new(cfg), reqwest::Client::new());
        up.tmdb_candidates("test-key", "tt0111161", "movie", "en").await;
        assert!(up.hard_faults() > 0, "{status_line:?} on TMDB recorded nothing");
        assert_eq!(up.fallback_faults(), 0, "a TMDB fault reached the fallback counter");
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

        up.tmdb_candidates("wrong-key", "tt0111161", "movie", "en").await;
        assert!(up.hard_faults() > 0, "{status_line:?} was cached as a real 'no trailer'");
        assert_eq!(up.recent_failures(), 0, "{status_line:?} marked the upstream itself down");
    }
}

/// The resolve cache key is credential-free and shared by every install, so an install whose key is
/// wrong must not replace a working install's trailer list with an empty one — that is a cache HIT
/// for the whole window, so the title shows no trailer and no upstream call happens to correct it.
#[tokio::test]
async fn a_failing_install_does_not_blank_a_cached_trailer_for_everyone() {
    let fake = FakeUpstream::new(&["goodTrailer1"], None);
    let clock = TestClock::default();
    let state = build_state_clock(temp_dir(), Box::new(fake.clone()), clock.as_fn());

    // A healthy install caches a real answer, which then expires.
    let ids = crate::addon::resolve_youtube_ids(&state, "good-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(ids.first().map(String::as_str), Some("goodTrailer1"));
    clock.advance(crate::YT_TTL_MS + 1);

    // An install with a wrong key resolves the same title and gets nothing.
    fake.set_tmdb(&[]);
    fake.fail_next();
    let broken = crate::addon::resolve_youtube_ids(&state, "wrong-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(
        broken.first().map(String::as_str),
        Some("goodTrailer1"),
        "a failed lookup discarded the answer we already had"
    );

    // The healthy install must still see its trailer, and the failure must not be serving as a hit.
    fake.set_tmdb(&["goodTrailer1"]);
    let ids = crate::addon::resolve_youtube_ids(&state, "good-key", None, "tt0111161", "movie", "en").await;
    assert_eq!(
        ids.first().map(String::as_str),
        Some("goodTrailer1"),
        "one install's bad key blanked the trailer for every install"
    );
}

/// "error decoding response body" is what reqwest Displays for a truncation and for a timeout
/// alike — one is the upstream dying mid-response, the other is it wedging, and that difference is
/// the whole diagnostic during the outage this arm exists for.
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

    let slow = serve_once("HTTP/1.1 200 OK", 500, "{\"re").await;
    let stalled = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(120))
        .build()
        .unwrap()
        .get(format!("{slow}/x?api_key=SUPERSECRETKEY"))
        .send()
        .await
        .expect("headers arrive")
        .bytes()
        .await
        .expect_err("a stalled body must fail");

    let a = crate::upstream::body_fault_why(&truncated);
    let b = crate::upstream::body_fault_why(&stalled);
    assert!(!a.contains("SUPERSECRETKEY") && !b.contains("SUPERSECRETKEY"), "{a} / {b}");
    assert_ne!(a, b, "a truncated body and a stalled one log the same line: {a}");
}
