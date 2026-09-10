//! Runtime configuration, all from the environment (same knobs as the Node service).
//!
//! Env: PORT, CACHE_DIR, YTDLP_PATH, MAX_HEIGHT, CACHE_MAX_BYTES, CACHE_TTL_DAYS, YTDLP_PLAYER_CLIENTS (playback);
//!      PUBLIC_BASE_URL (optional); CONFIG_KEY / CONFIG_KEYS_PREV (sealed config-in-URL);
//!      REEL_PLAY_SECRET (optional signing of the /play + /crop URLs); METRICS_TOKEN (turns on /metrics).
//!      TMDB_KEY / KINOCHECK_KEY are the legacy server-side discovery keys — now a MIGRATION FALLBACK
//!      used only when a request carries no per-install config; new installs carry a BYOK TMDB key
//!      sealed in the URL (den-scout/docs/SEALED-CONFIG.md). Drop the env keys once installs migrate.

use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub struct Config {
    pub port: u16,
    pub cache_dir: PathBuf,
    pub ytdlp: String,
    /// ffmpeg binary (same one yt-dlp merges with) — used for the /crop cropdetect pass.
    pub ffmpeg: String,
    /// GPAC MP4Box binary — writes the `clap` (clean aperture) box so the billboard AVPlayer crops
    /// baked-in letterbox with no app change.
    pub mp4box: String,
    /// Bake a `clap` box into the cached MP4 when a letterbox is detected. On by default; set
    /// `CLAP=0` to disable (escape hatch if a trailer ever crops wrong in prod).
    pub bake_clap: bool,
    pub max_height: String,
    pub cache_max_bytes: u64,
    /// Last-access TTL: a cached trailer not served within this window is evicted regardless of the
    /// size cap. atime is bumped on every serve, so a rewatched trailer keeps a fresh timestamp and
    /// survives; only genuinely-stale ones age out. `CACHE_TTL_DAYS=0` disables it (size cap only).
    pub cache_ttl: Duration,
    /// Persist yt-dlp's nsig/player-JS cache across restarts (a subdir of the media cache).
    pub ytdlp_cache: PathBuf,
    /// Where the resolve cache is parked across a restart. In a SUBDIRECTORY of the media cache, and
    /// that is not cosmetic: `sweep_partials` treats every top-level file that is not `<vid>.mp4` as
    /// abandoned scratch and deletes it, so a `resolve.json` next to the trailers would be reaped
    /// within the hour. Both cleanup passes skip directories.
    pub resolve_cache: PathBuf,
    /// Legacy server-side discovery keys — a MIGRATION FALLBACK used only when a request carries no
    /// per-install config. New installs seal a BYOK TMDB (+ optional KinoCheck) key into the URL.
    pub tmdb_key: Option<String>,
    pub kinocheck_key: Option<String>,
    /// Sealed config-in-URL (den-scout/docs/SEALED-CONFIG.md). `config_key` = current X25519 private key
    /// (base64); `config_keys_prev` = comma-separated prior keys (rotation). Empty → sealed URLs disabled.
    pub config_key: String,
    pub config_keys_prev: String,
    /// `REEL_PLAY_SECRET` — when set, `/meta` signs the ids it hands out and `/play` + `/crop`
    /// require the signature (see `sign.rs`). `None` disables it, which is the default and has to
    /// be: `/meta` ships `max-age=604800`, so clients hold unsigned play URLs for up to a week and
    /// turning this on unconditionally would break every install for that week.
    pub play_secret: Option<String>,
    /// `REEL_PLAY_SECRET_PREV` — comma-separated prior secrets, accepted when VERIFYING and never
    /// used to sign. Same rotation shape as `config_keys_prev`, and needed for the same reason:
    /// `/meta` ships `max-age=604800`, so a client can present a tag made with the previous secret
    /// for up to a week after it is rotated. Without this, rotating means a week of 403s.
    pub play_secrets_prev: Vec<String>,
    /// `METRICS_TOKEN` — the bearer token `/metrics` requires. `None` turns the endpoint off: it
    /// answers 404, the same as a path that does not exist.
    pub metrics_token: Option<String>,
    pub public_base_url: Option<String>,
    /// The yt-dlp format string we serve — H.264(avc1) + AAC(mp4a), ≤max_height (avc1's ceiling on
    /// YouTube), faststart-muxable. Forced so trailers play on AVPlayer's HARDWARE decode path
    /// rather than the app's software VP9/AV1 path (a CPU/heat cost not worth it for a short clip).
    /// Shared by the extract path AND the resolve-time probe, so a probe validates exactly what
    /// playback needs (a candidate that can't produce it — geo-blocked, removed, VP9/AV1-only — is
    /// skipped in favour of the next trailer).
    pub ytdlp_format: String,
    /// The `--extractor-args` value forcing YouTube's innertube **player client(s)**. `None` — the
    /// default — passes no flag at all, so yt-dlp picks.
    ///
    /// It used to pin `tv_embedded,android`, chosen because that client returned clean H.264 over
    /// non-signature URLs and so sidestepped both BotGuard and a broken nsig runtime. **`tv_embedded`
    /// no longer exists.** yt-dlp's client table renamed and reshuffled it, an unrecognised name is
    /// answered with `Skipping unsupported client "..."` — a *warning*, which `--no-warnings`
    /// swallowed — and the pin therefore silently degraded to `android` alone, which as of this
    /// yt-dlp requires a GVS PO token for both HTTPS and DASH.
    ///
    /// So do not pin by default. Picking a client is a judgement about what YouTube is currently
    /// enforcing, it changes every few weeks, and it is the judgement the yt-dlp team makes daily and
    /// we bump their releases to receive. Their current unauthenticated default is `visionos,web`,
    /// and `visionos` declares no PO-token policy and no JS-player requirement — precisely the
    /// properties `tv_embedded` was pinned for.
    ///
    /// `YTDLP_PLAYER_CLIENTS` still pins it, comma-separated, for riding out a specific outage. Be
    /// aware that a name yt-dlp has retired fails quietly, in exactly the way described above.
    pub ytdlp_extractor_args: Option<String>,
    // Upstream bases are fields (not constants) so tests can point them at a local mock.
    pub tmdb_base: String,
    pub kinocheck_base: String,
    /// Until when `play::cache_available` may answer "yes" without touching the disk again
    /// (monotonic ms, 0 = never asked). It memoises an answer about THIS config's volume, which is
    /// why it lives here rather than in a static.
    pub cache_ok_until: std::sync::atomic::AtomicU64,
    /// Bumped every time something observes the volume misbehaving. A probe reads it before starting
    /// and refuses to publish its verdict if it changed while it was in flight — the probe's awaits
    /// yield the runtime thread, so a download can fail and invalidate the memo in the middle of one,
    /// and a blind store would then re-arm from an answer taken before the failure existed.
    pub cache_epoch: std::sync::atomic::AtomicU64,
}

fn env_opt(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

/// Fallback when MAX_HEIGHT is unset or not a number. avc1's practical ceiling on YouTube.
const DEFAULT_MAX_HEIGHT: u32 = 1080;

impl Config {
    pub fn from_env() -> Config {
        let port = env_opt("PORT").and_then(|v| v.parse().ok()).unwrap_or(8092);
        let cache_dir =
            env_opt("CACHE_DIR").map(PathBuf::from).unwrap_or_else(|| env::temp_dir().join("den-reel-cache"));
        let max_height = env_opt("MAX_HEIGHT").unwrap_or_else(|| DEFAULT_MAX_HEIGHT.to_string());
        // Sized to fit den's ~10 GB container volume with headroom (was 8 GB, which could fill it).
        // Floored, for the same reason MAX_HEIGHT is: a cap smaller than one trailer parses fine and
        // inverts the setting. Eviction runs right after the rename and sees the file it just
        // published, so an unreachable cap deletes every trailer as it is produced — each /play then
        // downloads for up to 240s, evicts, retries once, and 500s, forever.
        let cache_max_bytes = env_opt("CACHE_MAX_BYTES")
            .and_then(|v| v.parse().ok())
            .filter(|b| *b >= 256 * 1024 * 1024)
            .unwrap_or(4 * 1024 * 1024 * 1024); // 4 GB
                                                // Last-access TTL: evict trailers not served within CACHE_TTL_DAYS (default 14). 0 disables it.
        let cache_ttl = Duration::from_secs(
            env_opt("CACHE_TTL_DAYS").and_then(|v| v.parse::<u64>().ok()).unwrap_or(14) * 24 * 60 * 60,
        );
        let ytdlp_cache = cache_dir.join("yt-dlp");
        let resolve_cache = cache_dir.join("state").join("resolve.json");
        // The ladder degrades in QUALITY ORDER. It used to fall from the ≤max_height rungs straight to itag
        // 18 — 360p — so any trailer whose 1080p avc1 stream was unavailable was served at 360p on a 4K
        // panel even when a perfectly good 720p existed. The intermediate rungs cost nothing when the top
        // one resolves (yt-dlp stops at the first match) and only matter when it doesn't.
        // Only rungs BELOW the cap. A fixed 720/480 ladder meant MAX_HEIGHT=480 still matched a
        // 720p rendition — looser than the cap it was asked to honour — whenever the ≤480 avc1
        // stream was missing. The last rung keeps the avc1+mp4a filter for the same reason the
        // whole string exists: an unfiltered fallback can hand AVPlayer a VP9/AV1 file.
        let rung = |h: &str| {
            format!(
                "bv*[height<={h}][vcodec^=avc1]+ba[acodec^=mp4a]/b[height<={h}][vcodec^=avc1][acodec^=mp4a]/"
            )
        };
        // The PARSED cap everywhere, including the first rung. Interpolating the raw string put
        // `height<=abc` into the selector, and yt-dlp rejects a malformed filter while BUILDING it
        // — so the whole `/`-chain dies, terminal fallback included, and every trailer 502s until
        // the env var is fixed. A typo should cost the setting, not the service.
        // A parse alone is not enough: `0` parses, and `height<=0` matches nothing, so every rung
        // fails through to the uncapped terminal fallback — the cap inverted into no cap at all.
        // 144 is YouTube's lowest rendition; below it no rung can ever match.
        let cap: u32 = max_height.parse().ok().filter(|c| *c >= 144).unwrap_or(DEFAULT_MAX_HEIGHT);
        let mut ytdlp_format = rung(&cap.to_string());
        for step in [720u32, 480] {
            if step < cap {
                ytdlp_format.push_str(&rung(&step.to_string()));
            }
        }
        ytdlp_format.push_str("18/b[ext=mp4][vcodec^=avc1][acodec^=mp4a]");
        // Unset = pass no flag = yt-dlp chooses. See the field's doc for why pinning was removed:
        // the pinned client no longer existed, and an unknown name is skipped with a warning that
        // `--no-warnings` hid.
        let ytdlp_extractor_args =
            env::var("YTDLP_PLAYER_CLIENTS").map(|v| v.trim().to_string()).unwrap_or_default();
        let ytdlp_extractor_args = if ytdlp_extractor_args.is_empty() {
            None
        } else {
            Some(format!("youtube:player_client={ytdlp_extractor_args}"))
        };
        Config {
            port,
            cache_dir,
            ytdlp: env_opt("YTDLP_PATH").unwrap_or_else(|| "yt-dlp".to_string()),
            ffmpeg: env_opt("FFMPEG_PATH").unwrap_or_else(|| "ffmpeg".to_string()),
            mp4box: env_opt("MP4BOX_PATH").unwrap_or_else(|| "MP4Box".to_string()),
            // The documented escape hatch for a mis-cropped trailer, so it has to answer to more than
            // the one spelling: `CLAP=false` silently leaving baking ON is the worst time to be strict.
            // Read through `env` rather than `env_opt`, which maps an empty value to None: someone
            // writing `-e CLAP=` is reaching for the off switch and must not get baking left on.
            bake_clap: !matches!(
                env::var("CLAP").map(|v| v.trim().to_ascii_lowercase()).as_deref(),
                Ok("0" | "false" | "off" | "no" | "")
            ),
            max_height: cap.to_string(), // normalised: a bad value falls back, never propagates
            cache_max_bytes,
            cache_ttl,
            ytdlp_cache,
            resolve_cache,
            tmdb_key: env_opt("TMDB_KEY"),
            kinocheck_key: env_opt("KINOCHECK_KEY"),
            config_key: env_opt("CONFIG_KEY").unwrap_or_default(),
            config_keys_prev: env_opt("CONFIG_KEYS_PREV").unwrap_or_default(),
            play_secret: env_opt("REEL_PLAY_SECRET"),
            play_secrets_prev: env_opt("REEL_PLAY_SECRET_PREV")
                .map(|v| v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect())
                .unwrap_or_default(),
            metrics_token: env_opt("METRICS_TOKEN"),
            public_base_url: env_opt("PUBLIC_BASE_URL"),
            ytdlp_format,
            ytdlp_extractor_args,
            tmdb_base: "https://api.themoviedb.org/3".to_string(),
            kinocheck_base: "https://api.kinocheck.com".to_string(),
            cache_ok_until: std::sync::atomic::AtomicU64::new(0),
            cache_epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }
}
