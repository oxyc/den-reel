//! Runtime configuration, all from the environment (same knobs as the Node service).
//!
//! Env: PORT, CACHE_DIR, YTDLP_PATH, MAX_HEIGHT, CACHE_MAX_BYTES, YTDLP_PLAYER_CLIENTS (playback);
//!      PUBLIC_BASE_URL (optional); REEL_CONFIG_KEY / REEL_CONFIG_KEYS_PREV (sealed config-in-URL).
//!      TMDB_KEY / KINOCHECK_KEY are the legacy server-side discovery keys — now a MIGRATION FALLBACK
//!      used only when a request carries no per-install config; new installs carry a BYOK TMDB key
//!      sealed in the URL (den-scout/docs/SEALED-CONFIG.md). Drop the env keys once installs migrate.

use std::env;
use std::path::PathBuf;

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
    /// EXPERIMENTAL: serve trailers as **HLS** (ffmpeg stream-copy remux of the two DASH streams into
    /// fMP4 segments) so the client plays the first segment while the rest is still muxing — instead of
    /// the download-whole-file-then-serve `/play` path. Set `STREAM_REMUX=1` to opt in; `/meta` then
    /// points `trailers` at `/hls/<id>/index.m3u8`. Off = the proven `/play/<id>.mp4` path. den-reel
    /// still proxies the bytes (googlevideo URLs are IP-locked to this server), so this is a latency
    /// win, not a bandwidth one. Letterbox is handled client-side (resizeAspectFill), not clap-baked.
    pub stream_remux: bool,
    pub max_height: String,
    pub cache_max_bytes: u64,
    /// Persist yt-dlp's nsig/player-JS cache across restarts (a subdir of the media cache).
    pub ytdlp_cache: PathBuf,
    /// Legacy server-side discovery keys — a MIGRATION FALLBACK used only when a request carries no
    /// per-install config. New installs seal a BYOK TMDB (+ optional KinoCheck) key into the URL.
    pub tmdb_key: Option<String>,
    pub kinocheck_key: Option<String>,
    /// Sealed config-in-URL (den-scout/docs/SEALED-CONFIG.md). `config_key` = current X25519 private key
    /// (base64); `config_keys_prev` = comma-separated prior keys (rotation). Empty → sealed URLs disabled.
    pub config_key: String,
    pub config_keys_prev: String,
    pub public_base_url: Option<String>,
    /// The yt-dlp format string we serve — H.264(avc1) + AAC(mp4a), ≤max_height (avc1's ceiling on
    /// YouTube), faststart-muxable. Forced so trailers play on AVPlayer's HARDWARE decode path
    /// rather than the app's software VP9/AV1 path (a CPU/heat cost not worth it for a short clip).
    /// Shared by the extract path AND the resolve-time probe, so a probe validates exactly what
    /// playback needs (a candidate that can't produce it — geo-blocked, removed, VP9/AV1-only — is
    /// skipped in favour of the next trailer).
    pub ytdlp_format: String,
    /// The `--extractor-args` value forcing YouTube's innertube **player client(s)** — `None` disables
    /// the flag (yt-dlp's own defaults). Default `youtube:player_client=tv_embedded,android`: yt-dlp
    /// queries both clients and merges their formats, so the format selector still prefers
    /// `tv_embedded`'s clean H.264 **non-signature** URLs (which sidestep the BotGuard "confirm you're
    /// not a bot" challenge AND a broken nsig/JS-runtime) whenever that client can serve the video —
    /// but falls back to `android` for the videos `tv_embedded` alone reports as "not available"
    /// (some trailers only expose formats to the android client). Without the fallback those trailers
    /// 502 on every candidate. Override with `YTDLP_PLAYER_CLIENTS` (comma-separated), or set it empty
    /// to fall back to yt-dlp's defaults.
    pub ytdlp_extractor_args: Option<String>,
    // Upstream bases are fields (not constants) so tests can point them at a local mock.
    pub tmdb_base: String,
    pub kinocheck_base: String,
}

fn env_opt(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

impl Config {
    pub fn from_env() -> Config {
        let port = env_opt("PORT").and_then(|v| v.parse().ok()).unwrap_or(8092);
        let cache_dir = env_opt("CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir().join("den-reel-cache"));
        let max_height = env_opt("MAX_HEIGHT").unwrap_or_else(|| "1080".to_string());
        let cache_max_bytes = env_opt("CACHE_MAX_BYTES")
            .and_then(|v| v.parse().ok())
            .unwrap_or(8 * 1024 * 1024 * 1024); // 8 GB
        let ytdlp_cache = cache_dir.join("yt-dlp");
        let ytdlp_format = format!(
            "bv*[height<={h}][vcodec^=avc1]+ba[acodec^=mp4a]/\
             b[height<={h}][vcodec^=avc1][acodec^=mp4a]/18/b[ext=mp4]",
            h = max_height
        );
        // tv_embedded first (BotGuard/nsig-resistant, clean avc1) with android as fallback for the
        // videos tv_embedded reports "not available"; yt-dlp merges both clients' formats. Empty disables.
        let ytdlp_extractor_args = env::var("YTDLP_PLAYER_CLIENTS")
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "tv_embedded,android".to_string());
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
            bake_clap: env_opt("CLAP").as_deref() != Some("0"),
            stream_remux: env_opt("STREAM_REMUX").as_deref() == Some("1"),
            max_height,
            cache_max_bytes,
            ytdlp_cache,
            tmdb_key: env_opt("TMDB_KEY"),
            kinocheck_key: env_opt("KINOCHECK_KEY"),
            config_key: env_opt("REEL_CONFIG_KEY").unwrap_or_default(),
            config_keys_prev: env_opt("REEL_CONFIG_KEYS_PREV").unwrap_or_default(),
            public_base_url: env_opt("PUBLIC_BASE_URL"),
            ytdlp_format,
            ytdlp_extractor_args,
            tmdb_base: "https://api.themoviedb.org/3".to_string(),
            kinocheck_base: "https://api.kinocheck.com".to_string(),
        }
    }
}
