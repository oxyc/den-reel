//! Runtime configuration, all from the environment (same knobs as the Node service).
//!
//! Env: PORT, CACHE_DIR, YTDLP_PATH, MAX_HEIGHT, CACHE_MAX_BYTES (playback);
//!      TMDB_KEY (required for the addon), KINOCHECK_KEY (optional), PUBLIC_BASE_URL (optional).

use std::env;
use std::path::PathBuf;

pub struct Config {
    pub port: u16,
    pub cache_dir: PathBuf,
    pub ytdlp: String,
    /// ffmpeg binary (same one yt-dlp merges with) — used for the /crop cropdetect pass.
    pub ffmpeg: String,
    pub max_height: String,
    pub cache_max_bytes: u64,
    /// Persist yt-dlp's nsig/player-JS cache across restarts (a subdir of the media cache).
    pub ytdlp_cache: PathBuf,
    pub tmdb_key: Option<String>,
    pub kinocheck_key: Option<String>,
    pub public_base_url: Option<String>,
    /// The yt-dlp format string we serve — H.264(avc1) + AAC(mp4a), ≤max_height (avc1's ceiling on
    /// YouTube), faststart-muxable. Forced so trailers play on AVPlayer's HARDWARE decode path
    /// rather than the app's software VP9/AV1 path (a CPU/heat cost not worth it for a short clip).
    /// Shared by the extract path AND the resolve-time probe, so a probe validates exactly what
    /// playback needs (a candidate that can't produce it — geo-blocked, removed, VP9/AV1-only — is
    /// skipped in favour of the next trailer).
    pub ytdlp_format: String,
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
        Config {
            port,
            cache_dir,
            ytdlp: env_opt("YTDLP_PATH").unwrap_or_else(|| "yt-dlp".to_string()),
            ffmpeg: env_opt("FFMPEG_PATH").unwrap_or_else(|| "ffmpeg".to_string()),
            max_height,
            cache_max_bytes,
            ytdlp_cache,
            tmdb_key: env_opt("TMDB_KEY"),
            kinocheck_key: env_opt("KINOCHECK_KEY"),
            public_base_url: env_opt("PUBLIC_BASE_URL"),
            ytdlp_format,
            tmdb_base: "https://api.themoviedb.org/3".to_string(),
            kinocheck_base: "https://api.kinocheck.com".to_string(),
        }
    }
}
