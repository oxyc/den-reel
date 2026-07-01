# den trailer-service

Resolves a YouTube video id to a **playable, App-Store-safe MP4** for inline tvOS playback.

## Why this exists

Every trailer source (TMDB, KinoCheck) ultimately points to a YouTube video, and YouTube's
BotGuard blocks server-side downloads (Cobalt, headless session generators) by withholding a
`poToken`. **yt-dlp** sidesteps this — it rotates through the `android`/`ios`/`tv` innertube
clients that don't require BotGuard, and the yt-dlp team keeps it current as YouTube changes
(that's the entire maintenance burden, and it's theirs, not ours).

This service runs yt-dlp + ffmpeg, caches, and **proxies** the result (the googlevideo URL is
IP-bound to this server, so the Apple TV must fetch from us, not from YouTube directly).

## What it guarantees

- **AVPlayer-decodable**: forces H.264 video + AAC audio (YouTube's "best" is VP9/AV1 + Opus,
  none of which Apple TV decodes). Copy-mux, no transcode.
- **Faststart MP4**: `moov` atom up front → progressive playback, no black-screen wait.
- **Cached + seekable**: first play fetches (~3–6s), subsequent plays are instant; HTTP range
  requests supported so the player can scrub.
- **Bounded cache**: LRU eviction at `CACHE_MAX_BYTES`.

## API

```
GET /play/<youtube_id>.mp4   →  200/206 video/mp4   (preferred)
GET /play?v=<youtube_id>     →  200/206 video/mp4
GET /health                  →  200 ok
```

## Run

```bash
docker build -t den-trailer-service .
docker run -d --name trailers -p 8092:8092 -v den-trailer-cache:/cache den-trailer-service
curl -o t.mp4 http://localhost:8092/play/dSdWpY2Bxsc.mp4   # smoke test
```

Or without Docker (needs `node`, `ffmpeg`, and the `yt-dlp` binary on PATH):

```bash
YTDLP_PATH=/usr/local/bin/yt-dlp node server.js
```

## Config (env)

| Var | Default | Notes |
|---|---|---|
| `PORT` | `8092` | |
| `CACHE_DIR` | `$TMPDIR/den-trailer-cache` | persist with a volume |
| `YTDLP_PATH` | `yt-dlp` | path to the yt-dlp binary |
| `MAX_HEIGHT` | `1080` | avc1 caps at 1080p on YouTube |
| `CACHE_MAX_BYTES` | `8589934592` (8 GB) | LRU eviction threshold |

## Maintenance

YouTube changes frequently. Keep yt-dlp current — bump `YTDLP_VERSION` in the `Dockerfile`
(or `yt-dlp -U`) when extraction starts failing. That's the only upkeep.
