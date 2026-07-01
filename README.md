# den trailer-service

The whole trailer path for [Den](https://github.com/oxyc/den) in **one container**: the addon
that finds a movie's trailer **and** the proxy that makes it play inline on tvOS.

```
Den (Apple TV) ──/meta/movie/<imdbId>.json──►  addon    imdbId → TMDB → ytId
                                                  │
                ◄──── { links:[{ trailers }] }────┘   trailers = <this host>/play/<ytId>.mp4
Den (AVPlayer) ──GET /play/<ytId>.mp4─────────►  proxy   yt-dlp + ffmpeg → cached faststart MP4
```

Previously this was two pieces (a Cloudflare Worker addon + this proxy). They're merged, so
there's **no Cloudflare**: the addon returns a play URL on *its own host*, so a LAN-only deploy
works with nothing exposed.

## Why it exists

Every trailer source (TMDB, KinoCheck) points to a YouTube video, and YouTube's BotGuard blocks
server-side downloads (Cobalt, headless session generators) by withholding a `poToken`.
**yt-dlp** sidesteps this — it rotates through the `android`/`ios`/`tv` innertube clients that
don't need BotGuard, and the yt-dlp team keeps it current (that maintenance burden is theirs).
We then **proxy** the result: the googlevideo URL is IP-bound to this server, so the Apple TV
fetches from us, not from YouTube.

## What playback guarantees

- **AVPlayer-decodable**: forces H.264 + AAC (YouTube's "best" is VP9/AV1 + Opus, which Apple TV
  can't decode). Copy-mux, no transcode.
- **Faststart MP4**: `moov` up front → progressive playback, no black-screen wait.
- **Cached + seekable**: first play fetches (~3–6s), later plays are instant; HTTP range supported.
- **Bounded cache**: LRU eviction at `CACHE_MAX_BYTES`.

## API

```
GET /manifest.json                       →  addon manifest (add this URL to Den)
GET /meta/<movie|series>/<imdbId>.json    →  { meta: { links: [ { trailers: <play url> } ] } }
GET /play/<youtube_id>.mp4  (or ?v=…)     →  200/206 video/mp4  (range-enabled, seekable)
GET /health                               →  200 ok
```

The addon returns `links: []` (never an error) when there's no trailer or `TMDB_KEY` is unset.

## Run

```bash
docker build -t den-trailer-service .
docker run -d --name trailers -p 8092:8092 -v den-trailer-cache:/cache \
  -e TMDB_KEY=<your-tmdb-key> den-trailer-service
curl http://localhost:8092/meta/movie/tt0111161.json          # → a /play URL
curl -o t.mp4 http://localhost:8092/play/dSdWpY2Bxsc.mp4       # playback smoke test
```

In the homelab it runs behind Caddy at `https://trailers.<domain>` (compose profile
`trailers`); Caddy forwards `Host` + `X-Forwarded-Proto`, so the addon builds correct
`https://trailers.<domain>/play/…` URLs with no extra config. Add
`https://trailers.<domain>/manifest.json` to Den (Settings → Plugins, or `dev-addons.json`).

Without Docker (needs `node`, `ffmpeg`, `yt-dlp` on PATH): `TMDB_KEY=… node server.js`.
Tests: `npm test` (Node's built-in runner, no deps — stubs `fetch`).

## Config (env)

| Var | Default | Notes |
|---|---|---|
| `TMDB_KEY` | — | **required for the addon** (`/meta`); playback works without it |
| `KINOCHECK_KEY` | — | optional discovery fallback when TMDB has no trailer |
| `PUBLIC_BASE_URL` | *(from request)* | override the base used in play URLs; usually unneeded behind Caddy |
| `PORT` | `8092` | |
| `CACHE_DIR` | `$TMPDIR/den-trailer-cache` | persist with a volume |
| `YTDLP_PATH` | `yt-dlp` | path to the yt-dlp binary |
| `MAX_HEIGHT` | `1080` | avc1 caps at 1080p on YouTube |
| `CACHE_MAX_BYTES` | `8589934592` (8 GB) | LRU eviction threshold |

## Maintenance

YouTube changes frequently. Keep yt-dlp current — bump `YTDLP_VERSION` in the `Dockerfile`
when extraction starts failing. That's the only upkeep. The GH Action publishes
`ghcr.io/oxyc/den-trailer-service` on every push to `main` and on `v*` tags.
