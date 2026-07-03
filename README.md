# den-reel

The whole trailer path for [Den](https://github.com/oxyc/den) in **one container**: the addon
that finds a movie's trailer **and** the proxy that makes it play inline on tvOS.

A single ~2 MB Rust binary (async, no GC) + yt-dlp + ffmpeg — sized for a homelab: a few MB of
resident RAM at idle, the image weight is just the extractor toolchain.

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
GET /crop/<youtube_id>.json               →  detected content rectangle (letterbox trim hint)
GET /health                               →  200 ok
```

Resolving a trailer at `/meta` also **prewarms** its download in the background, so the
following `/play` is warm. Two knobs:
- `?prewarm=0` — resolve + validate only, don't pull bytes yet (for a browse-time prefetch that
  isn't sure the user will watch). Prewarm on the real detail view.
- A **successful** `/meta` sends `Cache-Control: public, max-age=604800` (7d) so clients cache the
  resolution; an empty result (no trailer / geo-blocked / transient) is left uncached to re-check.

`/meta` returns the first **playable** trailer: it probes TMDB's candidates (official first,
then KinoCheck) with yt-dlp and skips ones that are geo-blocked / removed / undecodable *here*,
so the URL it hands back actually plays. `links: []` means "nothing playable in this region"
(or no trailer, or `TMDB_KEY` unset) — never an error.

`/crop` lets the app trim baked-in **letterbox bars** with no re-encode: it runs ffmpeg
`cropdetect` (keyframe-sampled, so cheap) over the cached MP4 and returns the non-black content
rectangle; the app aspect-fills that rect instead of the full frame.

```
{ "id":"…", "source":{"w":1920,"h":1080}, "content":{"x":0,"y":132,"w":1920,"h":816},
  "letterboxed":true, "aspect":2.35 }
```

`letterboxed:false` (or a missing `content`) means "play the full frame". cropdetect runs with
`reset=1` (a fresh box per keyframe), and we crop to the **typical (median) box** snapped to a
standard cinematic aspect. So a **transient** logo / laurel / "in theaters" card in a bar — present
on only a minority of keyframes — is **cropped away** rather than holding the bar open; a logo that
persists for the whole trailer still keeps its bar. We only ever trim a **full-width, landscape
top/bottom letterbox** that keeps enough height; everything else plays the full frame. Guards: a
trailer that genuinely **uses the full frame** on more than a stray keyframe (mixed framing — e.g. a
mostly-letterboxed animated trailer with full-frame hero shots) is left uncropped, since slicing
those shots is worse than keeping bars; a **portrait** source (or a landscape clip padded into a tall
frame) is never letterbox-cropped (its huge top/bottom padding isn't a cinematic bar); and a
minimum-content floor catches dark trailers whose frames momentarily read as mostly black. `/crop`
shares the download with `/play` (call it at play time) and caches the result; the `/play`
download+serve path is untouched.

**Baked `clap`.** When a letterbox is detected, den-reel also writes a `clap` (clean aperture) box
into the cached MP4 (via MP4Box — ~13 ms, +40 bytes, no re-encode, faststart preserved). Apple's
AVPlayer honors clean aperture, so a direct-to-`AVPlayer` client (Den's billboard trailer) crops
the bars with **zero client changes** — no `/crop` call needed. Offsets are content-centre-relative,
so the snapped, centred letterbox is `0`. Clients that ignore `clap` just see the full frame. Set
`CLAP=0` to disable baking.

`/play` failures return a real status + JSON so the caller can say *why*:

```
451 {"error":"geo_blocked","message":"This trailer is not available in your region.","id":…}
403 {"error":"restricted", …}   # private / age-restricted
404 {"error":"unavailable", …}  # removed
502 {"error":"extraction_failed", …}
```

## Run

```bash
docker build -t den-reel .
docker run -d --name trailers -p 8092:8092 -v den-reel-cache:/cache \
  -e TMDB_KEY=<your-tmdb-key> den-reel
curl http://localhost:8092/meta/movie/tt0111161.json          # → a /play URL
curl -o t.mp4 http://localhost:8092/play/dSdWpY2Bxsc.mp4       # playback smoke test
```

In the homelab it runs behind Caddy at `https://trailers.<domain>` (compose profile
`trailers`); Caddy forwards `Host` + `X-Forwarded-Proto`, so the addon builds correct
`https://trailers.<domain>/play/…` URLs with no extra config. Add
`https://trailers.<domain>/manifest.json` to Den (Settings → Plugins, or `dev-addons.json`).

Without Docker (needs `ffmpeg`, `yt-dlp`, and a JS runtime like `deno` on PATH):
`TMDB_KEY=… cargo run --release`.
Tests: `cargo test` (hermetic — a fake upstream + stubbed prober, no network, no yt-dlp).

## Config (env)

| Var | Default | Notes |
|---|---|---|
| `REEL_CONFIG_KEY` | — | sealed config-in-URL: base64 32-byte X25519 private key. Set it and `/configure` seals a BYOK TMDB key into the install URL (`crypto_box_seal`) so no discovery key lives on the server. Generate: `head -c 32 /dev/urandom \| base64` — and **back it up** (losing it breaks sealed installs). Unset = sealed disabled, legacy plaintext URLs still work. See `den-scout/docs/SEALED-CONFIG.md`. |
| `REEL_CONFIG_KEYS_PREV` | — | comma-separated prior keys for rotation (old sealed URLs keep decrypting) |
| `TMDB_KEY` | — | **migration fallback** only: the legacy server-side discovery key, used when a request carries no per-install config. New installs seal their own key; drop this once migrated. |
| `KINOCHECK_KEY` | — | migration fallback for the optional KinoCheck discovery source |
| `PUBLIC_BASE_URL` | *(from request)* | override the base used in play URLs; usually unneeded behind Caddy |
| `PORT` | `8092` | |
| `CACHE_DIR` | `$TMPDIR/den-reel-cache` | persist with a volume |
| `YTDLP_PATH` | `yt-dlp` | path to the yt-dlp binary |
| `FFMPEG_PATH` | `ffmpeg` | path to ffmpeg (used by `/crop` cropdetect) |
| `MP4BOX_PATH` | `MP4Box` | path to GPAC MP4Box (writes the baked `clap` box) |
| `CLAP` | `1` | set `0` to disable baking the `clap` letterbox-crop box |
| `MAX_HEIGHT` | `1080` | avc1 caps at 1080p on YouTube |
| `CACHE_MAX_BYTES` | `8589934592` (8 GB) | LRU eviction threshold |

## Maintenance

YouTube changes frequently. Keep yt-dlp current — bump `YTDLP_VERSION` in the `Dockerfile`
when extraction starts failing. The image also bundles **deno** (`DENO_VERSION`): recent
yt-dlp needs a JS runtime to solve YouTube's signature challenge, and without it extraction
degrades and fails intermittently. That's the whole upkeep. The GH Action runs `cargo clippy`
+ `cargo test`, then publishes `ghcr.io/oxyc/den-reel` on every push to `main` and on `v*` tags.
