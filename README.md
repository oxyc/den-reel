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

## Maintenance

`/health` always returns 200 (liveness) with a JSON `status`: `ok`, or `degraded` with a `reason` —
`tmdb_key_missing` (no discovery key), `upstream_unavailable` (TMDB failing — KinoCheck is a
fallback and its outage is deliberately invisible here), or
`extractor_unavailable` (trailers resolve upstream but yt-dlp can't extract **any** of them here —
YouTube BotGuard / a stale yt-dlp / broken nsig-JS; bump `YTDLP_VERSION` — pinning
`YTDLP_PLAYER_CLIENTS` is a stopgap, not the fix),
or `downloads_failing` (yt-dlp extracts fine but no file is produced — check the cache volume and
MP4Box). The two are separate because the remedy is: an outage where every download fails locally
would otherwise report `ok`, and "bump yt-dlp" is the wrong advice for a full disk.
The `extractor_unavailable` signal exists because that outage is otherwise invisible — upstreams keep
answering while every trailer silently comes back empty.

The log is state changes, not events: one line when `/health` turns degraded (with its reason) and
one when it recovers; upstream, search and download failures at most once a minute per condition,
with a count of what was held back; the version and a secret-free summary at startup. It never
carries a key, a config segment, a play signature or a query string. `LOG_REQUESTS` adds a
per-request line.

`/metrics` is the detail behind that verdict, as Prometheus gauges prefixed `reel_`: bytes and
trailers on the volume against `CACHE_MAX_BYTES` (plus the scratch that also counts against it),
downloads in flight against their caps, the size of each in-memory cache, the three
consecutive-failure counters `/health` collapses into one word (`reel_consecutive_failures{kind}`),
and `reel_build_info{version}`. The cache figures come from the eviction pass — which runs after
every download and hourly — not from a directory walk per request, so
`reel_cache_measured_at_seconds` says how fresh they are and reads `0` until the first pass on a new
process. Nothing is computed until a scrape asks.

It is **off unless `METRICS_TOKEN` is set**, and then answers only
`Authorization: Bearer <METRICS_TOKEN>`; every refusal is the same 404 an unknown path gets. In-flight
counts and occupancy polled over time say when the household is watching, which is why it is gated.

YouTube changes frequently. Keep yt-dlp current — bump `YTDLP_VERSION` in the `Dockerfile`
when extraction starts failing. The image also bundles **deno** (`DENO_VERSION`): recent
yt-dlp needs a JS runtime to solve YouTube's signature challenge, and without it extraction
degrades and fails intermittently. That's the whole upkeep. The GH Action runs `cargo clippy`
+ `cargo test` on every push and PR; it publishes `ghcr.io/oxyc/den-reel` only on a `v*` tag or a
manual run.

## Routes

```
GET /configure                            →  the page that seals a BYOK key into an install URL
GET /config-key                           →  the public key /configure seals against
GET /<config>/manifest.json               →  addon manifest for a sealed install (add THIS to Den)
GET /<config>/meta/<type>/<imdbId>.json    →  as below, resolved with that install's own key
GET /manifest.json                       →  manifest with no config (uses the TMDB_KEY fallback)
GET /meta/<movie|series>/<imdbId>.json    →  { meta: { links: [ { trailers: <play url> } ] } }
GET /play/<youtube_id>.mp4  (or ?v=…)     →  200/206 video/mp4  (range-enabled, seekable)
GET /crop/<youtube_id>.json               →  detected content rectangle (letterbox trim hint)
     …/play requires ?s=<tag> when PLAY_SECRET is set (403 without); the same tag
       opens /crop, which without it answers "play the full frame" instead of refusing
GET /health                               →  200 {status} — ok, or degraded (see below)
GET /metrics                              →  Prometheus text (bearer METRICS_TOKEN; 404 without it)
OPTIONS <any path>                        →  204 CORS preflight
anything else                             →  404 {"error":"not_found"}
```

Every response carries `Access-Control-Allow-Origin: *`.

`/meta`, `/play` and `/crop` send `Server-Timing` naming what the handler did — `tmdb`,
`kinocheck`, `search`, `download`, `cropdetect` and `bake` with `dur` in milliseconds, or
`cache;desc=hit` / `cache;desc=stale` when the answer came from memory or the volume — then
`total`, the time to headers (a streamed video is still being sent when it is measured).

An answer that is a fallback says so in `X-Den-Degraded`, which is absent otherwise:
`stale_answer` (`/meta` serving the last known trailers while the lookup fails),
`upstream_unavailable` (an empty `/meta` because the lookup could not be made, not because there is
no trailer), and `crop_unavailable` (`/crop`'s "play the full frame", when no rect could be measured
or the call was unsigned). A `/meta` reordered around trailers `/play` found dead is not degraded.

Resolving a trailer at `/meta` also **prewarms** its download in the background, so the
following `/play` is warm. Two knobs:
- `?prewarm=0` — resolve only, don't pull bytes yet (for a browse-time prefetch that isn't sure
  the user will watch). Prewarm on the real detail view.
- A **successful** `/meta` sends `Cache-Control: public, max-age=604800` (7d) so clients cache the
  resolution; an empty result (no trailer / geo-blocked / transient) is left uncached to re-check.

`/meta` returns TMDB's candidates in rank order (official first, then KinoCheck) — it does **not**
probe them, so yt-dlp stays off the `/meta` path and the response is fast. The client plays the
first that works and advances past a dead or portrait one; playability is settled lazily on
`/play`. `links: []` means no trailer was found (or `TMDB_KEY` is unset) — never an error.

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
451 {"error":"geo_blocked","detail":"This trailer is not available in your region.","id":…}
403 {"error":"restricted", …}   # private / age-restricted
404 {"error":"unavailable", …}  # removed
503 {"error":"busy", …}           # IN_FLIGHT_MAX distinct ids already downloading
502 {"error":"extraction_failed", …}
502 {"error":"incomplete_download", …}  # yt-dlp was fine; no usable file came out of it
504 {"error":"timeout", …}
```

A failure is **remembered**, for a while that depends on why: `unavailable`/`restricted` 1h,
`geo_blocked` 30m, `extraction_failed` 5m, `incomplete_download` 2m, `timeout` 60s — and the
response says so in `Retry-After`, counting down as the window elapses. Without this, every request
for a video YouTube has removed spent another of three download slots, and another yt-dlp run, to
rediscover it. `/meta` uses the same knowledge: a candidate `/play` has found dead is moved
**behind** the ones that might work (never dropped — a region block can lift) and is not prewarmed;
a response whose order was shaped that way drops to `max-age=3600`, because the signal behind it can
be 60 seconds old and the usual 7 days would outlive it by a factor of ten thousand.

The TTLs differ because the reasons do: a removal is a fact, a timeout is usually our network. Even
"removed" is capped at an hour rather than the day it deserves — yt-dlp says "video unavailable"
both for a removed video and for an extractor that has been rejected outright, and that bucket is
the one `/health` deliberately ignores, so a misread must not be able to empty the library for an
evening. A restart clears all of it.

`incomplete_download` is deliberately distinct from `extraction_failed`: yt-dlp extracted, but no
trailer reached the cache — it exited 0 with no file, or the `clap` bake was killed part-way through
its in-place rewrite and the result cannot be trusted. Nothing about yt-dlp or the player clients
will help, so it feeds `downloads_failing` rather than `extractor_unavailable`.

## Configuration

Every variable is optional; `.env.example` lists them all with their defaults.

| Variable | Default | Purpose |
|---|---|---|
| `CONFIG_KEY` | — | sealed config-in-URL: base64 32-byte X25519 private key. Set it and `/configure` seals a BYOK TMDB key into the install URL (`crypto_box_seal`) so no discovery key lives on the server. Generate: `head -c 32 /dev/urandom \| base64` — and **back it up** (losing it breaks sealed installs). Unset = sealed disabled, legacy plaintext URLs still work. See `den-scout/docs/SEALED-CONFIG.md`. |
| `CONFIG_KEYS_PREV` | — | comma-separated prior keys for rotation (old sealed URLs keep decrypting) |
| `PLAY_SECRET` | — | sign the play URLs. Set it and `/meta` emits `…/play/<id>.mp4?s=<tag>` (keyed BLAKE2b over the id), which `/play` then requires. Without it, `/play` and `/crop` will extract and cache any YouTube id anyone asks for, which matters the moment the instance is reachable off-LAN. **Unset by default, and it must stay unset on an existing install until its clients have re-fetched `/meta`** — those responses carry `max-age=604800`, so turning it on strands already-issued unsigned URLs for up to 7 days. Any string. `/crop` takes the same tag (it covers the id, not the path, so a client can carry the one from the play URL across) but an unsigned `/crop` is answered `letterboxed:false` rather than refused — it is a hint, and the download behind it is what the gate protects. |
| `PLAY_SECRETS_PREV` | — | comma-separated prior play secrets, accepted when verifying and never used to sign. Rotate through it for the same reason `CONFIG_KEYS_PREV` exists: clients hold signed URLs for up to 7 days, so rotating without it is a week of 403s. |
| `METRICS_TOKEN` | — | turns on `/metrics`, which then requires `Authorization: Bearer <token>`. Unset, `/metrics` answers 404 like any unknown path. |
| `LOG_REQUESTS` | *(unset — off)* | `1` writes one stderr line per response: `<METHOD> <path> <status> <ms>ms`. The query string is dropped (`?s=` is a signature) and a config segment is written as `<config>`. |
| `TMDB_KEY` | — | **migration fallback** only: the legacy server-side discovery key, used when a request carries no per-install config. New installs seal their own key; drop this once migrated. |
| `KINOCHECK_KEY` | — | migration fallback for the optional KinoCheck discovery source |
| `PUBLIC_BASE_URL` | *(from request)* | override the base used in play URLs; usually unneeded — they follow the request's `Host` (and a proxy's `X-Forwarded-Host`/`-Proto`) |
| `PORT` | `8092` | |
| `CACHE_DIR` | `$TMPDIR/den-reel-cache` | persist with a volume. Must be **exclusively** den-reel's: any top-level *file* that is not a `<youtube_id>.mp4` is treated as abandoned scratch and deleted after 30 minutes. Subdirectories are left alone — den-reel keeps yt-dlp's player cache in `yt-dlp/` and parks its resolve cache in `state/` across restarts. |
| `YTDLP_PATH` | `yt-dlp` | path to the yt-dlp binary |
| `FFMPEG_PATH` | `ffmpeg` | path to ffmpeg (used by `/crop` cropdetect) |
| `MP4BOX_PATH` | `MP4Box` | path to GPAC MP4Box (writes the baked `clap` box) |
| `CLAP` | `1` | set `0`/`false`/`off`/`no` to disable baking the `clap` letterbox-crop box |
| `MAX_HEIGHT` | `1080` | avc1 caps at 1080p on YouTube; below 144 (no rendition can match) it falls back to the default |
| `CACHE_MAX_BYTES` | `4294967296` (4 GB) | LRU eviction threshold; below 256 MB (under one trailer) it falls back to the default |
| `CACHE_TTL_SECS` | `1209600` (14 days) | Drop a trailer this many seconds after it was last served; `0` = size cap only |
| `YTDLP_PLAYER_CLIENTS` | *(unset — yt-dlp chooses)* | Pin the YouTube innertube client(s) for `--extractor-args player_client`, comma-separated. **Normally leave this alone.** It used to default to `tv_embedded,android`; yt-dlp has since retired `tv_embedded`, and an unrecognised client is answered with a *warning* (`Skipping unsupported client`) that `--no-warnings` hides — so the pin silently degraded to `android` alone, which now needs a PO token for both HTTPS and DASH. Which client works is a judgement about what YouTube is enforcing this month, it is the judgement the yt-dlp team makes daily, and bumping `YTDLP_VERSION` is how we receive it. Set this only to ride out a specific outage, and check the name against the yt-dlp release you have pinned — a retired one fails quietly. |

## Run

```bash
docker build -t den-reel .
docker run -d --name trailers -p 8092:8092 -v den-reel-cache:/cache \
  -e TMDB_KEY=<your-tmdb-key> den-reel
curl http://localhost:8092/meta/movie/tt0111161.json          # → a /play URL
curl -o t.mp4 http://localhost:8092/play/dSdWpY2Bxsc.mp4       # playback smoke test
```

Without Docker (needs `ffmpeg`, `yt-dlp`, and a JS runtime like `deno` on PATH):
`TMDB_KEY=… cargo run --release`.

Tests: `cargo test` (hermetic — a fake upstream + stubbed prober, no network, no yt-dlp).

## Deploy

The homelab runs it as a Podman Quadlet unit, `den-reel.container`, from the `den` repo's
`deploy/`: LAN host port 8092, a 1 GiB memory cap, every capability dropped, `no-new-privileges`,
uid 65532 (the image's non-root user), and the cache bind-mounted from `/var/lib/den/reel-cache`
(owned by 65532). New images reach it through the health-gated `den-update` script. The env files,
digest pinning and rollback are described once, in that repo's `deploy/README.md`.

Nothing sits in front of it, so play URLs are built from the host the client asked for
(`http://<den-ip>:8092/play/…`). Add the URL `/configure` gives you —
`http://<den-ip>:8092/<config>/manifest.json` — to Den (Settings → Plugins, or `dev-addons.json`).
The config-less `/manifest.json` works only while `TMDB_KEY` is still set, and resolves with that
shared key rather than the install's own.
