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
We then **proxy** the result — because AVPlayer needs one file with sound in it, and YouTube now
answers with a video stream and a separate audio one. *Not* because the URL is pinned here: it
carries `ip=<this box>` inside its signed `sparams`, which reads like an IP binding, but Google
does not enforce it. `/direct` (below) relies on exactly that to let the web app stream from
Google without the bytes ever crossing this box.

## What playback guarantees

- **AVPlayer-decodable**: forces H.264 + AAC (YouTube's "best" is VP9/AV1 + Opus, which Apple TV
  can't decode). Copy-mux, no transcode.
- **Faststart MP4**: `moov` up front → progressive playback, no black-screen wait.
- **Cached + seekable**: first play fetches (~3–6s), later plays are instant; HTTP range supported.
- **Bounded cache**: LRU eviction at `CACHE_MAX_BYTES`.

## Maintenance

`/health` always returns 200 (liveness) with a JSON `status`: `ok`, or `degraded` with a `reason` —
`tmdb_key_missing` (no discovery key), `upstream_unavailable` (TMDB failing — KinoCheck is a
fallback and its outage is deliberately invisible here), `youtube_throttled` (YouTube answered a 429
or a bot check, so every new extraction is paused — 5 min, doubling to 3 h, until it lifts or one
works; `retry_after_s` says when; bumping yt-dlp does not help), or
`extractor_unavailable` (trailers resolve upstream but yt-dlp can't extract **any** of them here —
YouTube BotGuard / a stale yt-dlp / broken nsig-JS; bump `YTDLP_VERSION` — pinning
`YTDLP_PLAYER_CLIENTS` is a stopgap, not the fix),
or `downloads_failing` (yt-dlp extracts fine but no file is produced — check the cache volume and
MP4Box). The two are separate because the remedy is: an outage where every download fails locally
would otherwise report `ok`, and "bump yt-dlp" is the wrong advice for a full disk.
The `extractor_unavailable` signal exists because that outage is otherwise invisible — upstreams keep
answering while every trailer silently comes back empty.

A 429 or 5xx from TMDB or KinoCheck pauses that host: for its `Retry-After` (seconds or an HTTP-date,
capped at an hour), else 30 s doubling, and until the reset when a response spends the rate limit
(`X-RateLimit-Remaining: 0`, or the draft `RateLimit` field). Lookups inside the pause get no answer
without a request; the host's next answer ends it.

The log is state changes, not events: one line when `/health` turns degraded (with its reason) and
one when it recovers; one when a host pause starts and one when an answer lifts it; upstream, search and download failures at most once a minute per condition,
with a count of what was held back; the version and a secret-free summary at startup. It never
carries a key, a config segment, a play signature or a query string. `LOG_REQUESTS` adds a
per-request line. A `/progressive` index build that takes 3 s or more writes
`progressive: [<id>] index took N s for K stream(s)`, at most once a minute: whoever asked for it first
waited all of it.

`/metrics` is the detail behind that verdict, as Prometheus gauges prefixed `reel_`: bytes and
trailers on the volume against `CACHE_MAX_BYTES` (plus the scratch that also counts against it),
downloads in flight against their caps, the size of each in-memory cache, the three
consecutive-failure counters `/health` collapses into one word (`reel_consecutive_failures{kind}`),
and `reel_build_info{version}`. The cache figures come from the eviction pass — which runs after
every download and hourly — not from a directory walk per request, so
`reel_cache_measured_at_seconds` says how fresh they are and reads `0` until the first pass on a new
process. Nothing is computed until a scrape asks.

What the web's trailers cost is there too, as counters since the process started:
`reel_index_builds_total{streams="video"|"video+audio"}` with `reel_index_build_milliseconds_total` beside it
(divide for the mean), `reel_index_build_failures_total`, `reel_index_ranges_retried_total` (ranges refused for the moment and asked again), `reel_index_requests_total{index="built"|"waited"}`
(how often a `/progressive` request found its index ready), and `reel_resolves_total` with
`reel_resolve_milliseconds_total`. `reel_last_milliseconds{of=…}` holds the most recent of each, for a glance
without a second scrape.

It is **off unless `METRICS_TOKEN` is set**, and then answers only
`Authorization: Bearer <METRICS_TOKEN>`; every refusal is the same 404 an unknown path gets. In-flight
counts and occupancy polled over time say when the household is watching, which is why it is gated.

YouTube changes frequently. Keep yt-dlp current — bump `YTDLP_VERSION` in the `Dockerfile`
when extraction starts failing. The image also bundles **deno** (`DENO_VERSION`): recent
yt-dlp needs a JS runtime to solve YouTube's signature challenge, and without it extraction
degrades and fails intermittently. That's the whole upkeep. The GH Action runs `cargo clippy`
+ `cargo test` on every push and PR; it publishes `ghcr.io/oxyc/den-reel` on a `v*` tag, and rebuilds
the newest tag weekly for security fixes (see Deploy).

## Routes

```
GET /configure                            →  the page that seals a BYOK key into an install URL
GET /config-key                           →  the public key /configure seals against, and CONFIG_EPOCH
GET /<config>/manifest.json               →  addon manifest for a sealed install (add THIS to Den),
                                             with "denInstallId": the install's id, when it has one;
                                             400 bad_config if undecodable or revoked
GET /<config>/meta/<type>/<imdbId>.json    →  as below, resolved with that install's own key
GET /manifest.json                       →  manifest with no config (uses the TMDB_KEY fallback)
GET /meta/<movie|series>/<imdbId>.json    →  { meta: { links: [ { trailers: <play url>,
                                                                 sources: <sources url> } ] } }
GET /sources/<youtube_id>.json            →  the forms of that trailer a page should try, in order:
                                             ?surface=silent|audible&player=native|hls.js
GET /m/<n|s>/<blob>                       →  one form /sources minted: n a native master (only its
                                             playlist crosses the box), s anything carried here;
                                             /m/s/seg serves a proxied master's URIs
GET /play/<youtube_id>.mp4  (or ?v=…)     →  200/206 video/mp4  (range-enabled, seekable)
GET /crop/<youtube_id>.json               →  detected content rectangle (letterbox trim hint);
                                             ?detect=keyframes measures it now, with no download
GET /direct/<youtube_id>.json             →  YouTube's own URLs, for a client that can play them
                                             without this server in the middle (the web app);
                                             ?height=720 caps the rung
GET /progressive/<youtube_id>.mp4         →  that video stream as an ordinary MP4, index first
                                             (range-enabled); same query as /direct; ?audio=1
                                             adds its sound as a second track
GET /hls/<youtube_id>.m3u8                →  YouTube's own HLS master, best variant first, every URI
                                             through /hls/seg; ?native=1 keeps Google's URIs and no
                                             rung under 540p; with X-Den-Playable or ?playable=,
                                             only the variants that browser plays
GET /hls/seg?u=…&s=…                      →  one googlevideo URL a master named, fetched here
     …/play requires ?s=<tag>&i=<iid>&e=<ep> when PLAY_SECRET is set (403 without,
       or for a revoked install, unless PLAY_SIGNING_GRACE_UNTIL is still ahead and
       the tag is missing); the same query opens /crop, /direct and /progressive — /crop
       without it answers "play the full frame" instead of refusing, the others refuse like /play
GET /health                               →  200 {status} — ok, or degraded (see below)
GET /metrics                              →  Prometheus text (bearer METRICS_TOKEN; 404 without it)
OPTIONS <any path>                        →  204 CORS preflight
anything else                             →  404 {"error":"not_found"}
```

Every response carries `Access-Control-Allow-Origin: *`.

### Choosing a route for a browser: mind the index

Ask `/sources` and play what it lists; it already orders forms by what was measured. If you pick a route
yourself, the one trap is `/progressive`'s index. It serves an ordinary MP4 with its index in front, which
is far faster to a first frame, but it has to *build* that index first — per video, height and URL — and
the first caller waits for it. `/hls` builds none. Safari 18.6 on macOS, same trailer, milliseconds to a
painted frame (2026-09-15):

| route | cold | warm |
|---|---|---|
| `/progressive?audio=1` | 5131 (`resolve;dur=1559, index;dur=2874`) | 115–232 |
| `/hls` (native master) | no index to build | 748–848 |

The build is not one number: 0.3–0.6 s video-only, about 2.9 s with `?audio=1`, which indexes the audio
stream too. So use `/progressive` where something builds the index well before the play — a carousel
warming its next slide, a press that comes seconds before navigation, `/meta?prewarm=progressive`, or a
`/sources` request made ahead. Use `/hls` where playback starts on demand and nothing warmed it; the warm
figure you see while developing is not what a first viewer gets. `Server-Timing` says which case a
response was: `cache;desc=hit` is warm, `index;dur=` means that caller just paid for the build.

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
following `/play` is warm. The warm-up is started, not awaited: `/meta` answers as soon as it has the
ids, so a `/direct`, `/hls` or `/progressive` request sent the moment `/meta` answers joins the
resolve still running and waits out the rest of it (its `Server-Timing` says `resolve;dur=…`). To be
warm by the time it plays, a surface has to ask `/meta` well ahead, for example for the next slide
while the current one shows. Knobs:
- `?prewarm=0` — resolve only, don't pull bytes yet (for a browse-time prefetch that isn't sure
  the user will watch). Prewarm on the real detail view.
- `?prewarm=direct` — warm the direct resolve and not the download, for a browser that plays
  `/direct`, `/hls` or `/progressive`; add the same `?height=` the play request will carry.
- `?prewarm=progressive` — the direct resolve, then `/progressive`'s index, so its first request
  starts at once; add the same `?height=` and `?audio=` the `/progressive` request will carry.
- A **successful** `/meta` sends `Cache-Control: public, max-age=86400, stale-while-revalidate=518400,
  stale-if-error=604800`: fresh for the day the server trusts a resolve, then usable for the rest of the
  week while the client re-asks (or while this server is down). `/<config>/meta` and
  `/<config>/manifest.json` send `private` instead, since the path carries the sealed key. An empty
  result (no trailer / geo-blocked / transient) is left uncached to re-check. A body with links carries
  `Vary: X-Forwarded-Host, X-Forwarded-Proto, Host`, the inputs its play URLs are built from.
- `/play` sends `private, max-age=604800, immutable` with an `ETag` and `Last-Modified` taken from the
  cached file, and honours `If-Range`: a trailer evicted and downloaded again is a new file, so a range
  resumed against the old one gets the whole new file (200). `/crop`'s rect is `public, max-age=604800`
  for the same reason. `/hls/seg` passes googlevideo's `ETag`/`Last-Modified` through (and
  `If-Range`/`If-None-Match` upstream) and caches for as long as the signed URL lives, six hours at
  most; playlists are `private` for five minutes at most, and never past their soonest URL's expiry.

`/meta` returns TMDB's candidates in rank order (official first, then KinoCheck) — it does **not**
probe them, so yt-dlp stays off the `/meta` path and the response is fast. The client plays the
first that works and advances past a dead or portrait one; playability is settled lazily on
`/play`. `links: []` means no trailer was found (or `TMDB_KEY` is unset) — never an error.

`/hls/<id>.m3u8` lists only what the asking browser plays when it sends Den Web's capability report
(den-edge `web/src/lib/playable.ts`, the same JSON den-remux and den-scout take): in the `X-Den-Playable`
header, which hls.js can set, or percent-encoded in `?playable=`, for Safari's own player, which fetches a
bare URL. The header wins where both come. A variant is left out when a codec its `CODECS` names is past
the report — H.264, HEVC or AV1 beyond its level or tier, High 10, HDR without `hdr`/`av1Hdr`, VP9 without
`vp9`/`vp9Profile2`, Dolby Vision without `dolbyVision`, E-AC-3 without `eac3`. Anything the report has no
field for (AAC) is kept, a master of which nothing plays is served whole, and with no report, or one that
doesn't parse, every variant is listed as before. The report is not part of the signature: it only narrows
what is listed. Playlists say `Vary: X-Den-Playable`.

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

## `/direct` — the URLs themselves

`/play` exists because AVPlayer needs one file with sound in it. A browser does not, so `/direct`
resolves with yt-dlp (`--print`, **no download** — it takes a probe permit, not a download one) and
answers with the googlevideo URLs:

```
{ "id":"…", "video":"https://rr7…/videoplayback?itag=137&…",
  "audio":"https://rr7…/videoplayback?itag=140&…", "width":1920, "height":1080,
  "expires":1789357731 }
```

The page then streams from Google directly: no wait for a download, no cache volume, and none of the
trailer's bytes through this box. Same format ladder as `/play` (avc1 + mp4a under `MAX_HEIGHT`), so
it cannot start handing out a VP9/AV1 stream only some browsers decode.

**`?height=`** caps that ladder for a surface that needs less: a muted preview behind text asks for
`720` and gets the 720p/480p rungs, so a browser that buffers ahead before it says it can play
(Safari) has fewer bytes to wait for. The number is rounded down to a step of the ladder (720, 480;
anything lower is 480) and never raises `MAX_HEIGHT`, so one video resolves at most once per step.
It is not part of the signature. Each step is cached apart, so `/meta?prewarm=direct` takes the same
`height` and warms the answer the `/direct` that follows will ask for.

**The URLs work away from this server.** They carry `ip=<this box>` inside the signed `sparams` set,
which reads like the IP binding the section above describes — but Google does not enforce it; a URL
resolved here plays from an unrelated address (verified against both `videoplayback` and
`manifest.googlevideo.com`). They do expire in about six hours, so an answer is cached, and sent,
only until shortly before its URLs stop working — `expires` and `max-age` both say when.

**Usually two streams.** YouTube still lists the muxed itag 18 but no longer serves it, so `video` is
video-only and `audio` is separate. A muted surface — Den Web's billboard, which is unpressable and
forces `muted` — ignores `audio` entirely, and that is the case this exists for. Anything wanting
sound needs a player that accepts two sources: browsers can, **AetherEngine cannot** (one
`MediaSource` per session; `LoadOptions` has `externalSubtitles` and no audio equivalent), which is
why the Apple TV keeps `/play`.

**No crop.** The `clap` box is baked into the *cached MP4* by the download path, so a trailer with
baked-in letterbox keeps its bars here; `/crop` cannot help, as it reads that same file.

Failures are the `/play` shapes below, with the same reason-aware `Retry-After` cooldown, plus
`502 no_direct_url` — yt-dlp exited 0 and printed nothing usable, which is not an extraction failure
and deliberately does not feed `extractor_unavailable`.

## `/sources` — what a page should play, in order

A page asks `/sources/<id>.json` (each `/meta` link names it as `sources`, under the play URL's own tag)
with two facts it knows and this server does not: its **surface** — `silent`, which never gets sound (a
billboard slide), or `audible`, which has sound from the first frame or on demand in place (a detail
hero) — and its **player**, `native` (Safari's own) or `hls.js`. It may add its playable report
(`X-Den-Playable` or `?playable=`), which the HLS entries carry on. The answer lists the forms to try,
best first, and never the same URL twice:

```
{ "id":"…", "expires":1789521638,
  "sources":[ {"kind":"mp4","url":"../m/s/<blob>?s=…","audio":false,"width":1280,"height":720},
              {"kind":"hls","url":"../m/n/<blob>?s=…","audio":true,"width":null,"height":null} ],
  "crop":{"letterboxed":true,"aspect":2.4,"rect":[0.0,0.1296,1.0,0.7407]} }
```

`width` and `height` are the frame of the rendition that entry plays — so `width < height` is a portrait trailer,
which a page cannot learn from the element on Safari (`videoWidth`/`videoHeight` are 0 at `loadedmetadata` on its
native HLS path). Both are null until the resolve is in, which for an audible surface is after the answer, so treat
unknown as landscape. A `hls` entry names none: its master carries a ladder of frames, not one.

`crop` is the trailer's letterbox as fractions of the frame (`[x, y, width, height]`), so it holds at
whatever height is played, or `null` until it has been measured. No answer waits for it: an unmeasured
trailer is measured in the background from the keyframes of the stream the first entry plays (the pass
`/crop?detect=keyframes` runs), and the next answer carries it. A whole-file measurement from `/play`
is kept over it.

`kind` says how to play it (`mp4` in the element, `hls` in the page's HLS player); the URL says nothing.
The order follows what was measured on macOS:

| surface | player | forms, best first |
|---|---|---|
| silent | native | progressive 720p · HLS master, segments on googlevideo |
| silent | hls.js | Google's own 720p file · progressive 720p · proxied HLS master |
| audible | native | HLS master, segments on googlevideo · progressive with sound |
| audible | hls.js | proxied HLS master · progressive with sound |

Asking is the warm-up. For a **silent** surface the answer waits for the resolve its first entry plays
from, and for its index when that entry is a progressive file; a file that turns out not to be indexable is
left off the list. A billboard asks seconds ahead, so that wait is spent where nobody sees it.

An **audible** surface is answered at once: its first entry is a master whose URL needs no resolve, and a
page asks as it opens, so waiting would only put a round trip in front of a player that waits on the same
resolve anyway. The resolve is started instead (`Server-Timing: resolve;desc=background`), the master's
request joins it, and `height` and `crop` are more often `null` in that first answer. A video already known
to be unavailable is still refused at once. Behind the resolve the index of the progressive file with sound
— the audible fallback — is built too, and the letterbox is read from it, so that fallback is warm if the
master ever fails.

`intent=warm` marks an ask made because a viewer might open the trailer (a press on a title link) rather than
because a surface is about to play it. The answer is the same; behind it only the resolve is started, with no
fallback index and no letterbox measured, since most such asks are a title glanced at and left. The surface's
own ask, without it, does the rest. The two are different URLs, so a browser cache never hands the play the
warm-up's answer.

A cold index costs 0.7–3.9 s, and measured from this box on 2026-09-15 almost all of that is Google's edge:
on one reused connection, cold 16 KB ranges waited 522–773 ms for their first byte and 9–11 ms when asked
again, while a new connection costs 40–180 ms. So no connection tuning moves it much — HTTP/2 does not apply
(the media hosts speak HTTP/1.1), and more parallel ranges draw refusals — and the only lever is building the
index before a viewer needs it. A range googlevideo refuses for the moment (401, 429, 5xx; a burst drew 17
refusals in 29) is asked again after 250 ms and then 1 s rather than failing the build.

Every URL but Google's own is `../m/<n|s>/<blob>`, **relative to the `/sources` URL that was asked**: resolve it
against that (`new URL(url, sourcesUrl)`), which leaves Google's absolute URL as it is. This server is reached
at several addresses and under a relay's prefix, and can know neither, so it names no host — as a proxied
playlist's URIs do not. What it names is the variant — video, form, height, sound, the install it
was minted for, and an expiry a day out — as base64url JSON, tagged with `PLAY_SECRET` over the blob. It is
refused (403) with a wrong tag or for a revoked install, and answers 410 once expired, when the page asks
for the list again. Everything playable sits under one prefix a relay can treat as media, and the segment
says how much of it crosses this box: `n` is a native master, which keeps its segment URIs on googlevideo so
only the playlist does; `s` is anything carried here — a progressive file, or a proxied master, whose
`seg?u=…` URIs resolve to `/m/s/seg` beside it. A relay can meter on the segment because it is enforced: a
blob filed under the other one answers 404.

### Measuring a letterbox from keyframes

`/crop/<id>.json?detect=keyframes` (signed like `/crop`) measures the letterbox now, from YouTube's own
720p stream rather than a downloaded file: the `/progressive` index already says where every keyframe
sits, so only those are fetched — every one in a trailer, thinned past 64 — and handed to the same
cropdetect pass as an H.264 stream of still pictures. About half a megabyte to a few, and half a second to
a second once the index is built; `Server-Timing: keyframes;dur=…`.

Set against the whole-file pass on eight cached trailers (2026-09-15) it agreed on seven. The eighth's
downloaded file carries twice the keyframes of Google's stream, two of them full frame, which holds the
mixed-framing guard there (play the full frame) and not here (a 2.4 letterbox); Google's 1080p stream
reads the same as its 720p one. Reading only the first keyframe of each fragment disagreed on one more,
which is why every keyframe is read.

## `/progressive` — the same stream, index first

YouTube's adaptive streams are fragmented MP4: a `moov` with empty sample tables, a `sidx`, then a
`moof`+`mdat` pair every few seconds (28 of them in a two-and-a-half-minute trailer). Chrome plays
that from `/direct` in under a second. Safari's progressive player reads every `moof` first, one range
request each, and took about five seconds where `/play`'s faststart copy took one.

`/progressive/<id>.mp4` serves the stream `/direct` names as `video` with a complete `moov` at the
front, built from Google's own boxes: the `sidx` says where each fragment starts, the `moof`s are
fetched in small ranged requests, and their sample sizes, durations and sync flags become ordinary
sample tables. The body is that header, then the fragments' sample bytes end to end, and each byte
range a player asks for is fetched from the matching range of Google's file as it is sent. There is
no download, no ffmpeg and nothing on the cache volume, but the video bytes do cross this box, unlike
`/direct`'s. Only the index is kept, until Google's URL is close to expiring; the bodies are not cached
here, only by the browser (`private, max-age` until then, and a stable `ETag`).

By default it carries the picture only, for muted surfaces. `?audio=1` adds YouTube's separate audio
stream as a second track: it is fragmented the same way (15 fragments for the same trailer), so its
index is built alongside the video's, and the two tracks' chunks are interleaved by time in the body so
a player reading along the file finds picture and sound together.

It takes `/direct`'s query, `?height=` included, and resolves through the same cache. With
`/meta?prewarm=direct` the first request for a stream still builds the index
(`Server-Timing: index;dur=…`); `/meta?prewarm=progressive` builds it during the warm-up, so that request
reads it from memory. A resolve failure answers like `/direct`.

**The index is a cliff, and the caller pays it in full.** One is built per video, height and URL, and there
is no single figure for it. Measured on 2026-09-15: about 0.3–0.6 s for a video-only stream, and 2.9 s with
`?audio=1` from a browser (Safari, O3S7aKk0ALw at 480p, resolve 1.6 s on top). Warm, the same request is a
cache hit in under 20 ms. The same URL painted its first frame in 232 ms warm and 5131 ms cold. So choose
`/progressive` only where something builds the index well ahead of the viewer: a billboard warming its next
slide can, a detail hero opened on a click cannot. The HLS master builds no index and has no cold case,
which is why `/sources` offers it first for an audible surface. A stream that cannot be indexed (not
fragmented, a box it cannot read, a failed fetch) answers `302` to Google's raw URL with
`X-Den-Degraded: progressive_unavailable` and a log line, which is what the page played before; with
`?audio=1` it answers `502 progressive_unavailable` instead, since that raw URL has no sound.

`/play` failures return a real status + JSON so the caller can say *why*:

```
451 {"error":"geo_blocked","detail":"This trailer is not available in your region.","id":…}
403 {"error":"restricted", …}   # private / age-restricted
404 {"error":"unavailable", …}  # removed
503 {"error":"busy", …}           # IN_FLIGHT_MAX distinct ids already downloading
503 {"error":"throttled", …}      # YouTube is throttling this server; Retry-After = what is left of the pause
503 {"error":"cache_unavailable", …}  # the cache volume is unusable; Retry-After: 60
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
be 60 seconds old and the usual week of client caching would outlive it by a factor of ten thousand.

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
| `CONFIG_KEYS_PREV` | — | comma-separated prior keys for rotation (old sealed URLs keep decrypting). **Rotation is not revocation:** because old links keep opening, rotating `CONFIG_KEY` does nothing to a leaked link — use `REVOKED_INSTALLS` / `CONFIG_EPOCH`. |
| `REVOKED_INSTALLS` | — | comma-separated install ids refused outright. Every `/configure` link carries one (`iid`, 16 random bytes as 22 base64url characters) inside the sealed config; a malformed entry is skipped with a warning. A refusal is the same 400 `bad_config` an undecodable segment gets, logged as `bad_config: … refused — install revoked (iid=<first 6 chars>…)`. `/configure` shows each link's id when it builds it, and the configured manifest carries it as `denInstallId`. With `PLAY_SECRET` set, the trailer links an install was handed are revoked too (see `PLAY_SECRET`). |
| `CONFIG_EPOCH` | `0` | links stamped with an `ep` below this are refused (`install epoch too old`); raise it to revoke every existing link without rotating `CONFIG_KEY`. Links from before install ids existed count as `0`. `/config-key` hands the current value to `/configure`, which stamps it into new links. Not a non-negative integer → warned and enforced as `0`. |
| `PLAY_SECRET` | — | sign the play URLs. Set it and `/meta` emits `…/play/<id>.mp4?s=<tag>&i=<iid>&e=<ep>` (keyed BLAKE2b over the id and the install id and epoch of the config that asked; the config-less `/meta` emits `?s=<tag>` bound to no install), which `/play` then requires. The install is signed, so editing `i`/`e` breaks the tag, and `/play` and `/crop` check it against `REVOKED_INSTALLS`, `CONFIG_EPOCH` and `REQUIRE_INSTALL_ID`: a revoked install's links get the same 403 `bad_signature` as a bad tag, logged as `bad_signature: link for <id> refused — install revoked (iid=<first 6 chars>…)`. Without it, `/play` and `/crop` will extract and cache any YouTube id anyone asks for, which matters the moment the instance is reachable off-LAN. **Unset by default. On an existing install, turn it on together with `PLAY_SIGNING_GRACE_UNTIL`** — a `/meta` response can stand in a client's cache for up to 7 days, so on its own it strands already-issued unsigned URLs for that long. Any string. `/crop` takes the same query (the tag does not cover the path, so a client can carry the one from the play URL across) but an unsigned `/crop` is answered `letterboxed:false` rather than refused — it is a hint, and the download behind it is what the gate protects. Releases up to 0.12 signed the id alone; those tags are accepted only while `PLAY_SIGNING_GRACE_UNTIL` is ahead. |
| `PLAY_SECRETS_PREV` | — | comma-separated prior play secrets, accepted when verifying and never used to sign. Rotate through it for the same reason `CONFIG_KEYS_PREV` exists: clients hold signed URLs for up to 7 days, so rotating without it is a week of 403s. |
| `PLAY_SIGNING_GRACE_UNTIL` | — | the switch-over for `PLAY_SECRET`: an RFC 3339 timestamp with an offset, e.g. `2026-09-17T12:00:00Z`. `/meta` signs from the start, but until this moment a `/play` or `/crop` with **no** `s` is still served, so the unsigned URLs clients cached before signing keep playing — and so is one whose `s` covers the id alone, as releases up to 0.12 signed it, so upgrading does not strand the signed URLs clients hold. **Upgrading a signed install from ≤0.12: keep this at least 7 days past the upgrade.** Those URLs name no install, so a revocation cannot reach them until the grace ends. Any other wrong `s` is refused throughout. **Set it together with `PLAY_SECRET`, to now + 7 days** (`date -u -d +7days +%Y-%m-%dT%H:%M:%SZ`), and **remove it once it has passed**. Each id served this way is logged once (`play signing grace: served <id> without a tag` / `… on a tag from before install binding`), so the stragglers are visible, and the startup line reads `play_signing=grace(until=…)`. A value that doesn't parse is logged and ignored, so signing is enforced at once. Ignored without `PLAY_SECRET`. |
| `METRICS_TOKEN` | — | turns on `/metrics`, which then requires `Authorization: Bearer <token>`. Unset, `/metrics` answers 404 like any unknown path. |
| `LOG_REQUESTS` | *(unset — off)* | `1` writes one stderr line per response: `<METHOD> <path> <status> <ms>ms[ rid=<id>]`. The query string is dropped (`?s=` is a signature) and a config segment is written as `<config>`; a `/sources` line adds what was asked — `surface=`, `player=` and `intent=warm` — as known values only, so a detail page opening and a press can be told apart. |
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

**Release images.** `docker-publish` builds on a `v*` tag, and again every Monday: the weekly run
rebuilds the newest `v*` tag (never `main`) with the base images re-pulled, no build cache and a fresh
`apt-get upgrade`, and publishes it as `:X.Y.Z-patch.<date>.<run>` and `:latest`. That is how a Debian
security fix — ffmpeg parses every trailer downloaded here — reaches the box between releases, through
`den-update`'s probe and rollback like any release. It rebuilds the tag's pinned yt-dlp and deno; a newer
yt-dlp still ships only with a release. Trivy scans each image before `:latest` moves: a CRITICAL with a
fix available fails the run (on the weekly rebuild only in OS packages, the part a rebuild can fix), and
fixable HIGH and CRITICAL findings go to code scanning. A finding that does not apply goes in
`.trivyignore` with a reason. Every image carries SLSA provenance and an SBOM and is signed keylessly with
cosign; verify a digest with:

```bash
cosign verify \
  --certificate-identity-regexp '^https://github\.com/oxyc/den-reel/\.github/workflows/docker-publish\.yml@refs/(heads/main|tags/v[0-9]+\.[0-9]+\.[0-9]+)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/oxyc/den-reel@sha256:<digest>
```

Nothing sits in front of it, so play URLs are built from the host the client asked for
(`http://<den-ip>:8092/play/…`). Add the URL `/configure` gives you —
`http://<den-ip>:8092/<config>/manifest.json` — to Den (Settings → Plugins, or `dev-addons.json`).
The config-less `/manifest.json` works only while `TMDB_KEY` is still set, and resolves with that
shared key rather than the install's own.
