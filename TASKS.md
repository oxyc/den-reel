# den-reel — audited task list

## Audit status (2026-09-04) — CONVERGED

Seven review rounds, two independent auditors each. Counting only correctness defects and hot-path
or memory regressions — nits excluded — the rounds found **5, 2, 4, 3, 3, 1, 1**. Rounds 6 and 7 are
the two consecutive rounds under two findings that the loop was waiting for. Round 7's single item
was raised by both auditors, and the correctness one classed it as not-a-defect.

The shape of the middle rounds is the useful part: after round 1, almost every finding was a defect
in the *previous round's fix* rather than in the original work. The `touch_atime` optimisation was
wrong twice in a row — the second time wrong in kind, not degree — and ended up reverted to the
behaviour already in the repo. `load_resolve_cache` needed three passes. Two lessons worth carrying:
a fix written to answer an audit deserves the same scrutiny as the code it replaces, and when an
optimisation keeps producing defects, the behaviour it replaced was probably right.

Nothing was verified against a running binary. Everything here is `cargo test` plus clippy, with no
real yt-dlp, ffmpeg, MP4Box or TMDB in the loop. What is still open is in the sections marked
STILL OPEN and ACCEPTED RISK.


Every item below survived an adversarial audit against the source. File:line anchors are from
`c89b4a4` (v0.5.0). Items are ordered by value-for-effort, not by dependency — but T3 and T4 share a
mechanism, and T5's two halves compose, so keep those adjacent.

**Priorities: correctness first, then runtime performance and resource usage (RAM, CPU, disk,
subprocess count), then everything else.** This runs on a constrained homelab box. A change that
costs steady-state memory, an extra thread, or an extra subprocess has to pay for itself.

**Global constraints**

- No new dependencies. `blake2` 0.10.6 and `subtle` are already in `Cargo.lock` (via `crypto_box`),
  so promoting one to a direct dep adds no compiled code — that is the only exception.
- Keep the `current_thread` runtime. No new long-lived tasks beyond the existing hourly sweep.
- Every new map must be bounded the way `yt_cache` is (`YT_CACHE_MAX`, `main.rs:71`). No unbounded
  growth, and no per-request allocation on the serve path.
- Match the surrounding style: this codebase explains *why*, in prose, at the point of the decision.
- If a task's premise turns out to be invalidated by the code, **skip it**, note one line here
  saying what was wrong, and move on.

---

## T1 — Build arm64. One line.

`.github/workflows/docker-publish.yml:94-102` (the publish job) sets no `platforms:`, so buildx uses
the runner's native platform. `build-pr` at line 46 is explicitly `linux/amd64`. **No arm64 image has
ever been published**, despite `Dockerfile:6` claiming it builds both and the Dockerfile being fully
arm64-ready (`ARG TARGETARCH`, `aarch64` branches, pinned SHAs at `Dockerfile:30,41-46,61-66`).

Add `platforms: linux/amd64,linux/arm64` to the publish job. Then fix the three comments that
currently assert arm64 is already covered — they are wrong and will mislead the next reader:
`docker-publish.yml:31`, `ytdlp-update.yml:94`, `deno-update.yml:94`.

Leave `build-pr` amd64-only; its comment about speed is sound once the publish job is honest.

## T2 — The KinoCheck key needs the cache-key namespacing the TMDB key already has

`addon.rs:117-121` namespaces the resolve cache as `"{imdb}:{lang}:nokey"` when the TMDB key is
empty, and `addon.rs:111-116` explains why: a thinner keyless answer must not be shared with keyed
installs. `kinocheck_key` (`addon.rs:296-297`, passed at `addon.rs:138`) gets none of that. An
install *without* a KinoCheck key writes its thinner candidate list under the shared
`"{imdb}:{lang}"`, and an install *with* one reads it for the full 24h `YT_TTL_MS`.

Same bug, same function, other credential. Extend the namespace to cover KinoCheck presence.
Keep the key credential-*free* — namespace on presence (a bool), never on the key's value.

## T3 — Negative-cache `/play` failures, reason-aware, and feed them back into `/meta`

`fetch_trailer` (`play.rs:238`) has three states: disk hit, in-flight join, fresh download. The
driver clears the `in_flight` entry regardless of outcome (`play.rs:280-286`), so a failure leaves no
trace and the next request re-runs yt-dlp — taking one of three `download_sem` permits
(`play.rs:306`).

Add a bounded `vid -> (PlayError, exp)` map on `AppState`, checked in `fetch_trailer` before the
download. **TTL must be reason-aware** — a uniform TTL is wrong in both directions:

- `404 unavailable`, `403 restricted` — stable facts, cache hours.
- `451 geo_blocked` — cache minutes; a CDN/region change can lift it.
- `504 timeout` — cache barely or not at all. It is the case that burns a permit for the full 240s
  (`ytdlp.rs:14`), and also the most likely to be transient. Do not turn a slow network into a
  pinned failure.
- `502 extraction_failed` — short. A systemic extractor outage recovers when yt-dlp is bumped, and
  a long TTL would keep every trailer dead after the fix landed.

Then close the loop, which is the actual point: `resolve_youtube_ids` (`addon.rs:103-268`) reads only
`yt_cache` and the upstreams, so a dead candidate keeps being returned first and keeps being
prewarmed (`addon.rs:306-310`). Demote known-dead ids in the returned order and skip prewarming one.

Do **not** let this feed `/health`'s `extract_fails`/`local_fails` (`state.rs:76,81`) — those are
process-wide systemic signals and a per-id fact must not move them.

Note on severity, so you size the fix correctly: the prewarm waste is already bounded —
`try_acquire_owned` gives up rather than queueing (`state.rs:166-168`) and
`PREWARM_MAX < DOWNLOAD_CONCURRENCY` is a compile-time assert (`main.rs:66-69`). The win here is
avoiding repeat yt-dlp spawns, not rescuing a starved `/play`.

## T4 — `/crop` re-runs a whole ffmpeg pass on every call for unparsable trailers

`crop.rs:500-506`: a `None` from `detect` becomes `CropReport::unknown`, deliberately not cached
(`crop.rs:92-93`, `crop.rs:522-526`). The comment at `crop.rs:490-495` diagnoses exactly this
("re-runs it on every call") and then fixes only the concurrency half with a `probe_sem` permit.

Give the unknown report a short TTL entry in `crop_cache` so it stops re-spawning ffmpeg — a
whole-file keyframe decode (`crop.rs:284-325`, 60s timeout at `crop.rs:39`) is the most expensive
thing this service does per request. Short, because a re-download of the same id could well parse.
`CROP_CACHE_MAX` (`main.rs:72`) already bounds the map; keep it bounded.

## T5 — Shrink the download surface

Two halves; do (a) regardless, (b) is the real fix.

**(a) `is_valid_vid` accepts 6–15 chars (`main.rs:76-79`); a YouTube id is exactly 11.** One line,
shrinks the reachable id space by orders of magnitude, costs nothing. Test fixtures already use
11-char ids (`vidvidvid11`, `abcdefghij1`, `blockedvid1`) so the blast radius is small — run
`cargo test` to catch stragglers, and check `MAX_HEIGHT`-style boundary tests near `tests.rs:1067`.

**(b) There is no auth, signature or allowlist on `/play`** (`main.rs:207-222`) or on `/crop`
(`main.rs:201-205` → `crop.rs:484`), so anyone who can reach an exposed instance can make it extract
and cache arbitrary YouTube videos. Sign the vid into the play URL built by `build_meta`
(`addon.rs:75-89`) with a keyed MAC and reject unsigned requests.

- **`/crop` is a second door to the same download.** Sign both or the hole just moves.
- Use keyed BLAKE2 from the `blake2` crate already in the tree, and `subtle` for constant-time
  comparison. No new dependency.
- **Opt-in via env, default off.** `/meta` responses ship `max-age=604800` (`addon.rs:318`), so
  clients hold unsigned play URLs for up to 7 days; flipping this on unconditionally breaks every
  install for a week. Unset secret = current behaviour, exactly.

## T6 — The evicted-mid-serve retry is dead code exactly when it is needed

`handle_play` re-calls `fetch_trailer` when `serve_file` returns `Err(())` (`play.rs:447-453`), but
the driver clears the `in_flight` entry only after `driver.await` returns (`play.rs:280-286`) — the
same completion the waiter wakes on. If the retry re-enters first it joins the still-present entry
(`play.rs:266-268`), gets the same resolved future and the same deleted path, and falls through to
the 500 at `play.rs:460`.

Make the retry force a fresh download rather than joining a completed entry. Do not fix it by
sleeping or spinning.

## STILL OPEN — three things audits raised that were deliberately not changed

**`YT_CACHE_MAX = 10_000` is now a persistent memory ceiling, not a transient one.** The constant was
chosen when the resolve cache was discarded on every restart; it now survives a redeploy, so at a
full 10k entries the map is ~5 MB resident on a box budgeted at a few MB. At realistic homelab fill
(hundreds to low thousands of entries, 1–3 ids each) it is 0.2–1 MB and fine. Re-deciding a tuning
constant on speculation, at the end of an audit loop, with no occupancy data from the actual box, is
not an improvement — measure `/stats`'s `resolve_cache.entries` on the real instance first.

**`save_resolve_cache` holds the `yt_cache` guard across the file write.** The borrowed `live` map
keeps the guard alive through `File::create`, the writes and the flush. Unreachable today: the only
caller runs after the accept loop has broken, on the runtime thread of a current-thread runtime, so
nothing else can be running. It becomes real the moment someone switches to a multi-thread runtime or
wraps the call in `spawn_blocking` — at which point a wedged volume would block every `/meta` and
`/stats` for the duration of a disk write. Fixing it means giving the width back to a `Vec`, which is
the allocation that write was changed to avoid, so it is a genuine trade and not an obvious win.

~~**`in_flight` has no cap**~~ — DONE. Capped at `IN_FLIGHT_MAX` (64); a new id past the cap gets a
503 `busy`, while joining a download already in flight is always free. Not recorded in `play_fails`,
since it says nothing about the video.

## STILL OPEN — the cached-serve path takes 3–4 blocking-pool dispatches where 1 would do

Raised by the round-3 performance audit and deliberately **not** taken, because it is a refactor of
the most delicate code in the service rather than a fix.

Serving an already-cached `/play` currently pays: `tokio::fs::metadata` in `fetch_trailer`
(`play.rs`, the cache-hit check), `File::open` in `serve_file`, and `file.metadata()` right after it
— three `spawn_blocking` round-trips, each a full task handoff on a current-thread runtime, plus a
fourth for `seek` on a Range request. The stat largely duplicates what the open and fstat establish.

Collapsing them means `fetch_trailer` handing back an open `File` instead of a `PathBuf`, which
changes its contract for `/crop` (which wants the path, not the handle) and for the eviction retry in
`handle_play`. That is worth doing, with its own test pass — not as a late edit in an audit loop.

This is the largest remaining cost on the cached-serve path. To be precise about the comparison: the
`cache_available` memo removed 2 of about 5 blocking-pool handoffs from that path and is the single
largest win in the changeset; these 3 are what is left, not evidence the memo was small.

**Nits noted and consciously not taken** (each costs tens of nanoseconds on a cold path, and the
code is clearer as it stands): `cached_failure` discards the expiry that `remaining_fail_ms` then
re-locks to fetch. (`sign::key_of` re-deriving per id is now fixed — `sign::Signer` derives the key
once per response; it still re-derives once per configured secret when *verifying*, which is at most
a handful and only with signing on.)

An earlier version of this note also filed `save_resolve_cache`'s `to_vec` here. That was a
mis-triage — it was a multi-megabyte transient allocation at shutdown, not a nanosecond cost, and it
landed exactly when a redeploy has two processes alive. It now streams through a `BufWriter`.

## ACCEPTED RISK — `/stats` is served without a gate

Raised by three independent audit passes; resolved deliberately, not overlooked. `/stats` reports
cache occupancy, in-flight download counts and the failure counters with no signature check, on a
service whose `/play` and `/crop` can be gated. The reasons for leaving it open:

- The version it reports is **already public** in `/manifest.json`, which cannot be gated — Stremio
  clients fetch it unauthenticated by protocol.
- `/health` already tells an anonymous caller whether extraction is currently broken.
- The natural gate (the play tag) covers a *video id*; there is no id here, so gating would mean
  inventing a tag over a constant, which the operator then cannot compute without running BLAKE2b.

Blocking one path at the reverse proxy is the right layer, and the README says so. Revisit only if
`/stats` grows a field that is not already inferable from `/health` plus `/manifest.json`.

## T7 — DONE (2026-09-04). Premise CONFIRMED against the live API, using the key from den's `.env`:

| title | `language=en` | `language=de` | `de` + `include_video_language` | `language=fi` | `fi` + widened |
|---|---|---|---|---|---|
| Shawshank (278) | 21 | 1 | 22 | **0** | 21 |
| Oppenheimer (872585) | 51 | 6 | 57 | 1 | 52 |
| Inception (27205) | 27 | 2 | 29 | **0** | 27 |

`?lang=fi` returned zero candidates for two of three titles, so the resolver spent a yt-dlp search
and then negative-cached "no trailer" for an hour. Fixed by asking for `<lang>,en,null`, plus a
language-first ordering — the widening means an English trailer now arrives beside a native one, and
a viewer who asked for German should get the German trailer when one exists.

`valid_lang` deliberately still rejects `pt-BR` and falls back to `en`. Loosening it is now safe (the
widening removes the thin-result trap the earlier audit warned about) but buys little, since the `en`
fallback already works — left alone rather than widened on speculation.

## T7 — TMDB language filtering — VERIFY FIRST, SKIP IF YOU CANNOT

`upstream.rs:250-253` sends `language={lang}` to `/videos` with no `include_video_language`. The
suspicion is that a non-`en` lang returns few or zero candidates because most trailers are tagged
`en`.

**This is unverified live-API behaviour and it is the crux.** If a TMDB key is available in the
environment, settle it with one request — `/3/movie/278/videos?language=de` against the same URL
plus `&include_video_language=de,en,null` — and only then add the parameter. **If no key is
available, SKIP this task and say so.** Do not change the query on speculation.

**Do NOT loosen `valid_lang` (`addon.rs:38-40`) to accept `pt-BR`.** That reading was wrong on
audit: rejecting `pt-BR` downgrades to `en`, which is the value that *works*. Accepting `pt` is what
would yield the thin result. Loosening it without fixing the `/videos` query makes things worse.

Mitigations already in place, so do not over-engineer: KinoCheck is not filtered the same way
(`upstream.rs:303`) and runs concurrently, the negative cache is lang-scoped (`addon.rs:117-121`),
and an empty `/meta` ships `no-store` (`addon.rs:321-322`).

## T8 — Persist `yt_cache` across restarts — OPTIONAL, only if it stays cheap

`state.rs:107` is an in-memory map with no loader; shutdown (`main.rs:325-338`) does three things and
none of them is this. Every redeploy re-hits TMDB for every browsed title.

Dump to `$CACHE_DIR/resolve.json` on the way out, load on boot. Bounded by `YT_CACHE_MAX` already.

**The clock rule matters and is easy to get wrong.** `confirmed` (`state.rs:36-39`) is the only field
meaning "an upstream last vouched for these ids" — `exp` is rewritten to a retry cooldown by the
stale-substitution path (`addon.rs:239-247`). So validate *populated* entries against
`confirmed + YT_TTL_MS`. But `addon.rs:217-218` sets `confirmed = now` for **every** write including
empty ones, so applying that same rule to an empty entry would resurrect a 60-second
`YT_FAIL_TTL_MS` cooldown as a 24-hour "no trailer" — precisely what the
`YT_FAIL_TTL_MS < YT_NEG_TTL_MS` assert (`main.rs:59-62`) exists to prevent. **Honour the persisted
`exp` for empty entries; use `confirmed` only for populated ones.**

Skip if the write turns out to need more than a serialize-and-rename at shutdown.

## T9 — `/stats`

`route()` (`main.rs:117-223`) exposes no `/stats` or `/metrics`. The four counters are read at
`main.rs:126-128` only to pick a `health_body` branch; `in_flight` depth and cache bytes never
surface at all.

Add a `no-store` JSON `/stats`: cache entries + bytes + headroom against `cache_max_bytes`, resolve
cache size, in-flight downloads, the counters. **Do not walk the cache directory on request** — that
is a syscall storm on a hot endpoint. Reuse what the hourly sweep already computes, or cache the
figure with a timestamp.

## T10(b) — SKIPPED (2026-09-04): link labels hit this task's own disqualifying condition. Carrying a label costs a `String` per candidate through `Upstream::tmdb_candidates`, `Resolved.ids`, `YtEntry` (now also serialized to disk at shutdown) and `PrewarmFn` — an allocation per link on the resolve path, and now in the parked cache too, for a cosmetic gain.

## T10 — Small, cheap

- **`Retry-After`** on the 502/504 play errors. `play_error` (`play.rs:428-435`) passes `&[]`;
  `httputil::json` adds only `no-store` (`httputil.rs:66-69`).
- **Link labels** — `addon.rs:81` hardcodes `"name": "Trailer"`, discarding TMDB's per-video
  `name`/`type`, so fallback links are indistinguishable in the client. **This is not a one-liner**:
  `pick_trailer_candidates` (`upstream.rs:57-76`) reads `type`/`official` for the sort at line 65
  then discards the `Value`, so carrying a label means changing `Upstream::tmdb_candidates`
  (`upstream.rs:32`), `Resolved.ids` (`addon.rs:99`), `PrewarmFn` (`state.rs:25`) and `FakeUpstream`
  (`tests.rs:88`). Do it last, and skip it if it costs an allocation per link on the resolve path.

## PARKED — do not implement

**Progressive stream-through for cold `/play`.** The blocking is real: `download_cached`
(`play.rs:296-359`) is strictly sequential through download → `crop::detect` → `bake_clap` → rename,
and cropdetect is the larger post-download cost. But `-movflags +faststart` is applied by the
*merger's* ffmpeg (`ytdlp.rs:313`), so a leading `moov` exists only after the merge completes —
there is no intermediate file to tee from. A real fix means selecting a progressive rendition
instead of merging, which is a different download path, not an addition to this one.

Worth doing separately: the README's "~3–6s" (README:34) is the download alone and understates cold
time-to-first-byte by however long cropdetect takes. Measure it and correct the README.
