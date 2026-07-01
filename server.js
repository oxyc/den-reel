'use strict';
// den trailer-service — the whole trailer path in one container:
//
//   1. ADDON (Den/Fusion protocol):  imdbId -> TMDB /videos (KinoCheck fallback) -> ytId
//      GET /manifest.json                     -> addon manifest
//      GET /meta/<movie|series>/<imdbId>.json -> { meta: { links:[{ trailers: <play url> }] } }
//
//   2. PLAYBACK (yt-dlp + ffmpeg proxy):  ytId -> App-Store-safe, seekable MP4
//      GET /play/<id>.mp4  (or /play?v=<id>)  -> 200/206 video/mp4
//      GET /health                            -> 200 ok
//
// Extraction: yt-dlp rotates innertube clients that don't need a BotGuard poToken; ffmpeg
// muxes a faststart H.264/AAC MP4; we cache and PROXY it (the googlevideo URL is IP-bound
// to THIS server, so the Apple TV must hit us, not YouTube).
//
// Merged from the old den-trailers Cloudflare Worker so there's one container, one repo, and
// no Cloudflare: the addon returns a play URL on *this same host*, derived from the request
// (or PUBLIC_BASE_URL), so LAN-only works with nothing public.
//
// Env: PORT, CACHE_DIR, YTDLP_PATH, MAX_HEIGHT, CACHE_MAX_BYTES (playback);
//      TMDB_KEY (required for the addon), KINOCHECK_KEY (optional), PUBLIC_BASE_URL (optional).

const http = require('http');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const PORT = parseInt(process.env.PORT || '8092', 10);
const CACHE_DIR = process.env.CACHE_DIR || path.join(require('os').tmpdir(), 'den-trailer-cache');
const YTDLP = process.env.YTDLP_PATH || 'yt-dlp';
const MAX_HEIGHT = process.env.MAX_HEIGHT || '1080'; // avc1's ceiling on YouTube (max AVPlayer can HW-decode)
const CACHE_MAX_BYTES = parseInt(process.env.CACHE_MAX_BYTES || String(8 * 1024 * 1024 * 1024), 10); // 8 GB
const VID_RE = /^[A-Za-z0-9_-]{6,15}$/;
const MAX_PROBE = 6; // cap how many trailer candidates we validate per movie
const PREWARM_MAX = 3; // cap concurrent prewarm downloads (bounds a burst of /meta calls)
const YTDLP_CACHE = path.join(CACHE_DIR, 'yt-dlp'); // persist nsig/player-JS cache across restarts
// The yt-dlp format we serve: H.264(avc1) + AAC(mp4a), ≤MAX_HEIGHT (avc1's ceiling on YT),
// faststart-muxable. We force this so trailers play on AVPlayer's HARDWARE decode path —
// lighter/faster than routing a short clip through the app's Aether Engine, which *can*
// software-decode YouTube's VP9/AV1+Opus "best" but at a CPU/heat cost not worth it here.
// Keep it avc1. Shared by the extract path AND the resolve-time probe, so a probe validates
// exactly what playback needs (a candidate that can't produce it — geo-blocked, removed,
// VP9/AV1-only — is skipped in favour of the next trailer).
const YTDLP_FORMAT =
  `bv*[height<=${MAX_HEIGHT}][vcodec^=avc1]+ba[acodec^=mp4a]/`
  + `b[height<=${MAX_HEIGHT}][vcodec^=avc1][acodec^=mp4a]/18/b[ext=mp4]`;

fs.mkdirSync(CACHE_DIR, { recursive: true });
const inFlight = new Map(); // vid -> Promise<filepath>

const cachePath = (vid) => path.join(CACHE_DIR, `${vid}.mp4`);

// ---------------------------------------------------------------------------
// ADDON: imdbId -> official-trailer YouTube id
// ---------------------------------------------------------------------------

const MANIFEST = {
  id: 'fi.oxy.den-trailers',
  version: '0.2.0',
  name: 'Den Trailers',
  description: 'Direct-URL trailers (TMDB/KinoCheck → yt-dlp service) for inline playback.',
  resources: ['meta'],
  types: ['movie', 'series'],
  idPrefixes: ['tt', 'tmdb:'],
  catalogs: [],
};

// Cache the STABLE ytId (the expensive lookup); playback is just our /play proxy for it.
// In-memory (24h TTL) — cheap to rebuild on restart, no external store needed. "" = "no trailer".
const YT_TTL_MS = 24 * 60 * 60 * 1000;
const YT_NEG_TTL_MS = 60 * 60 * 1000; // "nothing playable" caches shorter (geo/transient may lift)
const ytCache = new Map(); // `${imdbId}:${lang}` -> { id: string, exp: number }

// Indirection so tests can hold time still without Date.now() flakiness.
let cacheClock = () => Date.now();

/** Parse a JSON response, null instead of throwing on a malformed body. */
async function safeJson(res) {
  try { return await res.json(); } catch { return null; }
}

/** Rank + dedupe a TMDB /videos result into an ordered list of YouTube ids
 *  (official trailer first, then trailer, teaser, anything else). Pure — unit-tested. */
function pickTrailerCandidates(results) {
  const yt = (results ?? []).filter((v) => v.site === 'YouTube' && v.key);
  const rank = (v) =>
    v.type === 'Trailer' && v.official ? 0 :
    v.type === 'Trailer' ? 1 :
    v.type === 'Teaser' ? 2 : 3;
  return [...new Set(yt.slice().sort((a, b) => rank(a) - rank(b)).map((v) => v.key))];
}

/** imdb → TMDB id (via /find) → /videos → ordered YouTube trailer candidates ([] on miss). */
async function tmdbTrailerCandidates(imdbId, type, lang) {
  const key = process.env.TMDB_KEY;
  if (!key) return [];
  const tmdbType = type === 'series' ? 'tv' : 'movie';
  let find;
  try {
    find = await fetch(`https://api.themoviedb.org/3/find/${imdbId}?external_source=imdb_id&api_key=${key}`);
  } catch { return []; }
  if (!find.ok) return [];
  const found = await safeJson(find);
  const hit = (tmdbType === 'movie' ? found?.movie_results : found?.tv_results)?.[0];
  if (!hit) return [];

  let videos;
  try {
    videos = await fetch(`https://api.themoviedb.org/3/${tmdbType}/${hit.id}/videos?api_key=${key}&language=${lang}`);
  } catch { return []; }
  if (!videos.ok) return [];
  const data = await safeJson(videos);
  return pickTrailerCandidates(data?.results);
}

/** KinoCheck discovery fallback: imdb → official trailer's YouTube id (or null). */
async function kinoCheckYouTubeId(imdbId, type, lang) {
  const endpoint = type === 'series' ? 'shows' : 'movies';
  const language = lang.startsWith('de') ? 'de' : 'en';
  const kkey = process.env.KINOCHECK_KEY;
  let res;
  try {
    res = await fetch(
      `https://api.kinocheck.com/${endpoint}?imdb_id=${encodeURIComponent(imdbId)}&categories=Trailer&language=${language}`,
      { headers: { Accept: 'application/json', ...(kkey ? { 'X-Api-Key': kkey, 'X-Api-Host': 'api.kinocheck.com' } : {}) } },
    );
  } catch { return null; }
  if (!res.ok) return null;
  const data = await safeJson(res);
  return data?.trailer?.youtube_video_id ?? null;
}

/** Resolve (and cache) the first PLAYABLE trailer ytId for an imdb id. Probes candidates with
 *  yt-dlp and skips geo-blocked / removed / undecodable ones, so the URL we hand Den actually
 *  plays here. "" = looked up, nothing playable (cached shorter, in case it's transient). */
async function resolveYouTubeId(imdbId, type, lang) {
  const cacheKey = `${imdbId}:${lang}`;
  const hit = ytCache.get(cacheKey);
  if (hit && hit.exp > cacheClock()) return hit.id;
  // TMDB + KinoCheck concurrently (KinoCheck is only a fallback source, but fetching it in
  // parallel costs no extra wall-clock). Official trailer first, KinoCheck appended.
  const [tmdb, kc] = await Promise.all([
    tmdbTrailerCandidates(imdbId, type, lang),
    kinoCheckYouTubeId(imdbId, type, lang),
  ]);
  const candidates = [...new Set([...tmdb, ...(kc ? [kc] : [])])].slice(0, MAX_PROBE);
  const id = await firstPlayable(candidates);
  ytCache.set(cacheKey, { id, exp: cacheClock() + (id ? YT_TTL_MS : YT_NEG_TTL_MS) });
  return id; // prewarm is the caller's decision (handleMeta) — a resolve-only path shouldn't download
}

/** First candidate yt-dlp can actually extract here, preserving rank order. Common case (top
 *  trailer plays) costs ONE probe; only on a miss do we probe the rest concurrently and take
 *  the highest-ranked that passes — so a geo-blocked top pick no longer serialises N×3s. */
async function firstPlayable(candidates) {
  if (!candidates.length) return '';
  if (await prober(candidates[0])) return candidates[0];
  const rest = candidates.slice(1);
  const probes = rest.map((c) => prober(c)); // run concurrently...
  for (let i = 0; i < rest.length; i++) {
    if (await probes[i]) return rest[i]; // ...but await in rank order → highest-ranked winner
  }
  return '';
}

// Fire-and-forget download so /play is warm. Bounded by inFlight size to survive a /meta burst;
// inFlight dedupes the later real /play. Swapped to a no-op in tests via _setPrewarm.
let prewarm = (id) => { if (id && inFlight.size < PREWARM_MAX) fetchTrailer(id).catch(() => {}); };

/** Does yt-dlp think this id is extractable HERE (right region, decodable formats)? Fast:
 *  --simulate, no download. Swapped out in tests via _setProber. */
function probeExtractable(ytId) {
  return new Promise((resolve) => {
    const proc = spawn(YTDLP, ['-q', '--simulate', '--no-warnings', '--cache-dir', YTDLP_CACHE,
      '-f', YTDLP_FORMAT, `https://www.youtube.com/watch?v=${ytId}`], { stdio: 'ignore' });
    proc.on('error', () => resolve(false));
    proc.on('close', (code) => resolve(code === 0));
  });
}
let prober = probeExtractable;

/** Map a yt-dlp failure to an HTTP status + short reason, so /play says why instead of a
 *  blanket 502. yt-dlp puts the cause in stderr; we match the common YouTube ones. */
function classifyYtdlpError(code, stderr) {
  const s = (stderr || '').toLowerCase();
  let status = 502, reason = 'extraction_failed', message = 'Could not fetch this trailer.';
  if (/available in your (country|location)|blocked it in your country|not available from your location/.test(s)) {
    status = 451; reason = 'geo_blocked'; message = 'This trailer is not available in your region.';
  } else if (/private video|sign in to confirm your age|age-restricted|members-only/.test(s)) {
    status = 403; reason = 'restricted'; message = 'This trailer is private or age-restricted.';
  } else if (/video unavailable|has been removed|no longer available|does not exist|removed by the uploader/.test(s)) {
    status = 404; reason = 'unavailable'; message = 'This trailer is no longer available.';
  }
  const e = new Error(`yt-dlp exit ${code}: ${(stderr || '').slice(-300)}`);
  e.status = status; e.reason = reason; e.userMessage = message;
  return e;
}

/** The base URL this server is reachable at (for building play URLs the device will fetch). */
function selfBase(req) {
  if (process.env.PUBLIC_BASE_URL) return process.env.PUBLIC_BASE_URL.replace(/\/+$/, '');
  const proto = String(req.headers['x-forwarded-proto'] || 'http').split(',')[0].trim();
  const host = req.headers['x-forwarded-host'] || req.headers.host || `localhost:${PORT}`;
  return `${proto}://${host}`;
}

/** Build the Fusion `meta` payload for a resolved (or missing) trailer. */
function buildMeta(type, imdbId, base, ytId) {
  if (!ytId) return { meta: { id: imdbId, type, links: [] } };
  const trailers = `${base.replace(/\/+$/, '')}/play/${ytId}.mp4`;
  return {
    meta: { id: imdbId, type, links: [{ name: 'Trailer', category: 'Trailer', trailers, provider: 'Den Trailers' }] },
  };
}

async function handleMeta(req, res, type, rawId) {
  const imdbId = rawId.split(':')[0]; // series may arrive as tt…:S:E — trailers are show-level
  // Only imdb ids reach the upstreams (and our URLs) — reject anything else so a crafted id
  // can't be interpolated into a TMDB/KinoCheck request.
  if (!/^tt\d+$/.test(imdbId)) return sendJson(res, buildMeta(type, imdbId, selfBase(req), ''));
  const url = new URL(req.url, 'http://localhost');
  const rawLang = url.searchParams.get('lang') ?? 'en';
  const lang = /^[a-z]{2}$/i.test(rawLang) ? rawLang : 'en';
  const ytId = await resolveYouTubeId(imdbId, type, lang);
  // Prewarm the download UNLESS the caller opted out (?prewarm=0) — e.g. a browse-time
  // poster-focus prefetch that only wants to resolve+validate, not pull bytes yet.
  if (ytId && url.searchParams.get('prewarm') !== '0') prewarm(ytId);
  const payload = buildMeta(type, imdbId, selfBase(req), ytId);
  // Only cache a SUCCESSFUL resolution (a real trailer). Empty results (no trailer, geo-blocked,
  // transient) stay uncached so the client re-checks — they may resolve later.
  const headers = payload.meta.links.length ? { 'cache-control': 'public, max-age=604800' } : {};
  return sendJson(res, payload, 200, headers);
}

function sendJson(res, body, status = 200, headers = {}) {
  const s = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'access-control-allow-origin': '*',
    'content-length': Buffer.byteLength(s),
    ...headers,
  });
  res.end(s);
}

// ---------------------------------------------------------------------------
// PLAYBACK: ytId -> cached faststart MP4 (yt-dlp + ffmpeg), proxied with range support
// ---------------------------------------------------------------------------

/** Evict least-recently-used cached files until under the size cap (bounded cache). */
function evictIfNeeded() {
  let files = fs.readdirSync(CACHE_DIR)
    .filter((f) => f.endsWith('.mp4'))
    .map((f) => { const p = path.join(CACHE_DIR, f); const s = fs.statSync(p); return { p, size: s.size, atime: s.atimeMs }; });
  let total = files.reduce((n, f) => n + f.size, 0);
  if (total <= CACHE_MAX_BYTES) return;
  files.sort((a, b) => a.atime - b.atime); // oldest first
  for (const f of files) {
    if (total <= CACHE_MAX_BYTES) break;
    try { fs.unlinkSync(f.p); total -= f.size; } catch { /* ignore */ }
  }
}

/** Download+mux a faststart MP4 for `vid`, cached. De-dupes concurrent requests. */
function fetchTrailer(vid) {
  const fp = cachePath(vid);
  if (fs.existsSync(fp) && fs.statSync(fp).size > 0) {
    fs.utimes(fp, new Date(), fs.statSync(fp).mtime, () => {}); // bump atime for LRU
    return Promise.resolve(fp);
  }
  if (inFlight.has(vid)) return inFlight.get(vid);

  // Temp MUST end in .mp4 — yt-dlp's merge step derives the output name from the extension,
  // so a `.tmp` suffix makes it write somewhere we don't expect.
  const tmp = path.join(CACHE_DIR, `.${vid}.${process.pid}.partial.mp4`);
  const p = new Promise((resolve, reject) => {
    const args = [
      '-q', '--no-playlist', '--no-warnings',
      '--cache-dir', YTDLP_CACHE,   // reuse the nsig/player-JS work the probe already did
      '-N', '4',                    // parallel DASH fragments → faster download
      // AVPlayer hardware-decodable: H.264 (avc1) + AAC (mp4a) — see YTDLP_FORMAT for why.
      // Same string the resolve-time probe validates.
      '-f', YTDLP_FORMAT,
      '--merge-output-format', 'mp4',
      // faststart during the merge's ffmpeg (one pass), not a separate whole-file rewrite.
      '--postprocessor-args', 'Merger+ffmpeg:-movflags +faststart',
      '-o', tmp,
      `https://www.youtube.com/watch?v=${vid}`,
    ];
    const proc = spawn(YTDLP, args, { stdio: ['ignore', 'ignore', 'pipe'] });
    let err = '';
    proc.stderr.on('data', (d) => { err += d.toString(); });
    proc.on('error', (e) => { inFlight.delete(vid); reject(e); });
    proc.on('close', (code) => {
      inFlight.delete(vid);
      if (code === 0 && fs.existsSync(tmp) && fs.statSync(tmp).size > 0) {
        fs.renameSync(tmp, fp);
        try { evictIfNeeded(); } catch { /* ignore */ }
        resolve(fp);
      } else {
        try { fs.unlinkSync(tmp); } catch { /* ignore */ }
        reject(classifyYtdlpError(code, err));
      }
    });
  });
  inFlight.set(vid, p);
  return p;
}

/** Resolve the direct googlevideo URL(s) without downloading (yt-dlp -g): [video] or
 *  [video, audio]. IP-bound URLs — only usable from THIS host, which is why we proxy. */
function ytdlpUrls(vid) {
  return new Promise((resolve, reject) => {
    const proc = spawn(YTDLP, ['-q', '--no-warnings', '--cache-dir', YTDLP_CACHE,
      '-f', YTDLP_FORMAT, '-g', `https://www.youtube.com/watch?v=${vid}`],
      { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { err += d; });
    proc.on('error', reject);
    proc.on('close', (code) => {
      const urls = out.trim().split('\n').filter(Boolean);
      if (code === 0 && urls.length) resolve(urls);
      else reject(classifyYtdlpError(code, err));
    });
  });
}

/** Remux a fragmented stream capture into a faststart MP4 for the cache, so replays/seeks use
 *  the fast ranged path. Local copy-mux, no re-encode. */
function remuxFaststart(src, vid) {
  const dst = cachePath(vid);
  const ff = spawn('ffmpeg', ['-y', '-hide_banner', '-loglevel', 'error', '-i', src,
    '-c', 'copy', '-movflags', '+faststart', dst], { stdio: 'ignore' });
  ff.on('close', (code) => {
    fs.unlink(src, () => {});
    if (code === 0) { try { evictIfNeeded(); } catch { /* ignore */ } }
    else { fs.unlink(dst, () => {}); }
  });
  ff.on('error', () => fs.unlink(src, () => {}));
}

/** Stream a trailer to the client AS ffmpeg live-muxes YouTube's video+audio into fragmented
 *  MP4 — first frame in seconds instead of after a full download. Tees to disk; on a clean
 *  finish, remuxes to a faststart cache file for future ranged serves. Resolves once bytes are
 *  flowing (headers sent); REJECTS before any output so the caller can fall back to buffered. */
async function streamPlay(req, res, vid) {
  const urls = await ytdlpUrls(vid);                 // may throw → caller falls back
  const inputs = urls.flatMap((u) => ['-i', u]);
  const tmp = path.join(CACHE_DIR, `.${vid}.${process.pid}.stream.mp4`);
  const ff = spawn('ffmpeg', ['-hide_banner', '-loglevel', 'error', ...inputs,
    '-c', 'copy', '-movflags', 'frag_keyframe+empty_moov+default_base_moof', '-f', 'mp4', 'pipe:1'],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  const cache = fs.createWriteStream(tmp);
  let started = false, aborted = false;
  const stop = () => { if (!aborted) { aborted = true; ff.kill('SIGKILL'); } };
  req.on('close', () => { if (!res.writableEnded) stop(); });
  res.on('error', stop);

  await new Promise((resolve, reject) => {
    ff.stdout.on('data', (chunk) => {
      if (!started) { started = true; res.writeHead(200, { 'Content-Type': 'video/mp4', 'Cache-Control': 'no-store' }); resolve(); }
      cache.write(chunk);
      if (!res.write(chunk)) { ff.stdout.pause(); res.once('drain', () => ff.stdout.resume()); } // backpressure
    });
    ff.on('error', (e) => { if (!started) reject(e); else stop(); });
    ff.on('close', (code) => {
      cache.end();
      if (!started) { fs.unlink(tmp, () => {}); reject(new Error('stream produced no output')); return; }
      if (!res.writableEnded) res.end();
      if (code === 0 && !aborted) remuxFaststart(tmp, vid); else fs.unlink(tmp, () => {});
    });
  });
}

/** Serve a file with HTTP range support (so the player can scrub). */
function serveFile(req, res, fp) {
  const { size } = fs.statSync(fp);
  const range = req.headers.range && /bytes=(\d+)-(\d*)/.exec(req.headers.range);
  if (range) {
    const start = parseInt(range[1], 10);
    const end = range[2] ? Math.min(parseInt(range[2], 10), size - 1) : size - 1;
    if (start >= size || start > end) { res.writeHead(416, { 'Content-Range': `bytes */${size}` }); return res.end(); }
    res.writeHead(206, {
      'Content-Range': `bytes ${start}-${end}/${size}`,
      'Accept-Ranges': 'bytes',
      'Content-Length': end - start + 1,
      'Content-Type': 'video/mp4',
    });
    fs.createReadStream(fp, { start, end }).pipe(res);
  } else {
    res.writeHead(200, { 'Content-Length': size, 'Content-Type': 'video/mp4', 'Accept-Ranges': 'bytes' });
    fs.createReadStream(fp).pipe(res);
  }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

async function handleRequest(req, res) {
  const url = new URL(req.url, 'http://localhost');

  if (url.pathname === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url.pathname === '/manifest.json') return sendJson(res, MANIFEST);

  const meta = /^\/meta\/(movie|series)\/(.+)\.json$/.exec(url.pathname);
  if (meta) return handleMeta(req, res, meta[1], decodeURIComponent(meta[2]));

  // playback
  let vid = url.searchParams.get('v');
  const m = /^\/play\/([A-Za-z0-9_-]{6,15})\.mp4$/.exec(url.pathname);
  if (m) vid = m[1];
  else if (url.pathname !== '/play') { res.writeHead(404); return res.end('not found'); }

  if (!vid || !VID_RE.test(vid)) { res.writeHead(400); return res.end('bad video id'); }

  const fp = cachePath(vid);
  if (fs.existsSync(fp) && fs.statSync(fp).size > 0) {           // cached → ranged serve (seeks)
    fs.utimes(fp, new Date(), fs.statSync(fp).mtime, () => {});  // LRU bump
    return serveFile(req, res, fp);
  }
  // A seek before we've cached the file can't be served from a live stream → buffer, then serve.
  const rm = req.headers.range && /bytes=(\d+)/.exec(req.headers.range);
  const seeking = rm && parseInt(rm[1], 10) > 0;
  try {
    if (seeking) return serveFile(req, res, await fetchTrailer(vid));
    await streamPlay(req, res, vid);          // cold play → stream as it muxes (fast first frame)
  } catch (e) {
    console.error(`[${vid}] ${e.message}`);
    if (!res.headersSent) {                    // couldn't start streaming → buffered, else typed error
      try { return serveFile(req, res, await fetchTrailer(vid)); }
      catch (e2) { return playError(res, vid, e2); }
    }
  }
}

/** Typed /play failure body (geo_blocked 451 / restricted 403 / unavailable 404 / 502). */
function playError(res, vid, e) {
  if (res.headersSent) return;
  const body = JSON.stringify({
    error: e.reason || 'extraction_failed',
    message: e.userMessage || 'Could not fetch this trailer.',
    id: vid,
  });
  res.writeHead(e.status || 502, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

const server = http.createServer(handleRequest);

if (require.main === module) {
  server.listen(PORT, () => console.log(
    `den trailer-service on :${PORT} (cache ${CACHE_DIR}, ≤${MAX_HEIGHT}p, `
    + `addon ${process.env.TMDB_KEY ? 'on' : 'off — set TMDB_KEY'})`));
}

module.exports = {
  server, handleRequest, MANIFEST, buildMeta, resolveYouTubeId,
  pickTrailerCandidates, tmdbTrailerCandidates, kinoCheckYouTubeId, classifyYtdlpError,
  _setClock: (fn) => { cacheClock = fn; },
  _setProber: (fn) => { prober = fn; },
  _setPrewarm: (fn) => { prewarm = fn; },
  _clearYtCache: () => ytCache.clear(),
};
