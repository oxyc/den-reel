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
const MAX_HEIGHT = process.env.MAX_HEIGHT || '1080';
const CACHE_MAX_BYTES = parseInt(process.env.CACHE_MAX_BYTES || String(8 * 1024 * 1024 * 1024), 10); // 8 GB
const VID_RE = /^[A-Za-z0-9_-]{6,15}$/;

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
const ytCache = new Map(); // `${imdbId}:${lang}` -> { id: string, exp: number }

// Indirection so tests can hold time still without Date.now() flakiness.
let cacheClock = () => Date.now();

/** Parse a JSON response, null instead of throwing on a malformed body. */
async function safeJson(res) {
  try { return await res.json(); } catch { return null; }
}

/** imdb → TMDB id (via /find) → /videos → official YouTube Trailer key (or null). */
async function tmdbTrailerYouTubeId(imdbId, type, lang) {
  const key = process.env.TMDB_KEY;
  if (!key) return null;
  const tmdbType = type === 'series' ? 'tv' : 'movie';
  let find;
  try {
    find = await fetch(`https://api.themoviedb.org/3/find/${imdbId}?external_source=imdb_id&api_key=${key}`);
  } catch { return null; }
  if (!find.ok) return null;
  const found = await safeJson(find);
  const hit = (tmdbType === 'movie' ? found?.movie_results : found?.tv_results)?.[0];
  if (!hit) return null;

  let videos;
  try {
    videos = await fetch(`https://api.themoviedb.org/3/${tmdbType}/${hit.id}/videos?api_key=${key}&language=${lang}`);
  } catch { return null; }
  if (!videos.ok) return null;
  const data = await safeJson(videos);
  const yt = (data?.results ?? []).filter((v) => v.site === 'YouTube');
  const pick =
    yt.find((v) => v.type === 'Trailer' && v.official) ??
    yt.find((v) => v.type === 'Trailer') ??
    yt.find((v) => v.type === 'Teaser') ??
    yt[0];
  return pick?.key ?? null;
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

/** Resolve (and cache) the official-trailer ytId for an imdb id. "" means "looked up, none". */
async function resolveYouTubeId(imdbId, type, lang) {
  const cacheKey = `${imdbId}:${lang}`;
  const hit = ytCache.get(cacheKey);
  if (hit && hit.exp > cacheClock()) return hit.id;
  const id =
    (await tmdbTrailerYouTubeId(imdbId, type, lang)) ??
    (await kinoCheckYouTubeId(imdbId, type, lang)) ??
    '';
  ytCache.set(cacheKey, { id, exp: cacheClock() + YT_TTL_MS });
  return id;
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
  return sendJson(res, buildMeta(type, imdbId, selfBase(req), ytId));
}

function sendJson(res, body, status = 200) {
  const s = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'access-control-allow-origin': '*',
    'content-length': Buffer.byteLength(s),
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
      // MUST be AVPlayer-decodable: H.264 (avc1) video + AAC (mp4a) audio. YouTube's "best"
      // is VP9/AV1 video + Opus audio, none of which Apple TV decodes — forcing avc1+mp4a
      // keeps it a copy-mux (no transcode) and avoids the silent-Opus trap. avc1 caps at
      // 1080p on YT, which matches our ceiling. 18 = 360p progressive h264+aac fallback.
      '-f', `bv*[height<=${MAX_HEIGHT}][vcodec^=avc1]+ba[acodec^=mp4a]/`
          + `b[height<=${MAX_HEIGHT}][vcodec^=avc1][acodec^=mp4a]/18/b[ext=mp4]`,
      '--merge-output-format', 'mp4',
      '--postprocessor-args', 'ffmpeg:-movflags +faststart', // moov up front → progressive play
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
        reject(new Error(`yt-dlp exit ${code}: ${err.slice(-300)}`));
      }
    });
  });
  inFlight.set(vid, p);
  return p;
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
  try {
    const fp = await fetchTrailer(vid);
    serveFile(req, res, fp);
  } catch (e) {
    console.error(`[${vid}] ${e.message}`);
    if (!res.headersSent) { res.writeHead(502); res.end('extraction failed'); }
  }
}

const server = http.createServer(handleRequest);

if (require.main === module) {
  server.listen(PORT, () => console.log(
    `den trailer-service on :${PORT} (cache ${CACHE_DIR}, ≤${MAX_HEIGHT}p, `
    + `addon ${process.env.TMDB_KEY ? 'on' : 'off — set TMDB_KEY'})`));
}

module.exports = {
  server, handleRequest, MANIFEST, buildMeta, resolveYouTubeId,
  tmdbTrailerYouTubeId, kinoCheckYouTubeId,
  _setClock: (fn) => { cacheClock = fn; }, _clearYtCache: () => ytCache.clear(),
};
