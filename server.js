'use strict';
// den trailer-service — resolves a YouTube video id to a playable, App-Store-safe MP4 by
// extracting with yt-dlp (which rotates through innertube clients that don't need a
// BotGuard poToken), ffmpeg-muxing to a faststart MP4, caching on disk, and proxying it
// (the googlevideo URL is IP-bound to THIS server, so the Apple TV must hit us, not YT).
//
// GET /play/<id>.mp4  (or /play?v=<id>)  → 200/206 video/mp4 (range-enabled, seekable)
// GET /health                            → 200 ok

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

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/health') { res.writeHead(200); return res.end('ok'); }

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
});

server.listen(PORT, () => console.log(`den trailer-service on :${PORT} (cache ${CACHE_DIR}, ≤${MAX_HEIGHT}p)`));
