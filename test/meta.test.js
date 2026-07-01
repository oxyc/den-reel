'use strict';
// Node's built-in runner (no deps): `node --test`. Stubs global.fetch (TMDB) and the yt-dlp
// prober (via _setProber) so the addon path runs with no network and no yt-dlp binary.
const { test, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

process.env.TMDB_KEY = 'test-key';
const app = require('../server.js');

const realFetch = global.fetch;
beforeEach(() => {
  app._clearYtCache();
  app._setProber(async () => true); // default: every id plays
  app._setPrewarm(() => {});         // no-op: don't spawn yt-dlp in tests
});
afterEach(() => { global.fetch = realFetch; app._clearYtCache(); });

const jsonRes = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

/** Stub TMDB: /find -> one hit, /videos -> the given trailer results. */
function stubTmdb(results) {
  global.fetch = async (u) => {
    const url = String(u);
    if (url.includes('/find/')) return jsonRes({ movie_results: [{ id: 42 }] });
    if (url.includes('/videos')) return jsonRes({ results });
    return jsonRes({}, 404); // KinoCheck etc.
  };
}

test('pickTrailerCandidates orders official→trailer→teaser and dedupes', () => {
  const out = app.pickTrailerCandidates([
    { site: 'YouTube', type: 'Teaser', key: 'teaser00000' },
    { site: 'Vimeo', type: 'Trailer', key: 'ignored0000' },
    { site: 'YouTube', type: 'Trailer', official: true, key: 'official111' },
    { site: 'YouTube', type: 'Trailer', key: 'plain222222' },
    { site: 'YouTube', type: 'Trailer', official: true, key: 'official111' }, // dup
  ]);
  assert.deepEqual(out, ['official111', 'plain222222', 'teaser00000']);
});

test('buildMeta produces a same-host play URL', () => {
  const out = app.buildMeta('movie', 'tt0111161', 'https://trailers.example.com/', 'abc123DEF');
  assert.equal(out.meta.links[0].trailers, 'https://trailers.example.com/play/abc123DEF.mp4');
});

test('resolveYouTubeId returns the first playable candidate and caches it', async () => {
  let calls = 0;
  global.fetch = async (u) => { calls++; const url = String(u);
    if (url.includes('/find/')) return jsonRes({ movie_results: [{ id: 42 }] });
    if (url.includes('/videos')) return jsonRes({ results: [{ site: 'YouTube', type: 'Trailer', official: true, key: 'firstGood11' }] });
    return jsonRes({}, 404);
  };
  assert.equal(await app.resolveYouTubeId('tt0111161', 'movie', 'en'), 'firstGood11');
  const after = calls;
  assert.equal(await app.resolveYouTubeId('tt0111161', 'movie', 'en'), 'firstGood11'); // cached
  assert.equal(calls, after, 'second lookup is a cache hit (no new fetches)');
});

test('resolveYouTubeId skips a geo-blocked candidate and falls back to the next', async () => {
  stubTmdb([
    { site: 'YouTube', type: 'Trailer', official: true, key: 'blockedUS01' },
    { site: 'YouTube', type: 'Trailer', key: 'worldwide22' },
  ]);
  app._setProber(async (id) => id !== 'blockedUS01'); // the US-only one fails to probe
  assert.equal(await app.resolveYouTubeId('tt0111161', 'movie', 'en'), 'worldwide22');
});

test('resolveYouTubeId returns "" when no candidate is playable', async () => {
  stubTmdb([{ site: 'YouTube', type: 'Trailer', official: true, key: 'blockedUS01' }]);
  app._setProber(async () => false);
  assert.equal(await app.resolveYouTubeId('tt0111161', 'movie', 'en'), '');
});

test('classifyYtdlpError maps a geo-block to 451', () => {
  const e = app.classifyYtdlpError(1, 'ERROR: [youtube] X: The uploader has not made this video available in your country');
  assert.equal(e.status, 451);
  assert.equal(e.reason, 'geo_blocked');
});

test('classifyYtdlpError defaults to 502', () => {
  assert.equal(app.classifyYtdlpError(1, 'some other failure').status, 502);
});

test('GET /manifest.json returns the addon manifest', async () => {
  const body = await request('/manifest.json');
  assert.equal(body.resources[0], 'meta');
});

test('GET /meta rejects a non-imdb id with empty links (no upstream call)', async () => {
  global.fetch = async () => { throw new Error('must not be called'); };
  assert.deepEqual((await request('/meta/movie/not-an-id.json')).meta.links, []);
});

test('GET /meta resolves a real imdb id to a play URL on the request host', async () => {
  stubTmdb([{ site: 'YouTube', type: 'Trailer', official: true, key: 'vidKey12345' }]);
  const body = await request('/meta/movie/tt0111161.json', { host: 'trailers.example.com', 'x-forwarded-proto': 'https' });
  assert.equal(body.meta.links[0].trailers, 'https://trailers.example.com/play/vidKey12345.mp4');
});

test('GET /meta caches a successful resolution (Cache-Control), but not an empty one', async () => {
  stubTmdb([{ site: 'YouTube', type: 'Trailer', official: true, key: 'vidKey12345' }]);
  const ok = await requestRes('/meta/movie/tt0111161.json');
  assert.match(ok.headers['cache-control'] || '', /max-age=604800/);

  app._clearYtCache();
  stubTmdb([]); // no trailer → empty links → must NOT be cached
  const empty = await requestRes('/meta/movie/tt0111161.json');
  assert.deepEqual(empty.body.meta.links, []);
  assert.equal(empty.headers['cache-control'], undefined);
});

test('?prewarm=0 resolves without prewarming; default prewarms', async () => {
  stubTmdb([{ site: 'YouTube', type: 'Trailer', official: true, key: 'vidKey12345' }]);
  const warmed = [];
  app._setPrewarm((id) => warmed.push(id));
  await requestRes('/meta/movie/tt0111161.json?prewarm=0');
  assert.deepEqual(warmed, [], 'prewarm should be skipped');
  await requestRes('/meta/movie/tt0111161.json');
  assert.deepEqual(warmed, ['vidKey12345'], 'default should prewarm the resolved id');
});

/** Like request() but returns { status, headers, body }. */
function requestRes(path, headers = {}) {
  return new Promise((resolve, reject) => {
    const srv = app.server.listen(0, () => {
      const { port } = srv.address();
      http.get({ port, path, headers }, (res) => {
        let d = ''; res.on('data', (c) => (d += c));
        res.on('end', () => { srv.close(); let body; try { body = JSON.parse(d); } catch { body = d; }
          resolve({ status: res.statusCode, headers: res.headers, body }); });
      }).on('error', reject);
    });
  });
}

/** Fire a request at the in-process server and return the parsed JSON body. */
function request(path, headers = {}) {
  return new Promise((resolve, reject) => {
    const srv = app.server.listen(0, () => {
      const { port } = srv.address();
      http.get({ port, path, headers }, (res) => {
        let d = ''; res.on('data', (c) => (d += c));
        res.on('end', () => { srv.close(); try { resolve(JSON.parse(d)); } catch (e) { reject(e); } });
      }).on('error', reject);
    });
  });
}
