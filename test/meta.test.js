'use strict';
// Node's built-in runner (no deps): `node --test`. Stubs global.fetch to exercise the addon
// path (imdb -> TMDB -> ytId -> play URL) without network, plus manifest + id validation.
const { test, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

process.env.TMDB_KEY = 'test-key';
const app = require('../server.js');

const realFetch = global.fetch;
afterEach(() => { global.fetch = realFetch; app._clearYtCache(); });
beforeEach(() => { app._clearYtCache(); });

/** Stub fetch: TMDB /find -> one hit, /videos -> an official YouTube trailer. */
function stubTmdb(ytKey) {
  global.fetch = async (u) => {
    const url = String(u);
    if (url.includes('/find/')) return jsonRes({ movie_results: [{ id: 42 }] });
    if (url.includes('/videos')) return jsonRes({ results: [
      { site: 'YouTube', type: 'Teaser', key: 'teaseXXXXXX' },
      { site: 'YouTube', type: 'Trailer', official: true, key: ytKey },
    ] });
    return jsonRes({}, 404);
  };
}
const jsonRes = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

test('buildMeta produces a same-host play URL', () => {
  const out = app.buildMeta('movie', 'tt0111161', 'https://trailers.example.com/', 'abc123DEF');
  assert.equal(out.meta.links[0].trailers, 'https://trailers.example.com/play/abc123DEF.mp4');
  assert.equal(out.meta.links[0].provider, 'Den Trailers');
});

test('buildMeta returns empty links when no trailer', () => {
  assert.deepEqual(app.buildMeta('movie', 'tt1', 'https://x', '').meta.links, []);
});

test('resolveYouTubeId picks the official trailer and caches it', async () => {
  let calls = 0;
  const key = 'realTrailer1';
  global.fetch = async (u) => { calls++; return stubTmdbOnce(String(u), key); };
  assert.equal(await app.resolveYouTubeId('tt0111161', 'movie', 'en'), key);
  const after = calls;
  assert.equal(await app.resolveYouTubeId('tt0111161', 'movie', 'en'), key); // cache hit
  assert.equal(calls, after, 'second lookup should be served from cache (no new fetches)');
});

function stubTmdbOnce(url, ytKey) {
  if (url.includes('/find/')) return jsonRes({ movie_results: [{ id: 42 }] });
  if (url.includes('/videos')) return jsonRes({ results: [{ site: 'YouTube', type: 'Trailer', official: true, key: ytKey }] });
  return jsonRes({}, 404);
}

test('GET /manifest.json returns the addon manifest', async () => {
  const body = await request('/manifest.json');
  assert.equal(body.resources[0], 'meta');
  assert.deepEqual(body.types, ['movie', 'series']);
});

test('GET /meta rejects a non-imdb id with empty links (no upstream call)', async () => {
  global.fetch = async () => { throw new Error('must not be called'); };
  const body = await request('/meta/movie/not-an-id.json');
  assert.deepEqual(body.meta.links, []);
});

test('GET /meta resolves a real imdb id to a play URL on the request host', async () => {
  stubTmdb('vidKey12345');
  const body = await request('/meta/movie/tt0111161.json', { host: 'trailers.example.com', 'x-forwarded-proto': 'https' });
  assert.equal(body.meta.links[0].trailers, 'https://trailers.example.com/play/vidKey12345.mp4');
});

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
