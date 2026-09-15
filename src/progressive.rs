//! PROGRESSIVE: YouTube's video stream as an ordinary MP4 with its index first, without downloading it.
//!
//! `/direct` hands a browser Google's own video URL, and Chrome starts it in under a second. Safari
//! took five: YouTube's adaptive streams are FRAGMENTED MP4 — a `moov` whose sample tables are empty,
//! a `sidx`, then one `moof`+`mdat` pair per few seconds — and Safari's progressive player reads every
//! `moof` in the file, one abandoned range request each, before it will say it can play. `/play`'s
//! faststart copy started in about one second in the same browser, but it costs a download, an ffmpeg
//! run and a cache slot per trailer.
//!
//! This builds what that copy has — a complete `moov` at the front — from Google's own boxes. The
//! `sidx` says where every fragment starts, so the `moof`s are a couple of dozen small range requests
//! from here; their sample sizes, durations and flags become ordinary `stts`/`ctts`/`stss`/`stsc`/
//! `stsz`/`stco` tables, and the file served is that header followed by the fragments' sample bytes
//! laid end to end. A player's byte range maps back onto ranges of Google's file, fetched as they are
//! asked for. No media is decoded, re-muxed or stored: what is kept is the index, for as long as
//! Google's URL lives.
//!
//! **Sound when asked.** It serves the stream `/direct` names as `video`, which is a muted surface's
//! whole need, and with `?audio=1` the `audio` stream too: YouTube's audio is fragmented the same way,
//! so its index is built the same way and the file carries both tracks, their chunks interleaved by time.
//!
//! **Anything it cannot index is sent to Google.** A file that is not fragmented, a box it cannot read,
//! or a fetch that fails answers `302` to the raw URL — which is exactly what the page played before —
//! and says why in the log. With sound asked for it answers an error instead: the raw URL has none.

use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::Shared;
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use hyper::header::{HeaderMap, HeaderValue, IF_RANGE, RANGE};
use hyper::{Response, StatusCode};
use tokio::sync::mpsc;

use crate::httputil::{self, Body, RangeReq};
use crate::state::{AppState, BoxFuture};

/// The first bytes of a file, which must hold its `moov` and `sidx`. Both are a kilobyte or so.
const HEAD_BYTES: u64 = 64 * 1024;

/// How much of a fragment to ask for to read its `moof`: a few kilobytes for a few seconds of video.
/// A larger one is fetched again at its own length.
const MOOF_PROBE: u64 = 16 * 1024;

/// `moof` fetches in flight at once. Over HTTP/1 each is a connection to googlevideo.
const MOOF_FETCHES: usize = 8;

/// A trailer has a few thousand samples. Far past that is a box that is not what it says.
const MAX_SAMPLES: usize = 1_000_000;

/// One index fetch: small, so a stuck one is given up on quickly.
const INDEX_TIMEOUT: Duration = Duration::from_secs(15);

/// One fragment's bytes on their way to a player: long enough for a slow line.
const FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// Bound on the kept indexes. Each is tens of kilobytes and expires with its URL.
pub const PROGRESSIVE_MAX: usize = 64;

/// Why no index was built. `retry` is a fetch that may work next time; otherwise the file itself is
/// not one this can index, and asking again before its URL changes would find the same thing.
#[derive(Clone, Debug)]
pub struct Unbuilt {
    pub why: String,
    pub retry: bool,
}

fn unreadable(why: impl Into<String>) -> Unbuilt {
    Unbuilt { why: why.into(), retry: false }
}

/// A build shared by every request for the same stream, finished or not.
pub type SharedLayout = Shared<BoxFuture<Result<Arc<Layout>, Unbuilt>>>;

/// One file `layout` reads: its `moov` box, and its `moof`s with their offsets in that file.
pub(crate) type Source<'a> = (&'a [u8], &'a [(u64, Bytes)]);

/// One kept build: the URLs it indexes (one per line), when to stop using it (epoch ms), and the build.
pub type Entry = (String, u64, SharedLayout);

/// The file this serves: `head` (ftyp, the built moov, the mdat header), then each piece of Google's
/// file in order.
#[derive(Debug)]
pub struct Layout {
    pub head: Bytes,
    pub pieces: Vec<Piece>,
    pub total: u64,
    pub etag: String,
}

/// `len` bytes at `at` in the served file, which are `len` bytes at `from` in Google's file `source`
/// (the video, or its audio).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Piece {
    pub at: u64,
    pub source: usize,
    pub from: u64,
    pub len: u64,
}

fn be16(b: &[u8], at: usize) -> Result<u16, Unbuilt> {
    b.get(at..at + 2).map(|s| u16::from_be_bytes([s[0], s[1]])).ok_or_else(|| unreadable("a box ends early"))
}

fn be32(b: &[u8], at: usize) -> Result<u32, Unbuilt> {
    b.get(at..at + 4)
        .map(|s| u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
        .ok_or_else(|| unreadable("a box ends early"))
}

fn be64(b: &[u8], at: usize) -> Result<u64, Unbuilt> {
    Ok((be32(b, at)? as u64) << 32 | be32(b, at + 4)? as u64)
}

/// One box in a buffer: its type, where it starts, where its payload starts, and where it ends.
#[derive(Clone, Copy)]
struct Atom {
    kind: [u8; 4],
    start: usize,
    body: usize,
    end: usize,
}

/// The boxes laid end to end in `b[from..to]`. One that runs past `to` is an error, unless `open`: the
/// first bytes of a file, or of a fragment, stop part-way through a box, and the list stops there.
fn atoms(b: &[u8], from: usize, to: usize, open: bool) -> Result<Vec<Atom>, Unbuilt> {
    let mut out = Vec::new();
    let mut at = from;
    while at + 8 <= to {
        let kind = [b[at + 4], b[at + 5], b[at + 6], b[at + 7]];
        let (len, body) = match be32(b, at)? {
            0 => ((to - at) as u64, at + 8),
            1 if open && at + 16 > to => break,
            1 => (be64(b, at + 8)?, at + 16),
            n => (n as u64, at + 8),
        };
        if len < (body - at) as u64 {
            return Err(unreadable(format!("a {} box shorter than its header", name(&kind))));
        }
        let end = at as u64 + len;
        if end > to as u64 {
            if open {
                break;
            }
            return Err(unreadable(format!("a {} box runs past its parent", name(&kind))));
        }
        out.push(Atom { kind, start: at, body, end: end as usize });
        at = end as usize;
    }
    Ok(out)
}

fn name(kind: &[u8; 4]) -> String {
    String::from_utf8_lossy(kind).into_owned()
}

fn find(list: &[Atom], kind: &[u8; 4]) -> Result<Atom, Unbuilt> {
    list.iter().copied().find(|a| &a.kind == kind).ok_or_else(|| unreadable(format!("no {} box", name(kind))))
}

/// Where the `moov` sits in a file's first bytes, and where each fragment the `sidx` lists starts and
/// how long it is.
pub(crate) struct Index {
    pub moov: Range<usize>,
    pub fragments: Vec<(u64, u64)>,
}

pub(crate) fn index(head: &[u8]) -> Result<Index, Unbuilt> {
    let top = atoms(head, 0, head.len(), true)?;
    let moov = find(&top, b"moov")?;
    let sidx = top
        .iter()
        .copied()
        .find(|a| &a.kind == b"sidx")
        .ok_or_else(|| unreadable("no sidx in the first bytes, so not a fragmented file"))?;
    let b = sidx.body;
    // version+flags, reference_ID, timescale, then earliest_presentation_time and first_offset, 32 or
    // 64 bits each, then two reserved bytes and the reference count.
    let (first_offset, counted) = match head.get(b) {
        Some(0) => (be32(head, b + 16)? as u64, b + 20),
        Some(_) => (be64(head, b + 20)?, b + 28),
        None => return Err(unreadable("a sidx box ends early")),
    };
    let count = be16(head, counted + 2)? as usize;
    // The offsets count from the first byte after the sidx.
    let mut offset = sidx.end as u64 + first_offset;
    let mut fragments = Vec::with_capacity(count);
    for i in 0..count {
        let word = be32(head, counted + 4 + i * 12)?;
        if word & 0x8000_0000 != 0 {
            return Err(unreadable("a sidx that points at another sidx"));
        }
        let size = (word & 0x7fff_ffff) as u64;
        if size < 8 {
            return Err(unreadable("a sidx reference too small to hold a moof"));
        }
        fragments.push((offset, size));
        offset += size;
    }
    if fragments.is_empty() {
        return Err(unreadable("a sidx with no references"));
    }
    Ok(Index { moov: moov.start..moov.end, fragments })
}

/// What the `moov` says about the one track and the fragments' defaults.
struct Track {
    id: u32,
    duration: u32,
    size: u32,
    flags: u32,
}

/// Every sample the fragments carry, in order, and each run of them as it sits in Google's file.
#[derive(Default)]
struct Samples {
    durations: Vec<u32>,
    sizes: Vec<u32>,
    sync: Vec<bool>,
    offsets: Vec<i32>,
    /// (where in Google's file, how many bytes, how many samples)
    chunks: Vec<(u64, u64, u32)>,
}

/// Read one fragment's `moof` (at `moof_at` in Google's file) into `s`.
fn read_fragment(b: &[u8], moof_at: u64, track: &Track, s: &mut Samples) -> Result<(), Unbuilt> {
    let moof = atoms(b, 0, b.len(), true)?
        .first()
        .copied()
        .filter(|a| &a.kind == b"moof")
        .ok_or_else(|| unreadable("a sidx reference that does not start with a moof"))?;
    for traf in atoms(b, moof.body, moof.end, false)?.into_iter().filter(|a| &a.kind == b"traf") {
        let kids = atoms(b, traf.body, traf.end, false)?;
        let tfhd = find(&kids, b"tfhd")?;
        let flags = be32(b, tfhd.body)? & 0x00ff_ffff;
        if be32(b, tfhd.body + 4)? != track.id {
            continue;
        }
        let mut at = tfhd.body + 8;
        let mut take = |present: u32, width: usize| -> Result<Option<u64>, Unbuilt> {
            if flags & present == 0 {
                return Ok(None);
            }
            let v = if width == 8 { be64(b, at)? } else { be32(b, at)? as u64 };
            at += width;
            Ok(Some(v))
        };
        // Without a base_data_offset, a track fragment's data is counted from its moof.
        let base = take(0x01, 8)?.unwrap_or(moof_at);
        take(0x02, 4)?;
        let duration = take(0x08, 4)?.map_or(track.duration, |v| v as u32);
        let size = take(0x10, 4)?.map_or(track.size, |v| v as u32);
        let sample_flags = take(0x20, 4)?.map_or(track.flags, |v| v as u32);

        let mut data = base;
        for trun in kids.iter().filter(|a| &a.kind == b"trun") {
            let head = be32(b, trun.body)?;
            let tf = head & 0x00ff_ffff;
            let count = be32(b, trun.body + 4)? as usize;
            if s.sizes.len() + count > MAX_SAMPLES {
                return Err(unreadable("more samples than a trailer has"));
            }
            let mut at = trun.body + 8;
            if tf & 0x001 != 0 {
                let offset = be32(b, at)? as i32 as i64;
                data = base
                    .checked_add_signed(offset)
                    .ok_or_else(|| unreadable("a trun data offset before the file"))?;
                at += 4;
            }
            let first_flags = if tf & 0x004 != 0 {
                at += 4;
                Some(be32(b, at - 4)?)
            } else {
                None
            };
            let from = data;
            let mut len = 0u64;
            for i in 0..count {
                let mut field = |present: u32, default: u32| -> Result<u32, Unbuilt> {
                    if tf & present == 0 {
                        return Ok(default);
                    }
                    at += 4;
                    be32(b, at - 4)
                };
                let d = field(0x100, duration)?;
                let z = field(0x200, size)?;
                let f = field(0x400, sample_flags)?;
                let o = field(0x800, 0)? as i32;
                let f = if i == 0 { first_flags.unwrap_or(f) } else { f };
                s.durations.push(d);
                s.sizes.push(z);
                // sample_is_non_sync_sample
                s.sync.push(f & 0x0001_0000 == 0);
                s.offsets.push(o);
                len += z as u64;
            }
            if count > 0 {
                s.chunks.push((from, len, count as u32));
            }
            data = from + len;
        }
    }
    Ok(())
}

fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&((8 + body.len()) as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(body);
    out
}

/// A full box: version, 24 bits of flags (always zero here), then `words` as big-endian u32s.
fn full_box(kind: &[u8; 4], version: u8, words: &[u32]) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + words.len() * 4);
    body.extend_from_slice(&[version, 0, 0, 0]);
    for w in words {
        body.extend_from_slice(&w.to_be_bytes());
    }
    boxed(kind, &body)
}

/// Consecutive equal values as (how many, value) — the shape `stts` and `ctts` store.
fn runs<T: PartialEq + Copy>(values: &[T]) -> Vec<(u32, T)> {
    let mut out: Vec<(u32, T)> = Vec::new();
    for &v in values {
        match out.last_mut() {
            Some((n, last)) if *last == v => *n += 1,
            _ => out.push((1, v)),
        }
    }
    out
}

/// The sample table for `s`, with `stsd` kept from the original and `chunk_offsets` as `stco`.
fn sample_table(stsd: &[u8], s: &Samples, chunk_offsets: &[u32]) -> Vec<u8> {
    let entries = |pairs: Vec<[u32; 2]>| {
        let mut words = vec![pairs.len() as u32];
        words.extend(pairs.into_iter().flatten());
        words
    };
    let mut body = stsd.to_vec();
    body.extend(full_box(
        b"stts",
        0,
        &entries(runs(&s.durations).into_iter().map(|(n, d)| [n, d]).collect()),
    ));
    if s.offsets.iter().any(|o| *o != 0) {
        // Version 1 reads the offsets as signed, which a negative one needs.
        let version = u8::from(s.offsets.iter().any(|o| *o < 0));
        body.extend(full_box(
            b"ctts",
            version,
            &entries(runs(&s.offsets).into_iter().map(|(n, o)| [n, o as u32]).collect()),
        ));
    }
    if s.sync.iter().any(|sync| !sync) {
        let numbers: Vec<u32> = (1..).zip(&s.sync).filter(|(_, sync)| **sync).map(|(n, _)| n).collect();
        let mut words = vec![numbers.len() as u32];
        words.extend(numbers);
        body.extend(full_box(b"stss", 0, &words));
    }
    let mut stsc: Vec<[u32; 3]> = Vec::new();
    for (chunk, (_, _, count)) in (1..).zip(&s.chunks) {
        if stsc.last().is_none_or(|last| last[1] != *count) {
            stsc.push([chunk, *count, 1]);
        }
    }
    let mut words = vec![stsc.len() as u32];
    words.extend(stsc.into_iter().flatten());
    body.extend(full_box(b"stsc", 0, &words));
    let mut words = vec![0, s.sizes.len() as u32];
    words.extend(&s.sizes);
    body.extend(full_box(b"stsz", 0, &words));
    let mut words = vec![chunk_offsets.len() as u32];
    words.extend(chunk_offsets);
    body.extend(full_box(b"stco", 0, &words));
    boxed(b"stbl", &body)
}

/// A copy of `parent` with each child `each` names replaced (an empty replacement drops it) and every
/// other child kept byte for byte.
fn rebuilt(
    b: &[u8],
    parent: Atom,
    each: &dyn Fn(Atom) -> Result<Option<Vec<u8>>, Unbuilt>,
) -> Result<Vec<u8>, Unbuilt> {
    let mut body = Vec::new();
    for a in atoms(b, parent.body, parent.end, false)? {
        match each(a)? {
            Some(bytes) => body.extend(bytes),
            None => body.extend_from_slice(&b[a.start..a.end]),
        }
    }
    Ok(boxed(&parent.kind, &body))
}

/// `mvhd`, `tkhd` or `mdhd` with its duration set to `value`.
fn with_duration(b: &[u8], a: Atom, value: u64) -> Result<Vec<u8>, Unbuilt> {
    let mut out = b[a.start..a.end].to_vec();
    let body = a.body - a.start;
    let v1 = out.get(body) == Some(&1);
    // After version+flags and the creation and modification times: tkhd has its track id and four
    // reserved bytes first, the others their timescale.
    let at = body + if v1 { 20 } else { 12 } + if &a.kind == b"tkhd" { 8 } else { 4 };
    let slot = if v1 { out.get_mut(at..at + 8) } else { out.get_mut(at..at + 4) };
    let slot = slot.ok_or_else(|| unreadable(format!("a {} box ends early", name(&a.kind))))?;
    if v1 {
        slot.copy_from_slice(&value.to_be_bytes());
    } else {
        slot.copy_from_slice(&u32::try_from(value).unwrap_or(u32::MAX).to_be_bytes());
    }
    Ok(out)
}

/// A header box's timescale (`mvhd`, `mdhd`) or track id (`tkhd`): the field after the two times.
fn after_times(b: &[u8], a: Atom) -> Result<u32, Unbuilt> {
    let v1 = b.get(a.body) == Some(&1);
    be32(b, a.body + if v1 { 20 } else { 12 })
}

/// The big-endian `u32` at `at` in the copy of a `kind` box set to `value`.
fn put_u32(b: &mut [u8], at: usize, value: u32, kind: &[u8; 4]) -> Result<(), Unbuilt> {
    let slot = b.get_mut(at..at + 4).ok_or_else(|| unreadable(format!("a {} box ends early", name(kind))))?;
    slot.copy_from_slice(&value.to_be_bytes());
    Ok(())
}

/// One source's track as read: its `moov`, where the boxes this rewrites sit in it, and every sample its
/// fragments carry.
struct Read<'a> {
    moov: &'a [u8],
    root: Atom,
    trak: Atom,
    stsd: Atom,
    movie_scale: u64,
    media_scale: u64,
    samples: Samples,
}

impl Read<'_> {
    fn media_duration(&self) -> u64 {
        self.samples.durations.iter().map(|d| *d as u64).sum()
    }

    /// Where each chunk starts, in this track's own ticks.
    fn chunk_starts(&self) -> Vec<u64> {
        let mut starts = Vec::with_capacity(self.samples.chunks.len());
        let (mut sample, mut ticks) = (0usize, 0u64);
        for &(_, _, count) in &self.samples.chunks {
            starts.push(ticks);
            let next = sample + count as usize;
            ticks += self.samples.durations[sample..next].iter().map(|d| *d as u64).sum::<u64>();
            sample = next;
        }
        starts
    }
}

/// Read the one track of a fragmented file whose `moov` box is `moov` and whose `moof`s (each with its
/// offset in that file) are `moofs`.
fn read_track<'a>(moov: &'a [u8], moofs: &[(u64, Bytes)]) -> Result<Read<'a>, Unbuilt> {
    let root = find(&atoms(moov, 0, moov.len(), false)?, b"moov")?;
    let kids = atoms(moov, root.body, root.end, false)?;
    let mvhd = find(&kids, b"mvhd")?;
    let mvex = find(&kids, b"mvex")?;
    let trex = find(&atoms(moov, mvex.body, mvex.end, false)?, b"trex")?;
    let traks: Vec<Atom> = kids.iter().copied().filter(|a| &a.kind == b"trak").collect();
    let [trak] = traks[..] else {
        return Err(unreadable(format!("{} tracks, where one was expected", traks.len())));
    };
    let trak_kids = atoms(moov, trak.body, trak.end, false)?;
    let tkhd = find(&trak_kids, b"tkhd")?;
    let mdia = find(&trak_kids, b"mdia")?;
    let mdia_kids = atoms(moov, mdia.body, mdia.end, false)?;
    let mdhd = find(&mdia_kids, b"mdhd")?;
    let minf = find(&mdia_kids, b"minf")?;
    let stbl = find(&atoms(moov, minf.body, minf.end, false)?, b"stbl")?;
    let stsd = find(&atoms(moov, stbl.body, stbl.end, false)?, b"stsd")?;

    let track = Track {
        id: after_times(moov, tkhd)?,
        duration: be32(moov, trex.body + 12)?,
        size: be32(moov, trex.body + 16)?,
        flags: be32(moov, trex.body + 20)?,
    };
    let mut samples = Samples::default();
    for (at, bytes) in moofs {
        read_fragment(bytes, *at, &track, &mut samples)?;
    }
    if samples.sizes.is_empty() {
        return Err(unreadable("fragments with no samples"));
    }
    Ok(Read {
        moov,
        root,
        trak,
        stsd,
        movie_scale: after_times(moov, mvhd)? as u64,
        media_scale: after_times(moov, mdhd)? as u64,
        samples,
    })
}

/// `r`'s `trak` as track `id`, with real sample tables whose chunks sit at `offsets`. An `edts` counts in
/// its own movie's timescale, so it is dropped where that is not the one the served file keeps.
fn trak_out(r: &Read, id: u32, movie_scale: u64, offsets: &[u32]) -> Result<Vec<u8>, Unbuilt> {
    let media_duration = r.media_duration();
    let movie_duration = (media_duration * movie_scale).checked_div(r.media_scale).unwrap_or(0);
    let stbl = sample_table(&r.moov[r.stsd.start..r.stsd.end], &r.samples, offsets);
    let moov = r.moov;
    rebuilt(moov, r.trak, &|a| {
        Ok(match &a.kind {
            b"tkhd" => {
                let mut tkhd = with_duration(moov, a, movie_duration)?;
                let v1 = tkhd.get(a.body - a.start) == Some(&1);
                put_u32(&mut tkhd, a.body - a.start + if v1 { 20 } else { 12 }, id, b"tkhd")?;
                Some(tkhd)
            }
            b"edts" if r.movie_scale != movie_scale => Some(Vec::new()),
            b"mdia" => Some(rebuilt(moov, a, &|a| {
                Ok(match &a.kind {
                    b"mdhd" => Some(with_duration(moov, a, media_duration)?),
                    b"minf" => Some(rebuilt(moov, a, &|a| Ok((&a.kind == b"stbl").then(|| stbl.clone())))?),
                    _ => None,
                })
            })?),
            _ => None,
        })
    })
}

/// The whole served header — ftyp, a moov with real sample tables, the mdat header — and where each
/// chunk of media sits, for fragmented files holding one track each: `sources`, each its `moov` box and
/// its `moof`s with their offsets in that file. The first source's movie header is kept, the tracks are
/// numbered in order, and their chunks are interleaved by time, so a player reading along the file never
/// has to jump between far-apart parts of it for picture and sound.
pub(crate) fn layout(sources: &[Source]) -> Result<Layout, Unbuilt> {
    let reads = sources.iter().map(|(moov, moofs)| read_track(moov, moofs)).collect::<Result<Vec<_>, _>>()?;
    let first = reads.first().ok_or_else(|| unreadable("no stream to index"))?;
    let movie_scale = first.movie_scale;
    let movie_duration = reads
        .iter()
        .map(|r| (r.media_duration() * movie_scale).checked_div(r.media_scale).unwrap_or(0))
        .max()
        .unwrap_or(0);

    // Every chunk in time order across the tracks. The sort is stable, so a tie keeps the tracks' order
    // and each track's own chunks stay in theirs.
    let starts: Vec<Vec<u64>> = reads.iter().map(Read::chunk_starts).collect();
    let mut order: Vec<(usize, usize)> =
        starts.iter().enumerate().flat_map(|(t, s)| (0..s.len()).map(move |c| (t, c))).collect();
    order.sort_by(|&(ta, ca), &(tb, cb)| {
        let a = starts[ta][ca] as u128 * reads[tb].media_scale as u128;
        let b = starts[tb][cb] as u128 * reads[ta].media_scale as u128;
        a.cmp(&b)
    });

    let moov_with = |offsets: &[Vec<u32>]| -> Result<Vec<u8>, Unbuilt> {
        let mut traks = Vec::new();
        for (i, r) in reads.iter().enumerate() {
            traks.extend(trak_out(r, i as u32 + 1, movie_scale, &offsets[i])?);
        }
        rebuilt(first.moov, first.root, &|a| {
            Ok(match &a.kind {
                // mvex is what marks a file as fragmented; without it the tables are the whole index.
                b"mvex" => Some(Vec::new()),
                b"mvhd" => {
                    let mut mvhd = with_duration(first.moov, a, movie_duration)?;
                    let v1 = mvhd.get(a.body - a.start) == Some(&1);
                    let next = a.body - a.start + if v1 { 108 } else { 96 };
                    put_u32(&mut mvhd, next, reads.len() as u32 + 1, b"mvhd")?;
                    Some(mvhd)
                }
                b"trak" => Some(traks.clone()),
                _ => None,
            })
        })
    };

    let ftyp = boxed(b"ftyp", b"isom\x00\x00\x02\x00isomiso2avc1mp41");
    // The chunk offsets depend on the moov's length, which does not depend on their values.
    let mut offsets: Vec<Vec<u32>> = reads.iter().map(|r| vec![0; r.samples.chunks.len()]).collect();
    let draft = moov_with(&offsets)?;
    let payload: u64 = reads.iter().flat_map(|r| r.samples.chunks.iter().map(|c| c.1)).sum();
    let mdat_start = (ftyp.len() + draft.len() + 8) as u64;
    if mdat_start + payload > u32::MAX as u64 {
        return Err(unreadable("too large for 32-bit chunk offsets"));
    }
    let mut pieces = Vec::with_capacity(order.len());
    let mut at = mdat_start;
    for (t, c) in order {
        let (from, len, _) = reads[t].samples.chunks[c];
        offsets[t][c] = at as u32;
        if len > 0 {
            pieces.push(Piece { at, source: t, from, len });
        }
        at += len;
    }
    let mut head = ftyp;
    head.extend(moov_with(&offsets)?);
    head.extend_from_slice(&((8 + payload) as u32).to_be_bytes());
    head.extend_from_slice(b"mdat");
    let etag = httputil::etag_of(&head);
    Ok(Layout { total: mdat_start + payload, head: head.into(), pieces, etag })
}

/// What serving `start..=end` of a layout takes: some of the header, then ranges of Google's files.
#[derive(Debug, PartialEq)]
pub(crate) enum Part {
    Inline(Bytes),
    Remote { source: usize, from: u64, to: u64 },
}

pub(crate) fn parts(layout: &Layout, start: u64, end: u64) -> Vec<Part> {
    let mut out = Vec::new();
    let head = layout.head.len() as u64;
    if start < head {
        out.push(Part::Inline(layout.head.slice(start as usize..(end + 1).min(head) as usize)));
    }
    let first = layout.pieces.partition_point(|p| p.at + p.len <= start);
    for p in &layout.pieces[first..] {
        if p.at > end {
            break;
        }
        let (s, e) = (start.max(p.at), end.min(p.at + p.len - 1));
        out.push(Part::Remote { source: p.source, from: p.from + (s - p.at), to: p.from + (e - p.at) });
    }
    out
}

/// Bytes `from..=to` of `url`, which must come back as that range.
async fn fetch(http: &reqwest::Client, url: &str, from: u64, to: u64) -> Result<Bytes, Unbuilt> {
    let fault = |why: String| Unbuilt { why, retry: true };
    let res = http
        .get(url)
        .header(RANGE.as_str(), format!("bytes={from}-{to}"))
        .timeout(INDEX_TIMEOUT)
        .send()
        .await
        .map_err(|e| fault(crate::upstream::body_fault_why(e)))?;
    if res.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(fault(format!("googlevideo answered {} for bytes {from}-{to}", res.status())));
    }
    res.bytes().await.map_err(|e| fault(crate::upstream::body_fault_why(e)))
}

/// The `moof` at the start of the fragment `at`, `size` bytes long.
async fn moof(http: &reqwest::Client, url: &str, at: u64, size: u64) -> Result<(u64, Bytes), Unbuilt> {
    let first = fetch(http, url, at, at + size.min(MOOF_PROBE) - 1).await?;
    let len = be32(&first, 0)? as u64;
    if len <= first.len() as u64 {
        return Ok((at, first));
    }
    if len > size {
        return Err(unreadable("a moof longer than its fragment"));
    }
    Ok((at, fetch(http, url, at, at + len - 1).await?))
}

/// One file's first bytes, where its `moov` sits in them, and its `moof`s with their offsets.
async fn read_source(
    http: &reqwest::Client,
    url: String,
) -> Result<(Bytes, Range<usize>, Vec<(u64, Bytes)>), Unbuilt> {
    let head = fetch(http, &url, 0, HEAD_BYTES - 1).await?;
    let index = index(&head)?;
    // Owned pairs: a closure over borrowed ones is not general enough for a future that must be Send.
    let moofs: Vec<(u64, Bytes)> =
        futures_util::stream::iter(index.fragments.into_iter().map(|(at, size)| moof(http, &url, at, size)))
            .buffered(MOOF_FETCHES)
            .try_collect()
            .await?;
    Ok((head, index.moov, moofs))
}

/// The layout for `urls`: the video stream, and its audio where that was asked for. Both files are read
/// at once.
async fn build(http: &reqwest::Client, urls: &[String]) -> Result<Layout, Unbuilt> {
    let read =
        futures_util::future::try_join_all(urls.iter().cloned().map(|url| read_source(http, url))).await?;
    let sources: Vec<Source> =
        read.iter().map(|(head, moov, moofs)| (&head[moov.clone()], moofs.as_slice())).collect();
    layout(&sources)
}

/// The index for `urls`, kept under `key` until `until`, built once however many ask. The timing is
/// `None` when it was already built.
async fn layout_for(
    state: &Arc<AppState>,
    key: &str,
    urls: &[String],
    until: u64,
) -> (Result<Arc<Layout>, Unbuilt>, Option<Duration>) {
    let now = (state.clock)();
    let source = urls.join("\n");
    let shared = {
        let mut map = state.progressive.lock().unwrap_or_else(|e| e.into_inner());
        match map.get(key) {
            Some((kept, kept_until, shared)) if *kept == source && *kept_until > now => shared.clone(),
            _ => {
                let (st, urls, k, s) = (state.clone(), urls.to_vec(), key.to_string(), source.clone());
                let fut: BoxFuture<Result<Arc<Layout>, Unbuilt>> = Box::pin(async move {
                    let built = build(&st.http, &urls).await.map(Arc::new);
                    // A fetch that failed may work next time; a file that cannot be indexed will not.
                    if matches!(&built, Err(e) if e.retry) {
                        let mut map = st.progressive.lock().unwrap_or_else(|e| e.into_inner());
                        if map.get(&k).is_some_and(|(kept, _, _)| *kept == s) {
                            map.remove(&k);
                        }
                    }
                    built
                });
                let shared = fut.shared();
                if map.len() >= PROGRESSIVE_MAX {
                    map.retain(|_, (_, kept_until, _)| *kept_until > now);
                    if map.len() >= PROGRESSIVE_MAX {
                        map.clear();
                    }
                }
                map.insert(key.to_string(), (source, until, shared.clone()));
                // Driven by a task of its own, so a player that gives up mid-build leaves a finished
                // index rather than a future nobody polls.
                let driver = shared.clone();
                tokio::spawn(async move {
                    let _ = driver.await;
                });
                shared
            }
        }
    };
    let waited = shared.peek().is_none().then(Instant::now);
    let built = shared.await;
    (built, waited.map(|t| t.elapsed()))
}

/// Bytes `from..=to` of `url` down `tx` as they arrive. False once there is no point going on: the
/// player went away, or Google did not send the bytes — in which case the body ends in an error.
async fn relay(
    http: &reqwest::Client,
    url: &str,
    from: u64,
    to: u64,
    tx: &mpsc::Sender<io::Result<Bytes>>,
) -> bool {
    let fault = match http
        .get(url)
        .header(RANGE.as_str(), format!("bytes={from}-{to}"))
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
    {
        Err(e) => crate::upstream::body_fault_why(e),
        Ok(res) if res.status() != reqwest::StatusCode::PARTIAL_CONTENT => {
            format!("googlevideo answered {}", res.status())
        }
        Ok(res) => {
            let mut stream = res.bytes_stream();
            loop {
                match stream.next().await {
                    None => return true,
                    Some(Ok(chunk)) => {
                        if tx.send(Ok(chunk)).await.is_err() {
                            return false;
                        }
                    }
                    Some(Err(e)) => break crate::upstream::body_fault_why(e),
                }
            }
        }
    };
    crate::log_limited("progressive upstream", || {
        format!("progressive: bytes {from}-{to} of a stream: {fault}")
    });
    let _ = tx.send(Err(io::Error::other(fault))).await;
    false
}

/// The response body for `parts`, fetched in order by a task that stops when the player hangs up.
fn body(http: reqwest::Client, urls: Vec<String>, parts: Vec<Part>) -> Body {
    let (tx, mut rx) = mpsc::channel::<io::Result<Bytes>>(4);
    tokio::spawn(async move {
        for part in parts {
            let going = match part {
                Part::Inline(bytes) => tx.send(Ok(bytes)).await.is_ok(),
                Part::Remote { source, from, to } => relay(&http, &urls[source], from, to, &tx).await,
            };
            if !going {
                return;
            }
        }
    });
    let stream = futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx)).map_ok(Frame::data);
    BodyExt::boxed(StreamBody::new(stream))
}

fn serve(
    state: &AppState,
    layout: &Layout,
    urls: &[String],
    headers: &HeaderMap,
    max_age: u64,
) -> Response<Body> {
    let total = layout.total;
    // A range against a different file than the player holds gets the whole file instead.
    let current = match headers.get(IF_RANGE).and_then(|v| v.to_str().ok()) {
        Some(if_range) => httputil::if_range_holds(if_range, &layout.etag, ""),
        None => true,
    };
    let asked = headers.get(RANGE).and_then(|v| v.to_str().ok()).filter(|_| current);
    let (status, start, end) = match httputil::parse_range(asked, total) {
        None => (StatusCode::OK, 0, total - 1),
        Some(RangeReq::Satisfiable { start, end }) => (StatusCode::PARTIAL_CONTENT, start, end),
        Some(RangeReq::Unsatisfiable) => {
            return Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header("content-range", format!("bytes */{total}"))
                .header("cache-control", "no-store")
                .body(httputil::full(""))
                .unwrap()
        }
    };
    let mut out = Response::builder()
        .status(status)
        .header("content-type", "video/mp4")
        .header("content-length", end - start + 1)
        .header("accept-ranges", "bytes")
        .header("etag", layout.etag.as_str())
        .header("cache-control", format!("private, max-age={max_age}"));
    if status == StatusCode::PARTIAL_CONTENT {
        out = out.header("content-range", format!("bytes {start}-{end}/{total}"));
    }
    out.body(body(state.http.clone(), urls.to_vec(), parts(layout, start, end))).unwrap()
}

/// Where `vid`'s served file comes from — its video stream, and its audio when `audio` asks and YouTube
/// answered with one apart — and the key its index is kept under.
fn streams(
    vid: &str,
    cap: Option<u32>,
    direct: &crate::direct::Direct,
    audio: bool,
) -> (String, Vec<String>) {
    let key = crate::direct::key(vid, cap);
    match (&direct.audio, audio) {
        (Some(sound), true) => (format!("{key}+audio"), vec![direct.video.clone(), sound.clone()]),
        _ => (key, vec![direct.video.clone()]),
    }
}

/// Build a stream's index ahead of the request that will play it, so that request costs a lookup rather
/// than a round of range requests. `direct` is the resolve it is built from.
pub(crate) async fn prepare(
    state: &Arc<AppState>,
    vid: &str,
    cap: Option<u32>,
    direct: &crate::direct::Direct,
    audio: bool,
) -> Result<(), Unbuilt> {
    let until = direct.expires.saturating_sub(crate::direct::EXPIRY_MARGIN_MS);
    let (key, urls) = streams(vid, cap, direct, audio);
    layout_for(state, &key, &urls, until).await.0.map(|_| ())
}

/// `prepare`, as `/meta?prewarm=progressive` asks for it: nobody is waiting, so a failure is only said.
pub(crate) async fn warm(
    state: &Arc<AppState>,
    vid: &str,
    cap: Option<u32>,
    direct: &crate::direct::Direct,
    audio: bool,
) {
    if let Err(e) = prepare(state, vid, cap, direct, audio).await {
        crate::log_limited("progressive warm", || format!("[{vid}] no index built ahead ({})", e.why));
    }
}

/// `/progressive/<vid>.mp4`: the stream `/direct` would name, with its index first, and its sound with it
/// when `audio` asks.
pub async fn handle_progressive(
    state: Arc<AppState>,
    headers: &HeaderMap,
    vid: String,
    cap: Option<u32>,
    audio: bool,
) -> Response<Body> {
    let (answer, spent) = crate::direct::answer(&state, &vid, cap).await;
    let mut timing = match spent {
        Some(d) => httputil::timing("resolve", d),
        None => "cache;desc=hit".to_string(),
    };
    let direct = match answer {
        Ok(d) => d,
        Err(e) => return httputil::timed(crate::play::play_error(&state, &vid, &e), &timing),
    };
    let now = (state.clock)();
    let until = direct.expires.saturating_sub(crate::direct::EXPIRY_MARGIN_MS);
    let (key, urls) = streams(&vid, cap, &direct, audio);
    let (built, spent) = layout_for(&state, &key, &urls, until).await;
    if let Some(d) = spent {
        timing.push_str(", ");
        timing.push_str(&httputil::timing("index", d));
    }
    let resp = match built {
        Ok(layout) => serve(&state, &layout, &urls, headers, until.saturating_sub(now) / 1000),
        // Google's raw video URL has no sound, so where sound was asked for that is not an answer.
        Err(e) if urls.len() > 1 => {
            crate::log_limited("progressive audio", || format!("[{vid}] no index with sound ({})", e.why));
            let mut resp = httputil::error(
                StatusCode::BAD_GATEWAY,
                "progressive_unavailable",
                "Could not index this trailer with its sound.",
            );
            resp.headers_mut().insert("x-den-degraded", HeaderValue::from_static("progressive_unavailable"));
            resp
        }
        Err(e) => {
            let reason = if e.retry { "progressive fetch" } else { "progressive layout" };
            crate::log_limited(reason, || format!("[{vid}] no index ({}); sending the raw stream", e.why));
            Response::builder()
                .status(StatusCode::FOUND)
                .header("location", direct.video.as_str())
                .header("x-den-degraded", "progressive_unavailable")
                .header("cache-control", "no-store")
                .body(httputil::full(""))
                .unwrap_or_else(|_| {
                    httputil::error(
                        StatusCode::BAD_GATEWAY,
                        "upstream_unreadable",
                        "Could not index this stream.",
                    )
                })
        }
    };
    httputil::timed(resp, &timing)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(ws: &[u32]) -> Vec<u8> {
        ws.iter().flat_map(|w| w.to_be_bytes()).collect()
    }

    fn make(kind: &[u8; 4], parts: &[&[u8]]) -> Vec<u8> {
        boxed(kind, &parts.concat())
    }

    /// The byte every sample `i` of fragment `f` of file `mark` is filled with, so a misplaced byte shows.
    fn fill(mark: usize, f: usize, i: usize) -> u8 {
        (mark * 64 + f * 16 + i + 1) as u8
    }

    /// A file shaped like YouTube's adaptive streams: a moov with empty sample tables and an mvex, a sidx,
    /// then one moof+mdat per entry of `fragments` (each a list of sample sizes, each sample 1001 of 24000
    /// long), its samples filled by `mark`. Only the first sample of a fragment is a sync sample, as trex's
    /// default flags say.
    fn fragmented(fragments: &[&[u32]], mark: usize) -> Vec<u8> {
        let stbl = make(
            b"stbl",
            &[
                &make(b"stsd", &[&words(&[0, 1]), &make(b"avc1", &[&[0u8; 78]])]),
                &make(b"stts", &[&words(&[0, 0])]),
                &make(b"stsc", &[&words(&[0, 0])]),
                &make(b"stsz", &[&words(&[0, 0, 0])]),
                &make(b"stco", &[&words(&[0, 0])]),
            ],
        );
        let minf = make(b"minf", &[&make(b"vmhd", &[&words(&[1, 0, 0])]), &make(b"dinf", &[]), &stbl]);
        let mdia = make(
            b"mdia",
            &[
                &make(b"mdhd", &[&words(&[0, 0, 0, 24000, 0, 0])]),
                &make(b"hdlr", &[&words(&[0, 0]), b"vide", &[0u8; 13]]),
                &minf,
            ],
        );
        let trak = make(b"trak", &[&make(b"tkhd", &[&words(&[0, 0, 0, 1, 0, 0]), &[0u8; 60]]), &mdia]);
        let trex = make(b"trex", &[&words(&[0, 1, 1, 1001, 0, 0x0001_0000])]);
        let moov = make(
            b"moov",
            &[&make(b"mvhd", &[&words(&[0, 0, 0, 1000, 0]), &[0u8; 80]]), &make(b"mvex", &[&trex]), &trak],
        );

        let mut pairs = Vec::new();
        for (f, sizes) in fragments.iter().enumerate() {
            let moof_with = |data_offset: u32| {
                let mut trun = words(&[0x0205, sizes.len() as u32, data_offset, 0]);
                trun.extend(words(sizes));
                make(
                    b"moof",
                    &[
                        &make(b"mfhd", &[&words(&[0, f as u32 + 1])]),
                        &make(
                            b"traf",
                            &[
                                &make(b"tfhd", &[&words(&[0x0002_0000, 1])]),
                                &make(b"tfdt", &[&words(&[0, 0])]),
                                &make(b"trun", &[&trun]),
                            ],
                        ),
                    ],
                )
            };
            let moof = moof_with(moof_with(0).len() as u32 + 8);
            let data: Vec<u8> = sizes
                .iter()
                .enumerate()
                .flat_map(|(i, &z)| std::iter::repeat_n(fill(mark, f, i), z as usize))
                .collect();
            pairs.push([moof, make(b"mdat", &[&data])].concat());
        }
        let mut sidx = words(&[0, 1, 24000, 0, 0]);
        sidx.extend([0, 0, 0, pairs.len() as u8]);
        for pair in &pairs {
            sidx.extend(words(&[pair.len() as u32, 5 * 24000, 0x9000_0000]));
        }
        [make(b"ftyp", &[b"dash\0\0\0\0iso6mp41"]), moov, make(b"sidx", &[&sidx]), pairs.concat()].concat()
    }

    /// The layout for whole files held in memory, their moofs read as `build` would fetch them.
    fn layout_of(files: &[&[u8]]) -> Result<Layout, Unbuilt> {
        let mut read = Vec::new();
        for file in files {
            let index = index(file)?;
            let moofs: Vec<(u64, Bytes)> = index
                .fragments
                .iter()
                .map(|&(at, size)| (at, Bytes::copy_from_slice(&file[at as usize..(at + size) as usize])))
                .collect();
            read.push((&file[index.moov], moofs));
        }
        let sources: Vec<Source> = read.iter().map(|(moov, moofs)| (*moov, &moofs[..])).collect();
        layout(&sources)
    }

    /// What a player receives for `start..=end`, with the remote parts read from `files`.
    fn served(files: &[&[u8]], l: &Layout, start: u64, end: u64) -> Vec<u8> {
        let mut out = Vec::new();
        for part in parts(l, start, end) {
            match part {
                Part::Inline(bytes) => out.extend_from_slice(&bytes),
                Part::Remote { source, from, to } => {
                    out.extend_from_slice(&files[source][from as usize..=to as usize])
                }
            }
        }
        out
    }

    /// The payload of the box at `path` under the top level of `b`.
    fn payload<'a>(b: &'a [u8], path: &[&[u8; 4]]) -> &'a [u8] {
        let (mut from, mut to) = (0, b.len());
        for kind in path {
            let a = find(&atoms(b, from, to, false).unwrap(), kind).unwrap();
            (from, to) = (a.body, a.end);
        }
        &b[from..to]
    }

    fn table(b: &[u8], path: &[&[u8; 4]], skip: usize) -> Vec<u32> {
        payload(b, path)[4 + skip..].chunks(4).map(|c| u32::from_be_bytes(c.try_into().unwrap())).collect()
    }

    /// The whole point: one moov with every sample in it, no mvex, and chunk offsets that land on the
    /// sample bytes Google's fragments carried.
    #[test]
    fn a_fragmented_file_is_served_with_its_index_first() {
        let file = fragmented(&[&[5, 3, 4], &[6, 2]], 0);
        let l = layout_of(&[&file]).expect("a layout");
        let out = served(&[&file], &l, 0, l.total - 1);
        assert_eq!(out.len() as u64, l.total);

        let top: Vec<[u8; 4]> = atoms(&out, 0, out.len(), false).unwrap().iter().map(|a| a.kind).collect();
        assert_eq!(top, [*b"ftyp", *b"moov", *b"mdat"], "the index must come before the media");
        let moov = payload(&out, &[b"moov"]);
        assert!(!moov.windows(4).any(|w| w == b"mvex"), "a moov still marked fragmented");

        let stbl = [b"moov", b"trak", b"mdia", b"minf", b"stbl"];
        let at = |kind| [&stbl[..], &[kind]].concat();
        assert_eq!(table(&out, &at(b"stsz"), 4), [5, 5, 3, 4, 6, 2]);
        assert_eq!(table(&out, &at(b"stts"), 0), [1, 5, 1001]);
        assert_eq!(table(&out, &at(b"stss"), 0), [2, 1, 4]);
        assert_eq!(table(&out, &at(b"stsc"), 0), [2, 1, 3, 1, 2, 2, 1]);
        let chunks = table(&out, &at(b"stco"), 0);
        assert_eq!(chunks[0], 2);
        assert_eq!(out[chunks[1] as usize], fill(0, 0, 0));
        assert_eq!(out[chunks[2] as usize], fill(0, 1, 0));
        assert_eq!(out[chunks[2] as usize + 6], fill(0, 1, 1));
        assert_eq!(table(&out, &[b"moov", b"trak", b"mdia", b"mdhd"], 12)[0], 5 * 1001, "mdhd duration");
        assert!(!payload(&out, &[b"moov", b"trak", b"mdia", b"minf", b"stbl"])
            .windows(4)
            .any(|w| w == b"ctts"));
    }

    /// A player's range can start in the header and end part-way into the media, and each piece of it
    /// maps onto just the bytes of Google's file that it covers.
    #[test]
    fn a_range_maps_onto_the_header_and_the_fragments_it_covers() {
        let file = fragmented(&[&[5, 3, 4], &[6, 2]], 0);
        let l = layout_of(&[&file]).unwrap();
        let whole = served(&[&file], &l, 0, l.total - 1);
        let (first, second) = (l.pieces[0], l.pieces[1]);
        let (start, end) = (l.head.len() as u64 - 2, second.at);
        assert_eq!(
            parts(&l, start, end),
            [
                Part::Inline(l.head.slice(l.head.len() - 2..)),
                Part::Remote { source: 0, from: first.from, to: first.from + first.len - 1 },
                Part::Remote { source: 0, from: second.from, to: second.from },
            ]
        );
        for (start, end) in
            [(0, 0), (3, first.at + 1), (first.at + 2, first.at + 4), (second.at + 1, l.total - 1)]
        {
            assert_eq!(
                served(&[&file], &l, start, end),
                whole[start as usize..=end as usize],
                "{start}-{end}"
            );
        }
    }

    /// With sound, the served file carries both tracks, numbered 1 and 2 and each with tables pointing at
    /// its own bytes, and their chunks alternate by time rather than all the picture before all the sound.
    #[test]
    fn a_video_and_its_audio_are_served_as_one_file_interleaved_by_time() {
        // The video's fragments start at 0 and 3003 of 24000, the audio's at 0, 2002 and 4004.
        let video = fragmented(&[&[5, 3, 4], &[6, 2, 1]], 0);
        let audio = fragmented(&[&[2, 2], &[2, 2], &[2, 2]], 1);
        let l = layout_of(&[&video, &audio]).expect("a layout");
        let sources: Vec<usize> = l.pieces.iter().map(|p| p.source).collect();
        assert_eq!(sources, [0, 1, 1, 0, 1], "chunks in time order, the picture first on a tie");
        let out = served(&[&video, &audio], &l, 0, l.total - 1);
        assert_eq!(out.len() as u64, l.total);

        let moov = find(&atoms(&out, 0, out.len(), false).unwrap(), b"moov").unwrap();
        let kids = atoms(&out, moov.body, moov.end, false).unwrap();
        assert_eq!(be32(&out, find(&kids, b"mvhd").unwrap().body + 96).unwrap(), 3, "next_track_ID");
        let traks: Vec<Atom> = kids.iter().copied().filter(|a| &a.kind == b"trak").collect();
        assert_eq!(traks.len(), 2);
        for (i, trak) in traks.iter().enumerate() {
            let tkhd = find(&atoms(&out, trak.body, trak.end, false).unwrap(), b"tkhd").unwrap();
            assert_eq!(be32(&out, tkhd.body + 12).unwrap(), i as u32 + 1, "track id");
            let stco = payload(&out[trak.body..trak.end], &[b"mdia", b"minf", b"stbl", b"stco"]);
            for (f, at) in stco[8..].chunks(4).map(|c| u32::from_be_bytes(c.try_into().unwrap())).enumerate()
            {
                assert_eq!(out[at as usize], fill(i, f, 0), "track {} chunk {f}", i + 1);
            }
        }
    }

    /// A progressive file already has its index; nothing here should pretend to rebuild one.
    #[test]
    fn a_file_that_is_not_fragmented_is_left_to_google() {
        let file =
            [make(b"ftyp", &[b"isom\0\0\0\0"]), make(b"moov", &[]), make(b"mdat", &[&[1, 2, 3]])].concat();
        let Err(e) = index(&file) else { panic!("indexed a file with no sidx") };
        assert!(!e.retry, "a file that cannot be indexed is not worth asking again");
    }

    /// Against real googlevideo URLs: `REEL_PROGRESSIVE_URL=<video url> REEL_PROGRESSIVE_OUT=<path>`, with
    /// `REEL_PROGRESSIVE_AUDIO_URL=<audio url>` for both tracks, writes the served file for `ffprobe` or a
    /// browser to judge.
    #[tokio::test]
    #[ignore]
    async fn a_real_stream_is_served_whole() {
        let mut urls = vec![std::env::var("REEL_PROGRESSIVE_URL").expect("REEL_PROGRESSIVE_URL")];
        urls.extend(std::env::var("REEL_PROGRESSIVE_AUDIO_URL").ok());
        let out = std::env::var("REEL_PROGRESSIVE_OUT").expect("REEL_PROGRESSIVE_OUT");
        let http = reqwest::Client::new();
        let started = Instant::now();
        let l = build(&http, &urls).await.expect("a layout");
        eprintln!("indexed {} pieces in {:?}; {} bytes", l.pieces.len(), started.elapsed(), l.total);
        let bytes = body(http, urls, parts(&l, 0, l.total - 1)).collect().await.expect("the body").to_bytes();
        assert_eq!(bytes.len() as u64, l.total);
        std::fs::write(out, &bytes).unwrap();
    }
}
