//! Host-wide backoff: when a remote tells this box to slow down, stop asking it for a while.
//!
//! A per-id or per-title failure cache cannot do this. When YouTube throttles the box, or TMDB
//! answers 429, the refusal is about US, not about the video or title we asked for — so every new
//! id went straight back to the host that had just refused, and a burst of browsing kept a throttle
//! alive that would otherwise have lifted.

use std::hash::BuildHasher;
use std::sync::Mutex;

pub struct Backoff {
    /// What the log line calls this host.
    name: &'static str,
    base_ms: u64,
    cap_ms: u64,
    /// (paused until, epoch ms — 0 is "not paused"; consecutive trips since the last answer)
    state: Mutex<(u64, u32)>,
}

impl Backoff {
    pub fn new(name: &'static str, base_ms: u64, cap_ms: u64) -> Backoff {
        Backoff { name, base_ms, cap_ms, state: Mutex::new((0, 0)) }
    }

    /// What is left of the pause, or `None` when the host may be asked.
    pub fn remaining_ms(&self, now: u64) -> Option<u64> {
        let (until, _) = *self.state.lock().unwrap_or_else(|e| e.into_inner());
        (until > now).then(|| until - now)
    }

    /// The host refused us: pause for `hint_ms` when it said how long, otherwise for the next
    /// exponential window. Returns what is left of the pause.
    ///
    /// A trip while already paused is the same episode — a request that left before the pause
    /// reporting back — so it adds no strike and writes no line; a longer hint still extends it.
    /// Otherwise one burst of in-flight requests would double the window once per request.
    pub fn trip(&self, now: u64, hint_ms: Option<u64>, why: &str) -> u64 {
        let (ms, strikes) = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if s.0 > now {
                if let Some(h) = hint_ms.filter(|h| now + h > s.0) {
                    s.0 = now + h;
                }
                return s.0 - now;
            }
            s.1 = s.1.saturating_add(1);
            let ms = hint_ms.unwrap_or_else(|| {
                window_ms(self.base_ms, self.cap_ms, s.1, std::hash::RandomState::new().hash_one(now) % 200)
            });
            s.0 = now + ms;
            (ms, s.1)
        };
        // Written after the lock is released: stderr is a pipe someone else drains.
        eprintln!("{}: {why} — pausing requests for {}s (strike {strikes})", self.name, ms.div_ceil(1000));
        ms
    }

    /// The host answered: end any pause and forget the strikes. Says so only when there was a pause
    /// to end, so the log carries the transition and not every success.
    pub fn clear(&self) {
        let had = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *s).1
        };
        if had > 0 {
            eprintln!("{}: answering again — pause lifted", self.name);
        }
    }
}

/// The pause for the `strikes`-th consecutive trip: `base` doubling per strike, plus `jitter_permille`
/// thousandths of itself (0..200, so up to a fifth), never past `cap`. The jitter keeps a pause from
/// ending on the same instant as the one before it, which a remote counting per window would see as
/// the same burst arriving on schedule.
pub fn window_ms(base: u64, cap: u64, strikes: u32, jitter_permille: u64) -> u64 {
    let exp = base.saturating_mul(1u64 << strikes.saturating_sub(1).min(20)).min(cap);
    (exp + exp * jitter_permille / 1000).min(cap)
}
