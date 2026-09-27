//! Bounds the public direct-media data plane independently of request-task count.
//!
//! A permit belongs to the response body, not just the handler future: returning headers does not
//! release the upstream socket/FD or the buffers behind it. Dropping the client response releases
//! the permit immediately. Idle and absolute deadlines also drop the inner body, cancelling an HLS
//! upstream response and the progressive producer that owns its range fetches.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Frame;
use hyper::{Response, StatusCode};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::httputil::{self, Body};

pub struct MediaGate {
    permits: Arc<Semaphore>,
    limit: usize,
    idle: Duration,
    lifetime: Duration,
    active: AtomicU64,
    high_water: AtomicU64,
    refused: AtomicU64,
}

pub struct Lease {
    gate: Arc<MediaGate>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.gate.active.fetch_sub(1, Relaxed);
    }
}

impl MediaGate {
    pub fn new(limit: usize, idle: Duration, lifetime: Duration) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit)),
            limit,
            idle,
            lifetime,
            active: AtomicU64::new(0),
            high_water: AtomicU64::new(0),
            refused: AtomicU64::new(0),
        }
    }

    pub fn try_enter(self: &Arc<Self>) -> Option<Lease> {
        match self.permits.clone().try_acquire_owned() {
            Ok(permit) => {
                let active = self.active.fetch_add(1, Relaxed) + 1;
                self.high_water.fetch_max(active, Relaxed);
                Some(Lease { gate: self.clone(), _permit: permit })
            }
            Err(_) => {
                self.refused.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// Keep `lease` for the lifetime of a successful media body. Short errors and redirects do not
    /// own a media stream and release immediately.
    pub fn guard(&self, response: Response<Body>, lease: Lease) -> Response<Body> {
        if !matches!(response.status(), StatusCode::OK | StatusCode::PARTIAL_CONTENT) {
            return response;
        }
        let (parts, inner) = response.into_parts();
        let hint = hyper::body::Body::size_hint(&inner);
        // One frame may wait for Hyper and one may be in hand from upstream. That is enough read-ahead
        // to keep the socket moving without letting a backpressured client turn this into a body-sized
        // buffer. Crucially, the task owns both `inner` and `lease`: its clocks keep advancing when
        // Hyper never polls the returned body at all.
        let (tx, rx) = mpsc::channel(1);
        let (start, started_body) = oneshot::channel();
        let (idle, lifetime) = (self.idle, self.lifetime);
        let started = Instant::now();
        let task = tokio::spawn(async move {
            pump(inner, tx, lease, started_body, started + idle, started + lifetime, idle).await
        });
        let body = PumpBody { rx, task, hint, start: Some(start) };
        Response::from_parts(parts, body.boxed())
    }

    pub fn active(&self) -> u64 {
        self.active.load(Relaxed)
    }
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Relaxed)
    }
    pub fn refused(&self) -> u64 {
        self.refused.load(Relaxed)
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
}

async fn pump(
    mut inner: Body,
    tx: mpsc::Sender<Result<Frame<Bytes>, io::Error>>,
    lease: Lease,
    mut start: oneshot::Receiver<()>,
    idle_at: Instant,
    lifetime_at: Instant,
    idle: Duration,
) {
    // Preserve the old lazy-body contract: HEAD/304, or a response dropped before Hyper asks for its
    // first frame, opens no upstream range. The task still owns and times the lease while it waits.
    let began = tokio::select! {
        _ = tokio::time::sleep_until(lifetime_at) => {
            timed_out(&tx, "direct media response lifetime exceeded");
            false
        }
        _ = tokio::time::sleep_until(idle_at) => {
            timed_out(&tx, "direct media response idle timeout");
            false
        }
        began = &mut start => began.is_ok(),
    };
    if began {
        pump_inner(&mut inner, &tx, idle_at, lifetime_at, idle).await;
    }
    // Close/release in this order. A consumer observing EOF has then already stopped the upstream
    // body and returned the scarce permit, rather than racing the task's local-destructor order.
    drop(inner);
    drop(lease);
    drop(tx);
}

async fn pump_inner(
    inner: &mut Body,
    tx: &mpsc::Sender<Result<Frame<Bytes>, io::Error>>,
    mut idle_at: Instant,
    lifetime_at: Instant,
    idle: Duration,
) {
    loop {
        let frame = tokio::select! {
            _ = tokio::time::sleep_until(lifetime_at) => {
                timed_out(tx, "direct media response lifetime exceeded");
                return;
            }
            _ = tokio::time::sleep_until(idle_at) => {
                timed_out(tx, "direct media response idle timeout");
                return;
            }
            frame = inner.frame() => frame,
        };
        let Some(frame) = frame else { return };
        let item = frame;
        let sent = tokio::select! {
            _ = tokio::time::sleep_until(lifetime_at) => {
                timed_out(tx, "direct media response lifetime exceeded");
                return;
            }
            _ = tokio::time::sleep_until(idle_at) => {
                timed_out(tx, "direct media response idle timeout");
                return;
            }
            sent = tx.send(item) => sent,
        };
        if sent.is_err() {
            return;
        }
        idle_at = Instant::now() + idle;
    }
}

fn timed_out(tx: &mpsc::Sender<Result<Frame<Bytes>, io::Error>>, why: &'static str) {
    // If backpressure has filled the one-frame queue there is nowhere to put the error, but closing
    // it still cuts the response short and, more importantly, dropping the task releases the lease
    // and upstream body. A reader that is keeping up receives the useful timeout error.
    let _ = tx.try_send(Err(io::Error::new(io::ErrorKind::TimedOut, why)));
}

struct PumpBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, io::Error>>,
    task: tokio::task::JoinHandle<()>,
    hint: hyper::body::SizeHint,
    start: Option<oneshot::Sender<()>>,
}

impl hyper::body::Body for PumpBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if let Some(start) = self.start.take() {
            let _ = start.send(());
        }
        self.rx.poll_recv(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.rx.is_closed() && self.rx.is_empty()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.hint
    }
}

impl Drop for PumpBody {
    fn drop(&mut self) {
        // Dropping a JoinHandle detaches it. Abort instead so dropping the client response immediately
        // drops the task-owned upstream body and lease rather than waiting for either deadline.
        self.task.abort();
    }
}

pub fn overloaded() -> Response<Body> {
    let mut response = httputil::error(
        StatusCode::SERVICE_UNAVAILABLE,
        "media_busy",
        "This Reel instance is serving its direct-media limit; retry shortly.",
    );
    response.headers_mut().insert("retry-after", hyper::header::HeaderValue::from_static("1"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full, StreamBody};

    fn response(body: &'static str) -> Response<Body> {
        Response::new(Full::new(Bytes::from_static(body.as_bytes())).map_err(|never| match never {}).boxed())
    }

    #[tokio::test]
    async fn permit_lives_until_body_finishes_or_is_dropped() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(30), Duration::from_secs(60)));
        let guarded = gate.guard(response("one"), gate.try_enter().unwrap());
        assert_eq!(gate.active(), 1);
        assert!(gate.try_enter().is_none());
        assert_eq!(gate.refused(), 1);
        drop(guarded);
        tokio::task::yield_now().await;
        assert_eq!(gate.active(), 0);

        let guarded = gate.guard(response("two"), gate.try_enter().unwrap());
        let _ = guarded.into_body().collect().await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(gate.active(), 0);
        assert_eq!(gate.high_water(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_cancels_and_releases() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(5), Duration::from_secs(60)));
        let pending = futures_util::stream::pending::<Result<Frame<Bytes>, io::Error>>();
        let response = Response::new(StreamBody::new(pending).boxed());
        let body = gate.guard(response, gate.try_enter().unwrap()).into_body();
        let read = tokio::spawn(async move { body.collect().await });
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(read.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(gate.active(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_releases_an_unpolled_body() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(5), Duration::from_secs(60)));
        let pending = futures_util::stream::pending::<Result<Frame<Bytes>, io::Error>>();
        let response = Response::new(StreamBody::new(pending).boxed());
        let _unpolled = gate.guard(response, gate.try_enter().unwrap());
        assert_eq!(gate.active(), 1);

        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        assert_eq!(gate.active(), 0, "a body Hyper never polled kept its permit past the idle deadline");
        assert!(gate.try_enter().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn lifetime_releases_an_unpolled_body_before_its_longer_idle_limit() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(60), Duration::from_secs(5)));
        let pending = futures_util::stream::pending::<Result<Frame<Bytes>, io::Error>>();
        let response = Response::new(StreamBody::new(pending).boxed());
        let _unpolled = gate.guard(response, gate.try_enter().unwrap());

        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        assert_eq!(gate.active(), 0, "an unpolled body outlived its absolute lifetime");
    }

    #[tokio::test(start_paused = true)]
    async fn lifetime_timeout_cancels_even_when_the_body_is_not_idle() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(60), Duration::from_secs(5)));
        let pending = futures_util::stream::pending::<Result<Frame<Bytes>, io::Error>>();
        let response = Response::new(StreamBody::new(pending).boxed());
        let body = gate.guard(response, gate.try_enter().unwrap()).into_body();
        let read = tokio::spawn(async move { body.collect().await });
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(read.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(gate.active(), 0);
    }

    #[tokio::test]
    async fn a_partial_range_keeps_its_contract_while_guarded() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(30), Duration::from_secs(60)));
        let response = Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", "bytes 10-12/100")
            .header("accept-ranges", "bytes")
            .body(httputil::full("abc"))
            .unwrap();
        let response = gate.guard(response, gate.try_enter().unwrap());
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-range"], "bytes 10-12/100");
        assert_eq!(response.into_body().collect().await.unwrap().to_bytes(), "abc");
        assert_eq!(gate.active(), 0);
    }
}
