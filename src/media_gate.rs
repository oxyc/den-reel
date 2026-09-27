//! Bounds the public direct-media data plane independently of request-task count.
//!
//! A permit belongs to the response body, not just the handler future: returning headers does not
//! release the upstream socket/FD or the buffers behind it. Dropping the client response releases
//! the permit immediately. Idle and absolute deadlines also drop the inner body, cancelling an HLS
//! upstream response and the progressive producer that owns its range fetches.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
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
    idle_timeouts: AtomicU64,
    lifetime_timeouts: AtomicU64,
    cancellations: Arc<AtomicU64>,
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
            idle_timeouts: AtomicU64::new(0),
            lifetime_timeouts: AtomicU64::new(0),
            cancellations: Arc::new(AtomicU64::new(0)),
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
        let (terminal_tx, terminal) = oneshot::channel();
        let (start, started_body) = oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let task_completed = completed.clone();
        let (idle, lifetime) = (self.idle, self.lifetime);
        let started = Instant::now();
        let task = tokio::spawn(async move {
            let control = PumpControl {
                terminal_tx,
                lease,
                start: started_body,
                idle_at: started + idle,
                lifetime_at: started + lifetime,
                idle,
                completed: task_completed,
            };
            pump(inner, tx, control).await
        });
        let body = PumpBody {
            rx,
            terminal,
            task,
            hint,
            start: Some(start),
            finished: false,
            completed,
            cancellations: self.cancellations.clone(),
        };
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
    pub fn idle_timeouts(&self) -> u64 {
        self.idle_timeouts.load(Relaxed)
    }
    pub fn lifetime_timeouts(&self) -> u64 {
        self.lifetime_timeouts.load(Relaxed)
    }
    pub fn cancellations(&self) -> u64 {
        self.cancellations.load(Relaxed)
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
}

struct PumpControl {
    terminal_tx: oneshot::Sender<Option<io::Error>>,
    lease: Lease,
    start: oneshot::Receiver<()>,
    idle_at: Instant,
    lifetime_at: Instant,
    idle: Duration,
    completed: Arc<AtomicBool>,
}

async fn pump(mut inner: Body, tx: mpsc::Sender<Frame<Bytes>>, control: PumpControl) {
    let PumpControl { terminal_tx, lease, mut start, idle_at, lifetime_at, idle, completed } = control;
    // Preserve the old lazy-body contract: HEAD/304, or a response dropped before Hyper asks for its
    // first frame, opens no upstream range. The task still owns and times the lease while it waits.
    let end = tokio::select! {
        _ = tokio::time::sleep_until(lifetime_at) => {
            End::Lifetime
        }
        _ = tokio::time::sleep_until(idle_at) => {
            End::Idle
        }
        began = &mut start => {
            if began.is_ok() {
                pump_inner(&mut inner, &tx, idle_at, lifetime_at, idle).await
            } else {
                End::Clean
            }
        },
    };
    match &end {
        End::Idle => {
            lease.gate.idle_timeouts.fetch_add(1, Relaxed);
        }
        End::Lifetime => {
            lease.gate.lifetime_timeouts.fetch_add(1, Relaxed);
        }
        End::Clean | End::Error(_) | End::ConsumerGone => {}
    }
    // Close/release in this order. A consumer observing EOF has then already stopped the upstream
    // body and returned the scarce permit, rather than racing the task's local-destructor order.
    drop(inner);
    drop(lease);
    drop(tx);
    completed.store(true, std::sync::atomic::Ordering::Release);
    let error = match end {
        End::Idle => Some(io::Error::new(io::ErrorKind::TimedOut, "direct media response idle timeout")),
        End::Lifetime => {
            Some(io::Error::new(io::ErrorKind::TimedOut, "direct media response lifetime exceeded"))
        }
        End::Error(error) => Some(error),
        End::Clean | End::ConsumerGone => None,
    };
    let _ = terminal_tx.send(error);
}

enum End {
    Clean,
    Error(io::Error),
    ConsumerGone,
    Idle,
    Lifetime,
}

async fn pump_inner(
    inner: &mut Body,
    tx: &mpsc::Sender<Frame<Bytes>>,
    mut idle_at: Instant,
    lifetime_at: Instant,
    idle: Duration,
) -> End {
    loop {
        let frame = tokio::select! {
            _ = tokio::time::sleep_until(lifetime_at) => {
                return End::Lifetime;
            }
            _ = tokio::time::sleep_until(idle_at) => {
                return End::Idle;
            }
            frame = inner.frame() => frame,
        };
        let Some(frame) = frame else { return End::Clean };
        let item = match frame {
            Ok(frame) => frame,
            Err(error) => return End::Error(error),
        };
        let sent = tokio::select! {
            _ = tokio::time::sleep_until(lifetime_at) => {
                return End::Lifetime;
            }
            _ = tokio::time::sleep_until(idle_at) => {
                return End::Idle;
            }
            sent = tx.send(item) => sent,
        };
        if sent.is_err() {
            return End::ConsumerGone;
        }
        idle_at = Instant::now() + idle;
    }
}

struct PumpBody {
    rx: mpsc::Receiver<Frame<Bytes>>,
    terminal: oneshot::Receiver<Option<io::Error>>,
    task: tokio::task::JoinHandle<()>,
    hint: hyper::body::SizeHint,
    start: Option<oneshot::Sender<()>>,
    finished: bool,
    completed: Arc<AtomicBool>,
    cancellations: Arc<AtomicU64>,
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
        if self.finished {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(frame)) => Poll::Ready(Some(Ok(frame))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => match Pin::new(&mut self.terminal).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(Some(error))) => {
                    self.finished = true;
                    Poll::Ready(Some(Err(error)))
                }
                Poll::Ready(Ok(None) | Err(_)) => {
                    self.finished = true;
                    Poll::Ready(None)
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished && self.rx.is_empty()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.hint
    }
}

impl Drop for PumpBody {
    fn drop(&mut self) {
        // Dropping a JoinHandle detaches it. Abort instead so dropping the client response immediately
        // drops the task-owned upstream body and lease rather than waiting for either deadline.
        if !self.finished && !self.completed.load(std::sync::atomic::Ordering::Acquire) {
            self.cancellations.fetch_add(1, Relaxed);
        }
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
        assert_eq!(gate.cancellations(), 1);

        let guarded = gate.guard(response("two"), gate.try_enter().unwrap());
        let _ = guarded.into_body().collect().await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(gate.active(), 0);
        assert_eq!(gate.high_water(), 1);
        assert_eq!(gate.cancellations(), 1, "a fully drained response was not cancelled");
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
        assert_eq!(gate.idle_timeouts(), 1);
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
        assert_eq!(gate.lifetime_timeouts(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn backpressured_timeout_follows_queued_data_with_exactly_one_error() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(5), Duration::from_secs(60)));
        let frames = futures_util::StreamExt::chain(
            futures_util::stream::iter([
                Ok::<_, io::Error>(Frame::data(Bytes::from_static(b"one"))),
                Ok(Frame::data(Bytes::from_static(b"two"))),
            ]),
            futures_util::stream::pending(),
        );
        let response = Response::new(StreamBody::new(frames).boxed());
        let mut body = Box::pin(gate.guard(response, gate.try_enter().unwrap()).into_body());

        // Signal the task to start without consuming its first frame. It fills the one-frame queue,
        // holds the second frame at send, and is therefore genuinely downstream-backpressured.
        {
            let waker = futures_util::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(hyper::body::Body::poll_frame(body.as_mut(), &mut cx).is_pending());
        }
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        assert_eq!(gate.active(), 0, "timeout must release the permit before the reader resumes");

        let first = body.as_mut().frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(first, "one");
        let error = body.as_mut().frame().await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(body.as_mut().frame().await.is_none(), "terminal timeout must be emitted exactly once");
        assert_eq!(gate.idle_timeouts(), 1);
    }

    #[tokio::test]
    async fn an_inner_body_error_is_terminal() {
        let gate = Arc::new(MediaGate::new(1, Duration::from_secs(30), Duration::from_secs(60)));
        let frames = futures_util::stream::iter([
            Err::<Frame<Bytes>, _>(io::Error::other("upstream broke")),
            Ok(Frame::data(Bytes::from_static(b"must not follow"))),
        ]);
        let response = Response::new(StreamBody::new(frames).boxed());
        let mut body = gate.guard(response, gate.try_enter().unwrap()).into_body();
        assert_eq!(body.frame().await.unwrap().unwrap_err().kind(), io::ErrorKind::Other);
        assert!(body.frame().await.is_none());
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
