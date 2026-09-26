//! Bounds the public direct-media data plane independently of request-task count.
//!
//! A permit belongs to the response body, not just the handler future: returning headers does not
//! release the upstream socket/FD or the buffers behind it. Dropping the client response releases
//! the permit immediately. Idle and absolute deadlines also drop the inner body, cancelling an HLS
//! upstream response and the progressive producer that owns its range fetches.

use std::future::Future;
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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

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
        let body = GuardedBody {
            inner: Some(inner),
            lease: Some(lease),
            idle_for: self.idle,
            idle: Box::pin(tokio::time::sleep(self.idle)),
            lifetime: Box::pin(tokio::time::sleep(self.lifetime)),
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
    pub fn limit(&self) -> usize {
        self.limit
    }
}

struct GuardedBody {
    inner: Option<Body>,
    lease: Option<Lease>,
    idle_for: Duration,
    idle: Pin<Box<Sleep>>,
    lifetime: Pin<Box<Sleep>>,
}

impl GuardedBody {
    fn stop(&mut self) {
        self.inner = None;
        self.lease = None;
    }

    fn timed_out(&mut self, why: &'static str) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        self.stop();
        Poll::Ready(Some(Err(io::Error::new(io::ErrorKind::TimedOut, why))))
    }
}

impl hyper::body::Body for GuardedBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if self.inner.is_none() {
            return Poll::Ready(None);
        }
        if self.lifetime.as_mut().poll(cx).is_ready() {
            return self.timed_out("direct media response lifetime exceeded");
        }
        if self.idle.as_mut().poll(cx).is_ready() {
            return self.timed_out("direct media response idle timeout");
        }
        let polled = Pin::new(self.inner.as_mut().expect("checked above")).poll_frame(cx);
        match polled {
            Poll::Ready(Some(Ok(frame))) => {
                let next_idle = Instant::now() + self.idle_for;
                self.idle.as_mut().reset(next_idle);
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.stop();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.stop();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(hyper::body::Body::is_end_stream)
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.as_ref().map_or_else(hyper::body::SizeHint::default, hyper::body::Body::size_hint)
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
        assert_eq!(gate.active(), 0);

        let guarded = gate.guard(response("two"), gate.try_enter().unwrap());
        let _ = guarded.into_body().collect().await.unwrap();
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
