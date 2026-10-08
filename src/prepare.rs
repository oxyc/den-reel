//! Combined trailer discovery and source-ladder preparation.
//!
//! This removes the browser's discovery-to-sources round trip. It deliberately does not activate
//! a listener: Den Edge remains the authority that observed the client.

use std::sync::Arc;

use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Response, StatusCode};
use serde_json::{json, Value};

use crate::httputil::{self, query_param, Body};
use crate::state::AppState;

pub async fn handle_prepare(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    cfg: Option<&crate::userconfig::UserConfig>,
    ty: &str,
    raw_id: &str,
    query: &str,
) -> Response<Body> {
    let intent = match query_param(query, "intent").as_deref() {
        None | Some("play") => "play",
        Some("warm") => "warm",
        _ => return httputil::error(StatusCode::BAD_REQUEST, "bad_request", "Expected intent=warm or play."),
    };
    // Reject malformed asks before spending a TMDB/KinoCheck lookup. `prepare_sources` parses the same
    // function again when it consumes the values, so validation and execution cannot drift apart.
    if let Err(detail) = crate::sources::parse_ask(query) {
        return httputil::error(StatusCode::BAD_REQUEST, "bad_request", detail);
    }
    let warm = intent == "warm";
    let discovered = crate::addon::prepare_discovery(state, headers, cfg, ty, raw_id, query).await;
    let binding = match cfg {
        Some(c) => crate::sign::Binding::Install { iid: c.iid.as_deref(), ep: c.ep },
        None => crate::sign::Binding::Unbound,
    };
    let meta = crate::addon::build_meta(
        ty,
        &discovered.id,
        &discovered.base,
        &discovered.yt_ids,
        state.cfg.play_secret.as_deref(),
        binding,
    );
    let Some(primary_id) = discovered.yt_ids.first().cloned() else {
        let body = json!({
            "meta": meta["meta"],
            "primary": Value::Null,
            "prepared": Value::Null,
            "intent": intent,
            "sources": [],
            "crop": Value::Null,
            "expires": Value::Null,
        });
        let mut response = httputil::timed(
            httputil::json(StatusCode::OK, &body, &[("cache-control", "no-store")]),
            &discovered.timing,
        );
        add_degraded(&mut response, discovered.degraded);
        return response;
    };

    // The ladder's `../m/...` URLs are relative to `/sources/<id>.json`, not this endpoint. Give
    // the client that exact signed base so relay prefixes, host selection and install binding stay
    // intact: resolve every relative `sources[].url` against `primary.sourcesBase`.
    let sources_base = meta["meta"]["links"][0]["sources"].clone();
    let prepared = match crate::sources::prepare_sources(
        state.clone(),
        headers,
        primary_id.clone(),
        query,
        Some(binding),
        warm,
        !discovered.head_dead,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            let failed_status = error.status().as_u16();
            let retry_after = error.headers().get("retry-after").cloned();
            let retry_after_text = retry_after.as_ref().and_then(|v| v.to_str().ok()).map(str::to_owned);
            let source_timing =
                error.headers().get("server-timing").and_then(|v| v.to_str().ok()).unwrap_or("");
            let timing = match (discovered.timing.is_empty(), source_timing.is_empty()) {
                (true, _) => source_timing.to_owned(),
                (_, true) => discovered.timing.clone(),
                _ => format!("{}, {source_timing}", discovered.timing),
            };
            let body = json!({
                "meta": meta["meta"],
                "primary": { "id": primary_id, "sourcesBase": sources_base },
                "prepared": Value::Null,
                "intent": intent,
                "sources": [],
                "crop": Value::Null,
                "expires": Value::Null,
                "degraded": {
                    "reason": "primary_unavailable",
                    "status": failed_status,
                    "retryAfter": retry_after_text,
                },
            });
            let mut response = httputil::timed(
                httputil::json(StatusCode::OK, &body, &[("cache-control", "no-store")]),
                &timing,
            );
            if let Some(retry_after) = retry_after {
                response.headers_mut().insert("retry-after", retry_after);
            }
            add_degraded(&mut response, Some("primary_unavailable"));
            return response;
        }
    };
    let body = json!({
        "meta": meta["meta"],
        "primary": { "id": primary_id, "sourcesBase": sources_base },
        "prepared": { "intent": intent, "playReady": true, "provisional": warm },
        "intent": intent,
        "sources": prepared.body["sources"],
        "crop": prepared.body["crop"],
        "expires": prepared.body["expires"],
    });
    let timing = match (discovered.timing.is_empty(), prepared.timing.is_empty()) {
        (true, _) => prepared.timing.clone(),
        (_, true) => discovered.timing.clone(),
        _ => format!("{}, {}", discovered.timing, prepared.timing),
    };
    // The body contains short-lived, install-bound media capabilities. Keep it in the caller's
    // cache, never a shared cache even when discovery itself used the config-less route.
    let max_age =
        if discovered.stale || discovered.reordered { prepared.max_age.min(3600) } else { prepared.max_age };
    let cache = format!("private, max-age={max_age}");
    let mut response = httputil::timed(
        httputil::json(
            StatusCode::OK,
            &body,
            &[("cache-control", cache.as_str()), ("vary", crate::client::HEADER), httputil::VARY_SELF_BASE],
        ),
        &timing,
    );
    add_degraded(&mut response, discovered.degraded);
    response
}

fn add_degraded(response: &mut Response<Body>, reason: Option<&'static str>) {
    if let Some(reason) = reason {
        response.headers_mut().insert("x-den-degraded", HeaderValue::from_static(reason));
    }
}
