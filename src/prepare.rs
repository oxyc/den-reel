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
    let contract = match crate::sources::contract(query) {
        Ok(contract) => contract,
        Err(detail) => return httputil::error(StatusCode::BAD_REQUEST, "bad_request", detail),
    };
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
    let mut meta = crate::addon::build_meta(
        ty,
        &discovered.id,
        &discovered.base,
        &discovered.yt_ids,
        state.cfg.play_secret.as_deref(),
        binding,
    );
    if contract == crate::sources::Contract::V2 {
        add_plan_urls(&mut meta, query);
    }
    let Some(primary_id) = discovered.yt_ids.first().cloned() else {
        let body = match contract {
            crate::sources::Contract::V1 => json!({
                "meta": meta["meta"],
                "primary": Value::Null,
                "prepared": Value::Null,
                "intent": intent,
                "sources": [],
                "crop": Value::Null,
                "expires": Value::Null,
            }),
            crate::sources::Contract::V2 => json!({
                "v": 2,
                "meta": meta["meta"],
                "primary": Value::Null,
                "primaryPlan": Value::Null,
                "prepared": Value::Null,
                "intent": intent,
            }),
        };
        let mut response = httputil::timed(
            httputil::json(StatusCode::OK, &body, &[("cache-control", "no-store")]),
            &discovered.timing,
        );
        add_degraded(&mut response, discovered.degraded);
        return response;
    };

    let source_reference = match contract {
        crate::sources::Contract::V1 => meta["meta"]["links"][0]["sources"].clone(),
        crate::sources::Contract::V2 => meta["meta"]["links"][0]["planUrl"].clone(),
    };
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
            let degraded = json!({
                "reason": "primary_unavailable",
                "status": failed_status,
                "retryAfter": retry_after_text,
            });
            let body = match contract {
                crate::sources::Contract::V1 => json!({
                    "meta": meta["meta"],
                    "primary": { "id": primary_id, "sourcesBase": source_reference },
                    "prepared": Value::Null,
                    "intent": intent,
                    "sources": [],
                    "crop": Value::Null,
                    "expires": Value::Null,
                    "degraded": degraded,
                }),
                crate::sources::Contract::V2 => json!({
                    "v": 2,
                    "meta": meta["meta"],
                    "primary": { "id": primary_id, "planUrl": source_reference },
                    "primaryPlan": Value::Null,
                    "prepared": Value::Null,
                    "intent": intent,
                    "degraded": degraded,
                }),
            };
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
    let body = match contract {
        crate::sources::Contract::V1 => {
            let source_body = prepared.body(contract);
            json!({
                "meta": meta["meta"],
                "primary": { "id": primary_id, "sourcesBase": source_reference },
                "prepared": { "intent": intent, "playReady": true, "provisional": warm },
                "intent": intent,
                "sources": source_body["sources"],
                "crop": source_body["crop"],
                "expires": source_body["expires"],
            })
        }
        crate::sources::Contract::V2 => json!({
            "v": 2,
            "meta": meta["meta"],
            "primary": { "id": primary_id, "planUrl": source_reference },
            "primaryPlan": prepared.body(contract),
            "prepared": { "intent": intent, "playReady": true, "provisional": warm },
            "intent": intent,
        }),
    };
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

/// Add complete v2 plan references to every discovered candidate and remove the v1 transport URLs.
/// The alternate can therefore be fetched lazily without deriving a route from `/play` (or from any
/// other URL). A plan reference always asks for a play-ready plan even when this prepare was only a
/// speculative warm-up; the sibling `prepared.provisional` still describes the embedded warm result.
fn add_plan_urls(meta: &mut Value, query: &str) {
    let surface = query_param(query, "surface").unwrap_or_default();
    let player = query_param(query, "player").unwrap_or_default();
    let plan_params = query
        .split('&')
        .filter(|part| {
            part.split_once('=').is_some_and(|(key, _)| key == "height" || key == "playable")
        })
        .fold(String::new(), |mut params, value| {
            params.push('&');
            params.push_str(value);
            params
        });
    let Some(links) = meta["meta"]["links"].as_array_mut() else {
        return;
    };
    for link in links {
        let Some(link) = link.as_object_mut() else {
            continue;
        };
        let Some(base) = link.get("sources").and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        let separator = if base.contains('?') { '&' } else { '?' };
        let plan_url =
            format!("{base}{separator}v=2&surface={surface}&player={player}&intent=play{plan_params}");
        link.remove("trailers");
        link.remove("sources");
        link.insert("planUrl".into(), Value::String(plan_url));
    }
}

fn add_degraded(response: &mut Response<Body>, reason: Option<&'static str>) {
    if let Some(reason) = reason {
        response.headers_mut().insert("x-den-degraded", HeaderValue::from_static(reason));
    }
}
