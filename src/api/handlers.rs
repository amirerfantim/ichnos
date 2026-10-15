//! The HTTP surface: two endpoints clients use, and a few for operators.

use crate::api::auth::{AuthError, KeyCache, Permission};
use crate::api::learn;
use crate::api::quota::{consume, QuotaCounter, QuotaVerdict};
use crate::api::schema::*;
use crate::config::Config;
use crate::geo::distance;
use crate::ingest::queue::Queue;
use crate::ingest::validate::validate_item;
use crate::locate::{self, Network, PositionSource};
use crate::metrics::{metrics, Timer};
use crate::model::key::{CellKey, Radio, StationKind, WifiKey};
use crate::model::{Observation, Station, StationKey};
use crate::store::Store;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Everything a handler needs.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
    pub queue: Arc<dyn Queue>,
    pub cfg: Arc<Config>,
    pub keys: Arc<KeyCache>,
    pub quota: Arc<dyn QuotaCounter>,
}

/// Query parameters both endpoints accept.
#[derive(Debug, Default, serde::Deserialize)]
pub struct CommonQuery {
    /// Clients pass the key in the query string, which is what Ichnaea does.
    pub key: Option<String>,
    /// Ask for the working behind an answer.
    pub explain: Option<String>,
}

impl CommonQuery {
    fn wants_explain(&self) -> bool {
        matches!(self.explain.as_deref(), Some("1") | Some("true"))
    }
}

pub fn router(state: AppState) -> Router {
    let timeout = Duration::from_millis(state.cfg.server.request_timeout_ms);
    Router::new()
        // A noun for the collection you write to, a verb for the question you
        // ask. Posting observations creates records; locating computes an
        // answer and stores nothing.
        .route("/v1/observations", post(observations))
        .route("/v1/locate", post(locate))
        // Only the client routes are bounded: they are the ones a slow
        // dependency can hold up, and the operator routes must keep answering
        // exactly when things are slow.
        .route_layer(middleware::from_fn_with_state(timeout, request_timeout))
        // A body over the limit fails to parse, and is answered as any
        // unreadable body is.
        .layer(DefaultBodyLimit::max(state.cfg.api.max_body_bytes))
        .route("/debug/station/{kind}/{key}", get(debug_station))
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(prometheus_metrics))
        .with_state(state)
}

/// The routes a worker process serves: liveness and its metrics.
///
/// A worker answers no requests, but everything it measures (leases, folding,
/// elision, write errors) lives in its own process. Without a listener those
/// numbers exist only in memory and no scrape ever sees them.
pub fn worker_router() -> Router {
    Router::new().route("/health", get(health)).route("/metrics", get(prometheus_metrics))
}

// ---------------------------------------------------------- observations ----

/// Accept a batch of reports.
///
/// Always answers 200, even when every report was rejected. Clients fire this
/// and forget it, so failing them over data quality gains nothing and loses the
/// reports that were fine. What happened is visible in the metrics instead.
async fn observations(
    State(state): State<AppState>,
    Query(query): Query<CommonQuery>,
    body: Result<Json<SubmitRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let started = Instant::now();
    let m = metrics();
    let _busy = InFlight::enter("observations");

    let api_key =
        match state.keys.check(&*state.store, query.key.as_deref(), Permission::Submit).await {
            Ok(key) => {
                m.auth_results.with_label_values(&["observations", "ok"]).inc();
                key
            }
            Err(e) => {
                m.auth_results.with_label_values(&["observations", e.as_str()]).inc();
                return finish("observations", started, auth_response(e));
            }
        };
    if over_quota(&state, api_key.as_ref(), "observations").await {
        return finish("observations", started, quota_response());
    }

    let Ok(Json(request)) = body else {
        // A body that will not parse is worth saying so about: it means the
        // client is broken, not that the data was poor.
        return finish(
            "observations",
            started,
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::invalid("Invalid request body")))
                .into_response(),
        );
    };

    m.submit_items.observe(request.items.len() as f64);
    let detail = state.cfg.metrics.observation_detail;

    let mut routed: Vec<(u32, Observation)> = Vec::new();
    for item in request.items.iter().take(state.cfg.api.max_items) {
        m.submit_stations.observe((item.wifi_access_points.len() + item.cell_towers.len()) as f64);

        let validated = validate_item(item, &state.cfg.validation);
        for reason in &validated.dropped {
            m.reports_dropped.with_label_values(&[reason.as_str()]).inc();
        }
        for observation in validated.observations {
            if detail {
                record_observation_shape(&observation, &state.cfg);
            }
            let shard = observation.key.shard(state.cfg.ingest.shards);
            routed.push((shard, observation));
        }
    }

    if !routed.is_empty() {
        let timer = Timer::start();
        // A caller that gives up drops this handler mid-publish, and nothing
        // after the await runs: the guard counts those observations as
        // dropped, unless the publish finishes.
        let guard = DroppedIfAbandoned(routed.len());
        let result = state.queue.publish(&routed).await;
        std::mem::forget(guard);
        timer.observe_on(&m.queue_publish_seconds);

        match result {
            Ok(accepted) => {
                for (_, observation) in &routed {
                    let kind = kind_label(observation.key.kind());
                    m.observations.with_label_values(&[kind, "queued"]).inc();
                }
                if accepted < routed.len() {
                    m.observations
                        .with_label_values(&["any", "dropped"])
                        .inc_by((routed.len() - accepted) as u64);
                }
            }
            Err(e) => {
                // The queue being down must not fail the client, who cannot do
                // anything useful about it. The loss is counted instead.
                tracing::warn!(error = %e, "could not queue observations");
                m.queue_errors.with_label_values(&["publish"]).inc();
                m.observations.with_label_values(&["any", "dropped"]).inc_by(routed.len() as u64);
            }
        }
    }

    finish("observations", started, (StatusCode::OK, Json(serde_json::json!({}))).into_response())
}

/// Counts its observations as dropped when the handler holding it is dropped
/// before the publish finishes, which is what happens when the caller gives
/// up. Forgotten once the publish has an answer, which is counted as usual.
struct DroppedIfAbandoned(usize);

impl Drop for DroppedIfAbandoned {
    fn drop(&mut self) {
        metrics().observations.with_label_values(&["any", "dropped"]).inc_by(self.0 as u64);
    }
}

/// Record what one kept observation looked like.
///
/// Separate from the loop above so the cost is one call that config can skip,
/// rather than four histogram writes threaded through the hot path.
fn record_observation_shape(observation: &Observation, cfg: &Config) {
    let m = metrics();
    m.observation_weight.observe(observation.base_weight(&cfg.validation));
    if let Some(accuracy) = observation.accuracy {
        m.observation_accuracy.observe(accuracy);
    }
    if let Some(speed) = observation.speed {
        m.observation_speed.observe(speed);
    }
    if let Some(age) = observation.age_ms {
        // Age is a magnitude: a scan taken after the fix is just as stale as
        // one taken the same distance before it.
        m.observation_age.observe(age.abs());
    }
    m.observation_source.with_label_values(&[source_label(observation.source)]).inc();
}

// --------------------------------------------------------------- locate ----

/// Work out where a device is from the stations it can see.
async fn locate(
    State(state): State<AppState>,
    Query(query): Query<CommonQuery>,
    body: Result<Json<LocateRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let started = Instant::now();
    let m = metrics();
    let _busy = InFlight::enter("locate");

    let api_key =
        match state.keys.check(&*state.store, query.key.as_deref(), Permission::Locate).await {
            Ok(key) => {
                m.auth_results.with_label_values(&["locate", "ok"]).inc();
                key
            }
            Err(e) => {
                m.auth_results.with_label_values(&["locate", e.as_str()]).inc();
                return finish("locate", started, auth_response(e));
            }
        };
    if over_quota(&state, api_key.as_ref(), "locate").await {
        return finish("locate", started, quota_response());
    }

    let Ok(Json(request)) = body else {
        return finish(
            "locate",
            started,
            (StatusCode::BAD_REQUEST, Json(ErrorResponse::invalid("Invalid request body")))
                .into_response(),
        );
    };

    let lookup = RequestNetworks::from_request(&request, &state.cfg);
    let sent = lookup.wifi.len() + lookup.cells.len();
    m.locate_networks.with_label_values(&["wifi", "sent"]).inc_by(lookup.wifi.len() as u64);
    m.locate_networks.with_label_values(&["cell", "sent"]).inc_by(lookup.cells.len() as u64);

    if lookup.is_empty() {
        m.locate_results.with_label_values(&["none"]).inc();
        m.locate_no_answer.with_label_values(&["nothing_submitted"]).inc();
        return finish("locate", started, not_found());
    }

    let keys: Vec<StationKey> = lookup.keys();
    let stations = match state.store.get_stations(&keys).await {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!(error = %e, "station lookup failed");
            m.degraded.set(1);
            m.locate_results.with_label_values(&["error"]).inc();
            m.locate_no_answer.with_label_values(&["store_unavailable"]).inc();
            return finish("locate", started, degraded_not_found());
        }
    };
    m.degraded.set(0);

    let now = Utc::now();
    let today = now.date_naive();
    let (wifi_networks, cell_networks) = lookup.to_networks(&stations, &state.cfg, now, today);

    let known = wifi_networks.len() + cell_networks.len();
    m.locate_networks.with_label_values(&["wifi", "known"]).inc_by(wifi_networks.len() as u64);
    m.locate_networks.with_label_values(&["cell", "known"]).inc_by(cell_networks.len() as u64);
    // Coverage is the slow-moving number that says whether the database is
    // still learning. It falls long before any error appears.
    if sent > 0 {
        m.locate_coverage.observe(known as f64 / sent as f64);
    }

    let candidates = locate::candidates(wifi_networks.clone(), cell_networks.clone(), &state.cfg);
    m.locate_candidates.observe(candidates.len() as f64);

    let Some(chosen) = locate::best_index(&candidates) else {
        m.locate_results.with_label_values(&["none"]).inc();
        // Knowing none of the stations is a coverage problem; knowing some but
        // finding no cluster among them is an algorithm or a spoofing problem.
        // They need different responses, so they are counted apart.
        let reason = if known == 0 { "none_known" } else { "no_cluster" };
        m.locate_no_answer.with_label_values(&[reason]).inc();
        return finish("locate", started, not_found());
    };
    let answer = &candidates[chosen];

    let source = match answer.source {
        PositionSource::Wifi => "wifi",
        PositionSource::Cell => "cell",
    };
    m.locate_results.with_label_values(&[source]).inc();
    m.locate_accuracy.with_label_values(&[source]).observe(answer.accuracy);
    m.locate_cluster_size.with_label_values(&[source]).observe(answer.used.len() as f64);
    m.locate_score.with_label_values(&[source]).observe(answer.score);
    m.locate_stale_networks.inc_by(answer.used.iter().filter(|u| !u.seen_today).count() as u64);
    record_disagreement(&candidates, answer);
    learn_from(&state, api_key.as_ref(), &request, answer);

    let explain = query
        .wants_explain()
        .then(|| build_explain(&lookup, &wifi_networks, &cell_networks, &candidates, chosen));

    let response = LocateResponse {
        location: LatLng { lat: answer.lat, lng: answer.lon },
        accuracy: answer.accuracy,
        explain,
    };
    finish("locate", started, (StatusCode::OK, Json(response)).into_response())
}

/// Feed an answer back as a report, when its key asks for that.
///
/// Deciding is cheap and done here; the publish runs on its own so the answer
/// never waits for the queue.
fn learn_from(
    state: &AppState,
    key: Option<&crate::store::ApiKey>,
    request: &LocateRequest,
    answer: &locate::Position,
) {
    let m = metrics();
    let Some(key) = key.filter(|k| k.store_sample_locate > 0) else {
        return;
    };
    if !learn::sampled(key.store_sample_locate, state.cfg.api.locate_sample_percent, rand::random())
    {
        m.locate_learned.with_label_values(&["sampled_out"]).inc();
        return;
    }
    let Some(item) = learn::report(request, answer, &state.cfg) else {
        m.locate_learned.with_label_values(&["nothing_new"]).inc();
        return;
    };
    let validated = validate_item(&item, &state.cfg.validation);
    let shards = state.cfg.ingest.shards;
    let routed: Vec<(u32, Observation)> =
        validated.observations.into_iter().map(|o| (o.key.shard(shards), o)).collect();
    if routed.is_empty() {
        m.locate_learned.with_label_values(&["nothing_valid"]).inc();
        return;
    }
    let queue = state.queue.clone();
    tokio::spawn(async move {
        match queue.publish(&routed).await {
            Ok(_) => m.locate_learned.with_label_values(&["queued"]).inc(),
            Err(e) => {
                tracing::warn!(error = %e, "could not queue what a locate taught");
                m.queue_errors.with_label_values(&["publish"]).inc();
                m.locate_learned.with_label_values(&["lost"]).inc();
            }
        }
    });
}

/// The stations a request asked about, with what the client reported of each,
/// in the order the client first named them. The order is kept so that the
/// same request always gives the same answer, to the last bit: the stations
/// are summed in this order, and a hash map's order is random per map.
struct RequestNetworks {
    wifi: HashMap<WifiKey, (Option<i32>, Option<f64>)>,
    wifi_order: Vec<WifiKey>,
    cells: HashMap<CellKey, (Option<i32>, Option<f64>)>,
    cell_order: Vec<CellKey>,
}

impl RequestNetworks {
    fn from_request(request: &LocateRequest, cfg: &Config) -> Self {
        let mut wifi = HashMap::new();
        let mut wifi_order = Vec::new();
        for ap in request.wifi_access_points.iter().take(cfg.api.max_networks) {
            if let Some(mac) = ap.mac_address.as_deref() {
                if let Ok(key) = WifiKey::parse(&mac.to_ascii_lowercase()) {
                    let signal = ap.signal_strength.filter(|s| *s != 0).map(|s| s as i32);
                    // A repeat keeps its first place and its last report.
                    if wifi.insert(key, (signal, ap.age)).is_none() {
                        wifi_order.push(key);
                    }
                }
            }
        }

        let mut cells = HashMap::new();
        let mut cell_order = Vec::new();
        for tower in request.cell_towers.iter().take(cfg.api.max_networks) {
            let Some(radio) = tower.radio_type.as_deref().and_then(Radio::parse) else {
                continue;
            };
            let (Some(mcc), Some(mnc), Some(lac), Some(cid)) = (
                tower.mobile_country_code,
                tower.mobile_network_code,
                tower.location_area_code,
                tower.cell_id,
            ) else {
                continue;
            };
            let key = CellKey { radio, mcc: mcc as i32, mnc: mnc as i32, lac, cid };
            let signal = tower.signal_strength.filter(|s| *s != 0).map(|s| s as i32);
            if cells.insert(key, (signal, tower.age)).is_none() {
                cell_order.push(key);
            }
        }

        Self { wifi, wifi_order, cells, cell_order }
    }

    fn is_empty(&self) -> bool {
        self.wifi.is_empty() && self.cells.is_empty()
    }

    fn keys(&self) -> Vec<StationKey> {
        self.wifi_order
            .iter()
            .map(|k| StationKey::Wifi(*k))
            .chain(self.cell_order.iter().map(|k| StationKey::Cell(*k)))
            .collect()
    }

    /// Pair each known station with what the client said about seeing it.
    fn to_networks(
        &self,
        stations: &[Station],
        cfg: &Config,
        now: chrono::DateTime<Utc>,
        today: chrono::NaiveDate,
    ) -> (Vec<Network>, Vec<Network>) {
        let mut wifi = Vec::new();
        let mut cells = Vec::new();

        // In the order the client named them, not the order the store
        // answered in, which a store reading in parallel does not fix.
        let by_key: HashMap<StationKey, &Station> = stations.iter().map(|s| (s.key, s)).collect();
        for station in self.keys().iter().filter_map(|k| by_key.get(k).copied()) {
            // A station with no position has nothing to contribute, and a
            // blocked one is untrusted on purpose.
            if !station.has_position() || station.is_blocked(today, &cfg.block) {
                continue;
            }
            let (Some(lat), Some(lon)) = (station.lat, station.lon) else { continue };

            let (reported_signal, reported_age, network_cfg) = match station.key {
                StationKey::Wifi(key) => {
                    let (signal, age) = self.wifi.get(&key).copied().unwrap_or((None, None));
                    (signal, age, &cfg.locate.wifi)
                }
                StationKey::Cell(key) => {
                    let (signal, age) = self.cells.get(&key).copied().unwrap_or((None, None));
                    (signal, age, &cfg.locate.cell)
                }
            };

            let network = Network {
                key: station.key,
                lat,
                lon,
                radius: f64::from(station.radius.unwrap_or(0)).max(network_cfg.min_accuracy_m),
                // Floored so a fresh scan cannot divide by zero later.
                age_ms: reported_age.map(f64::abs).unwrap_or(0.0).max(1_000.0),
                signal: f64::from(reported_signal.unwrap_or(network_cfg.min_signal_dbm)),
                score: locate::score::station_score(station, now, &cfg.score),
                seen_today: station.seen_today(today),
            };

            match station.key {
                StationKey::Wifi(_) => wifi.push(network),
                StationKey::Cell(_) => cells.push(network),
            }
        }

        (wifi, cells)
    }
}

fn build_explain(
    lookup: &RequestNetworks,
    wifi: &[Network],
    cells: &[Network],
    candidates: &[locate::Position],
    chosen: usize,
) -> Explain {
    let answer = &candidates[chosen];
    let clusters = candidates
        .iter()
        .enumerate()
        .map(|(i, c)| ExplainCluster {
            source: match c.source {
                PositionSource::Wifi => "wifi".into(),
                PositionSource::Cell => "cell".into(),
            },
            size: c.used.len(),
            score: c.score,
            accuracy: c.accuracy,
            // The winner itself, not whatever shares its position: two
            // candidates can sit on the same point.
            chosen: i == chosen,
        })
        .collect();

    let reason = match answer.source {
        PositionSource::Wifi => {
            format!("a wifi cluster of {} won on combined score", answer.used.len())
        }
        PositionSource::Cell => {
            if wifi.is_empty() {
                "no wifi was known, so the cell answered".into()
            } else {
                "the cell cluster beat the wifi one on combined score".into()
            }
        }
    };

    Explain {
        wifi_sent: lookup.wifi.len(),
        wifi_known: wifi.len(),
        cell_sent: lookup.cells.len(),
        cell_known: cells.len(),
        clusters,
        used: answer
            .used
            .iter()
            .map(|u| ExplainNetwork {
                key: u.key.to_token(),
                score: u.score,
                seen_today: u.seen_today,
            })
            .collect(),
        reason,
    }
}

// ----------------------------------------------------------------- debug ----

/// Everything the service knows about one station.
///
/// Behind an admin key, because it exposes a full record. It exists because the
/// alternative is opening a database shell every time somebody asks why an
/// answer was wrong.
async fn debug_station(
    State(state): State<AppState>,
    Path((kind, key)): Path<(String, String)>,
    Query(query): Query<CommonQuery>,
) -> Response {
    if let Err(e) = state.keys.check(&*state.store, query.key.as_deref(), Permission::Admin).await {
        return auth_response(e);
    }

    let token = match kind.as_str() {
        "wifi" => format!("w:{key}"),
        "cell" => format!("c:{key}"),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::invalid("kind must be wifi or cell")),
            )
                .into_response()
        }
    };

    let Some(station_key) = StationKey::from_token(&token) else {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::invalid("Malformed station key")))
            .into_response();
    };

    match state.store.get_station(&station_key).await {
        Ok(Some(station)) => {
            let today = Utc::now().date_naive();
            let body = serde_json::json!({
                "key": station.key.to_token(),
                "station": station,
                "blocked": station.is_blocked(today, &state.cfg.block),
                "score": locate::score::station_score(&station, Utc::now(), &state.cfg.score),
                "seen_today": station.seen_today(today),
            });
            (StatusCode::OK, Json(body)).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(ErrorResponse::not_found())).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "debug lookup failed");
            (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse::invalid("Storage unavailable")))
                .into_response()
        }
    }
}

/// Answer a client request that runs past `server.requestTimeoutMs` with a
/// 503, rather than holding the client for as long as a slow dependency does.
/// A locate that cannot answer quickly is worth less than a fast "no".
async fn request_timeout(State(limit): State<Duration>, request: Request, next: Next) -> Response {
    let started = Instant::now();
    let route = match request.uri().path() {
        "/v1/locate" => "locate",
        _ => "observations",
    };
    match tokio::time::timeout(limit, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            tracing::warn!(route, limit_ms = limit.as_millis() as u64, "request timed out");
            finish(
                route,
                started,
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse::invalid("Request timed out")),
                )
                    .into_response(),
            )
        }
    }
}

// ---------------------------------------------------------------- health ----

/// Liveness: the process is running.
async fn health() -> Response {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))).into_response()
}

/// Readiness: the process can actually serve.
async fn ready(State(state): State<AppState>) -> Response {
    let store_ok = state.store.health().await.is_ok();
    let queue_ok = state.queue.healthy().await;
    let status = if store_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (
        status,
        Json(serde_json::json!({
            "store": store_ok,
            // The queue being down stops learning but not answering, so it is
            // reported without failing readiness.
            "queue": queue_ok,
        })),
    )
        .into_response()
}

async fn prometheus_metrics() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics().render(),
    )
        .into_response()
}

// ---------------------------------------------------------------- shared ----

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(ErrorResponse::not_found())).into_response()
}

/// A miss caused by the database being unreachable rather than by a genuine
/// absence. The header lets a caller tell the two apart.
fn degraded_not_found() -> Response {
    (StatusCode::NOT_FOUND, [("X-Ichnos-Degraded", "1")], Json(ErrorResponse::not_found()))
        .into_response()
}

/// Has this key had its allowance for today?
async fn over_quota(state: &AppState, api_key: Option<&crate::store::ApiKey>, route: &str) -> bool {
    match api_key {
        Some(key) => {
            let exceeded =
                consume(&*state.quota, &key.key, key.daily_quota).await == QuotaVerdict::Exceeded;
            let result = if exceeded { "exceeded" } else { "allowed" };
            metrics().quota_results.with_label_values(&[route, result]).inc();
            exceeded
        }
        // No key means keys are not being required at all, which is a
        // development setting; there is nothing to count against.
        None => false,
    }
}

/// Holds the in-flight gauge up for as long as a request is being handled.
///
/// A guard rather than a pair of calls, because an early return on any of the
/// several refusal paths would otherwise leak the gauge upwards for ever.
struct InFlight(&'static str);

impl InFlight {
    fn enter(route: &'static str) -> Self {
        metrics().http_in_flight.with_label_values(&[route]).inc();
        Self(route)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        metrics().http_in_flight.with_label_values(&[self.0]).dec();
    }
}

/// How far the runner-up candidate sat from the winner.
///
/// One device should not see two sets of stations that disagree by kilometres.
/// When it does, either the stations are wrong or the report is not honest, and
/// this is the only metric that would show it.
fn record_disagreement(candidates: &[locate::Position], answer: &locate::Position) {
    let runner_up = candidates
        .iter()
        .filter(|c| *c != answer)
        .max_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(std::cmp::Ordering::Equal));
    if let Some(other) = runner_up {
        metrics()
            .locate_disagreement
            .observe(distance(answer.lat, answer.lon, other.lat, other.lon));
    }
}

fn source_label(source: crate::model::observation::ReportSource) -> &'static str {
    use crate::model::observation::ReportSource::*;
    match source {
        Fixed => "fixed",
        Gnss => "gnss",
        Fused => "fused",
        Query => "query",
    }
}

fn quota_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ErrorResponse::forbidden("Daily request allowance exhausted")),
    )
        .into_response()
}

fn auth_response(error: AuthError) -> Response {
    if error.is_our_fault() {
        // Not the caller's problem, so not a 4xx. Saying "invalid key" here
        // would send whoever investigates after the wrong thing entirely.
        return (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse::invalid(error.message())))
            .into_response();
    }
    (StatusCode::BAD_REQUEST, Json(ErrorResponse::forbidden(error.message()))).into_response()
}

fn kind_label(kind: StationKind) -> &'static str {
    match kind {
        StationKind::Wifi => "wifi",
        StationKind::Cell => "cell",
    }
}

/// Record the outcome and hand the response back.
fn finish(route: &str, started: Instant, response: Response) -> Response {
    let m = metrics();
    m.http_requests.with_label_values(&[route, response.status().as_str()]).inc();
    m.http_duration.with_label_values(&[route]).observe(started.elapsed().as_secs_f64());
    response
}
