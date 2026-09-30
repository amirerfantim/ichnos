//! End-to-end tests through the real HTTP surface.
//!
//! These drive the assembled service the way a client does: submit reports,
//! let a worker fold them, then ask where the device is. The storage and queue
//! are in-process, so the tests are fast and deterministic, but every layer
//! between the request and the answer is the real one.

use axum_test::TestServer;
use ichnos::api::auth::KeyCache;
use ichnos::api::handlers::{router, worker_router, AppState};
use ichnos::api::quota::MemoryQuota;
use ichnos::config::Config;
use ichnos::ingest::queue::MemoryQueue;
use ichnos::store::cache::{CachedStore, MemoryCache};
use ichnos::store::memory::MemoryStore;
use ichnos::store::{ApiKey, Store};
use ichnos::worker::process_shard;
use serde_json::{json, Value};
use std::sync::Arc;

/// The whole service, assembled in memory.
struct Harness {
    server: TestServer,
    queue: Arc<MemoryQueue>,
    store: Arc<CachedStore<MemoryStore, MemoryCache>>,
    cfg: Config,
}

impl Harness {
    async fn new() -> Self {
        Self::with_config(Config::for_tests()).await
    }

    async fn with_config(cfg: Config) -> Self {
        let inner = MemoryStore::new();
        inner.put_api_key(&ApiKey::full("test-key")).await.unwrap();
        inner
            .put_api_key(&ApiKey {
                allow_locate: true,
                allow_submit: false,
                allow_admin: false,
                ..ApiKey::full("read-only")
            })
            .await
            .unwrap();

        let store = Arc::new(CachedStore::new(inner, MemoryCache::new(), cfg.cache.clone()));
        let queue = Arc::new(MemoryQueue::new(cfg.ingest.shards, 10_000));

        let state = AppState {
            store: store.clone(),
            queue: queue.clone(),
            cfg: Arc::new(cfg.clone()),
            keys: Arc::new(KeyCache::new(&cfg.api)),
            quota: Arc::new(MemoryQuota::new()),
        };

        Self { server: TestServer::new(router(state)).unwrap(), queue, store, cfg }
    }

    /// Run the worker over every shard, as the fold window would.
    async fn drain(&self) {
        for shard in 0..self.cfg.ingest.shards {
            process_shard(&*self.queue, &*self.store, shard, &self.cfg).await.unwrap();
        }
    }

    async fn submit(&self, body: Value) -> u16 {
        self.server
            .post("/v1/observations")
            .add_query_param("key", "test-key")
            .json(&body)
            .await
            .status_code()
            .as_u16()
    }

    async fn locate(&self, body: Value) -> (u16, Value) {
        let response =
            self.server.post("/v1/locate").add_query_param("key", "test-key").json(&body).await;
        let status = response.status_code().as_u16();
        (status, response.json::<Value>())
    }
}

/// A report of some access points seen from one position.
fn report(lat: f64, lon: f64, macs: &[(&str, i64)]) -> Value {
    json!({"items": [{
        "position": {"latitude": lat, "longitude": lon, "accuracy": 10},
        "wifiAccessPoints": macs.iter().map(|(mac, signal)| json!({
            "macAddress": mac, "signalStrength": signal, "frequency": 2412
        })).collect::<Vec<_>>(),
    }]})
}

fn ask(macs: &[(&str, i64)]) -> Value {
    json!({"wifiAccessPoints": macs.iter().map(|(mac, signal)| json!({
        "macAddress": mac, "signalStrength": signal
    })).collect::<Vec<_>>()})
}

#[tokio::test]
async fn a_device_can_be_located_from_stations_it_taught_the_service() {
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];

    assert_eq!(h.submit(report(35.7219, 51.3347, &macs)).await, 200);
    h.drain().await;

    let (status, body) = h.locate(ask(&macs)).await;
    assert_eq!(status, 200);
    let lat = body["location"]["lat"].as_f64().unwrap();
    let lng = body["location"]["lng"].as_f64().unwrap();
    assert!((lat - 35.7219).abs() < 0.001, "lat {lat}");
    assert!((lng - 51.3347).abs() < 0.001, "lng {lng}");
    assert!(body["accuracy"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn nothing_is_known_before_the_worker_has_run() {
    // Submitting queues work; it does not write. Until a worker folds the
    // batch, the service genuinely knows nothing.
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];

    h.submit(report(35.7219, 51.3347, &macs)).await;
    assert_eq!(h.locate(ask(&macs)).await.0, 404);

    h.drain().await;
    assert_eq!(h.locate(ask(&macs)).await.0, 200);
}

#[tokio::test]
async fn one_known_access_point_is_not_enough_to_answer() {
    let h = Harness::new().await;
    h.submit(report(35.7219, 51.3347, &[("aabbccddee01", -65), ("aabbccddee02", -70)])).await;
    h.drain().await;

    // Asking about only one of them must not locate it.
    assert_eq!(h.locate(ask(&[("aabbccddee01", -65)])).await.0, 404);
}

#[tokio::test]
async fn an_unknown_device_gets_the_expected_error_body() {
    let h = Harness::new().await;
    let (status, body) = h.locate(ask(&[("ffeeddccbb01", -65), ("ffeeddccbb02", -70)])).await;
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], 404);
    assert_eq!(body["error"]["errors"][0]["reason"], "notFound");
}

#[tokio::test]
async fn a_moved_access_point_is_blocked_and_stops_answering() {
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];

    h.submit(report(35.7219, 51.3347, &macs)).await;
    h.drain().await;
    assert_eq!(h.locate(ask(&macs)).await.0, 200);

    // The same access points reported from 100 km away.
    h.submit(report(36.5000, 52.0000, &macs)).await;
    h.drain().await;

    assert_eq!(h.locate(ask(&macs)).await.0, 404, "a blocked station must not answer");
}

#[tokio::test]
async fn repeated_reports_do_not_multiply_writes() {
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];

    for _ in 0..20 {
        h.submit(report(35.7219, 51.3347, &macs)).await;
    }
    h.drain().await;

    // Forty observations become two stations. The two stations hash to
    // different shards, so there is one batched write per shard, not one per
    // observation. What matters is that it is a handful, not forty.
    assert!(h.store.inner().write_count() <= 2, "got {} writes", h.store.inner().write_count());
    assert_eq!(h.store.inner().len(), 2);
}

#[tokio::test]
async fn a_second_round_of_identical_reports_writes_nothing_at_all() {
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];

    h.submit(report(35.7219, 51.3347, &macs)).await;
    h.drain().await;
    let after_first = h.store.inner().write_count();

    h.submit(report(35.7219, 51.3347, &macs)).await;
    h.drain().await;

    assert_eq!(h.store.inner().write_count(), after_first, "elision should have skipped it");
}

#[tokio::test]
async fn a_bad_position_loses_the_whole_report_but_not_the_service() {
    let h = Harness::new().await;
    let body = json!({"items": [{
        "position": {"latitude": 999.0, "longitude": 51.3347, "accuracy": 10},
        "wifiAccessPoints": [{"macAddress": "aabbccddee01", "signalStrength": -65}]
    }]});

    // Still a 200: the client cannot act on this, and the metrics record it.
    assert_eq!(h.submit(body).await, 200);
    h.drain().await;
    assert_eq!(h.store.inner().len(), 0, "nothing should have been learned");
}

#[tokio::test]
async fn a_malformed_body_is_refused_plainly() {
    let h = Harness::new().await;
    let response = h
        .server
        .post("/v1/observations")
        .add_query_param("key", "test-key")
        .text("this is not json")
        .await;
    assert_eq!(response.status_code().as_u16(), 400);
}

#[tokio::test]
async fn permissions_are_enforced_per_endpoint() {
    let h = Harness::new().await;

    let refused = h
        .server
        .post("/v1/observations")
        .add_query_param("key", "read-only")
        .json(&report(35.7219, 51.3347, &[("aabbccddee01", -65)]))
        .await;
    assert_eq!(refused.status_code().as_u16(), 400, "a read-only key may not submit");

    let allowed =
        h.server.post("/v1/locate").add_query_param("key", "read-only").json(&json!({})).await;
    assert_eq!(allowed.status_code().as_u16(), 404, "but it may ask");
}

#[tokio::test]
async fn a_request_without_a_key_is_refused() {
    let h = Harness::new().await;
    let response = h.server.post("/v1/locate").json(&json!({})).await;
    assert_eq!(response.status_code().as_u16(), 400);
}

#[tokio::test]
async fn the_explain_output_shows_the_working() {
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];
    h.submit(report(35.7219, 51.3347, &macs)).await;
    h.drain().await;

    let response = h
        .server
        .post("/v1/locate")
        .add_query_param("key", "test-key")
        .add_query_param("explain", "1")
        .json(&ask(&macs))
        .await;
    let body: Value = response.json();

    assert_eq!(body["explain"]["wifi_sent"], 2);
    assert_eq!(body["explain"]["wifi_known"], 2);
    assert_eq!(body["explain"]["clusters"][0]["chosen"], true);
    assert!(body["explain"]["reason"].as_str().unwrap().contains("wifi"));
}

#[tokio::test]
async fn explain_is_absent_unless_it_is_asked_for() {
    let h = Harness::new().await;
    let macs = [("aabbccddee01", -65), ("aabbccddee02", -70)];
    h.submit(report(35.7219, 51.3347, &macs)).await;
    h.drain().await;

    let (_, body) = h.locate(ask(&macs)).await;
    assert!(body.get("explain").is_none(), "ordinary traffic pays nothing for it");
}

#[tokio::test]
async fn the_debug_route_needs_an_admin_key_and_shows_the_record() {
    let h = Harness::new().await;
    h.submit(report(35.7219, 51.3347, &[("aabbccddee01", -65), ("aabbccddee02", -70)])).await;
    h.drain().await;

    let refused =
        h.server.get("/debug/station/wifi/aabbccddee01").add_query_param("key", "read-only").await;
    assert_eq!(refused.status_code().as_u16(), 400);

    let allowed =
        h.server.get("/debug/station/wifi/aabbccddee01").add_query_param("key", "test-key").await;
    assert_eq!(allowed.status_code().as_u16(), 200);
    let body: Value = allowed.json();
    assert_eq!(body["key"], "w:aabbccddee01");
    assert_eq!(body["blocked"], false);
    assert!(body["station"]["lat"].as_f64().is_some());
}

#[tokio::test]
async fn a_cell_alone_answers_but_only_vaguely() {
    let h = Harness::new().await;
    let body = json!({"items": [{
        "position": {"latitude": 35.7219, "longitude": 51.3347, "accuracy": 10},
        "cellTowers": [{"radioType": "lte", "mobileCountryCode": 432, "mobileNetworkCode": 11,
                        "locationAreaCode": 1234, "cellId": 56789, "signalStrength": -105}]
    }]});
    h.submit(body).await;
    h.drain().await;

    let (status, answer) = h
        .locate(json!({"cellTowers": [{"radioType": "lte", "mobileCountryCode": 432,
                                       "mobileNetworkCode": 11, "locationAreaCode": 1234,
                                       "cellId": 56789, "signalStrength": -105}]}))
        .await;
    assert_eq!(status, 200);
    assert!(
        answer["accuracy"].as_f64().unwrap() >= 1_000.0,
        "a cell answer should not claim precision it does not have"
    );
}

#[tokio::test]
async fn a_precise_wifi_answer_beats_a_vague_cell_one() {
    let h = Harness::new().await;
    let both = json!({"items": [{
        "position": {"latitude": 35.7219, "longitude": 51.3347, "accuracy": 10},
        "wifiAccessPoints": [{"macAddress": "aabbccddee01", "signalStrength": -65, "frequency": 2412},
                             {"macAddress": "aabbccddee02", "signalStrength": -70, "frequency": 2412}],
        "cellTowers": [{"radioType": "lte", "mobileCountryCode": 432, "mobileNetworkCode": 11,
                        "locationAreaCode": 1234, "cellId": 56789, "signalStrength": -105}]
    }]});
    h.submit(both).await;
    h.drain().await;

    let (_, answer) = h
        .locate(json!({
            "wifiAccessPoints": [{"macAddress": "aabbccddee01", "signalStrength": -65},
                                 {"macAddress": "aabbccddee02", "signalStrength": -70}],
            "cellTowers": [{"radioType": "lte", "mobileCountryCode": 432, "mobileNetworkCode": 11,
                            "locationAreaCode": 1234, "cellId": 56789, "signalStrength": -105}]}))
        .await;
    assert!(answer["accuracy"].as_f64().unwrap() < 1_000.0, "wifi should have won");
}

#[tokio::test]
async fn the_service_reports_its_own_health() {
    let h = Harness::new().await;
    assert_eq!(h.server.get("/health").await.status_code().as_u16(), 200);

    let ready: Value = h.server.get("/ready").await.json();
    assert_eq!(ready["store"], true);
    assert_eq!(ready["queue"], true);
}

#[tokio::test]
async fn readiness_fails_when_storage_is_unreachable() {
    let h = Harness::new().await;
    h.store.inner().set_failing(true);
    assert_eq!(h.server.get("/ready").await.status_code().as_u16(), 503);
}

#[tokio::test]
async fn a_locate_during_a_storage_outage_says_so_in_a_header() {
    // A caller needs to tell "we do not know" from "we could not look".
    let h = Harness::new().await;

    // Warm the key first, so the outage is felt on the lookup rather than on
    // the authentication.
    h.locate(ask(&[("aabbccddee01", -65)])).await;
    h.store.inner().set_failing(true);

    let response = h
        .server
        .post("/v1/locate")
        .add_query_param("key", "test-key")
        .json(&ask(&[("aabbccddee01", -65), ("aabbccddee02", -70)]))
        .await;

    assert_eq!(response.status_code().as_u16(), 404);
    assert_eq!(response.headers().get("X-Ichnos-Degraded").unwrap(), "1");
}

#[tokio::test]
async fn an_outage_is_never_reported_as_a_bad_api_key() {
    // A caller with a perfectly good key must not be told it is invalid, or
    // whoever investigates goes hunting the wrong problem entirely.
    let h = Harness::new().await;
    h.store.inner().set_failing(true);

    let response = h
        .server
        .post("/v1/locate")
        .add_query_param("key", "never-seen-before")
        .json(&json!({}))
        .await;
    assert_eq!(response.status_code().as_u16(), 503);
}

#[tokio::test]
async fn metrics_are_published_in_the_expected_format() {
    let h = Harness::new().await;
    h.submit(report(35.7219, 51.3347, &[("aabbccddee01", -65)])).await;

    let response = h.server.get("/metrics").await;
    assert_eq!(response.status_code().as_u16(), 200);
    let text = response.text();
    assert!(text.contains("ichnos_http_requests_total"));
    assert!(text.contains("# TYPE"));
}

#[tokio::test]
async fn an_empty_request_is_answered_rather_than_crashed_on() {
    let h = Harness::new().await;
    assert_eq!(h.submit(json!({})).await, 200);
    assert_eq!(h.submit(json!({"items": []})).await, 200);
    assert_eq!(h.locate(json!({})).await.0, 404);
}

#[tokio::test]
async fn more_networks_than_the_limit_are_ignored_rather_than_refused() {
    let mut cfg = Config::for_tests();
    cfg.api.max_networks = 2;
    let h = Harness::new_with(cfg).await;

    let many: Vec<(&str, i64)> = vec![
        ("aabbccddee01", -65),
        ("aabbccddee02", -70),
        ("aabbccddee03", -75),
        ("aabbccddee04", -80),
    ];
    assert_eq!(h.submit(report(35.7219, 51.3347, &many)).await, 200);
    h.drain().await;

    let (status, _) = h.locate(ask(&many)).await;
    assert_eq!(status, 200, "the request is served from the first few");
}

impl Harness {
    async fn new_with(cfg: Config) -> Self {
        Self::with_config(cfg).await
    }
}

#[tokio::test]
async fn a_key_is_refused_once_it_has_had_its_daily_allowance() {
    let cfg = Config::for_tests();
    let inner = MemoryStore::new();
    inner.put_api_key(&ApiKey { daily_quota: 3, ..ApiKey::full("small-quota") }).await.unwrap();
    let store = Arc::new(CachedStore::new(inner, MemoryCache::new(), cfg.cache.clone()));
    let state = AppState {
        store,
        queue: Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
        cfg: Arc::new(cfg.clone()),
        keys: Arc::new(KeyCache::new(&cfg.api)),
        quota: Arc::new(MemoryQuota::new()),
    };
    let server = TestServer::new(router(state)).unwrap();

    for i in 1..=3 {
        let response =
            server.post("/v1/locate").add_query_param("key", "small-quota").json(&json!({})).await;
        assert_eq!(response.status_code().as_u16(), 404, "request {i} should be served");
    }

    let refused =
        server.post("/v1/locate").add_query_param("key", "small-quota").json(&json!({})).await;
    assert_eq!(refused.status_code().as_u16(), 429);
}

#[tokio::test]
async fn a_key_without_a_quota_is_never_refused() {
    let h = Harness::new().await;
    for _ in 0..50 {
        assert_eq!(h.locate(json!({})).await.0, 404, "no allowance means no ceiling");
    }
}

#[tokio::test]
async fn a_worker_serves_its_metrics_and_nothing_else() {
    // A worker answers no requests, but what it measures lives in its own
    // process, so without this route no scrape would ever see it.
    let server = TestServer::new(worker_router()).unwrap();

    let metrics = server.get("/metrics").await;
    assert_eq!(metrics.status_code().as_u16(), 200);
    assert!(metrics.text().contains("ichnos_leases_held"));

    assert_eq!(server.get("/health").await.status_code().as_u16(), 200);
    let locate = server.post("/v1/locate").json(&json!({})).await;
    assert_eq!(locate.status_code().as_u16(), 404, "a worker answers no requests");
}

/// A store that answers, but only after a pause.
struct SlowStore {
    inner: MemoryStore,
    pause: std::time::Duration,
}

#[async_trait::async_trait]
impl Store for SlowStore {
    async fn get_stations(
        &self,
        keys: &[ichnos::model::StationKey],
    ) -> ichnos::store::Result<Vec<ichnos::model::Station>> {
        tokio::time::sleep(self.pause).await;
        self.inner.get_stations(keys).await
    }
    async fn put_stations(&self, stations: &[ichnos::model::Station]) -> ichnos::store::Result<()> {
        self.inner.put_stations(stations).await
    }
    async fn delete_station(&self, key: &ichnos::model::StationKey) -> ichnos::store::Result<()> {
        self.inner.delete_station(key).await
    }
    async fn stations_near(
        &self,
        h3: i64,
        kind: ichnos::model::key::StationKind,
    ) -> ichnos::store::Result<Vec<ichnos::model::StationKey>> {
        self.inner.stations_near(h3, kind).await
    }
    async fn get_api_key(&self, key: &str) -> ichnos::store::Result<Option<ApiKey>> {
        self.inner.get_api_key(key).await
    }
    async fn put_api_key(&self, api_key: &ApiKey) -> ichnos::store::Result<()> {
        self.inner.put_api_key(api_key).await
    }
    async fn health(&self) -> ichnos::store::Result<()> {
        self.inner.health().await
    }
}

fn server_with(cfg: Config, store: Arc<dyn Store>) -> TestServer {
    let state = AppState {
        store,
        queue: Arc::new(MemoryQueue::new(cfg.ingest.shards, 1_000)),
        cfg: Arc::new(cfg.clone()),
        keys: Arc::new(KeyCache::new(&cfg.api)),
        quota: Arc::new(MemoryQuota::new()),
    };
    TestServer::new(router(state)).unwrap()
}

#[tokio::test]
async fn a_request_slower_than_the_timeout_is_answered_with_a_503() {
    // A locate that cannot answer quickly is worth less than a fast "no".
    let mut cfg = Config::for_tests();
    cfg.server.request_timeout_ms = 50;
    let inner = MemoryStore::new();
    inner.put_api_key(&ApiKey::full("test-key")).await.unwrap();
    let pause = std::time::Duration::from_millis(500);
    let server = server_with(cfg, Arc::new(SlowStore { inner, pause }));

    let started = std::time::Instant::now();
    let response = server
        .post("/v1/locate")
        .add_query_param("key", "test-key")
        .json(&ask(&[("aabbccddeeff", -60), ("aabbccddee00", -70)]))
        .await;
    assert!(started.elapsed() < pause, "the client was held for the whole pause");
    assert_eq!(response.status_code().as_u16(), 503);
    let body = response.json::<Value>();
    assert_eq!(body["error"]["message"], "Request timed out");

    // The operator routes are not bounded, and still answer.
    assert_eq!(server.get("/health").await.status_code().as_u16(), 200);
}

#[tokio::test]
async fn a_request_inside_the_timeout_is_answered_normally() {
    let mut cfg = Config::for_tests();
    cfg.server.request_timeout_ms = 2_000;
    let inner = MemoryStore::new();
    inner.put_api_key(&ApiKey::full("test-key")).await.unwrap();
    let pause = std::time::Duration::from_millis(20);
    let server = server_with(cfg, Arc::new(SlowStore { inner, pause }));
    let response = server
        .post("/v1/locate")
        .add_query_param("key", "test-key")
        .json(&ask(&[("aabbccddeeff", -60)]))
        .await;
    assert_eq!(response.status_code().as_u16(), 404, "no answer, but not a timeout");
}

#[tokio::test]
async fn a_body_over_the_limit_is_refused_as_an_unreadable_one() {
    let mut cfg = Config::for_tests();
    cfg.api.max_body_bytes = 256;
    let inner = MemoryStore::new();
    inner.put_api_key(&ApiKey::full("test-key")).await.unwrap();
    let server = server_with(cfg, Arc::new(inner));

    let small = report(35.7, 51.3, &[("aabbccddeeff", -60)]);
    let status = |r: axum_test::TestResponse| r.status_code().as_u16();
    assert_eq!(
        status(
            server.post("/v1/observations").add_query_param("key", "test-key").json(&small).await
        ),
        200
    );

    let many: Vec<(String, i64)> = (0..20).map(|i| (format!("aabbccdd{i:04x}"), -60)).collect();
    let macs: Vec<(&str, i64)> = many.iter().map(|(m, s)| (m.as_str(), *s)).collect();
    let big = report(35.7, 51.3, &macs);
    assert!(big.to_string().len() > 256);
    for route in ["/v1/observations", "/v1/locate"] {
        let response = server.post(route).add_query_param("key", "test-key").json(&big).await;
        assert_eq!(response.status_code().as_u16(), 400, "{route}");
        assert_eq!(response.json::<Value>()["error"]["message"], "Invalid request body");
    }
}
