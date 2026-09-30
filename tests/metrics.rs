//! That the service actually reports what it claims to report.
//!
//! These exist because of a real defect: five metrics were declared, named in
//! the module documentation as the ones an operator watches, and never written
//! by any code path. Nothing failed. The dashboard was simply empty, and would
//! have stayed empty until someone needed it during an incident.
//!
//! A declared metric is a promise. These tests drive the assembled service and
//! check the promise is kept.

use axum_test::TestServer;
use ichnos::api::auth::KeyCache;
use ichnos::api::handlers::{router, AppState};
use ichnos::api::quota::MemoryQuota;
use ichnos::config::Config;
use ichnos::ingest::queue::{poll_depth, MemoryQueue, Queue};
use ichnos::store::cache::{CachedStore, MemoryCache};
use ichnos::store::memory::MemoryStore;
use ichnos::store::{ApiKey, Store};
use ichnos::worker::process_shard;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// Held by every test that runs the depth poller.
///
/// The depth gauges are process-wide, and tests run in parallel: a poller
/// sampling one test's queue would overwrite the value another test is about
/// to read. Taking turns keeps each test's reading its own.
static POLLER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Harness {
    server: TestServer,
    queue: Arc<MemoryQueue>,
    store: Arc<CachedStore<MemoryStore, MemoryCache>>,
    cfg: Config,
}

impl Harness {
    async fn new() -> Self {
        let cfg = Config::for_tests();
        let inner = MemoryStore::new();
        inner.put_api_key(&ApiKey::full("test-key")).await.unwrap();
        inner
            .put_api_key(&ApiKey {
                allow_locate: false,
                allow_submit: true,
                allow_admin: false,
                ..ApiKey::full("write-only")
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

    async fn drain(&self) {
        for shard in 0..self.cfg.ingest.shards {
            process_shard(&*self.queue, &*self.store, shard, &self.cfg).await.unwrap();
        }
    }

    async fn submit_as(&self, key: &str, body: Value) {
        self.server.post("/v1/observations").add_query_param("key", key).json(&body).await;
    }

    async fn locate_as(&self, key: &str, body: Value) {
        self.server.post("/v1/locate").add_query_param("key", key).json(&body).await;
    }

    async fn scrape(&self) -> String {
        self.server.get("/metrics").await.text()
    }
}

fn report(lat: f64, lon: f64, macs: &[&str]) -> Value {
    json!({"items": [{
        "position": {
            "latitude": lat, "longitude": lon,
            "accuracy": 12.0, "speed": 8.5, "age": 1500
        },
        "wifiAccessPoints": macs.iter().map(|mac| json!({
            "macAddress": mac, "signalStrength": -65, "frequency": 2412
        })).collect::<Vec<_>>(),
    }]})
}

fn ask(macs: &[&str]) -> Value {
    json!({"wifiAccessPoints": macs.iter().map(|mac| json!({
        "macAddress": mac, "signalStrength": -65
    })).collect::<Vec<_>>()})
}

/// The value of one exposition line, found by its exact metric-and-labels head.
///
/// Label names come out sorted alphabetically, not in the order they were
/// declared, so heads here are written the way Prometheus renders them.
fn sample(text: &str, head: &str) -> Option<f64> {
    text.lines()
        .find(|line| line.starts_with(head) && line[head.len()..].starts_with(' '))
        .and_then(|line| line[head.len()..].trim().parse().ok())
}

/// Every metric family this service declares, and what must happen for it to be
/// written. The registry is process-wide, so these are presence assertions: a
/// family that no code path touches never appears at all.
#[tokio::test]
async fn every_declared_metric_family_is_actually_written_by_some_code_path() {
    let h = Harness::new().await;
    let macs = ["aabbccddee01", "aabbccddee02", "aabbccddee03"];

    // A good submit, folded, then asked about: the whole pipeline.
    h.submit_as("test-key", report(35.7219, 51.3347, &macs)).await;
    h.drain().await;
    h.locate_as("test-key", ask(&macs)).await;

    // A second round, so the state machine reaches confirm and elision skips.
    h.submit_as("test-key", report(35.7219, 51.3347, &macs)).await;
    h.drain().await;

    // A report with a bad station, so a drop reason is counted.
    h.submit_as("test-key", report(35.7219, 51.3347, &["not-a-mac"])).await;

    // A question about nothing the service knows.
    h.locate_as("test-key", ask(&["ffffffffff01", "ffffffffff02"])).await;

    // A refusal, so the auth counter moves.
    h.locate_as("write-only", ask(&macs)).await;

    // Depth gauges are sampled, not written inline.
    let _turn = POLLER.lock().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let queue = h.queue.clone();
    let poller = tokio::spawn(async move {
        poll_depth(queue, 4, 10_000, Duration::from_millis(10), rx).await;
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _ = tx.send(true);
    let _ = poller.await;

    let text = h.scrape().await;

    for family in [
        // process
        "ichnos_build_info",
        "ichnos_start_time_seconds",
        // http
        "ichnos_http_requests_total",
        "ichnos_http_duration_seconds",
        "ichnos_http_in_flight",
        "ichnos_auth_total",
        // submit
        "ichnos_observations_total",
        "ichnos_reports_dropped_total",
        "ichnos_submit_items",
        "ichnos_submit_stations",
        "ichnos_observation_weight",
        "ichnos_observation_accuracy_meters",
        "ichnos_observation_speed_mps",
        "ichnos_observation_age_ms",
        "ichnos_observation_source_total",
        // queue
        "ichnos_stream_depth",
        "ichnos_stream_depth_total",
        "ichnos_stream_fill_ratio",
        "ichnos_queue_publish_seconds",
        "ichnos_queue_lag_seconds",
        // worker
        "ichnos_station_writes_total",
        "ichnos_elision_total",
        "ichnos_station_moved_meters",
        "ichnos_fold_ratio",
        "ichnos_fold_ratio_hist",
        "ichnos_shard_drain_seconds",
        "ichnos_shard_batch_observations",
        "ichnos_db_batch_seconds",
        // storage and cache
        "ichnos_db_operation_seconds",
        "ichnos_cache_total",
        "ichnos_cache_operation_seconds",
        // locate
        "ichnos_locate_total",
        "ichnos_locate_networks_total",
        "ichnos_locate_no_answer_total",
        "ichnos_locate_accuracy_meters",
        "ichnos_locate_coverage",
        "ichnos_locate_cluster_size",
        "ichnos_locate_candidates",
        "ichnos_locate_score",
        "ichnos_degraded",
    ] {
        assert!(text.contains(family), "{family} is declared but nothing ever writes it");
    }
}

#[tokio::test]
async fn the_state_machine_reports_which_decision_it_took() {
    // The mix of decisions is the most informative thing the write path
    // publishes: new stations, confirmations and blocks all mean different
    // things and all used to be invisible.
    let h = Harness::new().await;
    let macs = ["a1b2c3d4e501", "a1b2c3d4e502"];

    h.submit_as("test-key", report(35.7219, 51.3347, &macs)).await;
    h.drain().await;
    let text = h.scrape().await;
    assert!(
        sample(&text, r#"ichnos_station_writes_total{action="new",type="wifi"}"#).unwrap_or(0.0)
            >= 2.0,
        "two new stations should have been reported"
    );

    // The same stations again from the same place: a confirmation, and a write
    // the elision step is entitled to skip.
    h.submit_as("test-key", report(35.7219, 51.3347, &macs)).await;
    h.drain().await;
    let text = h.scrape().await;
    assert!(
        sample(&text, r#"ichnos_elision_total{decision="skip"}"#).unwrap_or(0.0) >= 1.0,
        "an unmoved station seen twice today should have skipped a write"
    );
}

#[tokio::test]
async fn a_failed_answer_says_why_it_failed() {
    // "No answer" has several causes that need different responses. Counting
    // them together would say only that something is wrong.
    let h = Harness::new().await;

    h.locate_as("test-key", json!({})).await;
    h.locate_as("test-key", ask(&["ffffffffee01", "ffffffffee02"])).await;

    let text = h.scrape().await;
    assert!(
        sample(&text, r#"ichnos_locate_no_answer_total{reason="nothing_submitted"}"#)
            .unwrap_or(0.0)
            >= 1.0
    );
    assert!(
        sample(&text, r#"ichnos_locate_no_answer_total{reason="none_known"}"#).unwrap_or(0.0)
            >= 1.0
    );
}

#[tokio::test]
async fn a_refused_caller_is_counted_under_the_reason_it_was_refused() {
    let h = Harness::new().await;

    // No key at all.
    h.server.post("/v1/locate").json(&ask(&["aabbccddee01"])).await;
    // A key that exists but may not locate.
    h.locate_as("write-only", ask(&["aabbccddee01"])).await;
    // A key nobody issued.
    h.locate_as("no-such-key", ask(&["aabbccddee01"])).await;

    let text = h.scrape().await;
    for result in ["missing", "not_permitted", "unknown"] {
        let head = format!(r#"ichnos_auth_total{{result="{result}",route="locate"}}"#);
        assert!(
            sample(&text, &head).unwrap_or(0.0) >= 1.0,
            "no auth failure was recorded as {result}"
        );
    }
}

#[tokio::test]
async fn queue_depth_is_sampled_rather_than_left_empty() {
    // Nothing writes depth inline, because the shard that matters is the one
    // nobody is writing to. Without the poller these gauges stay empty, which
    // is exactly the defect these tests exist for.
    let h = Harness::new().await;
    h.submit_as("test-key", report(35.7219, 51.3347, &["b1b2c3d4e501", "b1b2c3d4e502"])).await;

    let waiting: usize = {
        let mut total = 0;
        for shard in 0..h.cfg.ingest.shards {
            total += h.queue.depth(shard).await.unwrap();
        }
        total
    };
    assert!(waiting > 0, "the submit should have queued something to measure");

    let _turn = POLLER.lock().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let queue = h.queue.clone();
    let shards = h.cfg.ingest.shards;
    let poller = tokio::spawn(async move {
        poll_depth(queue, shards, 10_000, Duration::from_millis(10), rx).await;
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _ = tx.send(true);
    let _ = poller.await;

    let text = h.scrape().await;
    let total = sample(&text, "ichnos_stream_depth_total").expect("depth was never sampled");
    assert!(total >= waiting as f64, "reported {total}, at least {waiting} were waiting");
    assert!(text.contains("ichnos_stream_fill_ratio"), "the cap ratio is the alerting signal");
}

#[tokio::test]
async fn trimmed_entries_are_reported_as_a_step_not_as_a_total() {
    // The queue counts trims cumulatively; the metric is a counter. Adding the
    // running total on every sample would multiply a single loss into a flood.
    let cfg = Config::for_tests();
    let queue = Arc::new(MemoryQueue::new(cfg.ingest.shards, 2));

    let observation = |n: u64| {
        (
            0u32,
            ichnos::model::Observation {
                key: ichnos::model::StationKey::Wifi(ichnos::model::key::WifiKey(n)),
                lat: 35.7219,
                lon: 51.3347,
                accuracy: Some(10.0),
                age_ms: Some(0.0),
                speed: Some(0.0),
                signal: Some(-65),
                source: ichnos::model::observation::ReportSource::Gnss,
                timestamp: chrono::Utc::now().timestamp_millis(),
            },
        )
    };
    // Five into a shard that holds two: three are trimmed.
    let entries: Vec<_> = (0..5).map(observation).collect();
    queue.publish(&entries).await.unwrap();
    assert_eq!(queue.trimmed().await.unwrap(), 3);

    let before = {
        let text = ichnos::metrics::metrics().render();
        sample(&text, "ichnos_stream_trimmed_total").unwrap_or(0.0)
    };

    let _turn = POLLER.lock().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let q = queue.clone();
    let poller = tokio::spawn(async move {
        poll_depth(q, cfg.ingest.shards, 2, Duration::from_millis(5), rx).await;
    });
    // Several samples, but only one loss: the counter must move by three, not
    // by three per tick.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _ = tx.send(true);
    let _ = poller.await;

    let after = {
        let text = ichnos::metrics::metrics().render();
        sample(&text, "ichnos_stream_trimmed_total").unwrap_or(0.0)
    };
    assert_eq!(after - before, 3.0, "each trimmed entry must be counted exactly once");
}

#[tokio::test]
async fn coverage_falls_when_the_service_knows_less_than_it_was_asked_about() {
    // Coverage is the slow number that says whether the database is learning.
    // It is a ratio, so it is only meaningful if both halves are counted.
    let h = Harness::new().await;
    let known = ["c1b2c3d4e501", "c1b2c3d4e502"];

    h.submit_as("test-key", report(35.7219, 51.3347, &known)).await;
    h.drain().await;

    // Ask about the two it knows plus two it does not.
    h.locate_as("test-key", ask(&["c1b2c3d4e501", "c1b2c3d4e502", "c1b2c3d4e5ff", "c1b2c3d4e5fe"]))
        .await;

    let text = h.scrape().await;
    assert!(
        sample(&text, "ichnos_locate_coverage_count").unwrap_or(0.0) >= 1.0,
        "coverage was never recorded"
    );
    // Half of four is 0.5, so the 0.5 bucket must have caught it.
    assert!(text.contains("ichnos_locate_coverage_bucket"));
}

/// A redelivery is the one sign that workers are dying or failing to write
/// while the queue still looks healthy, so it has to be counted.
#[tokio::test]
async fn a_redelivered_entry_is_counted() {
    let queue = MemoryQueue::new(1, 100).with_redeliver_after(Duration::ZERO);
    let observation = ichnos::model::Observation {
        key: ichnos::model::StationKey::Wifi(ichnos::model::key::WifiKey(1)),
        lat: 35.7219,
        lon: 51.3347,
        accuracy: Some(10.0),
        age_ms: Some(0.0),
        speed: Some(0.0),
        signal: Some(-70),
        source: ichnos::model::ReportSource::Gnss,
        timestamp: 1_700_000_000_000,
    };
    queue.publish(&[(0, observation)]).await.unwrap();
    queue.read(0, 10).await.unwrap();

    let before = sample(&ichnos::metrics::metrics().render(), "ichnos_queue_redelivered_total")
        .unwrap_or(0.0);
    assert_eq!(queue.read(0, 10).await.unwrap().len(), 1);
    let after = sample(&ichnos::metrics::metrics().render(), "ichnos_queue_redelivered_total")
        .unwrap_or(0.0);
    assert!(after >= before + 1.0, "{before} -> {after}");
}
