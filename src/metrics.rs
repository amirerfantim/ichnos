//! What the service reports about itself.
//!
//! The metrics fall into three groups, and the difference matters when you
//! build a dashboard:
//!
//! - **Health**: is the service up and keeping up. Requests, latency, queue
//!   depth, error counts. These tell you to wake someone.
//! - **Behaviour**: what the pipeline is doing with the traffic. Fold ratio,
//!   elision rate, the state machine's decisions. These tell you *why* the
//!   health numbers moved.
//! - **Quality**: how good the answers are. Returned accuracy, coverage,
//!   disagreement between candidates. These move slowly and no alert fires on
//!   them, but they are the only numbers that say whether the service is
//!   getting better or worse at its actual job.
//!
//! Four are worth putting on the top row of any dashboard:
//!
//! - `ichnos_stream_fill_ratio` is the early warning. Depth alone means
//!   nothing without the cap it is measured against; this is the ratio, and it
//!   rises before anything breaks.
//! - `ichnos_stream_trimmed_total` is data loss. It should be flat at zero.
//! - `ichnos_locate_coverage` says whether the database is learning. If it
//!   falls, answers get worse before any error appears.
//! - `ichnos_degraded` says the read path is answering from cache alone.

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::OnceLock;

/// Bucket sets, named so the intent is visible at the call site.
mod buckets {
    /// Sub-millisecond to a second: cache and queue round trips.
    pub const FAST_SECONDS: &[f64] =
        &[0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0];
    /// A single database operation.
    pub const DB_SECONDS: &[f64] =
        &[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];
    /// A whole request.
    pub const REQUEST_SECONDS: &[f64] =
        &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];
    /// A batch of work: draining a shard, writing a fold.
    pub const BATCH_SECONDS: &[f64] = &[0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
    /// How long an observation waited in the queue.
    pub const LAG_SECONDS: &[f64] = &[1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0];

    /// The accuracy of an answer we gave out.
    pub const ANSWER_METERS: &[f64] =
        &[10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 50000.0];
    /// The accuracy a client claimed for its own fix.
    pub const CLAIMED_METERS: &[f64] = &[5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0];
    /// How far a station's recorded position moved in one update.
    pub const MOVED_METERS: &[f64] =
        &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0, 50000.0];
    /// How far apart two competing answers were. The top bucket is wide on
    /// purpose: a spoofed report lands hundreds of kilometres out.
    pub const DISAGREE_METERS: &[f64] =
        &[10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 50000.0, 500000.0];

    /// A reporter's speed. The first bucket is the parked vehicle.
    pub const SPEED_MPS: &[f64] = &[0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 50.0];
    /// How stale a scan was when it was reported.
    pub const AGE_MS: &[f64] = &[100.0, 500.0, 1000.0, 2000.0, 4000.0, 8000.0, 16000.0, 20000.0];
    /// The weight one observation earned.
    pub const WEIGHT: &[f64] = &[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0];
    /// A station's trust score.
    pub const SCORE: &[f64] = &[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0];

    /// A fraction from none to all.
    pub const RATIO: &[f64] = &[0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
    /// Small counts: stations in a cluster, candidates in a race.
    pub const SMALL_COUNT: &[f64] = &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 40.0];
    /// Items in one request or one batch.
    pub const BATCH_COUNT: &[f64] = &[1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0];
    /// Observations per station in one fold. One means folding saved nothing.
    pub const FOLD: &[f64] = &[1.0, 1.5, 2.0, 3.0, 5.0, 8.0, 12.0, 20.0, 50.0];
}

/// Every metric the service publishes.
pub struct Metrics {
    pub registry: Registry,

    // ------------------------------------------------------------ process ---
    /// Always 1. Carries the version in its labels, so a dashboard can show
    /// what is running and an alert can catch a half-finished rollout.
    pub build_info: IntGaugeVec,
    pub start_time: IntGauge,

    // --------------------------------------------------------------- http ---
    pub http_requests: IntCounterVec,
    pub http_duration: HistogramVec,
    pub http_in_flight: IntGaugeVec,
    /// Why a caller was turned away, or that it was not.
    pub auth_results: IntCounterVec,
    pub quota_results: IntCounterVec,

    // ------------------------------------------------------------- submit ---
    pub observations: IntCounterVec,
    pub reports_dropped: IntCounterVec,
    pub submit_items: Histogram,
    pub submit_stations: Histogram,
    /// What the incoming data looks like. Off by default on the hot path.
    pub observation_weight: Histogram,
    pub observation_accuracy: Histogram,
    pub observation_speed: Histogram,
    pub observation_age: Histogram,
    pub observation_source: IntCounterVec,

    // -------------------------------------------------------------- queue ---
    pub stream_depth: IntGaugeVec,
    pub stream_depth_total: IntGauge,
    /// Depth against the cap. This is the number to alert on.
    pub stream_fill_ratio: Gauge,
    pub stream_trimmed: IntCounter,
    pub queue_publish_seconds: Histogram,
    pub queue_errors: IntCounterVec,
    pub queue_redelivered: IntCounter,
    /// How long the oldest observation in a drained batch had waited.
    pub queue_lag_seconds: Histogram,

    // ------------------------------------------------------------- worker ---
    pub station_writes: IntCounterVec,
    pub elision: IntCounterVec,
    pub station_moved: Histogram,
    pub fold_ratio: Gauge,
    pub fold_ratio_hist: Histogram,
    pub shard_drain_seconds: Histogram,
    pub shard_batch_observations: Histogram,
    pub worker_errors: IntCounterVec,
    pub db_batch_seconds: Histogram,

    // ---------------------------------------------------------- ownership ---
    pub leases_held: IntGauge,
    pub leases_target: IntGauge,
    pub workers_live: IntGauge,
    /// claimed / released / lost / renewed. `lost` is the failover signal.
    pub lease_events: IntCounterVec,

    // ------------------------------------------------------------ storage ---
    pub db_operation_seconds: HistogramVec,
    pub db_errors: IntCounterVec,
    pub db_rows: IntCounterVec,

    // -------------------------------------------------------------- cache ---
    pub cache_events: IntCounterVec,
    pub cache_operation_seconds: HistogramVec,

    // ------------------------------------------------------------- locate ---
    pub locate_results: IntCounterVec,
    pub locate_networks: IntCounterVec,
    /// Why no answer came back. The reason is the whole value of this metric.
    pub locate_no_answer: IntCounterVec,
    /// What became of each answer offered for learning: queued, sampled out,
    /// nothing new in it, nothing valid in it, or lost to the queue.
    pub locate_learned: IntCounterVec,
    pub locate_accuracy: HistogramVec,
    /// Known stations over submitted stations: is the database learning?
    pub locate_coverage: Histogram,
    pub locate_cluster_size: HistogramVec,
    pub locate_candidates: Histogram,
    pub locate_score: HistogramVec,
    /// Distance between the winning answer and the runner-up. A large value
    /// means the evidence pointed two ways at once.
    pub locate_disagreement: Histogram,
    /// Stations that answered but were last confirmed before today.
    pub locate_stale_networks: IntCounter,

    pub degraded: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let counter_vec = |name: &str, help: &str, labels: &[&str]| {
            let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
            registry.register(Box::new(c.clone())).expect("unique metric");
            c
        };
        let counter = |name: &str, help: &str| {
            let c = IntCounter::new(name, help).expect("valid metric");
            registry.register(Box::new(c.clone())).expect("unique metric");
            c
        };
        let gauge_vec = |name: &str, help: &str, labels: &[&str]| {
            let g = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
            registry.register(Box::new(g.clone())).expect("unique metric");
            g
        };
        let gauge = |name: &str, help: &str| {
            let g = IntGauge::new(name, help).expect("valid metric");
            registry.register(Box::new(g.clone())).expect("unique metric");
            g
        };
        let float_gauge = |name: &str, help: &str| {
            let g = Gauge::new(name, help).expect("valid metric");
            registry.register(Box::new(g.clone())).expect("unique metric");
            g
        };
        let histogram = |name: &str, help: &str, b: &[f64]| {
            let h = Histogram::with_opts(HistogramOpts::new(name, help).buckets(b.to_vec()))
                .expect("valid metric");
            registry.register(Box::new(h.clone())).expect("unique metric");
            h
        };
        let histogram_vec = |name: &str, help: &str, b: &[f64], labels: &[&str]| {
            let h = HistogramVec::new(HistogramOpts::new(name, help).buckets(b.to_vec()), labels)
                .expect("valid metric");
            registry.register(Box::new(h.clone())).expect("unique metric");
            h
        };

        let me = Self {
            build_info: gauge_vec(
                "ichnos_build_info",
                "Always 1; the labels carry what is running",
                &["version"],
            ),
            start_time: gauge(
                "ichnos_start_time_seconds",
                "Unix time this process started, for uptime",
            ),

            http_requests: counter_vec(
                "ichnos_http_requests_total",
                "Requests answered",
                &["route", "status"],
            ),
            http_duration: histogram_vec(
                "ichnos_http_duration_seconds",
                "Time to answer a request",
                buckets::REQUEST_SECONDS,
                &["route"],
            ),
            http_in_flight: gauge_vec(
                "ichnos_http_in_flight",
                "Requests being handled right now",
                &["route"],
            ),
            auth_results: counter_vec(
                "ichnos_auth_total",
                "Key checks by outcome",
                &["route", "result"],
            ),
            quota_results: counter_vec(
                "ichnos_quota_total",
                "Allowance checks by outcome",
                &["route", "result"],
            ),

            observations: counter_vec(
                "ichnos_observations_total",
                "Station observations by kind and outcome",
                &["type", "outcome"],
            ),
            reports_dropped: counter_vec(
                "ichnos_reports_dropped_total",
                "Reports and stations rejected, by reason",
                &["reason"],
            ),
            submit_items: histogram(
                "ichnos_submit_items",
                "Reports carried by one submit request",
                buckets::BATCH_COUNT,
            ),
            submit_stations: histogram(
                "ichnos_submit_stations",
                "Stations carried by one report",
                buckets::SMALL_COUNT,
            ),
            observation_weight: histogram(
                "ichnos_observation_weight",
                "Weight earned by one kept observation",
                buckets::WEIGHT,
            ),
            observation_accuracy: histogram(
                "ichnos_observation_accuracy_meters",
                "Accuracy a client claimed for its own position",
                buckets::CLAIMED_METERS,
            ),
            observation_speed: histogram(
                "ichnos_observation_speed_mps",
                "Reporter speed; the lowest bucket is a stationary vehicle",
                buckets::SPEED_MPS,
            ),
            observation_age: histogram(
                "ichnos_observation_age_ms",
                "How stale a scan was when it was reported",
                buckets::AGE_MS,
            ),
            observation_source: counter_vec(
                "ichnos_observation_source_total",
                "Kept observations by what produced the position",
                &["source"],
            ),

            stream_depth: gauge_vec(
                "ichnos_stream_depth",
                "Entries waiting in each shard",
                &["shard"],
            ),
            stream_depth_total: gauge(
                "ichnos_stream_depth_total",
                "Entries waiting across every shard",
            ),
            stream_fill_ratio: float_gauge(
                "ichnos_stream_fill_ratio",
                "Fullest shard as a fraction of its cap; 1.0 means data is being dropped",
            ),
            stream_trimmed: counter(
                "ichnos_stream_trimmed_total",
                "Queue entries dropped because a shard was at its cap",
            ),
            queue_publish_seconds: histogram(
                "ichnos_queue_publish_seconds",
                "Time to hand one batch of observations to the queue",
                buckets::FAST_SECONDS,
            ),
            queue_errors: counter_vec(
                "ichnos_queue_errors_total",
                "Queue operations that failed, by operation",
                &["op"],
            ),
            queue_redelivered: counter(
                "ichnos_queue_redelivered_total",
                "Queue entries taken again after sitting unacknowledged; a steady rise means \
                 workers are dying or failing to write",
            ),
            queue_lag_seconds: histogram(
                "ichnos_queue_lag_seconds",
                "Age of the oldest observation in a drained batch",
                buckets::LAG_SECONDS,
            ),

            station_writes: counter_vec(
                "ichnos_station_writes_total",
                "Station updates by kind and what the state machine decided",
                &["type", "action"],
            ),
            elision: counter_vec(
                "ichnos_elision_total",
                "Folded updates by whether they needed a write",
                &["decision"],
            ),
            station_moved: histogram(
                "ichnos_station_moved_meters",
                "How far a station's recorded position moved in one update",
                buckets::MOVED_METERS,
            ),
            fold_ratio: float_gauge(
                "ichnos_fold_ratio",
                "Observations per station in the last drained batch",
            ),
            fold_ratio_hist: histogram(
                "ichnos_fold_ratio_hist",
                "Observations per station, over every drained batch",
                buckets::FOLD,
            ),
            shard_drain_seconds: histogram(
                "ichnos_shard_drain_seconds",
                "Time to drain, fold and write one batch from one shard",
                buckets::BATCH_SECONDS,
            ),
            shard_batch_observations: histogram(
                "ichnos_shard_batch_observations",
                "Observations taken in one read from one shard",
                buckets::BATCH_COUNT,
            ),
            worker_errors: counter_vec(
                "ichnos_worker_errors_total",
                "Worker failures by which stage failed",
                &["stage"],
            ),
            db_batch_seconds: histogram(
                "ichnos_db_batch_seconds",
                "Time to write one folded batch",
                buckets::BATCH_SECONDS,
            ),

            leases_held: gauge("ichnos_leases_held", "Shards this process owns"),
            leases_target: gauge(
                "ichnos_leases_target",
                "Shards this process should own; a gap from held means a rebalance is running",
            ),
            workers_live: gauge(
                "ichnos_workers_live",
                "Workers that have announced themselves recently",
            ),
            lease_events: counter_vec(
                "ichnos_lease_events_total",
                "Shard ownership changes; 'lost' means another worker took over",
                &["event"],
            ),

            db_operation_seconds: histogram_vec(
                "ichnos_db_operation_seconds",
                "Time for one storage operation",
                buckets::DB_SECONDS,
                &["op"],
            ),
            db_errors: counter_vec(
                "ichnos_db_errors_total",
                "Storage operations that failed, by operation",
                &["op"],
            ),
            db_rows: counter_vec(
                "ichnos_db_rows_total",
                "Rows read from or written to storage",
                &["op"],
            ),

            cache_events: counter_vec("ichnos_cache_total", "Cache lookups by result", &["result"]),
            cache_operation_seconds: histogram_vec(
                "ichnos_cache_operation_seconds",
                "Time for one cache operation",
                buckets::FAST_SECONDS,
                &["op"],
            ),

            locate_results: counter_vec(
                "ichnos_locate_total",
                "Locate requests by which source answered",
                &["source"],
            ),
            locate_networks: counter_vec(
                "ichnos_locate_networks_total",
                "Networks seen on the locate path, by stage",
                &["type", "stage"],
            ),
            locate_no_answer: counter_vec(
                "ichnos_locate_no_answer_total",
                "Locate requests that returned nothing, by why",
                &["reason"],
            ),
            locate_learned: counter_vec(
                "ichnos_locate_learned_total",
                "Answered locates offered for learning, by what became of them",
                &["outcome"],
            ),
            locate_accuracy: histogram_vec(
                "ichnos_locate_accuracy_meters",
                "Accuracy of the answers actually returned",
                buckets::ANSWER_METERS,
                &["source"],
            ),
            locate_coverage: histogram(
                "ichnos_locate_coverage",
                "Share of submitted networks the service already knew",
                buckets::RATIO,
            ),
            locate_cluster_size: histogram_vec(
                "ichnos_locate_cluster_size",
                "Stations behind the answer that was returned",
                buckets::SMALL_COUNT,
                &["source"],
            ),
            locate_candidates: histogram(
                "ichnos_locate_candidates",
                "Competing candidate positions per request",
                buckets::SMALL_COUNT,
            ),
            locate_score: histogram_vec(
                "ichnos_locate_score",
                "Combined trust behind the answer that was returned",
                buckets::SCORE,
                &["source"],
            ),
            locate_disagreement: histogram(
                "ichnos_locate_disagreement_meters",
                "Distance between the winning candidate and the runner-up",
                buckets::DISAGREE_METERS,
            ),
            locate_stale_networks: counter(
                "ichnos_locate_stale_networks_total",
                "Stations used in an answer that were last confirmed before today",
            ),

            degraded: gauge(
                "ichnos_degraded",
                "1 while the read path is answering from cache alone",
            ),
            registry,
        };

        me.build_info.with_label_values(&[env!("CARGO_PKG_VERSION")]).set(1);
        me.start_time.set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        );
        me
    }

    /// The Prometheus exposition text.
    pub fn render(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        if encoder.encode(&self.registry.gather(), &mut buffer).is_err() {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// The process-wide metrics.
///
/// A single registry, because Prometheus refuses duplicate metric names and a
/// second registry would silently publish nothing.
pub fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::new)
}

/// Times a block and records it, whatever the block returns.
///
/// Used rather than a guard type because several call sites need the elapsed
/// time to land on a different metric depending on whether the call failed.
pub struct Timer(std::time::Instant);

impl Timer {
    pub fn start() -> Self {
        Self(std::time::Instant::now())
    }

    pub fn seconds(&self) -> f64 {
        self.0.elapsed().as_secs_f64()
    }

    /// Record the elapsed time on a labelled histogram.
    pub fn observe(self, histogram: &HistogramVec, label: &str) {
        histogram.with_label_values(&[label]).observe(self.seconds());
    }

    /// Record the elapsed time on an unlabelled histogram.
    pub fn observe_on(self, histogram: &Histogram) {
        histogram.observe(self.seconds());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_metric_registers_without_colliding() {
        // Registration panics on a duplicate name, so simply building the set
        // is the assertion. Gathering proves the registry is not empty.
        let m = Metrics::new();
        assert!(!m.registry.gather().is_empty());
    }

    #[test]
    fn counters_appear_in_the_exposition_once_used() {
        let m = Metrics::new();
        m.http_requests.with_label_values(&["locate", "200"]).inc();
        m.observations.with_label_values(&["wifi", "queued"]).inc_by(5);

        let text = m.render();
        assert!(text.contains("ichnos_http_requests_total"));
        assert!(text.contains(r#"route="locate""#));
        assert!(text.contains("ichnos_observations_total"));
    }

    #[test]
    fn gauges_can_go_up_and_down() {
        let m = Metrics::new();
        m.degraded.set(1);
        assert!(m.render().contains("ichnos_degraded 1"));
        m.degraded.set(0);
        assert!(m.render().contains("ichnos_degraded 0"));
    }

    #[test]
    fn histograms_record_observations() {
        let m = Metrics::new();
        m.db_batch_seconds.observe(0.02);
        let text = m.render();
        assert!(text.contains("ichnos_db_batch_seconds_bucket"));
        assert!(text.contains("ichnos_db_batch_seconds_count 1"));
    }

    #[test]
    fn the_process_wide_registry_is_one_instance() {
        // Two registries would mean one of them silently publishes nothing.
        assert!(std::ptr::eq(metrics(), metrics()));
    }

    #[test]
    fn the_build_version_is_published_without_being_asked() {
        // A dashboard showing the wrong version during a rollout is worse than
        // no version at all, so this is set at construction.
        let text = Metrics::new().render();
        assert!(text.contains("ichnos_build_info"));
        assert!(text.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn the_process_reports_when_it_started() {
        let m = Metrics::new();
        assert!(m.start_time.get() > 1_600_000_000, "expected a plausible unix time");
    }

    #[test]
    fn the_four_headline_metrics_exist_under_the_names_operators_watch() {
        let m = Metrics::new();
        m.stream_fill_ratio.set(0.25);
        m.stream_trimmed.inc();
        m.locate_coverage.observe(0.8);
        m.degraded.set(0);

        let text = m.render();
        assert!(text.contains("ichnos_stream_fill_ratio 0.25"));
        assert!(text.contains("ichnos_stream_trimmed_total 1"));
        assert!(text.contains("ichnos_locate_coverage_bucket"));
        assert!(text.contains("ichnos_degraded 0"));
    }

    #[test]
    fn the_spoof_distance_bucket_reaches_far_enough_to_catch_one() {
        // Reports from the known spoof cluster land a few hundred kilometres
        // away. A histogram that tops out below that would show them as an
        // indistinguishable overflow.
        let m = Metrics::new();
        m.locate_disagreement.observe(470_000.0);
        let text = m.render();
        assert!(text.contains(r#"ichnos_locate_disagreement_meters_bucket{le="500000"} 1"#));
    }

    #[test]
    fn the_speed_histogram_separates_a_parked_vehicle_from_a_moving_one() {
        let m = Metrics::new();
        m.observation_speed.observe(0.0);
        m.observation_speed.observe(12.0);
        let text = m.render();
        assert!(text.contains(r#"ichnos_observation_speed_mps_bucket{le="0.5"} 1"#));
        assert!(text.contains(r#"ichnos_observation_speed_mps_bucket{le="20"} 2"#));
    }

    #[test]
    fn a_timer_records_onto_the_histogram_it_is_given() {
        let m = Metrics::new();
        Timer::start().observe(&m.db_operation_seconds, "get");
        assert!(m.render().contains(r#"ichnos_db_operation_seconds_count{op="get"} 1"#));
    }
}
