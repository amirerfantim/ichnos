//! What the service reports about itself.
//!
//! The metric names here are the ones an operator watches. Three matter more
//! than the rest:
//!
//! - `ichnos_stream_depth` is the early warning. It rises before anything
//!   breaks, because a worker falling behind shows up as a growing queue long
//!   before it shows up as a bad answer.
//! - `ichnos_fold_ratio` says whether the write path is doing its job. If it
//!   falls towards one, folding has stopped saving anything.
//! - `ichnos_degraded` says the read path is answering from cache alone.

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::OnceLock;

/// Every metric the service publishes.
pub struct Metrics {
    pub registry: Registry,

    pub http_requests: IntCounterVec,
    pub http_duration: HistogramVec,

    pub observations: IntCounterVec,
    pub reports_dropped: IntCounterVec,

    pub stream_depth: IntGaugeVec,
    pub stream_trimmed: IntCounter,

    pub station_writes: IntCounterVec,
    pub fold_ratio: prometheus::Gauge,
    pub db_batch_seconds: Histogram,

    pub cache_events: IntCounterVec,
    pub locate_results: IntCounterVec,
    pub locate_networks: IntCounterVec,

    pub leases_held: IntGauge,
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

        let http_duration = HistogramVec::new(
            HistogramOpts::new("ichnos_http_duration_seconds", "Time to answer a request")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
            &["route"],
        )
        .expect("valid metric");
        registry.register(Box::new(http_duration.clone())).expect("unique metric");

        let db_batch_seconds = Histogram::with_opts(
            HistogramOpts::new("ichnos_db_batch_seconds", "Time to write one folded batch")
                .buckets(vec![0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        )
        .expect("valid metric");
        registry.register(Box::new(db_batch_seconds.clone())).expect("unique metric");

        let fold_ratio = prometheus::Gauge::new(
            "ichnos_fold_ratio",
            "Observations per station in the last drained batch",
        )
        .expect("valid metric");
        registry.register(Box::new(fold_ratio.clone())).expect("unique metric");

        let stream_trimmed = IntCounter::new(
            "ichnos_stream_trimmed_total",
            "Queue entries dropped because a shard was at its cap",
        )
        .expect("valid metric");
        registry.register(Box::new(stream_trimmed.clone())).expect("unique metric");

        Self {
            http_requests: counter_vec(
                "ichnos_http_requests_total",
                "Requests answered",
                &["route", "status"],
            ),
            http_duration,
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
            stream_depth: gauge_vec(
                "ichnos_stream_depth",
                "Entries waiting in each shard",
                &["shard"],
            ),
            stream_trimmed,
            station_writes: counter_vec(
                "ichnos_station_writes_total",
                "Station updates by kind and action",
                &["type", "action"],
            ),
            fold_ratio,
            db_batch_seconds,
            cache_events: counter_vec(
                "ichnos_cache_total",
                "Cache lookups by result",
                &["result"],
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
            leases_held: gauge("ichnos_leases_held", "Shards this process owns"),
            degraded: gauge(
                "ichnos_degraded",
                "1 while the read path is answering from cache alone",
            ),
            registry,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_metric_registers_without_colliding() {
        let m = Metrics::new();
        assert!(!m.registry.gather().is_empty() || m.render().is_empty());
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
    fn the_early_warning_metrics_exist_under_the_names_operators_watch() {
        let m = Metrics::new();
        m.stream_depth.with_label_values(&["0"]).set(42);
        m.fold_ratio.set(3.5);
        let text = m.render();
        assert!(text.contains("ichnos_stream_depth"));
        assert!(text.contains("ichnos_fold_ratio 3.5"));
    }
}
