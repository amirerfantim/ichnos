//! Replaying recorded reports through the whole pipeline, offline.
//!
//! This exists because the alternative is deploying a change and waiting weeks
//! to see what it did. Replay runs validation, folding, the state machine and
//! elision exactly as production does, against an in-memory store, so a week of
//! traffic takes seconds and the answer is deterministic.
//!
//! The output is a station table. Two runs with different settings produce two
//! tables that can be compared directly.

use crate::api::schema::SubmitRequest;
use crate::config::Config;
use crate::ingest::queue::{MemoryQueue, Queue};
use crate::ingest::validate::validate_item;
use crate::model::Observation;
use crate::store::memory::MemoryStore;
use crate::worker::process_shard;
use anyhow::{Context, Result};
use std::io::Write;
use std::path::Path;

/// What a replay run did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReplayReport {
    pub reports_read: usize,
    pub reports_rejected: usize,
    pub observations: usize,
    pub stations: usize,
    pub written: usize,
    pub skipped: usize,
    pub blocked: usize,
}

impl ReplayReport {
    pub fn fold_ratio(&self) -> f64 {
        crate::ingest::fold::fold_ratio(self.observations, self.stations)
    }

    pub fn elision_rate(&self) -> f64 {
        let updates = self.written + self.skipped;
        if updates == 0 {
            return 0.0;
        }
        self.skipped as f64 / updates as f64
    }
}

/// Replay newline-delimited submit bodies and return the resulting stations.
pub async fn replay_lines(
    lines: impl Iterator<Item = String>,
    cfg: &Config,
) -> Result<(ReplayReport, MemoryStore)> {
    let store = MemoryStore::new();
    let queue = MemoryQueue::new(cfg.ingest.shards, usize::MAX);
    let mut report = ReplayReport::default();

    for line in lines {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        report.reports_read += 1;

        let Ok(request) = serde_json::from_str::<SubmitRequest>(&line) else {
            report.reports_rejected += 1;
            continue;
        };

        let mut routed: Vec<(u32, Observation)> = Vec::new();
        for item in &request.items {
            let validated = validate_item(item, &cfg.validation);
            for observation in validated.observations {
                let shard = observation.key.shard(cfg.ingest.shards);
                routed.push((shard, observation));
            }
        }
        report.observations += routed.len();
        queue.publish(&routed).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    // Drain every shard until nothing is left, so the outcome does not depend
    // on how the input happened to be batched.
    loop {
        let mut moved = false;
        for shard in 0..cfg.ingest.shards {
            let outcome = process_shard(&queue, &store, shard, cfg)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            if outcome.observations > 0 {
                moved = true;
                report.stations += outcome.stations;
                report.written += outcome.written;
                report.skipped += outcome.skipped;
                report.blocked += outcome.blocked;
            }
        }
        if !moved {
            break;
        }
    }

    Ok((report, store))
}

/// Replay a file and optionally write the resulting station table out.
pub async fn run(cfg: &Config, input: &Path, output: Option<&Path>) -> Result<()> {
    let text = std::fs::read_to_string(input).with_context(|| format!("reading {input:?}"))?;
    let (report, store) = replay_lines(text.lines().map(str::to_string), cfg).await?;

    if let Some(path) = output {
        let mut file =
            std::fs::File::create(path).with_context(|| format!("creating {path:?}"))?;
        for station in store.snapshot() {
            writeln!(file, "{}", serde_json::to_string(&station)?)?;
        }
    }

    println!(
        "reports {} ({} rejected) -> {} observations -> {} station updates\n\
         written {}, skipped {}, blocked {}\n\
         fold ratio {:.2}x, elision {:.0}%, stations held {}",
        report.reports_read,
        report.reports_rejected,
        report.observations,
        report.stations,
        report.written,
        report.skipped,
        report.blocked,
        report.fold_ratio(),
        report.elision_rate() * 100.0,
        store.len(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(mac: &str, lat: f64, lon: f64) -> String {
        format!(
            r#"{{"items":[{{"position":{{"latitude":{lat},"longitude":{lon},"accuracy":10}},
               "wifiAccessPoints":[{{"macAddress":"{mac}","signalStrength":-70,"frequency":2412}}]}}]}}"#
        )
    }

    #[tokio::test]
    async fn a_replayed_report_produces_a_station() {
        let cfg = Config::for_tests();
        let (report, store) =
            replay_lines(vec![body("aabbccddeeff", 35.7219, 51.3347)].into_iter(), &cfg)
                .await
                .unwrap();

        assert_eq!(report.reports_read, 1);
        assert_eq!(report.observations, 1);
        assert_eq!(report.stations, 1);
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn replaying_the_same_input_twice_gives_the_same_table() {
        // Determinism is the whole point: two runs with different settings must
        // differ only because of the settings.
        let cfg = Config::for_tests();
        let lines: Vec<String> =
            (0..50).map(|i| body(&format!("aabbccdd{i:04x}"), 35.72, 51.33)).collect();

        let (report_a, store_a) = replay_lines(lines.clone().into_iter(), &cfg).await.unwrap();
        let (report_b, store_b) = replay_lines(lines.into_iter(), &cfg).await.unwrap();

        assert_eq!(report_a, report_b);
        let keys_a: Vec<_> = store_a.snapshot().into_iter().map(|s| s.key).collect();
        let keys_b: Vec<_> = store_b.snapshot().into_iter().map(|s| s.key).collect();
        assert_eq!(keys_a, keys_b);
    }

    #[tokio::test]
    async fn repeated_sightings_fold_and_the_ratio_says_so() {
        let cfg = Config::for_tests();
        let lines: Vec<String> =
            (0..20).map(|_| body("aabbccddeeff", 35.7219, 51.3347)).collect();
        let (report, store) = replay_lines(lines.into_iter(), &cfg).await.unwrap();

        assert_eq!(report.observations, 20);
        assert_eq!(store.len(), 1);
        assert!(report.fold_ratio() > 1.0, "folding should have saved work");
    }

    #[tokio::test]
    async fn a_malformed_line_is_counted_and_skipped() {
        let cfg = Config::for_tests();
        let lines = vec![
            body("aabbccddeeff", 35.7219, 51.3347),
            "this is not json".to_string(),
            String::new(),
        ];
        let (report, _) = replay_lines(lines.into_iter(), &cfg).await.unwrap();

        assert_eq!(report.reports_read, 2, "the blank line is not a report");
        assert_eq!(report.reports_rejected, 1);
        assert_eq!(report.observations, 1);
    }

    #[tokio::test]
    async fn changing_a_setting_changes_the_outcome_measurably() {
        // This is what the harness is for: run the same traffic twice and read
        // the difference off the report.
        let lines: Vec<String> =
            (0..10).map(|_| body("aabbccddeeff", 35.7219, 51.3347)).collect();

        let mut with_elision = Config::for_tests();
        with_elision.elision.enabled = true;
        let (a, _) = replay_lines(lines.clone().into_iter(), &with_elision).await.unwrap();

        let mut without = Config::for_tests();
        without.elision.enabled = false;
        let (b, _) = replay_lines(lines.into_iter(), &without).await.unwrap();

        assert_eq!(a.observations, b.observations, "the same input either way");
        assert!(a.written <= b.written, "elision should never write more");
    }

    #[tokio::test]
    async fn an_empty_input_replays_to_nothing() {
        let cfg = Config::for_tests();
        let (report, store) = replay_lines(std::iter::empty(), &cfg).await.unwrap();
        assert_eq!(report, ReplayReport::default());
        assert!(store.is_empty());
    }
}
