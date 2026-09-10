//! How much a station's stored position is trusted.
//!
//! Ported from Ichnaea's `api/locate/score.py`. The score decides which
//! stations survive the cut into a cluster and how heavily each one pulls on
//! the fitted position, so it shapes every answer the service gives.

use crate::config::Score as ScoreCfg;
use crate::model::Station;
use chrono::{DateTime, NaiveDate, Utc};

/// Trustworthiness of a station, as a positive number with no upper bound in
/// principle and about 10 in practice.
///
/// Three factors multiplied:
///
/// - **age**: how long since the record was last written. A record touched this
///   month counts fully; a two-year-old one counts a fifth.
/// - **collection**: how long the station has been observed for. Something seen
///   on one day only could be a passing phone pretending to be an access point;
///   something seen over ten days is furniture.
/// - **samples**: how much evidence there is, on a log scale, so the thousandth
///   sighting adds far less than the second.
pub fn station_score(station: &Station, now: DateTime<Utc>, cfg: &ScoreCfg) -> f64 {
    age_weight(station, now) * collection_weight(station, cfg) * sample_weight(station, cfg)
}

/// 1.0 for this month, 0.277 a year ago, 0.2 two years ago.
fn age_weight(station: &Station, now: DateTime<Utc>) -> f64 {
    // Whole days, floored, and never negative: a record written in the future
    // is treated as brand new rather than as unusually good.
    let days = (now - station.modified).num_days().max(0);
    let month_old = days / 30;
    1.0 / ((month_old + 1) as f64).sqrt()
}

/// 0.1 for a station seen on a single day, rising to 1.0 at ten days.
fn collection_weight(station: &Station, cfg: &ScoreCfg) -> f64 {
    let mut last_seen = station.modified.date_naive();
    if let Some(seen) = station.last_seen {
        last_seen = last_seen.max(seen);
    }
    let collected_over = (last_seen - score_created(station)).num_days().max(1);
    (collected_over as f64 / cfg.collection_days).min(1.0)
}

/// When the current position started accumulating.
///
/// Not simply the creation date: after a block the station effectively starts
/// again, and counting from the original creation would credit the new position
/// with history that belongs to the old one.
fn score_created(station: &Station) -> NaiveDate {
    let created = station.created.date_naive();
    match station.block_last {
        Some(block_last) => created.max(block_last),
        None => created,
    }
}

/// 0.5 for one sample, 1.0 for two, 3.32 for ten, 6.64 for a hundred, capped at
/// 10 for a thousand or more.
fn sample_weight(station: &Station, cfg: &ScoreCfg) -> f64 {
    let mut samples = station.samples;
    // Many samples with no radius means the same observation arrived over and
    // over from one spot, which is one piece of evidence however often it was
    // repeated.
    if samples > 1 && station.radius.unwrap_or(0) == 0 {
        samples = 1;
    }
    let samples = samples.max(1) as f64;
    samples.log2().clamp(cfg.min_sample_score, cfg.max_sample_score)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{StationKey, WifiKey};
    use approx::assert_relative_eq;
    use std::f64::consts::LOG2_10;

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn at(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        day(y, m, d).and_hms_opt(0, 0, 0).unwrap().and_utc()
    }

    /// A well-established station: seen over a long period, plenty of samples,
    /// written recently.
    fn established() -> Station {
        let mut s =
            Station::new(StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap()), at(2026, 1, 1));
        s.modified = at(2026, 6, 1);
        s.last_seen = Some(day(2026, 6, 1));
        s.samples = 100;
        s.radius = Some(50);
        s
    }

    #[test]
    fn age_weight_falls_off_with_the_square_root_of_months() {
        let mut s = established();
        s.modified = at(2026, 6, 1);
        assert_relative_eq!(age_weight(&s, at(2026, 6, 1)), 1.0, epsilon = 1e-12);
        // A year later: 365 days is 12 whole months, so 1/sqrt(13).
        assert_relative_eq!(age_weight(&s, at(2027, 6, 1)), 0.2773, epsilon = 1e-3);
        // Two years later: 730 days is 24 months, so 1/sqrt(25).
        assert_relative_eq!(age_weight(&s, at(2028, 5, 31)), 0.2, epsilon = 1e-3);
    }

    #[test]
    fn a_record_written_in_the_future_is_treated_as_new_not_as_better() {
        let mut s = established();
        s.modified = at(2027, 1, 1);
        assert_relative_eq!(age_weight(&s, at(2026, 6, 1)), 1.0, epsilon = 1e-12);
    }

    #[test]
    fn collection_weight_rewards_being_seen_over_a_long_period() {
        let cfg = ScoreCfg::default();
        let mut s = established();
        s.created = at(2026, 6, 1);

        s.modified = at(2026, 6, 1);
        s.last_seen = Some(day(2026, 6, 1));
        assert_relative_eq!(collection_weight(&s, &cfg), 0.1, epsilon = 1e-9);

        s.last_seen = Some(day(2026, 6, 3));
        assert_relative_eq!(collection_weight(&s, &cfg), 0.2, epsilon = 1e-9);

        s.last_seen = Some(day(2026, 6, 11));
        assert_relative_eq!(collection_weight(&s, &cfg), 1.0, epsilon = 1e-9);

        s.last_seen = Some(day(2027, 6, 11));
        assert_relative_eq!(collection_weight(&s, &cfg), 1.0, epsilon = 1e-9,);
    }

    #[test]
    fn collection_counts_from_the_last_block_not_from_creation() {
        let cfg = ScoreCfg::default();
        let mut s = established();
        s.created = at(2020, 1, 1);
        s.modified = at(2026, 6, 5);
        s.last_seen = Some(day(2026, 6, 5));

        // Without a block, six years of history give full weight.
        s.block_last = None;
        assert_relative_eq!(collection_weight(&s, &cfg), 1.0, epsilon = 1e-9);

        // After a block four days ago, the station has only four days of
        // history behind its current position.
        s.block_last = Some(day(2026, 6, 1));
        assert_relative_eq!(collection_weight(&s, &cfg), 0.4, epsilon = 1e-9);
    }

    #[test]
    fn sample_weight_is_logarithmic_and_bounded() {
        let cfg = ScoreCfg::default();
        let mut s = established();
        for (samples, want) in
            [(1, 0.5), (2, 1.0), (10, LOG2_10), (100, 2.0 * LOG2_10), (1024, 10.0)]
        {
            s.samples = samples;
            assert_relative_eq!(sample_weight(&s, &cfg), want, epsilon = 1e-3);
        }
        s.samples = 1_000_000;
        assert_relative_eq!(sample_weight(&s, &cfg), 10.0, epsilon = 1e-9,);
        s.samples = 0;
        assert_relative_eq!(sample_weight(&s, &cfg), 0.5, epsilon = 1e-9);
    }

    #[test]
    fn many_samples_from_one_spot_count_as_one() {
        let cfg = ScoreCfg::default();
        let mut s = established();
        s.samples = 1_000;
        s.radius = Some(0);
        assert_relative_eq!(sample_weight(&s, &cfg), 0.5, epsilon = 1e-9);
        s.radius = None;
        assert_relative_eq!(sample_weight(&s, &cfg), 0.5, epsilon = 1e-9);
        s.radius = Some(1);
        assert!(sample_weight(&s, &cfg) > 9.0, "a real spread earns the full score");
    }

    #[test]
    fn a_well_established_station_outscores_a_fresh_one() {
        let cfg = ScoreCfg::default();
        let now = at(2026, 6, 1);
        let good = station_score(&established(), now, &cfg);

        let mut fresh = established();
        fresh.created = at(2026, 6, 1);
        fresh.samples = 1;
        fresh.radius = Some(0);
        let poor = station_score(&fresh, now, &cfg);

        assert!(good > poor * 10.0, "good {good} should dwarf poor {poor}");
    }

    #[test]
    fn score_is_never_negative() {
        let cfg = ScoreCfg::default();
        let mut s = established();
        s.samples = -5;
        s.modified = at(2019, 1, 1);
        assert!(station_score(&s, at(2026, 6, 1), &cfg) >= 0.0);
    }

    #[test]
    fn bounds_come_from_config() {
        let cfg = ScoreCfg { max_sample_score: 3.0, ..ScoreCfg::default() };
        let mut s = established();
        s.samples = 100_000;
        assert_relative_eq!(sample_weight(&s, &cfg), 3.0, epsilon = 1e-9);
    }
}
