//! What the service knows about one station, and whether it is trusted.

use crate::config::Block;
use crate::geo::round_half_to_even;
use crate::model::key::StationKey;
use crate::model::observation::ReportSource;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// A station's accumulated position and history.
///
/// A station with no position is not the same as a missing station: it is one
/// the service has seen but does not trust, usually because it appears to have
/// moved. That distinction is what stops a relocated access point from being
/// relearned from its own bad history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Station {
    pub key: StationKey,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    /// Radius in metres of a circle covering every observation seen so far.
    pub radius: Option<i32>,
    /// Bounding box of those observations. Kept because the radius is derived
    /// from it and has to be recomputed as the box grows.
    pub max_lat: Option<f64>,
    pub min_lat: Option<f64>,
    pub max_lon: Option<f64>,
    pub min_lon: Option<f64>,
    /// Spatial index cell, for "what is known near here".
    pub h3: Option<i64>,
    /// Observations folded in so far.
    pub samples: i32,
    /// Sum of those observations' weights.
    pub weight: f64,
    /// Best source that has contributed to the current position.
    pub source: Option<ReportSource>,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub last_seen: Option<NaiveDate>,
    pub block_first: Option<NaiveDate>,
    pub block_last: Option<NaiveDate>,
    pub block_count: i32,
}

impl Station {
    /// A station that has never been seen before.
    pub fn new(key: StationKey, now: DateTime<Utc>) -> Self {
        Self {
            key,
            lat: None,
            lon: None,
            radius: None,
            max_lat: None,
            min_lat: None,
            max_lon: None,
            min_lon: None,
            h3: None,
            samples: 0,
            weight: 0.0,
            source: None,
            created: now,
            modified: now,
            last_seen: None,
            block_first: None,
            block_last: None,
            block_count: 0,
        }
    }

    /// Does this station have a position worth using?
    pub fn has_position(&self) -> bool {
        self.lat.is_some() && self.lon.is_some()
    }

    /// Is the current position older than the configured limit?
    ///
    /// Past that age a disagreeing report is read as "the world changed" rather
    /// than "this station moved", so the position is replaced instead of the
    /// station being blocked.
    pub fn is_old_position(&self, today: NaiveDate, old_position_days: i64) -> bool {
        self.modified.date_naive() < today - chrono::Duration::days(old_position_days)
    }

    /// Is this station currently untrusted?
    ///
    /// Two independent rules, either of which blocks:
    ///
    /// 1. Blocked within the last `temporary_days`.
    /// 2. `block_count >= round(age_in_days / budget_days)`, which grants one
    ///    forgiven block per `budget_days` of known age.
    ///
    /// The second rule is why the recovery time is not simply `temporary_days`.
    /// With upstream's budget of 30 days, a station blocked once stays blocked
    /// until `round(age / 30)` reaches 2, which happens at 45 days. The number
    /// to change to make blocks shorter is the budget, not the duration.
    pub fn is_blocked(&self, today: NaiveDate, cfg: &Block) -> bool {
        if let Some(block_last) = self.block_last {
            let age_days = (today - block_last).num_days();
            if age_days < cfg.temporary_days {
                return true;
            }
        }

        if self.block_count > 0 {
            let age_days = (self.created.date_naive() - today).num_days().abs();
            let budget = round_half_to_even(age_days as f64 / cfg.budget_days) as i32;
            if self.block_count >= budget {
                return true;
            }
        }

        false
    }

    /// Was this station seen today? Used by the locate path to prefer fresh
    /// evidence, and by the write path to decide whether a touch is needed.
    pub fn seen_today(&self, today: NaiveDate) -> bool {
        self.last_seen.is_some_and(|d| d >= today)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::WifiKey;

    fn day(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn at(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        day(y, m, d).and_hms_opt(0, 0, 0).unwrap().and_utc()
    }

    fn station() -> Station {
        Station::new(
            StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap()),
            at(2026, 1, 1),
        )
    }

    #[test]
    fn a_new_station_has_no_position_and_no_history() {
        let s = station();
        assert!(!s.has_position());
        assert_eq!(s.samples, 0);
        assert_eq!(s.weight, 0.0);
        assert_eq!(s.block_count, 0);
        assert!(!s.is_blocked(day(2026, 1, 1), &Block::default()));
    }

    #[test]
    fn a_station_needs_both_coordinates_to_have_a_position() {
        let mut s = station();
        s.lat = Some(35.7);
        assert!(!s.has_position(), "latitude alone is not a position");
        s.lon = Some(51.3);
        assert!(s.has_position());
    }

    #[test]
    fn a_recently_blocked_station_is_blocked() {
        let cfg = Block { temporary_days: 7, budget_days: 10.0 };
        let mut s = station();
        s.block_last = Some(day(2026, 6, 1));
        s.block_count = 1;

        assert!(s.is_blocked(day(2026, 6, 1), &cfg), "same day");
        assert!(s.is_blocked(day(2026, 6, 6), &cfg), "six days later");
        // Day seven ends the temporary block, but the budget rule may still
        // apply; this station is old enough that it does not.
        assert!(!s.is_blocked(day(2026, 6, 8), &cfg));
    }

    #[test]
    fn the_budget_rule_keeps_a_young_station_blocked_after_the_temporary_period() {
        let cfg = Block { temporary_days: 7, budget_days: 10.0 };
        let mut s = station();
        s.created = at(2026, 6, 1);
        s.block_last = Some(day(2026, 6, 2));
        s.block_count = 1;

        // 10 days old: round(10/10) = 1, and block_count 1 >= 1, so blocked.
        assert!(s.is_blocked(day(2026, 6, 11), &cfg));
        // 15 days old: round(15/10) = round(1.5) = 2 by banker's rounding,
        // and 1 >= 2 is false, so it recovers.
        assert!(!s.is_blocked(day(2026, 6, 16), &cfg));
    }

    #[test]
    fn upstream_defaults_reproduce_the_forty_five_day_recovery() {
        // This is the behaviour the 45 days in the wild comes from: it is not a
        // constant anywhere, it falls out of round(age / 30).
        let upstream = Block { temporary_days: 2, budget_days: 30.0 };
        let mut s = station();
        s.created = at(2026, 1, 1);
        s.block_last = Some(day(2026, 1, 2));
        s.block_count = 1;

        // round(44/30) = 1, and block_count 1 >= 1, so still blocked.
        assert!(s.is_blocked(day(2026, 2, 14), &upstream), "day 44 is still blocked");
        // round(45/30) = round(1.5) = 2 by banker's rounding, and 1 >= 2 is
        // false. Recovery lands on day 45 exactly, which is what is observed in
        // production and is why the number is 45 and not 30 or 60.
        assert!(!s.is_blocked(day(2026, 2, 15), &upstream), "day 45 recovers");
    }

    #[test]
    fn shortening_the_budget_shortens_the_recovery() {
        let mut s = station();
        s.created = at(2026, 1, 1);
        s.block_last = Some(day(2026, 1, 2));
        s.block_count = 1;

        let fast = Block { temporary_days: 7, budget_days: 10.0 };
        let slow = Block { temporary_days: 7, budget_days: 30.0 };
        let day16 = day(2026, 1, 17);
        assert!(!s.is_blocked(day16, &fast), "16 days is past the fast budget");
        assert!(s.is_blocked(day16, &slow), "16 days is well inside the slow budget");
    }

    #[test]
    fn repeated_blocks_take_longer_and_longer_to_forgive() {
        let cfg = Block { temporary_days: 7, budget_days: 10.0 };
        let mut s = station();
        s.created = at(2026, 1, 1);
        s.block_last = Some(day(2026, 1, 2));

        s.block_count = 1;
        assert!(!s.is_blocked(day(2026, 1, 17), &cfg), "one block, 16 days: forgiven");
        s.block_count = 2;
        assert!(s.is_blocked(day(2026, 1, 17), &cfg), "two blocks at the same age: not yet");
        assert!(!s.is_blocked(day(2026, 2, 6), &cfg), "two blocks, 36 days: forgiven");
    }

    #[test]
    fn a_station_that_was_never_blocked_is_never_blocked_by_the_budget_rule() {
        let cfg = Block { temporary_days: 7, budget_days: 10.0 };
        let mut s = station();
        s.created = at(2026, 1, 1);
        s.block_count = 0;
        // Age zero would make round(0/10) = 0, and 0 >= 0 would block every
        // brand new station if the count were not checked first.
        assert!(!s.is_blocked(day(2026, 1, 1), &cfg));
    }

    #[test]
    fn an_old_position_is_recognised_by_the_configured_age() {
        let mut s = station();
        s.modified = at(2025, 1, 1);
        assert!(s.is_old_position(day(2026, 6, 1), 365));
        assert!(!s.is_old_position(day(2026, 6, 1), 1_000));
    }

    #[test]
    fn seen_today_is_true_only_from_today_onwards() {
        let mut s = station();
        assert!(!s.seen_today(day(2026, 6, 1)), "never seen");
        s.last_seen = Some(day(2026, 5, 31));
        assert!(!s.seen_today(day(2026, 6, 1)));
        s.last_seen = Some(day(2026, 6, 1));
        assert!(s.seen_today(day(2026, 6, 1)));
    }
}
