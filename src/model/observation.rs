//! A single sighting of a station from a known position, and how much it counts.
//!
//! The weights are ported from Ichnaea's `models/observation.py`. They are the
//! only thing standing between a careful fix and a careless one, so the shapes
//! of the curves are reproduced exactly, including the places where upstream
//! treats a zero as "not supplied".

use crate::config::Validation;
use crate::model::key::{Radio, StationKey, StationKind};
use serde::{Deserialize, Serialize};

/// What produced the position a report was taken from. Lower is better quality.
/// Values are stored, so they must not be renumbered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum ReportSource {
    /// Outside knowledge of where the station truly is.
    Fixed = 0,
    /// Satellite navigation.
    Gnss = 3,
    /// Several sensors combined.
    Fused = 6,
    /// Inferred from a lookup, so it cannot be evidence about itself.
    Query = 9,
}

impl ReportSource {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(ReportSource::Fixed),
            3 => Some(ReportSource::Gnss),
            6 => Some(ReportSource::Fused),
            9 => Some(ReportSource::Query),
            _ => None,
        }
    }

    pub fn as_i8(self) -> i8 {
        self as i8
    }
}

/// One station, seen once, from one place.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub key: StationKey,
    /// Where the reporter was, not where the station is.
    pub lat: f64,
    pub lon: f64,
    /// Reported accuracy of that position, in metres.
    pub accuracy: Option<f64>,
    /// How long before the position fix the station was seen, in milliseconds.
    pub age_ms: Option<f64>,
    /// Reporter's speed in metres per second.
    pub speed: Option<f64>,
    /// Received signal strength in dBm.
    pub signal: Option<i32>,
    pub source: ReportSource,
    /// Milliseconds since the Unix epoch.
    pub timestamp: i64,
}

impl Observation {
    /// Quality of the position this observation was taken from, ignoring signal.
    ///
    /// Zero means "do not use": too inaccurate, too stale, or too fast. The
    /// caller must treat a zero as a dropped observation rather than a
    /// weightless one, or a batch of zeros divides by zero later.
    pub fn base_weight(&self, cfg: &Validation) -> f64 {
        self.accuracy_weight(cfg) * self.age_weight(cfg) * self.speed_weight(cfg)
    }

    /// Maps 10 m to 1.0, 20 m to 0.7, 40 m to 0.5, 100 m to 0.32.
    ///
    /// A missing accuracy and an accuracy of exactly zero are both treated as
    /// 10 m, which is upstream's behaviour: a self-reported zero is not
    /// credible, so it is read as "not supplied" rather than "perfect".
    pub fn accuracy_weight(&self, cfg: &Validation) -> f64 {
        let max_allowed = self.max_observation_accuracy(cfg);
        let accuracy = match self.accuracy {
            Some(a) if a.abs() != 0.0 => a.abs(),
            _ => 10.0,
        };
        let accuracy = accuracy.max(10.0);
        if accuracy > max_allowed {
            return 0.0;
        }
        (10.0 / accuracy).sqrt()
    }

    /// Maps 0-2000 ms to 1.0, 4000 ms to 0.7, 8000 ms to 0.5, 18000 ms to 0.33.
    ///
    /// Age is used as a magnitude: a scan stamped slightly after the fix is no
    /// less useful than one stamped slightly before it.
    pub fn age_weight(&self, cfg: &Validation) -> f64 {
        let age = match self.age_ms {
            Some(a) if a.abs() != 0.0 => a.abs(),
            _ => 2_000.0,
        };
        let age = age.max(2_000.0);
        if age > cfg.max_observation_age_ms {
            return 0.0;
        }
        (2_000.0 / age).sqrt().min(1.0)
    }

    /// Maps 0-5 m/s to 1.0, 10 m/s to 0.7, 20 m/s to 0.5, 45 m/s to 0.33.
    ///
    /// Note the direction: standing still earns full weight, because for a
    /// pedestrian surveyor it means a settled fix. For a vehicle that idles in
    /// the same place this is generous, which is why repeats from one spot can
    /// dominate a station. Collapsing them is available in `ingest`.
    pub fn speed_weight(&self, cfg: &Validation) -> f64 {
        let speed = match self.speed {
            Some(s) if s.abs() != 0.0 => s.abs(),
            _ => 1.0,
        };
        let speed = speed.max(1.0);
        if speed > cfg.max_observation_speed_mps {
            return 0.0;
        }
        (5.0 / speed).sqrt().min(1.0)
    }

    /// Full weight, including signal strength.
    ///
    /// A strong signal means the reporter was close to the station, so it says
    /// more about where the station is. The fourth-power curve makes that
    /// preference sharp: for WiFi, -60 dBm counts about five times as much as
    /// -100 dBm.
    pub fn weight(&self, cfg: &Validation) -> f64 {
        self.signal_weight() * self.base_weight(cfg)
    }

    /// Signal term alone. Split out so tests can pin the curve.
    pub fn signal_weight(&self) -> f64 {
        match self.key {
            StationKey::Wifi(_) => {
                // Maps -100 to ~0.5, -80 to 1.0, -60 to 2.4, -30 to 16.
                let signal = f64::from(self.signal.unwrap_or(-80));
                ((1.0 / (signal - 20.0).powi(2)) * 10_000.0).powi(2)
            }
            StationKey::Cell(c) => {
                // Each radio has its own median and its own offset, because a
                // "good" signal means a different number on each technology.
                let (default, offset) = match c.radio {
                    Radio::Gsm => (Some(-95.0), -5.0),
                    Radio::Wcdma => (Some(-100.0), 0.0),
                    Radio::Lte => (Some(-105.0), 5.0),
                    Radio::Cdma => (None, 0.0),
                };
                let signal = self.signal.map(f64::from).or(default);
                match signal {
                    Some(s) => ((1.0 / (s + offset).powi(2)) * 10_000.0).powi(2),
                    // CDMA has no calibrated curve upstream, so an unknown
                    // signal contributes nothing beyond the base weight.
                    None => 1.0,
                }
            }
        }
    }

    /// The accuracy ceiling that applies to this kind of station. WiFi is held
    /// to a stricter standard because it is used for the precise answers.
    fn max_observation_accuracy(&self, cfg: &Validation) -> f64 {
        match self.key.kind() {
            StationKind::Wifi => cfg.wifi_max_observation_accuracy_m,
            StationKind::Cell => cfg.cell_max_observation_accuracy_m,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{CellKey, WifiKey};
    use approx::assert_relative_eq;
    use std::f64::consts::FRAC_1_SQRT_2;

    fn wifi_obs() -> Observation {
        Observation {
            key: StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap()),
            lat: 35.7219,
            lon: 51.3347,
            accuracy: Some(10.0),
            age_ms: Some(0.0),
            speed: Some(0.0),
            signal: Some(-80),
            source: ReportSource::Gnss,
            timestamp: 1_700_000_000_000,
        }
    }

    fn cell_obs(radio: Radio, signal: Option<i32>) -> Observation {
        Observation {
            key: StationKey::Cell(CellKey { radio, mcc: 432, mnc: 11, lac: 1234, cid: 56789 }),
            signal,
            ..wifi_obs()
        }
    }

    #[test]
    fn accuracy_weight_follows_the_documented_curve() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        for (accuracy, want) in [(10.0, 1.0), (20.0, FRAC_1_SQRT_2), (40.0, 0.5), (100.0, 0.3162)] {
            o.accuracy = Some(accuracy);
            assert_relative_eq!(o.accuracy_weight(&cfg), want, epsilon = 1e-4);
        }
    }

    #[test]
    fn a_missing_or_zero_accuracy_is_read_as_ten_metres_not_as_perfect() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        o.accuracy = None;
        assert_relative_eq!(o.accuracy_weight(&cfg), 1.0, epsilon = 1e-12);
        o.accuracy = Some(0.0);
        assert_relative_eq!(o.accuracy_weight(&cfg), 1.0, epsilon = 1e-12);
        // Better than 10 m is not rewarded beyond 10 m.
        o.accuracy = Some(1.0);
        assert_relative_eq!(o.accuracy_weight(&cfg), 1.0, epsilon = 1e-12);
    }

    #[test]
    fn accuracy_beyond_the_ceiling_is_worth_nothing() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        o.accuracy = Some(200.0); // exactly at the WiFi ceiling
        assert!(o.accuracy_weight(&cfg) > 0.0);
        o.accuracy = Some(200.1);
        assert_eq!(o.accuracy_weight(&cfg), 0.0);
    }

    #[test]
    fn wifi_is_held_to_a_stricter_accuracy_ceiling_than_cell() {
        let cfg = Validation::default();
        let mut w = wifi_obs();
        let mut c = cell_obs(Radio::Lte, Some(-105));
        w.accuracy = Some(500.0);
        c.accuracy = Some(500.0);
        assert_eq!(w.accuracy_weight(&cfg), 0.0, "500 m is past the 200 m WiFi ceiling");
        assert!(c.accuracy_weight(&cfg) > 0.0, "500 m is inside the 1000 m cell ceiling");
    }

    #[test]
    fn age_weight_follows_the_documented_curve() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        for (age, want) in [(0.0, 1.0), (2_000.0, 1.0), (4_000.0, FRAC_1_SQRT_2), (8_000.0, 0.5)] {
            o.age_ms = Some(age);
            assert_relative_eq!(o.age_weight(&cfg), want, epsilon = 1e-4);
        }
    }

    #[test]
    fn age_is_a_magnitude_so_a_scan_after_the_fix_counts_the_same() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        o.age_ms = Some(-8_000.0);
        assert_relative_eq!(o.age_weight(&cfg), 0.5, epsilon = 1e-4);
    }

    #[test]
    fn a_scan_older_than_the_limit_is_worth_nothing() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        o.age_ms = Some(20_000.0);
        assert!(o.age_weight(&cfg) > 0.0);
        o.age_ms = Some(20_000.1);
        assert_eq!(o.age_weight(&cfg), 0.0);
    }

    #[test]
    fn speed_weight_rewards_standing_still() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        for (speed, want) in [(0.0, 1.0), (5.0, 1.0), (10.0, FRAC_1_SQRT_2), (20.0, 0.5)] {
            o.speed = Some(speed);
            assert_relative_eq!(o.speed_weight(&cfg), want, epsilon = 1e-4);
        }
        o.speed = Some(50.1);
        assert_eq!(o.speed_weight(&cfg), 0.0, "faster than the limit is discarded");
    }

    #[test]
    fn wifi_signal_weight_follows_the_documented_curve() {
        let mut o = wifi_obs();
        for (signal, want) in [(-100, 0.4823), (-80, 1.0), (-60, 2.4414), (-30, 16.0)] {
            o.signal = Some(signal);
            assert_relative_eq!(o.signal_weight(), want, epsilon = 1e-3);
        }
    }

    #[test]
    fn an_unknown_wifi_signal_is_treated_as_minus_eighty() {
        let mut o = wifi_obs();
        o.signal = None;
        assert_relative_eq!(o.signal_weight(), 1.0, epsilon = 1e-12);
    }

    #[test]
    fn each_cell_radio_has_its_own_signal_curve() {
        // At each radio's median the weight is 1.0, which is what makes the
        // three technologies comparable to one another.
        assert_relative_eq!(cell_obs(Radio::Gsm, Some(-95)).signal_weight(), 1.0, epsilon = 1e-9);
        assert_relative_eq!(cell_obs(Radio::Wcdma, Some(-100)).signal_weight(), 1.0, epsilon = 1e-9);
        assert_relative_eq!(cell_obs(Radio::Lte, Some(-105)).signal_weight(), 1.0, epsilon = 1e-9);
    }

    #[test]
    fn an_unknown_cell_signal_falls_back_to_the_radio_median() {
        assert_relative_eq!(cell_obs(Radio::Lte, None).signal_weight(), 1.0, epsilon = 1e-9);
        // CDMA has no calibrated curve upstream, so it contributes nothing.
        assert_relative_eq!(cell_obs(Radio::Cdma, None).signal_weight(), 1.0, epsilon = 1e-9);
    }

    #[test]
    fn a_stronger_cell_signal_always_counts_for_more() {
        for radio in [Radio::Gsm, Radio::Wcdma, Radio::Lte] {
            let weak = cell_obs(radio, Some(-110)).signal_weight();
            let strong = cell_obs(radio, Some(-70)).signal_weight();
            assert!(strong > weak, "{radio}: {strong} should beat {weak}");
        }
    }

    #[test]
    fn full_weight_is_the_signal_term_times_the_base_term() {
        let cfg = Validation::default();
        let o = wifi_obs();
        assert_relative_eq!(o.weight(&cfg), o.signal_weight() * o.base_weight(&cfg), epsilon = 1e-12);
    }

    #[test]
    fn any_disqualifying_factor_zeroes_the_whole_weight() {
        let cfg = Validation::default();
        let mut o = wifi_obs();
        o.accuracy = Some(5_000.0);
        assert_eq!(o.weight(&cfg), 0.0);

        let mut o = wifi_obs();
        o.age_ms = Some(60_000.0);
        assert_eq!(o.weight(&cfg), 0.0);

        let mut o = wifi_obs();
        o.speed = Some(300.0);
        assert_eq!(o.weight(&cfg), 0.0);
    }

    #[test]
    fn report_source_numbering_matches_upstream_and_must_not_change() {
        assert_eq!(ReportSource::Fixed.as_i8(), 0);
        assert_eq!(ReportSource::Gnss.as_i8(), 3);
        assert_eq!(ReportSource::Fused.as_i8(), 6);
        assert_eq!(ReportSource::Query.as_i8(), 9);
        for s in [ReportSource::Fixed, ReportSource::Gnss, ReportSource::Fused, ReportSource::Query]
        {
            assert_eq!(ReportSource::from_i8(s.as_i8()), Some(s));
        }
        assert_eq!(ReportSource::from_i8(4), None);
    }

    #[test]
    fn thresholds_come_from_config_not_from_constants() {
        let mut cfg = Validation::default();
        let mut o = wifi_obs();
        o.age_ms = Some(30_000.0);
        assert_eq!(o.age_weight(&cfg), 0.0);
        cfg.max_observation_age_ms = 60_000.0;
        assert!(o.age_weight(&cfg) > 0.0, "raising the limit should admit the observation");
    }
}
