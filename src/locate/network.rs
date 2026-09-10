//! A candidate station as the locate path sees it: a stored position plus what
//! the requesting device reported about seeing it.

use crate::model::StationKey;

/// One known station, paired with the client's sighting of it.
#[derive(Debug, Clone, PartialEq)]
pub struct Network {
    pub key: StationKey,
    pub lat: f64,
    pub lon: f64,
    /// Stored radius in metres; the floor keeps a zero from making a station
    /// look impossibly precise.
    pub radius: f64,
    /// How stale the client's sighting is, in milliseconds, floored at 1000 so
    /// a fresh scan cannot divide by zero.
    pub age_ms: f64,
    /// Reported signal in dBm. Negative, and squared wherever it is used.
    pub signal: f64,
    /// Trust in the stored position.
    pub score: f64,
    /// Whether the station was confirmed today.
    pub seen_today: bool,
}

impl Network {
    /// Weight this station carries when the position is fitted.
    ///
    /// Trust times freshness divided by signal squared: a trusted, recently
    /// reported, strongly heard station dominates a doubtful, stale, faint one.
    pub fn weight(&self) -> f64 {
        self.score * (2_000.0 / self.age_ms).sqrt().min(1.0) / self.signal.powi(2)
    }

    /// The same weight without the trust term, used for residuals during the
    /// fit where every candidate is already known to be trusted enough.
    pub fn residual_weight(&self) -> f64 {
        (2_000.0 / self.age_ms).sqrt().min(1.0) / self.signal.powi(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::WifiKey;

    fn net(score: f64, age_ms: f64, signal: f64) -> Network {
        Network {
            key: StationKey::Wifi(WifiKey(1)),
            lat: 35.7,
            lon: 51.3,
            radius: 50.0,
            age_ms,
            signal,
            score,
            seen_today: true,
        }
    }

    #[test]
    fn a_more_trusted_station_weighs_more() {
        assert!(net(10.0, 1_000.0, -70.0).weight() > net(1.0, 1_000.0, -70.0).weight());
    }

    #[test]
    fn a_stronger_signal_weighs_more() {
        assert!(net(1.0, 1_000.0, -50.0).weight() > net(1.0, 1_000.0, -90.0).weight());
    }

    #[test]
    fn a_staler_sighting_weighs_less() {
        assert!(net(1.0, 1_000.0, -70.0).weight() > net(1.0, 16_000.0, -70.0).weight());
    }

    #[test]
    fn freshness_is_capped_so_a_very_fresh_scan_is_not_rewarded_without_limit() {
        // Anything at or under 2000 ms is equally fresh.
        assert_eq!(net(1.0, 1.0, -70.0).weight(), net(1.0, 2_000.0, -70.0).weight());
    }

    #[test]
    fn the_residual_weight_drops_the_trust_term() {
        let n = net(4.0, 1_000.0, -70.0);
        assert!((n.weight() / n.residual_weight() - 4.0).abs() < 1e-12);
    }
}
