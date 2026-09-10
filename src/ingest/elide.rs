//! Deciding whether an update is worth writing at all.
//!
//! Most folded batches land on a station the service already knows well, where
//! the blended position moves by centimetres. Writing that row again costs a
//! write, a replicated copy of it, and eventually the compaction to clean it
//! up, in exchange for a change nobody could measure.
//!
//! Ichnaea has no equivalent for satellite-sourced reports: its cheap
//! confirmation path fires only for query-sourced ones, so every agreeing
//! report rewrites the whole row. This is the one place the write path
//! deliberately does less work than upstream.
//!
//! There is a second reason to keep writing at least once a day, and it is not
//! about position at all: a written row has its lifetime refreshed. Skipping
//! every write would let a station that is still being seen quietly expire.

use crate::config::Elision;
use crate::geo::distance;
use crate::ingest::fold::{Action, StationUpdate};
use crate::model::Station;
use chrono::NaiveDate;

/// What to do with a folded update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// Write the row.
    Full,
    /// Nothing changed and the station has already been written today, so its
    /// lifetime is already refreshed.
    Skip,
}

/// Decide whether an update needs to reach storage.
///
/// `previous` is the station as it was before folding, or `None` if it is new.
pub fn decide(
    update: &StationUpdate,
    previous: Option<&Station>,
    today: NaiveDate,
    cfg: &Elision,
) -> Write {
    if !cfg.enabled {
        return match update.action {
            Action::Nothing => Write::Skip,
            _ => Write::Full,
        };
    }

    match update.action {
        // Nothing was decided, so there is nothing to write.
        Action::Nothing => Write::Skip,

        // Rare and consequential. Always written.
        Action::New | Action::NewBlock | Action::Block | Action::Replace => Write::Full,

        // A confirmation carries no new position. It is worth a write only to
        // record that the station was seen today, which also refreshes the
        // row's lifetime.
        Action::Confirm => {
            if previous.is_some_and(|p| p.seen_today(today)) {
                Write::Skip
            } else {
                Write::Full
            }
        }

        // The common case, and the one worth thinking about.
        Action::Change => {
            let Some(previous) = previous else {
                return Write::Full;
            };
            let moved = match (previous.lat, previous.lon, update.station.lat, update.station.lon) {
                (Some(old_lat), Some(old_lon), Some(new_lat), Some(new_lon)) => {
                    distance(old_lat, old_lon, new_lat, new_lon)
                }
                // Gaining or losing a position is always worth writing.
                _ => f64::INFINITY,
            };

            if moved >= cfg.min_move_m {
                Write::Full
            } else if !previous.seen_today(today) {
                // The position did not move, but the row still needs today's
                // date on it so its lifetime is refreshed.
                Write::Full
            } else {
                Write::Skip
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::key::{StationKey, WifiKey};
    use chrono::{DateTime, Duration, Utc};

    fn today() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()
    }

    fn at(date: NaiveDate) -> DateTime<Utc> {
        date.and_hms_opt(12, 0, 0).unwrap().and_utc()
    }

    fn station(lat: f64, lon: f64, last_seen: Option<NaiveDate>) -> Station {
        let mut s = Station::new(StationKey::Wifi(WifiKey(1)), at(today()));
        s.lat = Some(lat);
        s.lon = Some(lon);
        s.last_seen = last_seen;
        s
    }

    fn update(action: Action, station: Station) -> StationUpdate {
        StationUpdate { action, station }
    }

    #[test]
    fn a_station_that_did_not_move_and_was_already_seen_today_is_not_written() {
        // The common case, and the whole point of this module.
        let cfg = Elision::default();
        let previous = station(35.7219, 51.3347, Some(today()));
        let folded = update(Action::Change, station(35.721_900_1, 51.334_700_1, Some(today())));
        assert_eq!(decide(&folded, Some(&previous), today(), &cfg), Write::Skip);
    }

    #[test]
    fn a_station_that_moved_is_written() {
        let cfg = Elision::default();
        let previous = station(35.7219, 51.3347, Some(today()));
        let folded = update(Action::Change, station(35.7230, 51.3347, Some(today())));
        assert_eq!(decide(&folded, Some(&previous), today(), &cfg), Write::Full);
    }

    #[test]
    fn a_station_not_yet_seen_today_is_written_even_when_it_did_not_move() {
        // Not for the position, but because the row's lifetime has to be
        // refreshed or a live station would eventually expire.
        let cfg = Elision::default();
        let previous = station(35.7219, 51.3347, Some(today() - Duration::days(1)));
        let folded = update(Action::Change, station(35.7219, 51.3347, Some(today())));
        assert_eq!(decide(&folded, Some(&previous), today(), &cfg), Write::Full);
    }

    #[test]
    fn the_movement_threshold_comes_from_config() {
        let previous = station(35.7219, 51.3347, Some(today()));
        // About 11 m north.
        let folded = update(Action::Change, station(35.7220, 51.3347, Some(today())));

        let strict = Elision { min_move_m: 1.0, enabled: true };
        assert_eq!(decide(&folded, Some(&previous), today(), &strict), Write::Full);

        let relaxed = Elision { min_move_m: 100.0, enabled: true };
        assert_eq!(decide(&folded, Some(&previous), today(), &relaxed), Write::Skip);
    }

    #[test]
    fn consequential_actions_are_always_written() {
        let cfg = Elision::default();
        let previous = station(35.7219, 51.3347, Some(today()));
        for action in [Action::New, Action::NewBlock, Action::Block, Action::Replace] {
            let folded = update(action, station(35.7219, 51.3347, Some(today())));
            assert_eq!(
                decide(&folded, Some(&previous), today(), &cfg),
                Write::Full,
                "{action:?} must always be written"
            );
        }
    }

    #[test]
    fn a_brand_new_station_is_always_written() {
        let cfg = Elision::default();
        let folded = update(Action::Change, station(35.7219, 51.3347, Some(today())));
        assert_eq!(decide(&folded, None, today(), &cfg), Write::Full);
    }

    #[test]
    fn gaining_or_losing_a_position_is_always_written() {
        let cfg = Elision::default();
        let mut previous = station(35.7219, 51.3347, Some(today()));
        previous.lat = None;
        previous.lon = None;
        let folded = update(Action::Change, station(35.7219, 51.3347, Some(today())));
        assert_eq!(decide(&folded, Some(&previous), today(), &cfg), Write::Full);
    }

    #[test]
    fn a_confirmation_is_written_once_a_day_and_no_more() {
        let cfg = Elision::default();
        let seen = station(35.7219, 51.3347, Some(today()));
        let unseen = station(35.7219, 51.3347, Some(today() - Duration::days(1)));
        let folded = update(Action::Confirm, station(35.7219, 51.3347, Some(today())));

        assert_eq!(decide(&folded, Some(&seen), today(), &cfg), Write::Skip);
        assert_eq!(decide(&folded, Some(&unseen), today(), &cfg), Write::Full);
    }

    #[test]
    fn a_decision_of_nothing_never_writes() {
        let cfg = Elision::default();
        let previous = station(35.7219, 51.3347, Some(today()));
        let folded = update(Action::Nothing, previous.clone());
        assert_eq!(decide(&folded, Some(&previous), today(), &cfg), Write::Skip);
    }

    #[test]
    fn turning_elision_off_writes_everything_except_nothing() {
        let cfg = Elision { enabled: false, min_move_m: 1.0 };
        let previous = station(35.7219, 51.3347, Some(today()));
        let unmoved = update(Action::Change, station(35.7219, 51.3347, Some(today())));
        assert_eq!(decide(&unmoved, Some(&previous), today(), &cfg), Write::Full);

        let nothing = update(Action::Nothing, previous.clone());
        assert_eq!(decide(&nothing, Some(&previous), today(), &cfg), Write::Skip);
    }

    #[test]
    fn a_live_station_is_still_written_at_least_once_a_day() {
        // Walk a station through several days of unmoving traffic and check it
        // is written on each new day, so its lifetime never lapses.
        let cfg = Elision::default();
        let mut written_days = 0;
        for day in 0..5 {
            let date = today() + Duration::days(day);
            let previous = station(35.7219, 51.3347, Some(date - Duration::days(1)));
            let folded = update(Action::Change, station(35.7219, 51.3347, Some(date)));
            if decide(&folded, Some(&previous), date, &cfg) == Write::Full {
                written_days += 1;
            }
        }
        assert_eq!(written_days, 5, "one write per day keeps the row alive");
    }
}
