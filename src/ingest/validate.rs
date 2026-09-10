//! Turning a submitted report into observations worth keeping.
//!
//! Two levels of rejection, and the difference is deliberate:
//!
//! - A bad **position** drops the whole report. Every station in it would be
//!   learned at the wrong place, so keeping any of it would be worse than
//!   keeping none.
//! - A bad **station** drops only itself. A report with one malformed access
//!   point still carries good evidence about the others.

use crate::api::schema::{CellTower, Position, SubmitItem, WifiAccessPoint};
use crate::config::Validation;
use crate::model::key::{CellKey, Radio, StationKey, WifiKey};
use crate::model::observation::{Observation, ReportSource};
use chrono::Utc;

/// Why a report or a station was not kept. Counted, so the shape of the
/// incoming data is visible without reading logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    NoPosition,
    PositionOutOfRange,
    AccuracyOutOfRange,
    SpeedOutOfRange,
    AgeOutOfRange,
    /// The position was too poor to learn anything from, which the weights
    /// express as a weight of zero.
    Worthless,
    BadMac,
    PlaceholderMac,
    FrequencyOutOfRange,
    SignalOutOfRange,
    UnknownRadio,
    MccNotAllowed,
    CellFieldOutOfRange,
}

/// What came out of one report.
#[derive(Debug, Default, Clone)]
pub struct Validated {
    pub observations: Vec<Observation>,
    /// One entry per dropped station, or one per report when the position
    /// failed.
    pub dropped: Vec<DropReason>,
}

impl Validated {
    fn drop_all(reason: DropReason) -> Self {
        Self { observations: vec![], dropped: vec![reason] }
    }

    pub fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }
}

/// Validate one report and turn it into observations.
pub fn validate_item(item: &SubmitItem, cfg: &Validation) -> Validated {
    let Some(position) = item.position.as_ref() else {
        return Validated::drop_all(DropReason::NoPosition);
    };
    let (lat, lon) = match (position.latitude, position.longitude) {
        (Some(lat), Some(lon)) => (lat, lon),
        _ => return Validated::drop_all(DropReason::NoPosition),
    };
    if let Err(reason) = check_position(lat, lon, position, cfg) {
        return Validated::drop_all(reason);
    }

    let source = parse_source(position.source.as_deref());
    let timestamp = item.timestamp.unwrap_or_else(|| Utc::now().timestamp_millis());

    let mut out = Validated::default();

    for wifi in &item.wifi_access_points {
        match validate_wifi(wifi, cfg) {
            Ok((key, signal)) => {
                let observation = Observation {
                    key: StationKey::Wifi(key),
                    lat,
                    lon,
                    accuracy: position.accuracy,
                    age_ms: wifi.age.or(position.age),
                    speed: position.speed,
                    signal,
                    source,
                    timestamp,
                };
                keep_if_useful(observation, cfg, &mut out);
            }
            Err(reason) => out.dropped.push(reason),
        }
    }

    for cell in &item.cell_towers {
        match validate_cell(cell, cfg) {
            Ok((key, signal)) => {
                let observation = Observation {
                    key: StationKey::Cell(key),
                    lat,
                    lon,
                    accuracy: position.accuracy,
                    age_ms: cell.age.or(position.age),
                    speed: position.speed,
                    signal,
                    source,
                    timestamp,
                };
                keep_if_useful(observation, cfg, &mut out);
            }
            Err(reason) => out.dropped.push(reason),
        }
    }

    out
}

/// Keep an observation only if it carries weight.
///
/// A weight of zero is how the weighting expresses "too inaccurate, too stale,
/// or too fast to be evidence". Such an observation must be dropped rather than
/// stored, because a batch of zeros has nothing to average.
fn keep_if_useful(observation: Observation, cfg: &Validation, out: &mut Validated) {
    if observation.base_weight(cfg) > 0.0 {
        out.observations.push(observation);
    } else {
        out.dropped.push(DropReason::Worthless);
    }
}

fn check_position(
    lat: f64,
    lon: f64,
    position: &Position,
    cfg: &Validation,
) -> Result<(), DropReason> {
    if !lat.is_finite() || !lon.is_finite() {
        return Err(DropReason::PositionOutOfRange);
    }
    if lat < cfg.lat_bounds.0 || lat > cfg.lat_bounds.1 {
        return Err(DropReason::PositionOutOfRange);
    }
    if lon < cfg.lon_bounds.0 || lon > cfg.lon_bounds.1 {
        return Err(DropReason::PositionOutOfRange);
    }
    if let Some(accuracy) = position.accuracy {
        if !accuracy.is_finite() || accuracy < cfg.min_accuracy_m || accuracy > cfg.max_accuracy_m {
            return Err(DropReason::AccuracyOutOfRange);
        }
    }
    if let Some(speed) = position.speed {
        if !speed.is_finite() || speed < cfg.min_speed_mps || speed > cfg.max_speed_mps {
            return Err(DropReason::SpeedOutOfRange);
        }
    }
    if let Some(age) = position.age {
        if !age.is_finite() || age < cfg.min_age_ms as f64 || age > cfg.max_age_ms as f64 {
            return Err(DropReason::AgeOutOfRange);
        }
    }
    Ok(())
}

/// Absent means satellite navigation, which is what a client that says nothing
/// almost always means.
fn parse_source(source: Option<&str>) -> ReportSource {
    match source.map(str::to_ascii_lowercase).as_deref() {
        Some("fixed") => ReportSource::Fixed,
        Some("fused") => ReportSource::Fused,
        Some("query") => ReportSource::Query,
        _ => ReportSource::Gnss,
    }
}

fn validate_wifi(
    wifi: &WifiAccessPoint,
    cfg: &Validation,
) -> Result<(WifiKey, Option<i32>), DropReason> {
    let raw = wifi.mac_address.as_deref().ok_or(DropReason::BadMac)?;
    let normalised = raw.to_ascii_lowercase();
    if cfg.invalid_macs.iter().any(|m| m.eq_ignore_ascii_case(&normalised)) {
        return Err(DropReason::PlaceholderMac);
    }
    let key = WifiKey::parse(&normalised).map_err(|_| DropReason::BadMac)?;

    if let Some(frequency) = wifi.frequency {
        if frequency != 0
            && (frequency < i64::from(cfg.wifi_frequency_mhz.0)
                || frequency > i64::from(cfg.wifi_frequency_mhz.1))
        {
            return Err(DropReason::FrequencyOutOfRange);
        }
    }

    let signal = match wifi.signal_strength {
        // Zero is how clients spell "not measured", and it is not a plausible
        // reading anyway: 0 dBm at an access point would be extraordinary.
        Some(0) | None => None,
        Some(s) => {
            if s < i64::from(cfg.wifi_signal_dbm.0) || s > i64::from(cfg.wifi_signal_dbm.1) {
                return Err(DropReason::SignalOutOfRange);
            }
            Some(s as i32)
        }
    };

    Ok((key, signal))
}

fn validate_cell(cell: &CellTower, cfg: &Validation) -> Result<(CellKey, Option<i32>), DropReason> {
    let radio =
        cell.radio_type.as_deref().and_then(Radio::parse).ok_or(DropReason::UnknownRadio)?;

    let mcc = i32::try_from(cell.mobile_country_code.ok_or(DropReason::CellFieldOutOfRange)?)
        .map_err(|_| DropReason::CellFieldOutOfRange)?;
    if mcc < cfg.mcc.0 || mcc > cfg.mcc.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }
    if !cfg.mcc_allowlist.is_empty() && !cfg.mcc_allowlist.contains(&mcc) {
        return Err(DropReason::MccNotAllowed);
    }

    let mnc = i32::try_from(cell.mobile_network_code.ok_or(DropReason::CellFieldOutOfRange)?)
        .map_err(|_| DropReason::CellFieldOutOfRange)?;
    if mnc < cfg.mnc.0 || mnc > cfg.mnc.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }

    let lac = cell.location_area_code.ok_or(DropReason::CellFieldOutOfRange)?;
    if lac < cfg.lac.0 || lac > cfg.lac.1 {
        return Err(DropReason::CellFieldOutOfRange);
    }

    let cid = cell.cell_id.ok_or(DropReason::CellFieldOutOfRange)?;
    // GSM identifiers are 16 bits. A larger value on a GSM tower means the
    // client filled the field from a different radio's identifier.
    let cid_max = if radio == Radio::Gsm { cfg.cid_gsm_max } else { cfg.cid.1 };
    if cid < cfg.cid.0 || cid > cid_max {
        return Err(DropReason::CellFieldOutOfRange);
    }

    if let Some(psc) = cell.primary_scrambling_code {
        let psc_max =
            if radio == Radio::Lte { i64::from(cfg.psc_lte_max) } else { i64::from(cfg.psc.1) };
        if psc != 0 && (psc < i64::from(cfg.psc.0) || psc > psc_max) {
            return Err(DropReason::CellFieldOutOfRange);
        }
    }

    if let Some(ta) = cell.timing_advance {
        if ta != 0 && (ta < i64::from(cfg.cell_ta.0) || ta > i64::from(cfg.cell_ta.1)) {
            return Err(DropReason::CellFieldOutOfRange);
        }
    }

    let (min_signal, max_signal) = match radio {
        Radio::Gsm => (-113, -51),
        Radio::Wcdma => (-121, -25),
        Radio::Lte => (-140, -43),
        Radio::Cdma => (-140, -25),
    };
    let signal = match cell.signal_strength {
        Some(0) | None => None,
        Some(s) => {
            if s < min_signal || s > max_signal {
                return Err(DropReason::SignalOutOfRange);
            }
            Some(s as i32)
        }
    };

    Ok((CellKey { radio, mcc, mnc, lac, cid }, signal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::Position as ApiPosition;

    fn good_position() -> ApiPosition {
        ApiPosition {
            latitude: Some(35.7219),
            longitude: Some(51.3347),
            accuracy: Some(12.0),
            age: Some(0.0),
            speed: Some(5.0),
            source: None,
        }
    }

    fn good_wifi(mac: &str) -> WifiAccessPoint {
        WifiAccessPoint {
            mac_address: Some(mac.into()),
            signal_strength: Some(-70),
            frequency: Some(2_412),
            channel: Some(1),
            age: Some(500.0),
        }
    }

    fn good_cell() -> CellTower {
        CellTower {
            radio_type: Some("lte".into()),
            mobile_country_code: Some(432),
            mobile_network_code: Some(11),
            location_area_code: Some(1_234),
            cell_id: Some(56_789),
            primary_scrambling_code: Some(100),
            signal_strength: Some(-105),
            timing_advance: Some(3),
            age: Some(400.0),
        }
    }

    fn item() -> SubmitItem {
        SubmitItem {
            position: Some(good_position()),
            timestamp: Some(1_700_000_000_000),
            cell_towers: vec![good_cell()],
            wifi_access_points: vec![good_wifi("aabbccddeeff")],
        }
    }

    #[test]
    fn a_good_report_produces_one_observation_per_station() {
        let out = validate_item(&item(), &Validation::default());
        assert_eq!(out.observations.len(), 2);
        assert!(out.dropped.is_empty());
    }

    #[test]
    fn a_report_with_no_position_is_dropped_whole() {
        let mut bad = item();
        bad.position = None;
        let out = validate_item(&bad, &Validation::default());
        assert!(out.is_empty());
        assert_eq!(out.dropped, vec![DropReason::NoPosition]);
    }

    #[test]
    fn a_bad_position_drops_every_station_in_the_report() {
        // Learning even one station from a wrong position is worse than
        // learning nothing.
        for (label, position) in [
            ("latitude off the map", ApiPosition { latitude: Some(91.0), ..good_position() }),
            ("longitude off the map", ApiPosition { longitude: Some(200.0), ..good_position() }),
            ("not a number", ApiPosition { latitude: Some(f64::NAN), ..good_position() }),
            ("impossible accuracy", ApiPosition { accuracy: Some(2_000_000.0), ..good_position() }),
            ("faster than sound", ApiPosition { speed: Some(400.0), ..good_position() }),
        ] {
            let mut bad = item();
            bad.position = Some(position);
            let out = validate_item(&bad, &Validation::default());
            assert!(out.is_empty(), "{label} should have dropped the whole report");
        }
    }

    #[test]
    fn a_bad_station_drops_only_itself() {
        let mut mixed = item();
        mixed.wifi_access_points = vec![good_wifi("aabbccddeeff"), good_wifi("not-a-mac!")];
        let out = validate_item(&mixed, &Validation::default());
        assert_eq!(out.observations.len(), 2, "one good wifi and the cell survive");
        assert_eq!(out.dropped, vec![DropReason::BadMac]);
    }

    #[test]
    fn placeholder_addresses_are_never_stored() {
        for mac in ["000000000000", "ffffffffffff", "01005e901000", "FFFFFFFFFFFF"] {
            let mut bad = item();
            bad.cell_towers.clear();
            bad.wifi_access_points = vec![good_wifi(mac)];
            let out = validate_item(&bad, &Validation::default());
            assert!(out.is_empty(), "{mac} should have been rejected");
            assert_eq!(out.dropped, vec![DropReason::PlaceholderMac]);
        }
    }

    #[test]
    fn addresses_are_matched_regardless_of_case() {
        let mut upper = item();
        upper.cell_towers.clear();
        upper.wifi_access_points = vec![good_wifi("AABBCCDDEEFF")];
        let out = validate_item(&upper, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(
            out.observations[0].key,
            StationKey::Wifi(WifiKey::parse("aabbccddeeff").unwrap())
        );
    }

    #[test]
    fn a_signal_of_zero_means_not_measured_rather_than_extraordinary() {
        let mut zeroed = item();
        zeroed.cell_towers.clear();
        zeroed.wifi_access_points =
            vec![WifiAccessPoint { signal_strength: Some(0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&zeroed, &Validation::default());
        assert_eq!(out.observations.len(), 1);
        assert_eq!(out.observations[0].signal, None);
    }

    #[test]
    fn an_implausible_signal_drops_the_station() {
        let mut bad = item();
        bad.cell_towers.clear();
        bad.wifi_access_points =
            vec![WifiAccessPoint { signal_strength: Some(50), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&bad, &Validation::default());
        assert_eq!(out.dropped, vec![DropReason::SignalOutOfRange]);
    }

    #[test]
    fn a_frequency_outside_the_wifi_bands_drops_the_station() {
        let mut bad = item();
        bad.cell_towers.clear();
        bad.wifi_access_points =
            vec![WifiAccessPoint { frequency: Some(900), ..good_wifi("aabbccddeeff") }];
        assert_eq!(
            validate_item(&bad, &Validation::default()).dropped,
            vec![DropReason::FrequencyOutOfRange]
        );
    }

    #[test]
    fn an_unknown_radio_drops_the_cell() {
        let mut bad = item();
        bad.wifi_access_points.clear();
        bad.cell_towers =
            vec![CellTower { radio_type: Some("smoke signals".into()), ..good_cell() }];
        assert_eq!(
            validate_item(&bad, &Validation::default()).dropped,
            vec![DropReason::UnknownRadio]
        );
    }

    #[test]
    fn a_gsm_cell_id_beyond_sixteen_bits_is_rejected() {
        // A GSM identifier is 16 bits. Anything larger means the client filled
        // the field from a different radio.
        let mut bad = item();
        bad.wifi_access_points.clear();
        bad.cell_towers = vec![CellTower {
            radio_type: Some("gsm".into()),
            cell_id: Some(70_000),
            signal_strength: Some(-90),
            primary_scrambling_code: Some(0),
            ..good_cell()
        }];
        assert_eq!(
            validate_item(&bad, &Validation::default()).dropped,
            vec![DropReason::CellFieldOutOfRange]
        );
    }

    #[test]
    fn the_same_cell_id_is_fine_on_lte() {
        let mut ok = item();
        ok.wifi_access_points.clear();
        ok.cell_towers = vec![CellTower { cell_id: Some(70_000), ..good_cell() }];
        assert_eq!(validate_item(&ok, &Validation::default()).observations.len(), 1);
    }

    #[test]
    fn an_lte_scrambling_code_above_its_own_limit_is_rejected() {
        let mut bad = item();
        bad.wifi_access_points.clear();
        bad.cell_towers = vec![CellTower { primary_scrambling_code: Some(510), ..good_cell() }];
        assert_eq!(
            validate_item(&bad, &Validation::default()).dropped,
            vec![DropReason::CellFieldOutOfRange]
        );
        // The same value is legal on WCDMA, whose limit is higher.
        let mut ok = item();
        ok.wifi_access_points.clear();
        ok.cell_towers = vec![CellTower {
            radio_type: Some("wcdma".into()),
            primary_scrambling_code: Some(510),
            signal_strength: Some(-100),
            ..good_cell()
        }];
        assert_eq!(validate_item(&ok, &Validation::default()).observations.len(), 1);
    }

    #[test]
    fn an_allowlist_keeps_foreign_networks_out() {
        let cfg = Validation { mcc_allowlist: vec![432], ..Validation::default() };
        let mut foreign = item();
        foreign.wifi_access_points.clear();
        foreign.cell_towers = vec![CellTower { mobile_country_code: Some(262), ..good_cell() }];
        assert_eq!(validate_item(&foreign, &cfg).dropped, vec![DropReason::MccNotAllowed]);
        // And the allowed country still passes.
        assert_eq!(validate_item(&item(), &cfg).observations.len(), 2);
    }

    #[test]
    fn an_empty_allowlist_accepts_every_country() {
        let mut foreign = item();
        foreign.wifi_access_points.clear();
        foreign.cell_towers = vec![CellTower { mobile_country_code: Some(262), ..good_cell() }];
        assert_eq!(validate_item(&foreign, &Validation::default()).observations.len(), 1);
    }

    #[test]
    fn an_observation_with_no_weight_is_dropped_rather_than_stored() {
        // A stale scan carries no weight, and a batch of weightless
        // observations has nothing to average.
        let mut stale = item();
        stale.cell_towers.clear();
        stale.wifi_access_points =
            vec![WifiAccessPoint { age: Some(60_000.0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&stale, &Validation::default());
        assert!(out.is_empty());
        assert_eq!(out.dropped, vec![DropReason::Worthless]);
    }

    #[test]
    fn a_station_age_overrides_the_position_age() {
        let mut mixed = item();
        mixed.cell_towers.clear();
        mixed.position = Some(ApiPosition { age: Some(5_000.0), ..good_position() });
        mixed.wifi_access_points =
            vec![WifiAccessPoint { age: Some(1_000.0), ..good_wifi("aabbccddeeff") }];
        let out = validate_item(&mixed, &Validation::default());
        assert_eq!(out.observations[0].age_ms, Some(1_000.0));
    }

    #[test]
    fn the_report_source_is_read_when_given_and_assumed_otherwise() {
        let mut typed = item();
        typed.position = Some(ApiPosition { source: Some("query".into()), ..good_position() });
        assert_eq!(
            validate_item(&typed, &Validation::default()).observations[0].source,
            ReportSource::Query
        );

        assert_eq!(
            validate_item(&item(), &Validation::default()).observations[0].source,
            ReportSource::Gnss,
            "silence means satellite navigation"
        );
    }

    #[test]
    fn a_report_with_no_stations_yields_nothing_and_complains_about_nothing() {
        let bare = SubmitItem { position: Some(good_position()), ..Default::default() };
        let out = validate_item(&bare, &Validation::default());
        assert!(out.is_empty());
        assert!(out.dropped.is_empty());
    }
}
