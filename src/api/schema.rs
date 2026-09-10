//! Request and response shapes.
//!
//! Wire-compatible with Ichnaea's `/v2/geosubmit` and `/v1/geolocate`, so an
//! existing client can be pointed at this service without changing a line.
//! Field names are the ones clients already send, which is why they are camel
//! case here and snake case everywhere else.
//!
//! Unknown fields are ignored rather than rejected. Clients send fields this
//! service has no use for, and refusing a whole report over one of them would
//! lose data for no benefit.

use serde::{Deserialize, Serialize};

/// A batch of reports.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SubmitRequest {
    #[serde(default)]
    pub items: Vec<SubmitItem>,
}

/// One report: where the device was, and what it could see from there.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitItem {
    pub position: Option<Position>,
    /// Milliseconds since the Unix epoch.
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub cell_towers: Vec<CellTower>,
    #[serde(default)]
    pub wifi_access_points: Vec<WifiAccessPoint>,
}

/// Where the reporting device was when it saw these stations.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    /// Radius in metres the client believes its own fix is good to.
    pub accuracy: Option<f64>,
    /// Milliseconds between this fix and the moment the report was assembled.
    pub age: Option<f64>,
    /// Metres per second.
    pub speed: Option<f64>,
    /// What produced the fix. Absent means satellite navigation, which is what
    /// almost every client means when it says nothing.
    pub source: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CellTower {
    pub radio_type: Option<String>,
    pub mobile_country_code: Option<i64>,
    pub mobile_network_code: Option<i64>,
    pub location_area_code: Option<i64>,
    pub cell_id: Option<i64>,
    pub primary_scrambling_code: Option<i64>,
    pub signal_strength: Option<i64>,
    pub timing_advance: Option<i64>,
    /// Milliseconds before the position fix that this tower was seen.
    pub age: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WifiAccessPoint {
    /// Twelve hexadecimal characters. Clients may send a real address or the
    /// leading twelve characters of a hash of one; either is treated as an
    /// opaque identifier.
    pub mac_address: Option<String>,
    pub signal_strength: Option<i64>,
    pub frequency: Option<i64>,
    pub channel: Option<i64>,
    pub age: Option<f64>,
}

/// A locate request. The same two lists, with no position.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocateRequest {
    #[serde(default)]
    pub cell_towers: Vec<CellTower>,
    #[serde(default)]
    pub wifi_access_points: Vec<WifiAccessPoint>,
}

/// A successful locate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocateResponse {
    pub location: LatLng,
    /// Radius in metres.
    pub accuracy: f64,
    /// Present only when the caller asked for the working.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Explain>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LatLng {
    pub lat: f64,
    pub lng: f64,
}

/// Why the service answered the way it did. Off unless asked for, so ordinary
/// traffic pays nothing for it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Explain {
    pub wifi_sent: usize,
    pub wifi_known: usize,
    pub cell_sent: usize,
    pub cell_known: usize,
    pub clusters: Vec<ExplainCluster>,
    pub used: Vec<ExplainNetwork>,
    /// A sentence saying which candidate won and why.
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainCluster {
    pub source: String,
    pub size: usize,
    pub score: f64,
    pub accuracy: f64,
    pub chosen: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainNetwork {
    pub key: String,
    pub score: f64,
    pub seen_today: bool,
}

/// The error body, in the shape clients already parse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub errors: Vec<ErrorDetail>,
    pub code: u16,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub domain: String,
    pub reason: String,
    pub message: String,
}

impl ErrorResponse {
    /// The body returned when no position could be worked out. Byte-compatible
    /// with what clients expect from Ichnaea.
    pub fn not_found() -> Self {
        Self {
            error: ErrorBody {
                errors: vec![ErrorDetail {
                    domain: "geolocation".into(),
                    reason: "notFound".into(),
                    message: "Not found".into(),
                }],
                code: 404,
                message: "Not found".into(),
            },
        }
    }

    pub fn invalid(message: &str) -> Self {
        Self {
            error: ErrorBody {
                errors: vec![ErrorDetail {
                    domain: "global".into(),
                    reason: "parseError".into(),
                    message: message.into(),
                }],
                code: 400,
                message: message.into(),
            },
        }
    }

    pub fn forbidden(message: &str) -> Self {
        Self {
            error: ErrorBody {
                errors: vec![ErrorDetail {
                    domain: "usageLimits".into(),
                    reason: "keyInvalid".into(),
                    message: message.into(),
                }],
                code: 400,
                message: message.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minimal_submit_body_parses() {
        let body = r#"{"items":[{"position":{"latitude":35.7219,"longitude":51.3347,"accuracy":12.5},
            "wifiAccessPoints":[{"macAddress":"aabbccddeeff","signalStrength":-70,"frequency":2412}]}]}"#;
        let req: SubmitRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.items.len(), 1);
        let item = &req.items[0];
        assert_eq!(item.position.as_ref().unwrap().latitude, Some(35.7219));
        assert_eq!(item.wifi_access_points[0].mac_address.as_deref(), Some("aabbccddeeff"));
        assert!(item.cell_towers.is_empty());
    }

    #[test]
    fn unknown_fields_are_ignored_rather_than_rejected() {
        // Clients send fields this service has no use for. Losing a whole
        // report over one of them would be a poor trade.
        let body = r#"{"items":[{"position":{"latitude":1.0,"longitude":2.0,"altitude":500,
            "pressure":1013,"heading":270},"somethingNew":true}],"extra":42}"#;
        let req: SubmitRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.items.len(), 1);
    }

    #[test]
    fn an_empty_body_parses_to_an_empty_batch() {
        let req: SubmitRequest = serde_json::from_str("{}").unwrap();
        assert!(req.items.is_empty());
    }

    #[test]
    fn a_cell_tower_parses_with_every_field_clients_send() {
        let body = r#"{"radioType":"lte","mobileCountryCode":432,"mobileNetworkCode":11,
            "locationAreaCode":1234,"cellId":56789,"primaryScramblingCode":5,
            "signalStrength":-105,"timingAdvance":3,"age":1200}"#;
        let cell: CellTower = serde_json::from_str(body).unwrap();
        assert_eq!(cell.radio_type.as_deref(), Some("lte"));
        assert_eq!(cell.cell_id, Some(56789));
        assert_eq!(cell.timing_advance, Some(3));
    }

    #[test]
    fn a_locate_request_parses() {
        let body = r#"{"wifiAccessPoints":[{"macAddress":"aabbccddeeff","signalStrength":-70}],
            "cellTowers":[],"considerIp":false}"#;
        let req: LocateRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.wifi_access_points.len(), 1);
        assert!(req.cell_towers.is_empty());
    }

    #[test]
    fn a_locate_response_serialises_in_the_expected_shape() {
        let response = LocateResponse {
            location: LatLng { lat: 35.7219, lng: 51.3347 },
            accuracy: 96.0,
            explain: None,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert_eq!(json, r#"{"location":{"lat":35.7219,"lng":51.3347},"accuracy":96.0}"#);
    }

    #[test]
    fn the_not_found_body_matches_what_clients_parse() {
        let json = serde_json::to_value(ErrorResponse::not_found()).unwrap();
        assert_eq!(json["error"]["code"], 404);
        assert_eq!(json["error"]["message"], "Not found");
        assert_eq!(json["error"]["errors"][0]["reason"], "notFound");
        assert_eq!(json["error"]["errors"][0]["domain"], "geolocation");
    }

    #[test]
    fn missing_lists_default_to_empty_rather_than_failing() {
        let item: SubmitItem = serde_json::from_str(r#"{"position":{"latitude":1,"longitude":2}}"#).unwrap();
        assert!(item.cell_towers.is_empty());
        assert!(item.wifi_access_points.is_empty());
    }

    #[test]
    fn integers_are_accepted_where_floats_are_expected() {
        // Clients send whole numbers without a decimal point.
        let p: Position = serde_json::from_str(r#"{"latitude":35,"longitude":51,"accuracy":10}"#).unwrap();
        assert_eq!(p.latitude, Some(35.0));
        assert_eq!(p.accuracy, Some(10.0));
    }
}
