//! The domain types: what a station is, what a sighting of one is, and how each
//! is identified.

pub mod key;
pub mod observation;
pub mod station;

pub use key::{CellKey, KeyError, Radio, StationKey, StationKind, WifiKey};
pub use observation::{Observation, ReportSource};
pub use station::Station;
