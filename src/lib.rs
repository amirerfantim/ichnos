//! Ichnos: a geolocation service that learns where WiFi access points and cell
//! towers are, and answers where a device is from what it can see.
//!
//! The positioning maths is a port of Mozilla Ichnaea's, and Ichnos replaces
//! Ichnaea rather than running beside it. What differs is everything around the
//! maths: the write path folds and batches instead of updating rows
//! one at a time, workers own shards instead of taking locks, and reads are
//! cached.

pub mod api;
pub mod config;
pub mod geo;
pub mod ingest;
pub mod locate;
pub mod metrics;
pub mod model;
pub mod redis_backend;
pub mod replay;

pub use config::Config;
pub mod store;
pub mod worker;
