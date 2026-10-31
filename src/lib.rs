//! Guvna Rust core. Keep request policy in one place; never clone handler walks.
pub(crate) mod auto_decider;
pub mod auto_health;
pub mod chain_store;
pub mod circuit;
pub mod config;
pub mod gateway;
pub mod keypool;
pub mod metrics;
pub mod model_capabilities;
pub mod ops_log;
pub mod outcome;
pub mod routing;
pub mod scoring;
pub mod sse;
pub mod telemetry;
pub(crate) mod telemetry_rank;

pub(crate) mod native_responses_sse;
pub(crate) mod responses_sse;
pub(crate) mod structured_output;
