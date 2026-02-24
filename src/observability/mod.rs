use std::env;
use std::sync::OnceLock;

use thiserror::Error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

const ENV_LOG_FILTER: &str = "LSMDB_LOG";
const ENV_RUST_LOG: &str = "RUST_LOG";
const ENV_LOG_TARGET: &str = "LSMDB_LOG_TARGET";
const DEFAULT_LOG_FILTER: &str = "info";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityConfig {
    pub log_filter: String,
    pub with_target: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self { log_filter: DEFAULT_LOG_FILTER.to_string(), with_target: true }
    }
}

impl ObservabilityConfig {
    pub fn from_env() -> Self {
        let log_filter = env::var(ENV_LOG_FILTER)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| env::var(ENV_RUST_LOG).ok().filter(|value| !value.trim().is_empty()))
            .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_string());

        let with_target = env::var(ENV_LOG_TARGET)
            .ok()
            .map(|value| parse_bool_flag(value.trim()))
            .unwrap_or(true);

        Self { log_filter, with_target }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ObservabilityError {
    #[error("invalid log filter '{filter}': {message}")]
    InvalidLogFilter { filter: String, message: String },
    #[error("failed to initialize tracing subscriber: {0}")]
    SubscriberInit(String),
}

static OBSERVABILITY_INIT: OnceLock<Result<(), ObservabilityError>> = OnceLock::new();

pub fn init_tracing_from_env() -> Result<(), ObservabilityError> {
    init_tracing(ObservabilityConfig::from_env())
}

pub fn init_tracing(config: ObservabilityConfig) -> Result<(), ObservabilityError> {
    OBSERVABILITY_INIT
        .get_or_init(|| install_subscriber(config))
        .as_ref()
        .map(|_| ())
        .map_err(Clone::clone)
}

fn install_subscriber(config: ObservabilityConfig) -> Result<(), ObservabilityError> {
    if tracing::dispatcher::has_been_set() {
        return Ok(());
    }

    let filter = EnvFilter::try_new(config.log_filter.clone()).map_err(|err| {
        ObservabilityError::InvalidLogFilter {
            filter: config.log_filter.clone(),
            message: err.to_string(),
        }
    })?;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(config.with_target)
        .with_thread_ids(true)
        .with_thread_names(true);

    match tracing_subscriber::registry().with(filter).with(fmt_layer).try_init() {
        Ok(()) => Ok(()),
        Err(_) if tracing::dispatcher::has_been_set() => Ok(()),
        Err(err) => Err(ObservabilityError::SubscriberInit(err.to_string())),
    }
}

fn parse_bool_flag(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on" | "y")
}
