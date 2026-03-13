use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use crate::server::ServerLimits;
use crate::storage::compaction::{
    CompactionStrategy, LeveledCompactionConfig, TieredCompactionConfig,
};
use crate::storage::engine::StorageEngineOptions;
use crate::storage::sstable::SSTableBuilderOptions;
use crate::storage::wal::{HEADER_LEN, SyncMode, WalWriterOptions};

const MIN_WAL_SEGMENT_SIZE_BYTES: u64 = (HEADER_LEN + 1) as u64;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file '{path}': {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config TOML: {0}")]
    ParseToml(#[from] toml::de::Error),
    #[error("invalid config value for '{field}': {message}")]
    InvalidValue { field: &'static str, message: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LsmdbConfig {
    pub storage: StorageConfig,
    pub server: ServerConfig,
    pub wal: WalConfig,
    pub sstable: SstableConfig,
    pub compaction: CompactionConfig,
}

impl Default for LsmdbConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            server: ServerConfig::default(),
            wal: WalConfig::default(),
            sstable: SstableConfig::default(),
            compaction: CompactionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub memtable_size_bytes: usize,
    pub memtable_arena_block_size_bytes: usize,
    pub flush_poll_interval_ms: u64,
    pub flush_timeout_ms: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        let defaults = StorageEngineOptions::default();
        Self {
            memtable_size_bytes: defaults.memtable_size_bytes,
            memtable_arena_block_size_bytes: defaults.memtable_arena_block_size_bytes,
            flush_poll_interval_ms: defaults.flush_poll_interval.as_millis() as u64,
            flush_timeout_ms: defaults.flush_timeout.as_millis() as u64,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub max_concurrent_connections: usize,
    pub max_in_flight_requests_per_connection: usize,
    pub max_request_bytes: usize,
    pub max_statements_per_request: usize,
    pub statement_timeout_ms: Option<u64>,
    pub max_memory_intensive_requests: usize,
    pub max_scan_rows: usize,
    pub max_sort_rows: usize,
    pub max_join_rows: usize,
    pub max_query_result_rows: usize,
    pub max_query_result_bytes: usize,
    pub max_concurrent_queries_per_identity: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let defaults = ServerLimits::default();
        Self {
            max_concurrent_connections: defaults.max_concurrent_connections,
            max_in_flight_requests_per_connection: defaults.max_in_flight_requests_per_connection,
            max_request_bytes: defaults.max_request_bytes,
            max_statements_per_request: defaults.max_statements_per_request,
            statement_timeout_ms: defaults.statement_timeout_ms,
            max_memory_intensive_requests: defaults.max_memory_intensive_requests,
            max_scan_rows: defaults.max_scan_rows,
            max_sort_rows: defaults.max_sort_rows,
            max_join_rows: defaults.max_join_rows,
            max_query_result_rows: defaults.max_query_result_rows,
            max_query_result_bytes: defaults.max_query_result_bytes,
            max_concurrent_queries_per_identity: defaults.max_concurrent_queries_per_identity,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalConfig {
    pub segment_size_bytes: u64,
    pub sync_mode: SyncModeConfig,
}

impl Default for WalConfig {
    fn default() -> Self {
        let defaults = WalWriterOptions::default();
        Self {
            segment_size_bytes: defaults.segment_size_bytes,
            sync_mode: SyncModeConfig::from(defaults.sync_mode),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncModeConfig {
    Never,
    OnCommit,
    Always,
}

impl From<SyncModeConfig> for SyncMode {
    fn from(value: SyncModeConfig) -> Self {
        match value {
            SyncModeConfig::Never => SyncMode::Never,
            SyncModeConfig::OnCommit => SyncMode::OnCommit,
            SyncModeConfig::Always => SyncMode::Always,
        }
    }
}

impl From<SyncMode> for SyncModeConfig {
    fn from(value: SyncMode) -> Self {
        match value {
            SyncMode::Never => SyncModeConfig::Never,
            SyncMode::OnCommit => SyncModeConfig::OnCommit,
            SyncMode::Always => SyncModeConfig::Always,
        }
    }
}

impl SyncModeConfig {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncModeConfig::Never => "never",
            SyncModeConfig::OnCommit => "on_commit",
            SyncModeConfig::Always => "always",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SstableConfig {
    pub data_block_size_bytes: usize,
    pub restart_interval: usize,
    pub bloom_fpr: f64,
}

impl Default for SstableConfig {
    fn default() -> Self {
        let defaults = SSTableBuilderOptions::default();
        Self {
            data_block_size_bytes: defaults.data_block_size_bytes,
            restart_interval: defaults.restart_interval,
            bloom_fpr: bloom_fpr_from_params(
                defaults.bloom_bits_per_key,
                defaults.bloom_hash_functions,
            ),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactionConfig {
    pub strategy: CompactionMode,
    pub leveled: LeveledConfig,
    pub tiered: TieredConfig,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: CompactionMode::default(),
            leveled: LeveledConfig::default(),
            tiered: TieredConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMode {
    #[default]
    Leveled,
    Tiered,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LeveledConfig {
    pub level0_file_limit: usize,
    pub level_size_base_bytes: u64,
    pub level_size_multiplier: u64,
    pub max_levels: u32,
}

impl Default for LeveledConfig {
    fn default() -> Self {
        let defaults = LeveledCompactionConfig::default();
        Self {
            level0_file_limit: defaults.level0_file_limit,
            level_size_base_bytes: defaults.level_size_base_bytes,
            level_size_multiplier: defaults.level_size_multiplier,
            max_levels: defaults.max_levels,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TieredConfig {
    pub max_components_per_tier: usize,
    pub min_tier_size_bytes: u64,
    pub output_level: u32,
}

impl Default for TieredConfig {
    fn default() -> Self {
        let defaults = TieredCompactionConfig::default();
        Self {
            max_components_per_tier: defaults.max_components_per_tier,
            min_tier_size_bytes: defaults.min_tier_size_bytes,
            output_level: defaults.output_level,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub storage_engine: StorageEngineOptions,
    pub compaction_strategy: CompactionStrategy,
    pub server_limits: ServerLimits,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionDiagnostics {
    Leveled {
        level0_file_limit: usize,
        level_size_base_bytes: u64,
        level_size_multiplier: u64,
        max_levels: u32,
    },
    Tiered {
        max_components_per_tier: usize,
        min_tier_size_bytes: u64,
        output_level: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupDiagnostics {
    pub memtable_size_bytes: usize,
    pub memtable_arena_block_size_bytes: usize,
    pub flush_poll_interval_ms: u64,
    pub flush_timeout_ms: u64,
    pub server_max_concurrent_connections: usize,
    pub server_max_in_flight_requests_per_connection: usize,
    pub server_max_request_bytes: usize,
    pub server_max_statements_per_request: usize,
    pub server_statement_timeout_ms: Option<u64>,
    pub server_max_memory_intensive_requests: usize,
    pub server_max_scan_rows: usize,
    pub server_max_sort_rows: usize,
    pub server_max_join_rows: usize,
    pub server_max_query_result_rows: usize,
    pub server_max_query_result_bytes: usize,
    pub server_max_concurrent_queries_per_identity: Option<usize>,
    pub wal_segment_size_bytes: u64,
    pub wal_sync_mode: SyncModeConfig,
    pub sstable_data_block_size_bytes: usize,
    pub sstable_restart_interval: usize,
    pub sstable_bloom_bits_per_key: usize,
    pub sstable_bloom_hash_functions: u8,
    pub compaction: CompactionDiagnostics,
}

impl StartupDiagnostics {
    pub fn as_key_value_lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!("storage.memtable_size_bytes={}", self.memtable_size_bytes),
            format!(
                "storage.memtable_arena_block_size_bytes={}",
                self.memtable_arena_block_size_bytes
            ),
            format!("storage.flush_poll_interval_ms={}", self.flush_poll_interval_ms),
            format!("storage.flush_timeout_ms={}", self.flush_timeout_ms),
            format!("server.max_concurrent_connections={}", self.server_max_concurrent_connections),
            format!(
                "server.max_in_flight_requests_per_connection={}",
                self.server_max_in_flight_requests_per_connection
            ),
            format!("server.max_request_bytes={}", self.server_max_request_bytes),
            format!("server.max_statements_per_request={}", self.server_max_statements_per_request),
            format!(
                "server.statement_timeout_ms={}",
                format_optional_u64(self.server_statement_timeout_ms)
            ),
            format!(
                "server.max_memory_intensive_requests={}",
                self.server_max_memory_intensive_requests
            ),
            format!("server.max_scan_rows={}", self.server_max_scan_rows),
            format!("server.max_sort_rows={}", self.server_max_sort_rows),
            format!("server.max_join_rows={}", self.server_max_join_rows),
            format!("server.max_query_result_rows={}", self.server_max_query_result_rows),
            format!("server.max_query_result_bytes={}", self.server_max_query_result_bytes),
            format!(
                "server.max_concurrent_queries_per_identity={}",
                format_optional_usize(self.server_max_concurrent_queries_per_identity)
            ),
            format!("wal.segment_size_bytes={}", self.wal_segment_size_bytes),
            format!("wal.sync_mode={}", self.wal_sync_mode.as_str()),
            format!("sstable.data_block_size_bytes={}", self.sstable_data_block_size_bytes),
            format!("sstable.restart_interval={}", self.sstable_restart_interval),
            format!("sstable.bloom_bits_per_key={}", self.sstable_bloom_bits_per_key),
            format!("sstable.bloom_hash_functions={}", self.sstable_bloom_hash_functions),
        ];

        match self.compaction {
            CompactionDiagnostics::Leveled {
                level0_file_limit,
                level_size_base_bytes,
                level_size_multiplier,
                max_levels,
            } => {
                lines.push("compaction.strategy=leveled".to_string());
                lines.push(format!("compaction.leveled.level0_file_limit={level0_file_limit}"));
                lines.push(format!(
                    "compaction.leveled.level_size_base_bytes={level_size_base_bytes}"
                ));
                lines.push(format!(
                    "compaction.leveled.level_size_multiplier={level_size_multiplier}"
                ));
                lines.push(format!("compaction.leveled.max_levels={max_levels}"));
            }
            CompactionDiagnostics::Tiered {
                max_components_per_tier,
                min_tier_size_bytes,
                output_level,
            } => {
                lines.push("compaction.strategy=tiered".to_string());
                lines.push(format!(
                    "compaction.tiered.max_components_per_tier={max_components_per_tier}"
                ));
                lines.push(format!("compaction.tiered.min_tier_size_bytes={min_tier_size_bytes}"));
                lines.push(format!("compaction.tiered.output_level={output_level}"));
            }
        }

        lines
    }
}

impl LsmdbConfig {
    pub fn from_toml_str(raw: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(raw)?;
        config.validate()?;
        Ok(config)
    }

    pub fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref().to_path_buf();
        let raw = fs::read_to_string(&path)
            .map_err(|source| ConfigError::ReadFile { path: path.clone(), source })?;
        Self::from_toml_str(&raw)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.storage.memtable_size_bytes == 0 {
            return Err(invalid("storage.memtable_size_bytes", "must be > 0"));
        }
        if self.storage.memtable_arena_block_size_bytes == 0 {
            return Err(invalid("storage.memtable_arena_block_size_bytes", "must be > 0"));
        }
        if self.storage.memtable_arena_block_size_bytes > self.storage.memtable_size_bytes {
            return Err(invalid(
                "storage.memtable_arena_block_size_bytes",
                "must be <= storage.memtable_size_bytes",
            ));
        }
        if self.storage.flush_poll_interval_ms == 0 {
            return Err(invalid("storage.flush_poll_interval_ms", "must be > 0"));
        }
        if self.storage.flush_timeout_ms == 0 {
            return Err(invalid("storage.flush_timeout_ms", "must be > 0"));
        }
        if self.storage.flush_timeout_ms < self.storage.flush_poll_interval_ms {
            return Err(invalid(
                "storage.flush_timeout_ms",
                "must be >= storage.flush_poll_interval_ms",
            ));
        }
        if self.server.max_concurrent_connections == 0 {
            return Err(invalid("server.max_concurrent_connections", "must be > 0"));
        }
        if self.server.max_in_flight_requests_per_connection == 0 {
            return Err(invalid("server.max_in_flight_requests_per_connection", "must be > 0"));
        }
        if self.server.max_request_bytes == 0 {
            return Err(invalid("server.max_request_bytes", "must be > 0"));
        }
        if self.server.max_statements_per_request == 0 {
            return Err(invalid("server.max_statements_per_request", "must be > 0"));
        }
        if matches!(self.server.statement_timeout_ms, Some(0)) {
            return Err(invalid("server.statement_timeout_ms", "must be > 0 when set"));
        }
        if self.server.max_memory_intensive_requests == 0 {
            return Err(invalid("server.max_memory_intensive_requests", "must be > 0"));
        }
        if self.server.max_scan_rows == 0 {
            return Err(invalid("server.max_scan_rows", "must be > 0"));
        }
        if self.server.max_sort_rows == 0 {
            return Err(invalid("server.max_sort_rows", "must be > 0"));
        }
        if self.server.max_join_rows == 0 {
            return Err(invalid("server.max_join_rows", "must be > 0"));
        }
        if self.server.max_query_result_rows == 0 {
            return Err(invalid("server.max_query_result_rows", "must be > 0"));
        }
        if self.server.max_query_result_bytes == 0 {
            return Err(invalid("server.max_query_result_bytes", "must be > 0"));
        }
        if matches!(self.server.max_concurrent_queries_per_identity, Some(0)) {
            return Err(invalid(
                "server.max_concurrent_queries_per_identity",
                "must be > 0 when set",
            ));
        }
        if self.wal.segment_size_bytes < MIN_WAL_SEGMENT_SIZE_BYTES {
            return Err(invalid(
                "wal.segment_size_bytes",
                format!("must be >= {MIN_WAL_SEGMENT_SIZE_BYTES}"),
            ));
        }
        if self.sstable.data_block_size_bytes == 0 {
            return Err(invalid("sstable.data_block_size_bytes", "must be > 0"));
        }
        if self.sstable.restart_interval == 0 {
            return Err(invalid("sstable.restart_interval", "must be > 0"));
        }
        if !(self.sstable.bloom_fpr > 0.0 && self.sstable.bloom_fpr < 1.0) {
            return Err(invalid("sstable.bloom_fpr", "must be between 0 and 1"));
        }
        if self.compaction.leveled.level0_file_limit == 0 {
            return Err(invalid("compaction.leveled.level0_file_limit", "must be > 0"));
        }
        if self.compaction.leveled.level_size_base_bytes == 0 {
            return Err(invalid("compaction.leveled.level_size_base_bytes", "must be > 0"));
        }
        if self.compaction.leveled.level_size_multiplier == 0 {
            return Err(invalid("compaction.leveled.level_size_multiplier", "must be > 0"));
        }
        if self.compaction.leveled.max_levels < 2 {
            return Err(invalid("compaction.leveled.max_levels", "must be >= 2"));
        }
        if self.compaction.tiered.max_components_per_tier == 0 {
            return Err(invalid("compaction.tiered.max_components_per_tier", "must be > 0"));
        }
        if self.compaction.tiered.min_tier_size_bytes == 0 {
            return Err(invalid("compaction.tiered.min_tier_size_bytes", "must be > 0"));
        }

        Ok(())
    }

    pub fn startup_diagnostics(&self) -> Result<StartupDiagnostics, ConfigError> {
        let runtime = self.to_runtime_config()?;
        let storage = runtime.storage_engine;
        let compaction = match runtime.compaction_strategy {
            CompactionStrategy::Leveled(config) => CompactionDiagnostics::Leveled {
                level0_file_limit: config.level0_file_limit,
                level_size_base_bytes: config.level_size_base_bytes,
                level_size_multiplier: config.level_size_multiplier,
                max_levels: config.max_levels,
            },
            CompactionStrategy::Tiered(config) => CompactionDiagnostics::Tiered {
                max_components_per_tier: config.max_components_per_tier,
                min_tier_size_bytes: config.min_tier_size_bytes,
                output_level: config.output_level,
            },
        };

        Ok(StartupDiagnostics {
            memtable_size_bytes: storage.memtable_size_bytes,
            memtable_arena_block_size_bytes: storage.memtable_arena_block_size_bytes,
            flush_poll_interval_ms: storage.flush_poll_interval.as_millis() as u64,
            flush_timeout_ms: storage.flush_timeout.as_millis() as u64,
            server_max_concurrent_connections: runtime.server_limits.max_concurrent_connections,
            server_max_in_flight_requests_per_connection: runtime
                .server_limits
                .max_in_flight_requests_per_connection,
            server_max_request_bytes: runtime.server_limits.max_request_bytes,
            server_max_statements_per_request: runtime.server_limits.max_statements_per_request,
            server_statement_timeout_ms: runtime.server_limits.statement_timeout_ms,
            server_max_memory_intensive_requests: runtime
                .server_limits
                .max_memory_intensive_requests,
            server_max_scan_rows: runtime.server_limits.max_scan_rows,
            server_max_sort_rows: runtime.server_limits.max_sort_rows,
            server_max_join_rows: runtime.server_limits.max_join_rows,
            server_max_query_result_rows: runtime.server_limits.max_query_result_rows,
            server_max_query_result_bytes: runtime.server_limits.max_query_result_bytes,
            server_max_concurrent_queries_per_identity: runtime
                .server_limits
                .max_concurrent_queries_per_identity,
            wal_segment_size_bytes: storage.wal_options.segment_size_bytes,
            wal_sync_mode: SyncModeConfig::from(storage.wal_options.sync_mode),
            sstable_data_block_size_bytes: storage.sstable_builder_options.data_block_size_bytes,
            sstable_restart_interval: storage.sstable_builder_options.restart_interval,
            sstable_bloom_bits_per_key: storage.sstable_builder_options.bloom_bits_per_key,
            sstable_bloom_hash_functions: storage.sstable_builder_options.bloom_hash_functions,
            compaction,
        })
    }

    pub fn to_runtime_config(&self) -> Result<RuntimeConfig, ConfigError> {
        self.validate()?;

        Ok(RuntimeConfig {
            storage_engine: self.to_storage_engine_options_unchecked(),
            compaction_strategy: self.to_compaction_strategy_unchecked(),
            server_limits: self.to_server_limits_unchecked(),
        })
    }

    pub fn to_storage_engine_options(&self) -> Result<StorageEngineOptions, ConfigError> {
        self.validate()?;
        Ok(self.to_storage_engine_options_unchecked())
    }

    pub fn to_compaction_strategy(&self) -> Result<CompactionStrategy, ConfigError> {
        self.validate()?;
        Ok(self.to_compaction_strategy_unchecked())
    }

    pub fn to_server_limits(&self) -> Result<ServerLimits, ConfigError> {
        self.validate()?;
        Ok(self.to_server_limits_unchecked())
    }

    fn to_storage_engine_options_unchecked(&self) -> StorageEngineOptions {
        let (bits_per_key, hash_functions) = bloom_params_for_fpr(self.sstable.bloom_fpr);

        StorageEngineOptions {
            memtable_size_bytes: self.storage.memtable_size_bytes,
            memtable_arena_block_size_bytes: self.storage.memtable_arena_block_size_bytes,
            wal_options: WalWriterOptions {
                segment_size_bytes: self.wal.segment_size_bytes,
                sync_mode: self.wal.sync_mode.into(),
            },
            sstable_builder_options: SSTableBuilderOptions {
                data_block_size_bytes: self.sstable.data_block_size_bytes,
                restart_interval: self.sstable.restart_interval,
                bloom_bits_per_key: bits_per_key,
                bloom_hash_functions: hash_functions,
            },
            compaction_strategy: self.to_compaction_strategy_unchecked(),
            flush_poll_interval: Duration::from_millis(self.storage.flush_poll_interval_ms),
            flush_timeout: Duration::from_millis(self.storage.flush_timeout_ms),
        }
    }

    fn to_compaction_strategy_unchecked(&self) -> CompactionStrategy {
        match self.compaction.strategy {
            CompactionMode::Leveled => CompactionStrategy::Leveled(LeveledCompactionConfig {
                level0_file_limit: self.compaction.leveled.level0_file_limit,
                level_size_base_bytes: self.compaction.leveled.level_size_base_bytes,
                level_size_multiplier: self.compaction.leveled.level_size_multiplier,
                max_levels: self.compaction.leveled.max_levels,
            }),
            CompactionMode::Tiered => CompactionStrategy::Tiered(TieredCompactionConfig {
                max_components_per_tier: self.compaction.tiered.max_components_per_tier,
                min_tier_size_bytes: self.compaction.tiered.min_tier_size_bytes,
                output_level: self.compaction.tiered.output_level,
            }),
        }
    }

    fn to_server_limits_unchecked(&self) -> ServerLimits {
        ServerLimits {
            max_concurrent_connections: self.server.max_concurrent_connections,
            max_in_flight_requests_per_connection: self
                .server
                .max_in_flight_requests_per_connection,
            max_request_bytes: self.server.max_request_bytes,
            max_statements_per_request: self.server.max_statements_per_request,
            statement_timeout_ms: self.server.statement_timeout_ms,
            max_memory_intensive_requests: self.server.max_memory_intensive_requests,
            max_scan_rows: self.server.max_scan_rows,
            max_sort_rows: self.server.max_sort_rows,
            max_join_rows: self.server.max_join_rows,
            max_query_result_rows: self.server.max_query_result_rows,
            max_query_result_bytes: self.server.max_query_result_bytes,
            max_concurrent_queries_per_identity: self.server.max_concurrent_queries_per_identity,
        }
    }
}

fn invalid(field: &'static str, message: impl Into<String>) -> ConfigError {
    ConfigError::InvalidValue { field, message: message.into() }
}

fn format_optional_u64(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_else(|| "none".to_string())
}

fn format_optional_usize(value: Option<usize>) -> String {
    value.map(|value| value.to_string()).unwrap_or_else(|| "none".to_string())
}

fn bloom_params_for_fpr(fpr: f64) -> (usize, u8) {
    let ln2 = std::f64::consts::LN_2;
    let bits = (-(fpr.ln()) / (ln2 * ln2)).ceil().max(1.0) as usize;
    let hashes = ((bits as f64 * ln2).round()).clamp(1.0, u8::MAX as f64) as u8;
    (bits, hashes)
}

fn bloom_fpr_from_params(bits_per_key: usize, hash_functions: u8) -> f64 {
    let bits = bits_per_key.max(1) as f64;
    let hashes = hash_functions.max(1) as f64;
    (1.0 - (-hashes / bits).exp()).powf(hashes)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn temp_file_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        path.push(format!("lsmdb-config-{label}-{}-{nanos}.toml", std::process::id()));
        path
    }

    #[test]
    fn parses_defaults_from_empty_toml() {
        let config = LsmdbConfig::from_toml_str("").expect("parse defaults");
        config.validate().expect("validate defaults");

        let runtime = config.to_runtime_config().expect("runtime config");
        assert!(runtime.storage_engine.memtable_size_bytes > 0);
        assert!(runtime.server_limits.max_concurrent_connections > 0);
        match runtime.compaction_strategy {
            CompactionStrategy::Leveled(_) => {}
            CompactionStrategy::Tiered(_) => panic!("expected leveled strategy by default"),
        }
    }

    #[test]
    fn parses_and_maps_custom_config() {
        let raw = r#"
            [storage]
            memtable_size_bytes = 1048576
            memtable_arena_block_size_bytes = 16384
            flush_poll_interval_ms = 15
            flush_timeout_ms = 2000

            [server]
            max_concurrent_connections = 32
            max_in_flight_requests_per_connection = 1
            max_request_bytes = 65536
            max_statements_per_request = 8
            max_memory_intensive_requests = 4
            max_scan_rows = 2048
            max_sort_rows = 1024
            max_join_rows = 512
            max_query_result_rows = 256

            [wal]
            segment_size_bytes = 16777216
            sync_mode = "always"

            [sstable]
            data_block_size_bytes = 8192
            restart_interval = 8
            bloom_fpr = 0.02

            [compaction]
            strategy = "tiered"

            [compaction.tiered]
            max_components_per_tier = 6
            min_tier_size_bytes = 1024
            output_level = 2
        "#;

        let config = LsmdbConfig::from_toml_str(raw).expect("parse custom config");
        let options = config.to_storage_engine_options().expect("storage options");
        let server_limits = config.to_server_limits().expect("server limits");
        assert_eq!(options.memtable_size_bytes, 1_048_576);
        assert_eq!(server_limits.max_concurrent_connections, 32);
        assert_eq!(server_limits.max_request_bytes, 65_536);
        assert_eq!(server_limits.max_join_rows, 512);
        assert_eq!(options.wal_options.segment_size_bytes, 16_777_216);
        assert_eq!(options.wal_options.sync_mode, SyncMode::Always);
        assert_eq!(options.sstable_builder_options.data_block_size_bytes, 8192);
        assert_eq!(options.sstable_builder_options.restart_interval, 8);
        assert!(options.sstable_builder_options.bloom_bits_per_key > 0);
        assert!(options.sstable_builder_options.bloom_hash_functions > 0);

        let strategy = config.to_compaction_strategy().expect("compaction strategy");
        match strategy {
            CompactionStrategy::Tiered(tiered) => {
                assert_eq!(tiered.max_components_per_tier, 6);
                assert_eq!(tiered.min_tier_size_bytes, 1024);
                assert_eq!(tiered.output_level, 2);
            }
            CompactionStrategy::Leveled(_) => panic!("expected tiered strategy"),
        }
    }

    #[test]
    fn rejects_invalid_bloom_fpr() {
        let raw = r#"
            [sstable]
            bloom_fpr = 1.0
        "#;

        let err = LsmdbConfig::from_toml_str(raw).expect_err("invalid fpr should fail");
        assert!(
            matches!(err, ConfigError::InvalidValue { field, .. } if field == "sstable.bloom_fpr")
        );
    }

    #[test]
    fn loads_config_file_with_validation() {
        let path = temp_file_path("load");
        fs::write(
            &path,
            r#"
                [storage]
                memtable_size_bytes = 8192
            "#,
        )
        .expect("write temp config");

        let config = LsmdbConfig::load_from_path(&path).expect("load config from file");
        assert_eq!(config.storage.memtable_size_bytes, 8192);

        fs::remove_file(path).expect("cleanup temp config");
    }

    #[test]
    fn rejects_arena_block_larger_than_memtable() {
        let raw = r#"
            [storage]
            memtable_size_bytes = 4096
            memtable_arena_block_size_bytes = 8192
        "#;

        let err = LsmdbConfig::from_toml_str(raw).expect_err("invalid arena block size");
        assert!(matches!(
            err,
            ConfigError::InvalidValue { field, .. }
            if field == "storage.memtable_arena_block_size_bytes"
        ));
    }

    #[test]
    fn rejects_flush_timeout_smaller_than_poll_interval() {
        let raw = r#"
            [storage]
            flush_poll_interval_ms = 100
            flush_timeout_ms = 50
        "#;

        let err = LsmdbConfig::from_toml_str(raw).expect_err("invalid flush timing");
        assert!(
            matches!(err, ConfigError::InvalidValue { field, .. } if field == "storage.flush_timeout_ms")
        );
    }

    #[test]
    fn rejects_zero_server_connection_limit() {
        let raw = r#"
            [server]
            max_concurrent_connections = 0
        "#;

        let err = LsmdbConfig::from_toml_str(raw).expect_err("invalid server limit");
        assert!(
            matches!(err, ConfigError::InvalidValue { field, .. } if field == "server.max_concurrent_connections")
        );
    }

    #[test]
    fn emits_startup_diagnostics_for_runtime_config() {
        let raw = r#"
            [storage]
            memtable_size_bytes = 8192
            memtable_arena_block_size_bytes = 4096
            flush_poll_interval_ms = 25
            flush_timeout_ms = 100

            [server]
            max_concurrent_connections = 24
            max_in_flight_requests_per_connection = 1
            max_request_bytes = 32768
            max_statements_per_request = 4
            max_memory_intensive_requests = 2
            max_scan_rows = 128
            max_sort_rows = 64
            max_join_rows = 32
            max_query_result_rows = 16

            [wal]
            segment_size_bytes = 4096
            sync_mode = "on_commit"

            [compaction]
            strategy = "leveled"
        "#;

        let config = LsmdbConfig::from_toml_str(raw).expect("parse valid config");
        let diagnostics = config.startup_diagnostics().expect("startup diagnostics");
        assert_eq!(diagnostics.memtable_size_bytes, 8192);
        assert_eq!(diagnostics.memtable_arena_block_size_bytes, 4096);
        assert_eq!(diagnostics.flush_poll_interval_ms, 25);
        assert_eq!(diagnostics.flush_timeout_ms, 100);
        assert_eq!(diagnostics.server_max_concurrent_connections, 24);
        assert_eq!(diagnostics.server_max_request_bytes, 32_768);
        assert_eq!(diagnostics.server_max_query_result_rows, 16);
        assert_eq!(diagnostics.wal_segment_size_bytes, 4096);
        assert_eq!(diagnostics.wal_sync_mode, SyncModeConfig::OnCommit);

        let lines = diagnostics.as_key_value_lines();
        assert!(lines.iter().any(|line| line == "server.max_concurrent_connections=24"));
        assert!(lines.iter().any(|line| line == "compaction.strategy=leveled"));
        assert!(lines.iter().any(|line| line.starts_with("sstable.bloom_bits_per_key=")));
    }
}
