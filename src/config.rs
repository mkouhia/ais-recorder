//! Application configuration

use std::path::PathBuf;
use std::time::Duration;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use serde_with::serde_as;
use tracing::warn;

use crate::errors::AisLoggerError;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub mqtt: MqttConfig,
    pub database: DatabaseConfig,
    pub export: ExportConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MqttConfig {
    pub uri: String,
    pub topics: Vec<String>,
    pub client_id: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub path: PathBuf,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub flush_interval: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExportConfig {
    pub cron: String,
    pub directory: PathBuf,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(Environment::with_prefix("AISLOGGER"))
            .build()?;

        config.try_deserialize()
    }
}

impl DatabaseConfig {
    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), AisLoggerError> {
        // Validate path
        if self.path.to_str().unwrap_or("").is_empty() {
            return Err(AisLoggerError::ConfigurationError {
                message: "Database path cannot be empty".to_string(),
            });
        }

        // Validate flush interval
        if self.flush_interval.as_secs() <= 0 {
            return Err(AisLoggerError::ConfigurationError {
                message: "Flush interval must be greater than zero".to_string(),
            });
        }

        // Optional: Check if parent directory is writable
        if let Some(parent) = self.path.parent() {
            if !parent.exists() || !parent.is_dir() {
                warn!(
                    "Database path parent directory does not exist: {}",
                    parent.display()
                );
            }
        }

        Ok(())
    }
}
