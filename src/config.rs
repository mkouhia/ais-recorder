//! Application configuration

use std::path::{Path, PathBuf};
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

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let config = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(
                Environment::with_prefix("AISLOGGER")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("mqtt.topics"),
            )
            .build()?;

        config.try_deserialize()
    }
}

impl DatabaseConfig {
    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), AisLoggerError> {
        self.validate_path()?;
        self.validate_flush_interval()?;
        self.ensure_directory_exists(self.path.parent().ok_or_else(|| {
            AisLoggerError::ConfigurationError {
                message: "Could not get parent directory".to_string(),
            }
        })?)?;
        Ok(())
    }

    fn validate_path(&self) -> Result<(), AisLoggerError> {
        if self.path.to_str().unwrap_or("").is_empty() {
            return Err(AisLoggerError::ConfigurationError {
                message: "Database path cannot be empty".to_string(),
            });
        }
        Ok(())
    }

    fn validate_flush_interval(&self) -> Result<(), AisLoggerError> {
        if self.flush_interval.is_zero() {
            return Err(AisLoggerError::ConfigurationError {
                message: "Flush interval must be greater than zero".to_string(),
            });
        }
        Ok(())
    }

    fn ensure_directory_exists(&self, dir: &Path) -> Result<(), AisLoggerError> {
        if !dir.exists() {
            warn!("Database directory does not exist, attempting to create it");
            std::fs::create_dir_all(dir).map_err(|e| AisLoggerError::ConfigurationError {
                message: format!("Could not create database directory: {}", e),
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_load_config() {
        env::set_var("AISLOGGER__MQTT__URI", "mqtt://localhost");
        env::set_var("AISLOGGER__MQTT__TOPICS", "topic1,topic2");
        env::set_var("AISLOGGER__MQTT__CLIENT_ID", "test_client");
        env::set_var("AISLOGGER__DATABASE__PATH", "/tmp/test.db");
        env::set_var("AISLOGGER__DATABASE__FLUSH_INTERVAL", "10");

        let config = AppConfig::load().unwrap();
        assert_eq!(config.mqtt.uri, "mqtt://localhost");
        assert_eq!(config.mqtt.topics, vec!["topic1", "topic2"]);
        assert_eq!(config.mqtt.client_id, "test_client");
        assert_eq!(config.database.path, PathBuf::from("/tmp/test.db"));
        assert_eq!(config.database.flush_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_database_config_validate() {
        let config = DatabaseConfig {
            path: PathBuf::from("/tmp/test.db"),
            flush_interval: Duration::from_secs(10),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_database_config_validate_invalid_path() {
        let config = DatabaseConfig {
            path: PathBuf::from(""),
            flush_interval: Duration::from_secs(10),
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_database_config_validate_invalid_flush_interval() {
        let config = DatabaseConfig {
            path: PathBuf::from("/tmp/test.db"),
            flush_interval: Duration::from_secs(0),
        };

        assert!(config.validate().is_err());
    }
}
