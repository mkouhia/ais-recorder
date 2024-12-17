//! Application configuration

use std::path::PathBuf;
use std::time::Duration;

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use serde_with::serde_as;

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
            .add_source(Environment::with_prefix("AISLOGGER"))
            .build()?;

        config.try_deserialize()
    }
}
