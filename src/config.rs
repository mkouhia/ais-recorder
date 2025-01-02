//! Application configuration

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;
#[cfg(feature = "dotenvy")]
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database_url: String,
    pub digitraffic_marine: DigitrafficMarine,
}

#[derive(Debug, Deserialize)]
pub struct DigitrafficMarine {
    pub uri: String,
    pub id: String,
    pub topics: Vec<String>,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        // Load .env file if it exists (development only)
        #[cfg(feature = "dotenvy")]
        {
            info!("Read environment variables from .env file");
            dotenvy::dotenv().ok();
        }

        Config::builder()
            // App defaults, if present
            .add_source(File::with_name("config/default").required(false))
            // Second layer: config file (optional)
            .add_source(File::with_name("config").required(false))
            // Default database URL from environment variable
            .set_override("database_url", default_database_url())?
            // Other environment variables in structured format
            .add_source(
                Environment::with_prefix("AISLOGGER")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("digitraffic_marine.topics"),
            )
            .build()?
            .try_deserialize()
    }

    pub fn pg_options(&self) -> Result<PgConnectOptions, sqlx::Error> {
        PgConnectOptions::from_str(&self.database_url)
    }
}

fn default_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ais-recorder:password@localhost/ais-recorder".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_load_config() {
        env::set_var("AISLOGGER__Digitraffic_Marine__URI", "mqtt://localhost");
        env::set_var("AISLOGGER__Digitraffic_Marine__TOPICS", "topic1,topic2");
        env::set_var("AISLOGGER__Digitraffic_Marine__ID", "test_client");
        env::set_var(
            "DATABASE_URL",
            "postgres://username:password@localhost/ais-recorder",
        );

        let config = AppConfig::load().unwrap();
        assert_eq!(config.digitraffic_marine.uri, "mqtt://localhost");
        assert_eq!(config.digitraffic_marine.topics, vec!["topic1", "topic2"]);
        assert_eq!(config.digitraffic_marine.id, "test_client");
        assert_eq!(
            config.database_url,
            String::from("postgres://username:password@localhost/ais-recorder")
        );
    }
}
