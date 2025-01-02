//! Application configuration

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

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

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_load_config() {
        env::set_var("AISLOGGER__MQTT__URI", "mqtt://localhost");
        env::set_var("AISLOGGER__MQTT__TOPICS", "topic1,topic2");
        env::set_var("AISLOGGER__MQTT__CLIENT_ID", "test_client");
        env::set_var(
            "AISLOGGER__DATABASE__URL",
            "postgres://username:password@localhost/ais-recorder",
        );

        let config = AppConfig::load().unwrap();
        assert_eq!(config.mqtt.uri, "mqtt://localhost");
        assert_eq!(config.mqtt.topics, vec!["topic1", "topic2"]);
        assert_eq!(config.mqtt.client_id, "test_client");
        assert_eq!(
            config.database.url,
            String::from("postgres://username:password@localhost/ais-recorder")
        );
    }
}
