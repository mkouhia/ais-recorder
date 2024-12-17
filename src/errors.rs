//! Errors for AIS logger
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AisLoggerError {
    #[error("MQTT connection failed")]
    MqttConnectionError(#[from] rumqttc::ConnectionError),

    #[error("MQTT client error")]
    MqttClientError(#[from] rumqttc::ClientError),

    #[error("Database error")]
    DatabaseError(#[from] rusqlite::Error),

    #[error("Serialization error")]
    SerdeError(#[from] serde_json::Error),

    #[error("Configuration error")]
    ConfigError(#[from] config::ConfigError),

    #[error("Channel send error")]
    ChannelError(#[from] tokio::sync::mpsc::error::SendError<()>),

    #[error("Invalid topic")]
    InvalidTopic(String),

    #[error("Invalid MMSI")]
    InvalidMmsi(String),

    #[error("Unknown message type")]
    UnknownMessageType(String),
}
