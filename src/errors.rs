//! Errors for AIS logger
use std::path::PathBuf;
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

    #[error("IO error")]
    IoError(#[from] std::io::Error),

    #[error("Job scheduler error")]
    JobSchedulerError(#[from] tokio_cron_scheduler::JobSchedulerError),

    #[error("Lock error")]
    LockError(String),

    #[error("Parquet creation error")]
    ParquetCreationError(String),

    #[error("Parquet write error")]
    ParquetWriteError(String),

    #[error("Invalid topic")]
    InvalidTopic(String),

    #[error("Invalid MMSI")]
    InvalidMmsi(String),

    #[error("Unknown message type")]
    UnknownMessageType(String),

    #[error("Failed to open database at {path}: {origin}")]
    DatabaseOpenError { path: PathBuf, origin: String },

    #[error("Database configuration error - {message}: {origin}")]
    DatabaseConfigError { message: String, origin: String },

    #[error("Failed to create table {table}: {origin}")]
    TableCreationError { table: String, origin: String },

    #[error("Failed to create index {index}: {origin}")]
    IndexCreationError { index: String, origin: String },

    #[error("Database configuration invalid: {message}")]
    ConfigurationError { message: String },
}
