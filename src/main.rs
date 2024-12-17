//! AIS recorder utility

mod config;
mod database;
mod errors;
mod models;
mod mqtt;

use config::AppConfig;
use errors::AisLoggerError;
use mqtt::{MqttClient, MqttClientBuilder};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), AisLoggerError> {
    // Initialize logging with more configuration
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Load configuration, preferring environment variables and config files
    let config = AppConfig::load()?;

    // Create MQTT client with flexible configuration
    let mqtt_client = MqttClientBuilder::new(&config.mqtt)?
        .connect(&config.mqtt.topics)
        .await?;

    // Initialize database writer with builder-style configuration
    let database_writer = database::DatabaseWriterBuilder::new()
        .path(config.database.path.clone())
        .flush_interval(config.database.flush_interval)
        .build()?;

    // Setup signal handling for graceful shutdown
    let shutdown_signal = signal::ctrl_c();
    tokio::select! {
        result = run_ais_logger(mqtt_client, database_writer) => {
            tracing::info!("AIS Logger completed: {:?}", result);
        }
        _ = shutdown_signal => {
            tracing::info!("Received shutdown signal");
        }
    }

    Ok(())
}

async fn run_ais_logger(
    mut mqtt_client: MqttClient,
    database_writer: database::DatabaseWriter,
) -> Result<(), AisLoggerError> {
    loop {
        tokio::select! {
            message = mqtt_client.recv() => {
                match message {
                    Ok(Some(msg)) => {
                        if let Err(e) = database_writer.process_message(msg).await {
                            tracing::error!("Message processing error: {}", e);
                        }
                    }
                    Ok(None) => break, // Channel closed
                    Err(e) => {
                        tracing::error!("MQTT receive error: {}", e);
                        break;
                    }
                }
            }
            // Optional: Add periodic maintenance tasks or heartbeat
        }
    }

    // Graceful shutdown with explicit flush
    database_writer.flush().await?;
    Ok(())
}
