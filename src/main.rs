//! AIS recorder utility

mod config;
mod database;
mod errors;
mod models;
mod mqtt;

use config::AppConfig;
use database::Database;
use errors::AisLoggerError;
use mqtt::{MqttClient, MqttClientBuilder};
use tokio::signal;
use tracing::{error, info};

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

    let db = Database::from_url(&config.database.url).await?;

    // Setup signal handling for graceful shutdown
    let shutdown_signal = signal::ctrl_c();

    tokio::select! {
        result = run_ais_logger(mqtt_client, db) => {
            info!("AIS Logger completed: {:?}", result);
        }
        _ = shutdown_signal => {
            info!("Received shutdown signal");
        }
    }

    // DbDropGuard is dropped here, ensuring background task shutdown

    Ok(())
}

async fn run_ais_logger(
    mut mqtt_client: MqttClient,
    database: Database,
) -> Result<(), AisLoggerError> {
    loop {
        tokio::select! {
            message = mqtt_client.recv() => {
                match message {
                    Ok(Some(msg)) => {
                        if let Err(e) = database.process_message(msg).await {
                            error!("Message processing error: {}", e);
                        }
                    }
                    Ok(None) => break, // Channel closed
                    Err(e) => {
                        error!("MQTT receive error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
