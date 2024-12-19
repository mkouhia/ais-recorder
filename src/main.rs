//! AIS recorder utility

mod config;
mod database;
mod errors;
mod models;
mod mqtt;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use config::AppConfig;
use database::{DatabaseWriter, DatabaseWriterBuilder};
use errors::AisLoggerError;
use mqtt::{MqttClient, MqttClientBuilder};
use tokio::signal;
use tokio_cron_scheduler::{Job, JobScheduler};
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

    // Initialize database writer with builder-style configuration
    let database_writer = Arc::new(Mutex::new(
        DatabaseWriterBuilder::new()
            .path(config.database.path.clone())
            .flush_interval(config.database.flush_interval)
            .build()?,
    ));

    // Schedule daily export to parquet file
    let sched = JobScheduler::new().await?;
    setup_export(
        &sched,
        Arc::clone(&database_writer),
        &config.export.cron,
        &config.export.directory,
    )
    .await?;

    // Start the scheduler
    sched.start().await?;

    // Setup signal handling for graceful shutdown
    let shutdown_signal = signal::ctrl_c();

    let logger_writer = Arc::clone(&database_writer);
    tokio::select! {
        result = run_ais_logger(mqtt_client, logger_writer) => {
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
    database_writer: Arc<Mutex<DatabaseWriter>>,
) -> Result<(), AisLoggerError> {
    loop {
        tokio::select! {
            message = mqtt_client.recv() => {
                match message {
                    Ok(Some(msg)) => {
                        match database_writer.lock() {
                            Ok(mut db_writer) => {
                                if let Err(e) = (*db_writer).process_message(msg) {
                                    error!("Message processing error: {}", e);
                                };
                            }
                            Err(e) => {
                                error!("Could not access database mutex from MQTT receiver, {}", e);
                                break;
                            }
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
    match database_writer.lock() {
        Ok(mut db_writer) => {
            (*db_writer).flush()?;
        }
        Err(e) => {
            error!("Could not access database mutex from final flush, {}", e);
        }
    }

    Ok(())
}

async fn setup_export<P>(
    sched: &JobScheduler,
    export_writer: Arc<Mutex<DatabaseWriter>>,
    export_cron: &str,
    export_dir: P,
) -> Result<(), AisLoggerError>
where
    P: AsRef<Path>,
{
    let export_dir = PathBuf::from(export_dir.as_ref());
    info!(
        "Set up export cron job with schedule \"{}\" to {}",
        export_cron,
        export_dir.display()
    );
    sched
        .add(Job::new(export_cron, move |_uuid, _l| {
            info!("Perform daily export to {}", export_dir.display());
            match export_writer.lock() {
                Ok(mut db_writer) => match (*db_writer).daily_export(&export_dir) {
                    Ok(()) => {
                        info!("Performed daily export to {}", export_dir.display());
                    }
                    Err(e) => {
                        error!("Could not export to parquet: {}", e);
                    }
                },
                Err(e) => {
                    error!("Could not access database mutex from scheduler: {}", e);
                }
            };
        })?)
        .await?;
    Ok(())
}
