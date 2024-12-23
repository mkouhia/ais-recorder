//! AIS recorder utility

mod config;
mod database;
mod errors;
mod models;
mod mqtt;

use config::{AppConfig, ExportConfig};
use database::{Db, DbBuilder};
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

    // Initialize database with builder-style configuration
    // DbDropGuard ensures background task is shut down when dropped
    let db_guard = DbBuilder::new()
        .path(config.database.path.clone())
        .batch_config(config.database.batch)
        .flush_interval(config.database.flush_interval)
        .build()?;

    // Get handle to database
    let database = db_guard.db();

    // Use database clone for export
    let database_for_export = database.clone();

    // Schedule daily export to parquet file
    let sched = JobScheduler::new().await?;
    setup_export(&sched, database_for_export, &config.export).await?;

    // Start the scheduler
    sched.start().await?;

    // Setup signal handling for graceful shutdown
    let shutdown_signal = signal::ctrl_c();

    tokio::select! {
        result = run_ais_logger(mqtt_client, database) => {
            info!("AIS Logger completed: {:?}", result);
        }
        _ = shutdown_signal => {
            info!("Received shutdown signal");
        }
    }

    // DbDropGuard is dropped here, ensuring background task shutdown

    Ok(())
}

async fn run_ais_logger(mut mqtt_client: MqttClient, database: Db) -> Result<(), AisLoggerError> {
    loop {
        tokio::select! {
            message = mqtt_client.recv() => {
                match message {
                    Ok(Some(msg)) => {
                        if let Err(e) = database.process_message(msg) {
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

    // Graceful shutdown with explicit flush
    database.flush()?;
    Ok(())
}

async fn setup_export(
    sched: &JobScheduler,
    database: Db,
    config: &ExportConfig,
) -> Result<(), AisLoggerError> {
    config.validate()?;
    let schedule = config.cron.clone();
    let export_dir = config.directory.clone();
    info!(
        "Set up export cron job with schedule \"{}\" to {}",
        schedule,
        export_dir.display()
    );
    sched
        .add(Job::new(schedule, move |_uuid, _l| {
            if let Err(e) = database.daily_export(&export_dir) {
                error!("Export error: {}", e);
            }
        })?)
        .await?;
    Ok(())
}
