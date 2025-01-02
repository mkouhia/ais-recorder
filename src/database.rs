//! Database functionality for AIS message storage and export
//!
//! This module provides a thread-safe interface for:
//! - Storing AIS messages in SQLite database
//! - Periodic flushing of data to disk
//! - Daily export of historical data to Parquet files
//! - Automatic cleanup of exported data

use sqlx::postgres::{PgPool, PgPoolOptions};
use tracing::{debug, span, trace, Level};

use crate::errors::AisLoggerError;
use crate::models::{AisMessage, AisMessageType, Mmsi, VesselLocation, VesselMetadata};

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(pool: PgPool) -> Result<Self, AisLoggerError> {
        // Run migrations
        debug!("Running migrations");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| AisLoggerError::MigrationError(e.to_string()))?;

        Ok(Self { pool })
    }

    pub async fn from_url(database_url: &str) -> Result<Self, AisLoggerError> {
        span!(Level::INFO, "Setup Postgres connection", url = database_url);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| AisLoggerError::DatabaseConnectionError(e.to_string()))?;

        Self::new(pool).await
    }

    pub async fn process_message(&self, message: AisMessage) -> Result<(), AisLoggerError> {
        match message.message_type {
            AisMessageType::Location(loc) => self.insert_location(&message.mmsi, &loc).await?,
            AisMessageType::Metadata(meta) => self.insert_metadata(&message.mmsi, &meta).await?,
        }
        Ok(())
    }

    async fn insert_location(
        &self,
        mmsi: &Mmsi,
        location: &VesselLocation,
    ) -> Result<(), AisLoggerError> {
        trace!(mmsi=?mmsi, location=?location, "Insert location");
        sqlx::query!(
            r#"
            INSERT INTO locations
                (mmsi, time, sog, cog, nav_stat, rot, pos_acc, raim, heading, lon, lat)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
            mmsi.value() as i32,
            location.time,
            location.sog,
            location.cog,
            location.nav_stat.map(|v| v as i16),
            location.rot.map(|v| v as i16),
            location.pos_acc,
            location.raim,
            location.heading.map(|v| v as i16),
            location.lon,
            location.lat,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn insert_metadata(
        &self,
        mmsi: &Mmsi,
        metadata: &VesselMetadata,
    ) -> Result<(), AisLoggerError> {
        trace!(mmsi=?mmsi, metadata=?metadata, "Insert metadata");
        sqlx::query!(
            r#"
            INSERT INTO metadata
                (mmsi, time, name, destination, vessel_type, call_sign,
                 imo, draught, eta, pos_type, ref_a, ref_b, ref_c, ref_d)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
            mmsi.value() as i32,
            metadata.time,
            metadata.name.as_deref(),
            metadata.destination.as_deref(),
            metadata.vessel_type.map(|v| v as i16),
            metadata.call_sign.as_deref(),
            metadata.imo.map(|v| v as i32),
            metadata.draught.map(|v| v as i16), // Convert back to raw draught
            metadata.eta,                       // Store raw ETA bits
            metadata.pos_type.map(|v| v as i16),
            metadata.ref_a.map(|v| v as i16),
            metadata.ref_b.map(|v| v as i16),
            metadata.ref_c.map(|v| v as i16),
            metadata.ref_d.map(|v| v as i16),
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
