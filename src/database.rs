//! Database functionality for AIS message storage and export
//!
//! This module provides a thread-safe interface for:
//! - Storing AIS messages in SQLite database
//! - Periodic flushing of data to disk
//! - Daily export of historical data to Parquet files
//! - Automatic cleanup of exported data

use sqlx::postgres::PgPool;
use tracing::{debug, trace};

use crate::errors::AisLoggerError;
use crate::models::{AisMessage, AisMessageType, Mmsi, VesselLocation, VesselMetadata};

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn run_migrations(&self) -> Result<(), AisLoggerError> {
        debug!("Checking migrations");
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| AisLoggerError::MigrationError(e.to_string()))?;
        debug!("Migrations completed");
        Ok(())
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
        match sqlx::query!(
            r#"
            INSERT INTO locations
                (mmsi, time, sog, cog, nav_stat, rot, pos_acc, raim, heading, lon, lat)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (mmsi, time) DO UPDATE SET
                sog = EXCLUDED.sog,
                cog = EXCLUDED.cog,
                nav_stat = EXCLUDED.nav_stat,
                rot = EXCLUDED.rot,
                pos_acc = EXCLUDED.pos_acc,
                raim = EXCLUDED.raim,
                heading = EXCLUDED.heading,
                lon = EXCLUDED.lon,
                lat = EXCLUDED.lat
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
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!(
                    error = %e,
                    mmsi = ?mmsi.value(),
                    location = ?location,
                    "Failed to insert location"
                );
                Err(e.into())
            }
        }
    }

    async fn insert_metadata(
        &self,
        mmsi: &Mmsi,
        metadata: &VesselMetadata,
    ) -> Result<(), AisLoggerError> {
        trace!(mmsi=?mmsi, metadata=?metadata, "Insert metadata");
        match sqlx::query!(
            r#"
            INSERT INTO metadata
                (mmsi, time, name, destination, vessel_type, call_sign,
                 imo, draught, eta, pos_type, ref_a, ref_b, ref_c, ref_d)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (mmsi, time) DO UPDATE SET
                name = EXCLUDED.name,
                destination = EXCLUDED.destination,
                vessel_type = EXCLUDED.vessel_type,
                call_sign = EXCLUDED.call_sign,
                imo = EXCLUDED.imo,
                draught = EXCLUDED.draught,
                eta = EXCLUDED.eta,
                pos_type = EXCLUDED.pos_type,
                ref_a = EXCLUDED.ref_a,
                ref_b = EXCLUDED.ref_b,
                ref_c = EXCLUDED.ref_c,
                ref_d = EXCLUDED.ref_d
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
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!(
                    error = %e,
                    mmsi = ?mmsi.value(),
                    metadata = ?metadata,
                    "Failed to insert metadata"
                );
                Err(e.into())
            }
        }
    }
}
