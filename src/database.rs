// src/database.rs
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::{params, Connection, OpenFlags, Transaction};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::{
    config::DatabaseConfig,
    errors::AisLoggerError,
    models::{AisMessage, AisMessageType, VesselLocation, VesselMetadata},
};

/// Database writer for AIS data
pub struct DatabaseWriter {
    connection: Mutex<Connection>,
    config: DatabaseConfig,
    last_flush: Mutex<Instant>,
}

impl DatabaseWriter {
    /// Create a new database writer
    pub fn new(config: DatabaseConfig) -> Result<Self, AisLoggerError> {
        // Validate configuration
        config.validate()?;

        info!(
            "Initializing DatabaseWriter: path={}, flush_interval={:?}",
            config.path.display(),
            config.flush_interval
        );

        let conn = match Self::open_database(&config.path) {
            Ok(connection) => connection,
            Err(e) => {
                error!("Failed to open database: {}", e);
                return Err(e);
            }
        };

        match Self::create_tables_indices(&conn) {
            Ok(_) => Ok(Self {
                connection: Mutex::new(conn),
                config,
                last_flush: Mutex::new(Instant::now()),
            }),
            Err(e) => {
                error!("Failed to create database tables: {}", e);
                Err(e)
            }
        }
    }

    /// Open or create the database with optimized settings
    fn open_database(path: &Path) -> Result<Connection, AisLoggerError> {
        info!("Opening database at {}", path.display());
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        )
        .map_err(|e| AisLoggerError::DatabaseOpenError {
            path: path.to_path_buf(),
            origin: e.to_string(),
        })?;

        // Configure for performance
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| AisLoggerError::DatabaseConfigError {
                message: "Failed to set journal_mode".to_string(),
                origin: e.to_string(),
            })?;

        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| AisLoggerError::DatabaseConfigError {
                message: "Failed to set synchronous mode".to_string(),
                origin: e.to_string(),
            })?;

        conn.pragma_update(None, "temp_store", "MEMORY")
            .map_err(|e| AisLoggerError::DatabaseConfigError {
                message: "Failed to set temp_store".to_string(),
                origin: e.to_string(),
            })?;
        Ok(conn)
    }

    /// Create tables `locations` and `metadata`.
    ///
    /// Add indices on columns `mmsi` and `time`/`timestamp`
    fn create_tables_indices(conn: &Connection) -> Result<(), AisLoggerError> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS locations (
                mmsi INTEGER,
                time INTEGER,
                sog REAL,
                cog REAL,
                nav_stat INTEGER,
                rot INTEGER,
                pos_acc INTEGER,
                raim INTEGER,
                heading INTEGER,
                lon REAL,
                lat REAL,
                PRIMARY KEY (mmsi, time)
            )",
            [],
        )
        .map_err(|e| AisLoggerError::TableCreationError {
            table: "locations".to_string(),
            origin: e.to_string(),
        })?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                        mmsi INTEGER PRIMARY KEY,
                        timestamp INTEGER,
                        name TEXT,
                        destination TEXT,
                        vessel_type INTEGER,
                        call_sign TEXT,
                        imo INTEGER,
                        draught INTEGER,
                        eta INTEGER,
                        pos_type INTEGER,
                        ref_a INTEGER,
                        ref_b INTEGER,
                        ref_c INTEGER,
                        ref_d INTEGER
                    )",
            [],
        )
        .map_err(|e| AisLoggerError::TableCreationError {
            table: "metadata".to_string(),
            origin: e.to_string(),
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_locations_mmsi ON locations(mmsi)",
            [],
        )
        .map_err(|e| AisLoggerError::IndexCreationError {
            index: "idx_locations_mmsi".to_string(),
            origin: e.to_string(),
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_locations_time ON locations(time)",
            [],
        )
        .map_err(|e| AisLoggerError::IndexCreationError {
            index: "idx_locations_time".to_string(),
            origin: e.to_string(),
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_metadata_mmsi ON metadata(mmsi)",
            [],
        )
        .map_err(|e| AisLoggerError::IndexCreationError {
            index: "idx_metadata_mmsi".to_string(),
            origin: e.to_string(),
        })?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_metadata_timestamp ON metadata(timestamp)",
            [],
        )
        .map_err(|e| AisLoggerError::IndexCreationError {
            index: "idx_metadata_timestamp".to_string(),
            origin: e.to_string(),
        })?;

        Ok(())
    }

    /// Process an incoming AIS message
    pub async fn process_message(&self, message: AisMessage) -> Result<(), AisLoggerError> {
        let mut conn = self.connection.lock().await;
        let tx = conn.transaction()?;

        match message.message_type {
            AisMessageType::Location(location) => {
                self.insert_location(&tx, message.mmsi, &location)?;
            }
            AisMessageType::Metadata(metadata) => {
                self.insert_metadata(&tx, message.mmsi, &metadata)?;
            }
        }

        tx.commit()?;
        drop(conn); // Release self.connection lock, flush will acquire again

        // Check if we need to flush based on time
        self.maybe_flush().await?;

        Ok(())
    }

    /// Insert vessel location
    fn insert_location(
        &self,
        tx: &Transaction,
        mmsi: u32,
        location: &VesselLocation,
    ) -> Result<(), AisLoggerError> {
        tx.execute(
            "INSERT OR REPLACE INTO locations (
                mmsi, time, sog, cog, nav_stat, rot,
                pos_acc, raim, heading, lon, lat
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                mmsi,
                location.time,
                location.sog,
                location.cog,
                location.nav_stat,
                location.rot,
                location.pos_acc as i32,
                location.raim as i32,
                location.heading,
                location.lon,
                location.lat
            ],
        )?;

        Ok(())
    }

    /// Insert vessel metadata
    fn insert_metadata(
        &self,
        tx: &Transaction,
        mmsi: u32,
        metadata: &VesselMetadata,
    ) -> Result<(), AisLoggerError> {
        tx.execute(
            "INSERT OR REPLACE INTO metadata (
                mmsi, timestamp, name, destination, vessel_type,
                call_sign, imo, draught, eta, pos_type,
                ref_a, ref_b, ref_c, ref_d
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                mmsi,
                metadata.timestamp,
                metadata.name,
                metadata.destination,
                metadata.vessel_type,
                metadata.call_sign,
                metadata.imo,
                metadata.draught,
                metadata.eta,
                metadata.pos_type,
                metadata.ref_a,
                metadata.ref_b,
                metadata.ref_c,
                metadata.ref_d
            ],
        )?;

        Ok(())
    }

    /// Conditionally flush data based on time interval
    async fn maybe_flush(&self) -> Result<(), AisLoggerError> {
        let mut last_flush = self.last_flush.lock().await;

        if last_flush.elapsed() >= self.config.flush_interval {
            info!("Performing periodic flush");
            self.flush().await?;
            *last_flush = Instant::now();
        }

        Ok(())
    }

    /// Explicitly flush database
    pub async fn flush(&self) -> Result<(), AisLoggerError> {
        // In SQLite with WAL mode, this ensures data is written to disk
        let conn = self.connection.lock().await;
        conn.pragma_update(None, "wal_checkpoint", "PASSIVE")?;
        Ok(())
    }
}

/// Builder for DatabaseWriter with simplified configuration
pub struct DatabaseWriterBuilder {
    path: Option<PathBuf>,
    flush_interval: Option<Duration>,
}

impl DatabaseWriterBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            flush_interval: None,
        }
    }

    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = Some(interval);
        self
    }

    pub fn build(self) -> Result<DatabaseWriter, AisLoggerError> {
        let path = self
            .path
            .unwrap_or_else(|| PathBuf::from("ais-recorder.db"));
        let flush_interval = self.flush_interval.unwrap_or(Duration::from_secs(10));

        let config = DatabaseConfig {
            path,
            flush_interval,
        };

        DatabaseWriter::new(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_process_location() -> Result<(), AisLoggerError> {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db_writer = DatabaseWriterBuilder::new().path(db_path.clone()).build()?;

        let message = AisMessage {
            mmsi: 123456,
            message_type: AisMessageType::Location(VesselLocation {
                time: 1668075025,
                sog: 10.7,
                cog: 326.6,
                nav_stat: 0,
                rot: 0,
                pos_acc: true,
                raim: false,
                heading: 325,
                lon: 20.345818,
                lat: 60.03802,
            }),
        };

        db_writer.process_message(message).await?;
        db_writer.flush().await?;
        // drop(db_writer); // Ensures final flush

        // Verify database content
        let conn = Connection::open(&db_path)?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM locations WHERE mmsi = 123456 and time = 1668075025",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_process_metadata() -> Result<(), AisLoggerError> {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db_writer = DatabaseWriterBuilder::new().path(db_path.clone()).build()?;

        let message = AisMessage {
            mmsi: 123456,
            message_type: AisMessageType::Metadata(VesselMetadata {
                timestamp: 1668075026035,
                destination: "UST LUGA".to_string(),
                name: "ARUNA CIHAN".to_string(),
                draught: 68,
                eta: 733376,
                pos_type: 15,
                ref_a: 160,
                ref_b: 33,
                ref_c: 20,
                ref_d: 12,
                call_sign: "V7WW7".to_string(),
                imo: 9543756,
                vessel_type: 70,
            }),
        };

        db_writer.process_message(message).await?;
        db_writer.flush().await?;
        // drop(db_writer); // Ensures final flush

        // Verify database content
        let conn = Connection::open(&db_path)?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM metadata WHERE mmsi = 123456 and timestamp = 1668075026035",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1);
        Ok(())
    }
}
