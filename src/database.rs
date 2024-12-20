//! Database functionality for AIS message storage and export
//!
//! This module provides a thread-safe interface for:
//! - Storing AIS messages in SQLite database
//! - Periodic flushing of data to disk
//! - Daily export of historical data to Parquet files
//! - Automatic cleanup of exported data

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use polars::prelude::*;
use rusqlite::{params, Connection, OpenFlags, Transaction};
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::{
    config::DatabaseConfig,
    errors::AisLoggerError,
    models::{AisMessage, AisMessageType, VesselLocation, VesselMetadata},
};

/// A guard that ensures proper shutdown of database background tasks.
///
/// When dropped, this guard ensures that:
/// - Background flush task is terminated gracefully
/// - All pending writes are flushed to disk
/// - Resources are properly cleaned up
#[derive(Debug)]
pub struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbDropGuard` is dropped
    db: Db,
}

/// Thread-safe database handle for AIS message processing
///
/// This type is cloneable and can be shared between threads. It provides
/// a safe interface to the underlying database operations while managing
/// concurrent access and periodic maintenance tasks.
#[derive(Clone, Debug)]
pub struct Db {
    /// Handle to shared state
    shared: Arc<Shared>,
}

/// Shared state protected by a mutex
///
/// Uses std::sync::Mutex instead of tokio::sync::Mutex because:
/// - Critical sections are short
/// - No async operations are performed while holding the lock
/// - Operations are CPU-bound rather than IO-bound
#[derive(Debug)]
struct Shared {
    /// The database state protected by a mutex
    state: Mutex<DatabaseState>,
    /// Notifies the background task for flushing
    background_task: Notify,
}

/// Database connection and configuration state
///
/// Contains the active database connection and associated configuration.
/// This struct is not thread-safe on its own and must be protected by a mutex.
#[derive(Debug)]
struct DatabaseState {
    /// Active SQLite connection
    connection: Connection,
    /// Database configuration parameters
    config: DatabaseConfig,
    /// Timestamp of last flush operation
    last_flush: Instant,
    /// Flag indicating shutdown state
    shutdown: bool,
}

/// Transaction error wrapper for better context
#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Failed to execute transaction: {context}")]
    Execute {
        context: String,
        #[source]
        source: rusqlite::Error,
    },
    #[error("Failed to commit transaction: {context}")]
    Commit {
        context: String,
        #[source]
        source: rusqlite::Error,
    },
}

impl DbDropGuard {
    /// Creates a new database instance with the specified configuration
    ///
    /// # Arguments
    /// * `config` - Database configuration parameters
    ///
    /// # Returns
    /// A guard wrapping the database instance
    ///
    /// # Errors
    /// Returns error if:
    /// - Database file cannot be opened
    /// - Tables cannot be created
    /// - Indices cannot be created
    pub fn new(config: DatabaseConfig) -> Result<Self, AisLoggerError> {
        Ok(DbDropGuard {
            db: Db::new(config)?,
        })
    }

    /// Gets a handle to the database
    ///
    /// The returned handle is cheap to clone and can be shared between threads.
    /// The underlying database connection and state are shared between all clones.
    pub fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        self.db.shutdown();
    }
}

impl Db {
    /// Create a new database handle
    fn new(config: DatabaseConfig) -> Result<Self, AisLoggerError> {
        config.validate()?;

        info!(
            "Initializing Database: path={}, flush_interval={:?}",
            config.path.display(),
            config.flush_interval
        );

        let conn = DatabaseState::open_database(&config.path)?;
        DatabaseState::create_tables_indices(&conn)?;

        let shared = Arc::new(Shared {
            state: Mutex::new(DatabaseState {
                connection: conn,
                config,
                last_flush: Instant::now(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        #[cfg(not(test))]
        {
            // Only spawn background task in non-test mode
            tokio::spawn(background_flush(shared.clone()));
        }

        Ok(Self { shared })
    }

    /// Processes an AIS message, storing it in the appropriate table
    ///
    /// # Arguments
    /// * `message` - The AIS message to process
    ///
    /// # Errors
    /// Returns error if:
    /// - Database transaction fails
    /// - Insert operation fails
    /// - Database is locked
    pub fn process_message(&self, message: AisMessage) -> Result<(), AisLoggerError> {
        self.shared
            .execute_mut(|state| state.process_message(message))
    }

    /// Explicitly flush database
    pub fn flush(&self) -> Result<(), AisLoggerError> {
        self.shared.execute_mut(|state| state.flush())
    }

    /// Exports previous day's data to Parquet files
    ///
    /// Data is exported to:
    /// - `{base_dir}/locations/YYYY-MM-DD.parquet`
    /// - `{base_dir}/metadata/YYYY-MM-DD.parquet`
    ///
    /// After successful export, data is removed from the database.
    ///
    /// # Arguments
    /// * `base_dir` - Base directory for export files
    ///
    /// # Errors
    /// Returns error if:
    /// - Export directories cannot be created
    /// - Database queries fail
    /// - Parquet files cannot be written
    /// - Cleanup operation fails
    pub fn daily_export<P: AsRef<Path>>(&self, base_dir: P) -> Result<(), AisLoggerError> {
        self.shared
            .execute_mut(|state| state.daily_export(base_dir))
    }

    /// Signal the background task to shut down
    fn shutdown(&self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.shutdown = true;
        }
        self.shared.background_task.notify_one();
    }
}

pub struct DbBuilder {
    path: Option<PathBuf>,
    flush_interval: Option<Duration>,
}

impl DbBuilder {
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

    pub fn build(self) -> Result<DbDropGuard, AisLoggerError> {
        let path = self
            .path
            .unwrap_or_else(|| PathBuf::from("ais-recorder.db"));
        let flush_interval = self.flush_interval.unwrap_or(Duration::from_secs(10));

        let config = DatabaseConfig {
            path,
            flush_interval,
        };

        DbDropGuard::new(config)
    }
}

impl Shared {
    /// Perform flush operation while holding the lock
    fn perform_flush(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return None;
        }

        if state.should_flush() {
            if let Err(e) = state.flush() {
                error!("Flush error: {}", e);
            }
            Some(state.next_flush_time())
        } else {
            Some(state.next_flush_time())
        }
    }

    /// Check if the database is shutting down
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }

    /// Execute a query that requires mutable access to the database
    fn execute_mut<F, T>(&self, f: F) -> Result<T, AisLoggerError>
    where
        F: FnOnce(&mut DatabaseState) -> Result<T, AisLoggerError>,
    {
        let mut state = self
            .state
            .lock()
            .map_err(|e| AisLoggerError::LockError(e.to_string()))?;
        f(&mut state)
    }
}

impl DatabaseState {
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

    /// Check if it's time to flush
    fn should_flush(&self) -> bool {
        self.last_flush.elapsed() >= self.config.flush_interval
    }

    /// Calculate next flush time
    fn next_flush_time(&self) -> Instant {
        self.last_flush + self.config.flush_interval
    }

    /// Execute an operation within a transaction
    fn with_transaction<F, T>(&mut self, context: &str, f: F) -> Result<T, AisLoggerError>
    where
        F: FnOnce(&Transaction) -> Result<T, AisLoggerError>,
    {
        let tx = self.connection.transaction().map_err(|e| {
            AisLoggerError::DatabaseTransactionError(TransactionError::Execute {
                context: format!("{}: failed to start transaction", context),
                source: e,
            })
        })?;

        let result = f(&tx)?;

        tx.commit().map_err(|e| {
            AisLoggerError::DatabaseTransactionError(TransactionError::Commit {
                context: format!("{}: failed to commit", context),
                source: e,
            })
        })?;

        Ok(result)
    }

    /// Process an incoming AIS message
    fn process_message(&mut self, message: AisMessage) -> Result<(), AisLoggerError> {
        self.with_transaction("process_message", |tx| match &message.message_type {
            AisMessageType::Location(location) => Self::insert_location(tx, message.mmsi, location),
            AisMessageType::Metadata(metadata) => Self::insert_metadata(tx, message.mmsi, metadata),
        })
    }

    /// Insert vessel location
    fn insert_location(
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

    /// Explicitly flush database
    fn flush(&mut self) -> Result<(), AisLoggerError> {
        self.connection
            .pragma_update(None, "wal_checkpoint", "PASSIVE")?;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Export data with transaction protection
    ///
    /// Export previous day's data to Parquet files and delete records from database
    ///
    /// Locations and metadata will be placed to
    ///     base_dir/{locations,metadata}/<%Y-%m-%d>.parquet ,
    /// where date refers to yesterday's UTC date.
    /// Export data with transaction protection
    fn daily_export<P: AsRef<Path>>(&mut self, base_dir: P) -> Result<(), AisLoggerError> {
        let today = Utc::now().date_naive();
        let yesterday = today
            .pred_opt()
            .ok_or_else(|| AisLoggerError::ConfigurationError {
                message: "Failed to get previous day".to_string(),
            })?;
        let start_time = yesterday.and_hms_opt(0, 0, 0).unwrap().and_utc();
        let end_time = today.and_hms_opt(0, 0, 0).unwrap().and_utc();

        // Get data before starting transaction
        let mut locations = self.get_locations_df(start_time, end_time)?;
        let mut metadata = self.get_metadata_df(start_time, end_time)?;

        // Write parquet files (no transaction needed)
        let locations_path = base_dir
            .as_ref()
            .join("locations")
            .join(format!("{}.parquet", yesterday.format("%Y-%m-%d")));
        let metadata_path = base_dir
            .as_ref()
            .join("metadata")
            .join(format!("{}.parquet", yesterday.format("%Y-%m-%d")));

        Self::write_parquet(&mut locations, &locations_path)?;
        Self::write_parquet(&mut metadata, &metadata_path)?;

        // Only use transaction for cleanup
        self.with_transaction("daily_export_cleanup", |tx| {
            Self::cleanup_exported_data(tx, start_time, end_time)
        })?;

        Ok(())
    }

    /// Read locations from database, return results as DataFrame
    fn get_locations_df(
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<DataFrame, AisLoggerError> {
        let mut mmsi = Vec::new();
        let mut time = Vec::new();
        let mut sog = Vec::new();
        let mut cog = Vec::new();
        let mut nav_stat = Vec::new();
        let mut rot = Vec::new();
        let mut pos_acc = Vec::new();
        let mut raim = Vec::new();
        let mut heading = Vec::new();
        let mut lon = Vec::new();
        let mut lat = Vec::new();

        // Prepare the query to fetch yesterday's locations
        let mut stmt = self.connection.prepare(
            "SELECT mmsi, time, sog, cog, nav_stat, rot, pos_acc, raim,
                    heading, lon, lat
             FROM locations
             WHERE time >= ?1 AND time < ?2
             ORDER BY mmsi, time",
        )?;

        let mut rows = stmt.query(params![start_time.timestamp(), end_time.timestamp()])?;

        while let Some(row) = rows.next()? {
            mmsi.push(row.get::<_, u32>(0)?);
            time.push(row.get::<_, u64>(1)?);
            sog.push(row.get::<_, f32>(2)?);
            cog.push(row.get::<_, f32>(3)?);
            nav_stat.push(row.get::<_, u8>(4)?);
            rot.push(row.get::<_, i32>(5)?);
            pos_acc.push(row.get::<_, bool>(6)?);
            raim.push(row.get::<_, bool>(7)?);
            heading.push(row.get::<_, u16>(8)?);
            lon.push(row.get::<_, f64>(9)?);
            lat.push(row.get::<_, f64>(10)?);
        }

        // Convert to Polars DataFrame
        let df = DataFrame::new(vec![
            Column::new("mmsi".into(), mmsi),
            Column::new("time".into(), time),
            Column::new("sog".into(), sog),
            Column::new("cog".into(), cog),
            Column::new("nav_stat".into(), nav_stat),
            Column::new("rot".into(), rot),
            Column::new("pos_acc".into(), pos_acc),
            Column::new("raim".into(), raim),
            Column::new("heading".into(), heading),
            Column::new("lon".into(), lon),
            Column::new("lat".into(), lat),
        ])
        .map_err(|e| AisLoggerError::ParquetCreationError(e.to_string()))?;

        Ok(df)
    }

    /// Read metadata from database, return results as DataFrame
    fn get_metadata_df(
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<DataFrame, AisLoggerError> {
        let mut mmsi = Vec::new();
        let mut timestamp = Vec::new();
        let mut name = Vec::new();
        let mut destination = Vec::new();
        let mut vessel_type = Vec::new();
        let mut call_sign = Vec::new();
        let mut imo = Vec::new();
        let mut draught = Vec::new();
        let mut eta = Vec::new();
        let mut pos_type = Vec::new();
        let mut ref_a = Vec::new();
        let mut ref_b = Vec::new();
        let mut ref_c = Vec::new();
        let mut ref_d = Vec::new();

        // Prepare the query to fetch yesterday's locations
        let mut stmt = self.connection.prepare(
            "SELECT mmsi, timestamp, name, destination, vessel_type,
                        call_sign, imo, draught, eta, pos_type,
                        ref_a, ref_b, ref_c, ref_d
                 FROM metadata
                 WHERE timestamp >= ?1 AND timestamp < ?2
                 ORDER BY mmsi, timestamp",
        )?;

        let mut rows = stmt.query(params![
            start_time.timestamp_millis(),
            end_time.timestamp_millis()
        ])?;

        while let Some(row) = rows.next()? {
            mmsi.push(row.get::<_, u32>(0)?);
            timestamp.push(row.get::<_, u64>(1)?);
            name.push(row.get::<_, String>(2)?);
            destination.push(row.get::<_, String>(3)?);
            vessel_type.push(row.get::<_, u8>(4)?);
            call_sign.push(row.get::<_, String>(5)?);
            imo.push(row.get::<_, u32>(6)?);
            draught.push(row.get::<_, u8>(7)?);
            eta.push(row.get::<_, u32>(8)?);
            pos_type.push(row.get::<_, u8>(9)?);
            ref_a.push(row.get::<_, u16>(10)?);
            ref_b.push(row.get::<_, u16>(11)?);
            ref_c.push(row.get::<_, u16>(12)?);
            ref_d.push(row.get::<_, u16>(13)?);
        }

        // Convert to Polars DataFrame
        let df = DataFrame::new(vec![
            Column::new("mmsi".into(), mmsi),
            Column::new("timestamp".into(), timestamp),
            Column::new("name".into(), name),
            Column::new("destination".into(), destination),
            Column::new("vessel_type".into(), vessel_type),
            Column::new("call_sign".into(), call_sign),
            Column::new("imo".into(), imo),
            Column::new("draught".into(), draught),
            Column::new("eta".into(), eta),
            Column::new("pos_type".into(), pos_type),
            Column::new("ref_a".into(), ref_a),
            Column::new("ref_b".into(), ref_b),
            Column::new("ref_c".into(), ref_c),
            Column::new("ref_d".into(), ref_d),
        ])
        .map_err(|e| AisLoggerError::ParquetCreationError(e.to_string()))?;

        Ok(df)
    }

    /// Delete exported data from both tables
    fn cleanup_exported_data(
        tx: &Transaction,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<(), AisLoggerError> {
        tx.execute(
            "DELETE FROM locations WHERE time >= ?1 AND time < ?2",
            params![start_time.timestamp(), end_time.timestamp()],
        )?;

        tx.execute(
            "DELETE FROM metadata WHERE timestamp >= ?1 AND timestamp < ?2",
            params![start_time.timestamp_millis(), end_time.timestamp_millis()],
        )?;

        Ok(())
    }

    /// Write dataframe to Parquet file
    fn write_parquet<P>(df: &mut DataFrame, path: P) -> Result<PathBuf, AisLoggerError>
    where
        P: AsRef<Path>,
    {
        let output_path = PathBuf::from(path.as_ref());
        let mut file = std::fs::File::create(&output_path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Brotli(Some(
                BrotliLevel::try_new(6).unwrap(),
            )))
            .finish(df)
            .map_err(|e| AisLoggerError::ParquetWriteError(e.to_string()))?;

        Ok(output_path)
    }
}

/// Background task that handles periodic flushing
#[allow(dead_code)]
async fn background_flush(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(next_flush) = shared.perform_flush() {
            tokio::select! {
                _ = tokio::time::sleep_until(next_flush) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
    debug!("Background flush task shut down");
}

#[cfg(test)]
impl Db {
    // Test helpers
    fn get_state(&self) -> Result<std::sync::MutexGuard<DatabaseState>, AisLoggerError> {
        self.shared
            .state
            .lock()
            .map_err(|e| AisLoggerError::LockError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Helper function to create a test database
    fn setup_test_db() -> Result<(tempfile::TempDir, Db), AisLoggerError> {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db_guard = DbBuilder::new().path(db_path).build()?;
        Ok((temp_dir, db_guard.db()))
    }

    #[test]
    fn test_process_location() -> Result<(), AisLoggerError> {
        let (_temp_dir, db) = setup_test_db()?;

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

        db.process_message(message)?;
        db.flush()?;

        // Verify database content using the test helper
        let state = db.get_state()?;
        let count: i64 = state.connection.query_row(
            "SELECT COUNT(*) FROM locations WHERE mmsi = 123456 and time = 1668075025",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn test_process_metadata() -> Result<(), AisLoggerError> {
        let (_temp_dir, db) = setup_test_db()?;

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

        db.process_message(message)?;
        db.flush()?;

        // Verify database content
        let state = db.get_state()?;
        let count: i64 = state.connection.query_row(
            "SELECT COUNT(*) FROM metadata WHERE mmsi = 123456 and timestamp = 1668075026035",
            [],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn test_get_locations_df() -> Result<(), AisLoggerError> {
        let (_temp_dir, db) = setup_test_db()?;
        let mut state = db.get_state()?;

        // Prepare test data
        #[rustfmt::skip]
        let test_locations = vec![
            (123456, 1625097600, 10.5, 180.0, 0,  0, true,  false, 270, 20.345818,  60.03802),
            (123456, 1625184000, 11.2, 185.5, 1,  2, true,  true,  275, 20.446729,  60.14753),
            (789012, 1625097600, 8.7,   90.0, 2, -1, false, false, 180, 21.234567,  59.987654),
            (789013, 1625270401, 9.7,   91.0, 4, -4, true,  false, 511, 21.2345678, 59.987655),
        ];

        for location in &test_locations {
            let tx = state.connection.transaction()?;
            DatabaseState::insert_location(
                &tx,
                location.0,
                &VesselLocation {
                    time: location.1,
                    sog: location.2,
                    cog: location.3,
                    nav_stat: location.4,
                    rot: location.5,
                    pos_acc: location.6,
                    raim: location.7,
                    heading: location.8,
                    lon: location.9,
                    lat: location.10,
                },
            )?;
            tx.commit()?;
        }

        let df = state.get_locations_df(
            DateTime::from_timestamp(1625097600, 0).unwrap(),
            DateTime::from_timestamp(1625270400, 0).unwrap(),
        )?;

        // Verify DataFrame contents. Last row has too large timestamp.
        let expected = DataFrame::new(vec![
            Column::new("mmsi".into(), [123456u32, 123456u32, 789012u32]),
            Column::new("time".into(), [1625097600u64, 1625184000u64, 1625097600u64]),
            Column::new("sog".into(), [10.5f32, 11.2f32, 8.7f32]),
            Column::new("cog".into(), [180.0f32, 185.5f32, 90.0f32]),
            Column::new("nav_stat".into(), [0u8, 1u8, 2u8]),
            Column::new("rot".into(), [0i32, 2i32, -1i32]),
            Column::new("pos_acc".into(), [true, true, false]),
            Column::new("raim".into(), [false, true, false]),
            Column::new("heading".into(), [270, 275, 180]),
            Column::new("lon".into(), [20.345818f64, 20.446729f64, 21.234567f64]),
            Column::new("lat".into(), [60.03802f64, 60.14753f64, 59.987654f64]),
        ])
        .unwrap();

        assert_eq!(df, expected);

        Ok(())
    }

    #[test]
    fn test_get_metadata_df() -> Result<(), AisLoggerError> {
        let (_temp_dir, db) = setup_test_db()?;
        let mut state = db.get_state()?;

        // Prepare test data
        #[rustfmt::skip]
        let test_metadata = vec![
            (207124000, 1734518859139u64, "SAKAR",    "ST.PETERSBURG", 70, "LZFS",  9104811, 59, 822656, 15, 133, 36, 20, 5),
            (209530000, 1734438578165u64, "AMISIA",   "FIRAU",         70, "5BEM5", 9361378, 66, 823680, 1,  98,  13, 2,  12),
            (209543000, 1734438561157u64, "THETIS D", "DEHAM",         70, "5BEU5", 9372274, 93, 825408, 1,  157, 11, 13, 13),
            (209726000, 1734438565558u64, "SONORO",   "SE VAL",        70, "5BJG5", 9199397, 46, 823616, 1,  90,  10, 4,  12),
        ];

        for metadata in &test_metadata {
            let tx = state.connection.transaction()?;
            DatabaseState::insert_metadata(
                &tx,
                metadata.0,
                &VesselMetadata {
                    name: metadata.2.to_string(),
                    timestamp: metadata.1,
                    destination: metadata.3.to_string(),
                    vessel_type: metadata.4,
                    call_sign: metadata.5.to_string(),
                    imo: metadata.6,
                    draught: metadata.7,
                    eta: metadata.8,
                    pos_type: metadata.9,
                    ref_a: metadata.10,
                    ref_b: metadata.11,
                    ref_c: metadata.12,
                    ref_d: metadata.13,
                },
            )?;
            tx.commit()?;
        }

        // Retrieve DataFrame. Expect all except first row
        let df = state.get_metadata_df(
            DateTime::from_timestamp_millis(1734300000000).unwrap(),
            DateTime::from_timestamp_millis(1734500000000).unwrap(),
        )?;

        let expected = DataFrame::new(vec![
            Column::new("mmsi".into(), [209530000u32, 209543000u32, 209726000u32]),
            Column::new(
                "timestamp".into(),
                [1734438578165u64, 1734438561157u64, 1734438565558u64],
            ),
            Column::new("name".into(), ["AMISIA", "THETIS D", "SONORO"]),
            Column::new("destination".into(), ["FIRAU", "DEHAM", "SE VAL"]),
            Column::new("vessel_type".into(), [70u8, 70u8, 70u8]),
            Column::new("call_sign".into(), ["5BEM5", "5BEU5", "5BJG5"]),
            Column::new("imo".into(), [9361378u32, 9372274u32, 9199397u32]),
            Column::new("draught".into(), [66u8, 93u8, 46u8]),
            Column::new("eta".into(), [823680u32, 825408u32, 823616u32]),
            Column::new("pos_type".into(), [1u8, 1u8, 1u8]),
            Column::new("ref_a".into(), [98u16, 157u16, 90u16]),
            Column::new("ref_b".into(), [13u16, 11u16, 10u16]),
            Column::new("ref_c".into(), [2u16, 13u16, 4u16]),
            Column::new("ref_d".into(), [12u16, 13u16, 12u16]),
        ])
        .unwrap();

        assert_eq!(df, expected);

        Ok(())
    }

    #[test]
    fn test_daily_export() -> Result<(), AisLoggerError> {
        let (temp_dir, db) = setup_test_db()?;
        let export_dir = temp_dir.path().join("export");
        std::fs::create_dir_all(&export_dir.join("locations"))?;
        std::fs::create_dir_all(&export_dir.join("metadata"))?;

        // Get current time and calculate timestamps for test data
        let now = Utc::now();
        let today_start = now.date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc();
        let yesterday_start = today_start - chrono::Duration::days(1);
        let day_before_start = yesterday_start - chrono::Duration::days(1);

        // Insert test location data in a scope to ensure lock is released
        {
            let mut state = db.get_state()?;

            // Insert test location data
            #[rustfmt::skip]
        let test_locations = vec![
            // Day before yesterday
            VesselLocation { time: day_before_start.timestamp() as u64, sog: 10.5, cog: 180.0, nav_stat: 0, rot: 0, pos_acc: true, raim: false, heading: 270, lon: 20.345818, lat: 60.03802, },
            // Yesterday (should be exported)
            VesselLocation { time: (yesterday_start.timestamp() + 3600) as u64, sog: 11.2, cog: 185.5, nav_stat: 1, rot: 2, pos_acc: true, raim: true, heading: 275, lon: 20.446729, lat: 60.14753, },
            VesselLocation { time: (yesterday_start.timestamp() + 7200) as u64, sog: 8.7, cog: 90.0, nav_stat: 2, rot: -1, pos_acc: false, raim: false, heading: 180, lon: 21.234567, lat: 59.987654, },
            // Today
            VesselLocation { time: (today_start.timestamp() + 3600) as u64, sog: 9.7, cog: 91.0, nav_stat: 4, rot: -4, pos_acc: true, raim: false, heading: 511, lon: 21.2345678, lat: 59.987655, },
        ];

            let mmsis = vec![123456, 123456, 789012, 789013];

            for (mmsi, location) in mmsis.into_iter().zip(test_locations) {
                let tx = state.connection.transaction()?;
                DatabaseState::insert_location(&tx, mmsi, &location)?;
                tx.commit()?;
            }

            // Insert test metadata
            #[rustfmt::skip]
        let test_metadata = vec![
            // Day before yesterday
            ( 207124000, VesselMetadata { timestamp: day_before_start.timestamp_millis() as u64, name: "SHIP1".to_string(), destination: "PORT1".to_string(), vessel_type: 70, call_sign: "AAA1".to_string(), imo: 9104811, draught: 59, eta: 822656, pos_type: 15, ref_a: 133, ref_b: 36, ref_c: 20, ref_d: 5, }),
            // Yesterday (should be exported)
            ( 209530000, VesselMetadata { timestamp: (yesterday_start.timestamp_millis() + 3600000) as u64, name: "SHIP2".to_string(),  destination: "PORT2".to_string(), vessel_type: 70, call_sign: "BBB2".to_string(), imo: 9361378, draught: 66, eta: 823680, pos_type: 1, ref_a: 98, ref_b: 13, ref_c: 2, ref_d: 12, }),
            // Today
            ( 209543000, VesselMetadata { timestamp: (today_start.timestamp_millis() + 3600000) as u64, name: "SHIP3".to_string(), destination: "PORT3".to_string(),  vessel_type: 70, call_sign: "CCC3".to_string(), imo: 9372274, draught: 93, eta: 825408, pos_type: 1, ref_a: 157, ref_b: 11, ref_c: 13, ref_d: 13, }),
        ];

            for metadata in &test_metadata {
                let tx = state.connection.transaction()?;
                DatabaseState::insert_metadata(&tx, metadata.0, &metadata.1)?;
                tx.commit()?;
            }
        } // state lock is released here

        // Perform daily export
        db.daily_export(&export_dir)?;

        // Verify results in a new scope with fresh lock
        {
            let state = db.get_state()?;

            // Check that parquet files exist and have content
            let yesterday_date = yesterday_start.format("%Y-%m-%d").to_string();
            let locations_path = export_dir
                .join("locations")
                .join(format!("{}.parquet", yesterday_date));
            let metadata_path = export_dir
                .join("metadata")
                .join(format!("{}.parquet", yesterday_date));

            assert!(locations_path.exists());
            assert!(metadata_path.exists());

            // Verify file sizes are non-zero
            assert!(std::fs::metadata(&locations_path)?.len() > 0);
            assert!(std::fs::metadata(&metadata_path)?.len() > 0);

            // Verify that yesterday's data was removed from database
            let count: i64 = state.connection.query_row(
                "SELECT COUNT(*) FROM locations WHERE time >= ?1 AND time < ?2",
                params![yesterday_start.timestamp(), today_start.timestamp()],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0);

            let count: i64 = state.connection.query_row(
                "SELECT COUNT(*) FROM metadata WHERE timestamp >= ?1 AND timestamp < ?2",
                params![
                    yesterday_start.timestamp_millis(),
                    today_start.timestamp_millis()
                ],
                |row| row.get(0),
            )?;
            assert_eq!(count, 0);

            // Verify that data before yesterday and today's data remains in database
            let before_yesterday: i64 = state.connection.query_row(
                "SELECT COUNT(*) FROM locations WHERE time < ?1",
                params![yesterday_start.timestamp()],
                |row| row.get(0),
            )?;
            assert_eq!(before_yesterday, 1);

            let after_yesterday: i64 = state.connection.query_row(
                "SELECT COUNT(*) FROM locations WHERE time >= ?1",
                params![today_start.timestamp()],
                |row| row.get(0),
            )?;
            assert_eq!(after_yesterday, 1);

            let before_yesterday: i64 = state.connection.query_row(
                "SELECT COUNT(*) FROM metadata WHERE timestamp < ?1",
                params![yesterday_start.timestamp_millis()],
                |row| row.get(0),
            )?;
            assert_eq!(before_yesterday, 1);

            let after_yesterday: i64 = state.connection.query_row(
                "SELECT COUNT(*) FROM metadata WHERE timestamp >= ?1",
                params![today_start.timestamp_millis()],
                |row| row.get(0),
            )?;
            assert_eq!(after_yesterday, 1);
        }

        Ok(())
    }
}
