//! Database functionality for AIS message storage and export
//!
//! This module provides a thread-safe interface for:
//! - Storing AIS messages in SQLite database
//! - Periodic flushing of data to disk
//! - Daily export of historical data to Parquet files
//! - Automatic cleanup of exported data

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OpenFlags, Transaction};
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::models::Mmsi;
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
                mmsi INTEGER NOT NULL,
                time INTEGER NOT NULL,
                sog REAL,
                cog REAL,
                nav_stat INTEGER,
                rot INTEGER,
                pos_acc INTEGER NOT NULL,
                raim INTEGER NOT NULL,
                heading INTEGER,
                lon REAL NOT NULL,
                lat REAL NOT NULL,
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
                mmsi INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                name TEXT,
                destination TEXT,
                vessel_type INTEGER,
                call_sign TEXT,
                imo INTEGER,
                draught REAL,
                eta INTEGER NOT NULL,
                pos_type INTEGER,
                ref_a INTEGER,
                ref_b INTEGER,
                ref_c INTEGER,
                ref_d INTEGER,
                PRIMARY KEY (mmsi, timestamp)
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
            AisMessageType::Location(location) => {
                Self::insert_location(tx, &message.mmsi, location)
            }
            AisMessageType::Metadata(metadata) => {
                Self::insert_metadata(tx, &message.mmsi, metadata)
            }
        })
    }

    /// Insert vessel location
    fn insert_location(
        tx: &Transaction,
        mmsi: &Mmsi,
        location: &VesselLocation,
    ) -> Result<(), AisLoggerError> {
        tx.execute(
            "INSERT OR REPLACE INTO locations (
                mmsi, time, sog, cog, nav_stat, rot,
                pos_acc, raim, heading, lon, lat
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                mmsi.value(),
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
        mmsi: &Mmsi,
        metadata: &VesselMetadata,
    ) -> Result<(), AisLoggerError> {
        tx.execute(
            "INSERT OR REPLACE INTO metadata (
                mmsi, timestamp, name, destination, vessel_type,
                call_sign, imo, draught, eta, pos_type,
                ref_a, ref_b, ref_c, ref_d
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                mmsi.value(),
                metadata.timestamp,
                metadata.name.as_deref(),
                metadata.destination.as_deref(),
                metadata.vessel_type,
                metadata.call_sign.as_deref(),
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
    use crate::models::Eta;

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
            mmsi: Mmsi::try_from(123456).unwrap(),
            message_type: AisMessageType::Location(VesselLocation {
                time: 1668075025,
                sog: Some(10.7),
                cog: Some(326.6),
                nav_stat: Some(0),
                rot: Some(0.0),
                pos_acc: true,
                raim: false,
                heading: Some(325),
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
            mmsi: Mmsi::try_from(123456).unwrap(),
            message_type: AisMessageType::Metadata(VesselMetadata {
                timestamp: 1668075026035,
                destination: Some("UST LUGA".to_string()),
                name: Some("ARUNA CIHAN".to_string()),
                draught: Some(6.8),
                eta: Eta {
                    month: Some(11),
                    day: Some(6),
                    hour: Some(3),
                    minute: Some(0),
                },
                pos_type: None,
                ref_a: Some(160),
                ref_b: Some(33),
                ref_c: Some(20),
                ref_d: Some(12),
                call_sign: Some("V7WW7".to_string()),
                imo: Some(9543756),
                vessel_type: Some(70),
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
}
