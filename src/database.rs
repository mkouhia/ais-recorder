//! Database functionality

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use polars::prelude::*;
use rusqlite::{params, Connection, OpenFlags, Transaction};
use tracing::{error, info};

use crate::{
    config::DatabaseConfig,
    errors::AisLoggerError,
    models::{AisMessage, AisMessageType, VesselLocation, VesselMetadata},
};

/// Database writer for AIS data
pub struct DatabaseWriter {
    connection: Connection,
    config: DatabaseConfig,
    last_flush: Instant,
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
                connection: conn,
                config,
                last_flush: Instant::now(),
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
    pub fn process_message(&mut self, message: AisMessage) -> Result<(), AisLoggerError> {
        let tx = self.connection.transaction()?;

        match message.message_type {
            AisMessageType::Location(location) => {
                Self::insert_location(&tx, message.mmsi, &location)?;
            }
            AisMessageType::Metadata(metadata) => {
                Self::insert_metadata(&tx, message.mmsi, &metadata)?;
            }
        }

        tx.commit()?;

        // Check if we need to flush based on time
        self.maybe_flush()?;

        Ok(())
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

    /// Conditionally flush data based on time interval
    fn maybe_flush(&mut self) -> Result<(), AisLoggerError> {
        if self.last_flush.elapsed() >= self.config.flush_interval {
            info!("Performing periodic flush");
            self.flush()?;
        }

        Ok(())
    }

    /// Explicitly flush database
    pub fn flush(&mut self) -> Result<(), AisLoggerError> {
        // In SQLite with WAL mode, this ensures data is written to disk
        self.connection
            .pragma_update(None, "wal_checkpoint", "PASSIVE")?;
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Export previous day's data to Parquet files and delete records from database
    ///
    /// Locations and metadata will be placed to
    ///     base_dir/{locations,metadata}/<%Y-%m-%d>.parquet ,
    /// where date refers to yesterday's UTC date.
    pub fn daily_export<P>(&mut self, base_dir: P) -> Result<(), AisLoggerError>
    where
        P: AsRef<Path>,
    {
        // Determine the previous day's date range
        let today = Utc::now().date_naive();

        let yesterday = today.pred_opt().unwrap(); //FIXME all those unwraps!
        let start_time = yesterday.and_hms_opt(0, 0, 0).unwrap().and_utc();
        let end_time = today.and_hms_opt(0, 0, 0).unwrap().and_utc();

        // FIXME Perform export in a single transaction to ensure consistency

        // Export locations
        let mut locations = self.get_locations_df(start_time, end_time)?;
        Self::write_parquet(
            &mut locations,
            base_dir
                .as_ref()
                .join("locations")
                .join(format!("{}.parquet", yesterday.format("%Y-%m-%d"))),
        )?;

        // Export metadata
        let mut metadata = self.get_metadata_df(start_time, end_time)?;
        Self::write_parquet(
            &mut metadata,
            base_dir
                .as_ref()
                .join("metadata")
                .join(format!("{}.parquet", yesterday.format("%Y-%m-%d"))),
        )?;

        // Clean up exported data
        self.cleanup_exported_data(start_time, end_time)?;

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
            .with_compression(ParquetCompression::Lzo)
            .finish(df)
            .map_err(|e| AisLoggerError::ParquetWriteError(e.to_string()))?;

        Ok(output_path)
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
        &mut self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<(), AisLoggerError> {
        self.connection.execute(
            "DELETE FROM locations WHERE time >= ?1 AND time < ?2",
            params![start_time.timestamp(), end_time.timestamp()],
        )?;

        self.connection.execute(
            "DELETE FROM metadata WHERE timestamp >= ?1 AND timestamp < ?2",
            params![start_time.timestamp_millis(), end_time.timestamp_millis()],
        )?;

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

    #[test]
    fn test_process_location() -> Result<(), AisLoggerError> {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut db_writer = DatabaseWriterBuilder::new().path(db_path.clone()).build()?;

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

        db_writer.process_message(message)?;
        db_writer.flush()?;

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

    #[test]
    fn test_process_metadata() -> Result<(), AisLoggerError> {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let mut db_writer = DatabaseWriterBuilder::new().path(db_path.clone()).build()?;

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

        db_writer.process_message(message)?;
        db_writer.flush()?;

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

    #[test]
    fn test_get_locations_df() -> Result<(), AisLoggerError> {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_locations.db");
        let mut db_writer = DatabaseWriterBuilder::new().path(db_path.clone()).build()?;

        // Prepare test data
        #[rustfmt::skip]
        let test_locations = vec![
            (123456, 1625097600, 10.5, 180.0, 0,  0, 1, 0, 270, 20.345818,  60.03802),
            (123456, 1625184000, 11.2, 185.5, 1,  2, 1, 1, 275, 20.446729,  60.14753),
            (789012, 1625097600, 8.7,   90.0, 2, -1, 0, 0, 180, 21.234567,  59.987654),
            (789013, 1625270401, 9.7,   91.0, 4, -4, 1, 0, 511, 21.2345678, 59.987655),
        ];

        // Insert test data
        let mut stmt = db_writer.connection.prepare(
            "INSERT INTO locations
            (mmsi, time, sog, cog, nav_stat, rot, pos_acc, raim, heading, lon, lat)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?;

        for location in &test_locations {
            stmt.execute(params![
                location.0,
                location.1,
                location.2,
                location.3,
                location.4,
                location.5,
                location.6,
                location.7,
                location.8,
                location.9,
                location.10
            ])?;
        }
        drop(stmt);
        db_writer.flush()?;

        // Retrieve DataFrame
        let df = db_writer.get_locations_df(
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
        // Create a temporary directory for the test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_metadata.db");
        let mut db_writer = DatabaseWriterBuilder::new().path(db_path.clone()).build()?;

        // Prepare test data
        #[rustfmt::skip]
        let test_metadata = vec![
            (207124000, 1734518859139u64, "SAKAR",    "ST.PETERSBURG", 70, "LZFS",  9104811, 59, 822656, 15, 133, 36, 20, 5),
            (209530000, 1734438578165u64, "AMISIA",   "FIRAU",         70, "5BEM5", 9361378, 66, 823680, 1,  98,  13, 2,  12),
            (209543000, 1734438561157u64, "THETIS D", "DEHAM",         70, "5BEU5", 9372274, 93, 825408, 1,  157, 11, 13, 13),
            (209726000, 1734438565558u64, "SONORO",   "SE VAL",        70, "5BJG5", 9199397, 46, 823616, 1,  90,  10, 4,  12),
        ];

        // Insert test data
        let mut stmt = db_writer.connection.prepare(
            "INSERT INTO metadata (
                mmsi, timestamp, name, destination, vessel_type,
                call_sign, imo, draught, eta, pos_type,
                ref_a, ref_b, ref_c, ref_d
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        )?;

        for meta in &test_metadata {
            stmt.execute(params![
                meta.0, meta.1, meta.2, meta.3, meta.4, meta.5, meta.6, meta.7, meta.8, meta.9,
                meta.10, meta.11, meta.12, meta.13,
            ])?;
        }
        drop(stmt);
        db_writer.flush()?;

        // Retrieve DataFrame. Expect all except first row
        let df = db_writer.get_metadata_df(
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
}
