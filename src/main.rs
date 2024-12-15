use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use log::{error, info, trace, warn};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, Transport};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task;

/// Vessel location
///
/// See: https://meri.digitraffic.fi/swagger/#/AIS%20V1/vesselLocationsByMssiAndTimestamp
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct VesselLocation {
    /// Location record timestamp in milliseconds from Unix epoch.
    time: u64,
    /// Speed over ground in knots, 102.3 = not available
    sog: f32,
    /// Course over ground in degrees, 360 = not available (default)
    cog: f32,
    /// Navigational status
    ///
    /// Value range between 0 - 15.
    /// - 0 = under way using engine
    /// - 1 = at anchor
    /// - 2 = not under command
    /// - 3 = restricted maneuverability
    /// - 4 = constrained by her draught
    /// - 5 = moored
    /// - 6 = aground
    /// - 7 = engaged in fishing
    /// - 8 = under way sailing
    /// - 9 = reserved for future amendment of navigational status for ships
    ///   carrying DG, HS, or MP, or IMO hazard or pollutant category C,
    ///   high speed craft (HSC)
    /// - 10 = reserved for future amendment of navigational status for ships
    ///   carrying dangerous goods (DG), harmful substances (HS) or marine
    ///   pollutants (MP), or IMO hazard or pollutant category A, wing in
    ///   ground (WIG)
    /// - 11 = power-driven vessel towing astern (regional use)
    /// - 12 = power-driven vessel pushing ahead or towing alongside (regional use)
    /// - 13 = reserved for future use
    /// - 14 = AIS-SART (active), MOB-AIS, EPIRB-AIS
    /// - 15 = default
    #[serde(rename = "navStat")]
    nav_stat: u8,
    /// Rate of turn, ROT[AIS].
    ///
    /// Values range between -128 - 127. –128 indicates that value is not
    /// available (default). Coded by ROT[AIS] = 4.733 SQRT(ROT[IND]) where
    /// ROT[IND] is the Rate of Turn degrees per minute, as indicated by
    /// an external sensor.
    /// - +127 = turning right at 720 degrees per minute or higher
    /// - -127 = turning left at 720 degrees per minute or higher.
    rot: f32,
    /// Position accuracy, 1 = high, 0 = low
    #[serde(rename = "posAcc")]
    pos_acc: bool,
    /// Receiver autonomous integrity monitoring (RAIM) flag of electronic position fixing device
    raim: bool,
    /// Degrees (0-359), 511 = not available (default)
    heading: u16,
    lon: f64,
    lat: f64,
}

/// Vessel metadata
///
/// See: https://meri.digitraffic.fi/swagger/#/AIS%20V1/vesselMetadataByMssi
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct VesselMetadata {
    /// Name of the vessel, maximum 20 characters using 6-bit ASCII
    name: String,
    /// Record timestamp in milliseconds from Unix epoch
    timestamp: u64,
    /// Destination, maximum 20 characters using 6-bit ASCII
    destination: String,
    /// Vessel's AIS ship type
    #[serde(rename = "type")]
    vessel_type: u8,
    /// Call sign, maximum 7 6-bit ASCII characters
    #[serde(rename = "callSign")]
    call_sign: String,
    /// Vessel International Maritime Organization (IMO) number
    imo: u32,
    /// Maximum present static draught in 1/10m
    ///
    /// 255 = draught 25.5 m or greater, 0 = not available (default)
    draught: u8,
    /// Estimated time of arrival; MMDDHHMM UTC
    ///
    /// - Bits 19-16: month; 1-12; 0 = not available = default
    /// - Bits 15-11: day; 1-31; 0 = not available = default
    /// - Bits 10-6: hour; 0-23; 24 = not available = default
    /// - Bits 5-0: minute; 0-59; 60 = not available = default
    ///
    /// For SAR aircraft, the use of this field may be decided by the
    /// responsible administration.
    eta: u32,
    /// Type of electronic position fixing device
    ///
    /// - 0 = undefined (default)
    /// - 1 = GPS
    /// - 2 = GLONASS
    /// - 3 = combined GPS/GLONASS
    /// - 4 = Loran-C
    /// - 5 = Chayka
    /// - 6 = integrated navigation system
    /// - 7 = surveyed
    /// - 8 = Galileo,
    /// - 9-14 = not used
    /// - 15 = internal GNSS
    #[serde(rename = "posType")]
    pos_type: u8,
    /// Reference point for reported position dimension A
    #[serde(rename = "refA")]
    ref_a: u16,
    /// Reference point for reported position dimension B
    #[serde(rename = "refB")]
    ref_b: u16,
    /// Reference point for reported position dimension C
    #[serde(rename = "refC")]
    ref_c: u16,
    /// Reference point for reported position dimension D
    #[serde(rename = "refD")]
    ref_d: u16,
}

struct DatabaseWriter {
    connection: Connection,
    location_batch: VecDeque<(u32, VesselLocation)>,
    metadata_batch: VecDeque<(u32, VesselMetadata)>,
    batch_size: usize,
    last_flush: Instant,
}

impl DatabaseWriter {
    fn new(db_path: &str, batch_size: usize) -> Result<Self> {
        info!("Initializing database at {}", db_path);
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;

        // Create tables with indexes
        conn.execute(
            "CREATE TABLE IF NOT EXISTS locations (
                mmsi INTEGER,
                timestamp INTEGER,
                sog REAL,
                cog REAL,
                nav_stat INTEGER,
                rot REAL,
                pos_acc INTEGER,
                raim INTEGER,
                heading INTEGER,
                lon REAL,
                lat REAL
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_locations_mmsi ON locations(mmsi)",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                mmsi INTEGER,
                timestamp INTEGER,
                destination TEXT,
                name TEXT,
                draught INTEGER,
                eta INTEGER,
                pos_type INTEGER,
                ref_a INTEGER,
                ref_b INTEGER,
                ref_c INTEGER,
                ref_d INTEGER,
                call_sign TEXT,
                imo INTEGER,
                vessel_type INTEGER
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_metadata_mmsi ON metadata(mmsi)",
            [],
        )?;

        Ok(Self {
            connection: conn,
            location_batch: VecDeque::with_capacity(batch_size),
            metadata_batch: VecDeque::with_capacity(batch_size),
            batch_size,
            last_flush: Instant::now(),
        })
    }

    fn add_location(&mut self, mmsi: u32, location: VesselLocation) -> Result<()> {
        self.location_batch.push_back((mmsi, location));

        if self.location_batch.len() >= self.batch_size
            || self.last_flush.elapsed() >= Duration::from_secs(5)
        {
            self.flush_locations()?;
        }

        Ok(())
    }

    fn add_metadata(&mut self, mmsi: u32, metadata: VesselMetadata) -> Result<()> {
        self.metadata_batch.push_back((mmsi, metadata));

        if self.metadata_batch.len() >= self.batch_size
            || self.last_flush.elapsed() >= Duration::from_secs(5)
        {
            self.flush_metadata()?;
        }

        Ok(())
    }

    fn flush_locations(&mut self) -> Result<()> {
        if self.location_batch.is_empty() {
            return Ok(());
        }

        let tx = self.connection.transaction()?;

        let n_inserted = {
            let mut count = 0;
            let mut stmt = tx.prepare(
                "INSERT INTO locations (
                    mmsi, timestamp, sog, cog, nav_stat, rot,
                    pos_acc, raim, heading, lon, lat
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )?;

            for (mmsi, location) in self.location_batch.drain(..) {
                stmt.execute(params![
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
                ])?;
                count += 1;
            }
            count
        };
        tx.commit()?;
        self.last_flush = Instant::now();
        trace!("Inserted {} location rows", n_inserted);
        Ok(())
    }

    fn flush_metadata(&mut self) -> Result<()> {
        if self.metadata_batch.is_empty() {
            return Ok(());
        }

        let tx = self.connection.transaction()?;

        let n_inserted = {
            let mut count = 0;
            let mut stmt = tx.prepare(
                "INSERT INTO metadata (
                    mmsi, timestamp, destination, name, draught, eta,
                    pos_type, ref_a, ref_b, ref_c, ref_d,
                    call_sign, imo, vessel_type
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )?;

            for (mmsi, metadata) in self.metadata_batch.drain(..) {
                stmt.execute(params![
                    mmsi,
                    metadata.timestamp,
                    metadata.destination,
                    metadata.name,
                    metadata.draught,
                    metadata.eta,
                    metadata.pos_type,
                    metadata.ref_a,
                    metadata.ref_b,
                    metadata.ref_c,
                    metadata.ref_d,
                    metadata.call_sign,
                    metadata.imo,
                    metadata.vessel_type
                ])?;
                count += 1;
            }
            count
        };

        tx.commit()?;
        self.last_flush = Instant::now();
        trace!("Inserted {} metadata rows", n_inserted);
        Ok(())
    }

    // Ensure any remaining batched data is written on drop
    fn flush_all(&mut self) -> Result<()> {
        info!("Flush all data");
        self.flush_locations()?;
        self.flush_metadata()?;
        Ok(())
    }
}

// Implement Drop trait to ensure final flush on program exit
impl Drop for DatabaseWriter {
    fn drop(&mut self) {
        info!("Drop database writer on program exit");
        if let Err(e) = self.flush_all() {
            error!("Error flushing data on exit: {}", e);
        }
    }
}

async fn mqtt_listener(mut eventloop: EventLoop, tx: mpsc::Sender<(String, Vec<u8>)>) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                if let Err(e) = tx.send((p.topic, p.payload.to_vec())).await {
                    error!("Failed to send message: {}", e);
                }
            }
            Err(e) => {
                error!("MQTT Error: {}", e);
                break;
            }
            _ => continue,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut mqttoptions = MqttOptions::new(
        "ais-logger",
        "wss://meri.digitraffic.fi:443/mqtt",
        443, // port parameter is ignored when scheme is websocket
    );
    mqttoptions.set_transport(Transport::wss_with_default_config());
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe("vessels-v2/#", QoS::AtLeastOnce).await?;

    let (tx, mut rx) = mpsc::channel(100);

    // Spawn MQTT listener
    tokio::spawn(mqtt_listener(eventloop, tx));

    // Create database writer with batch size of 1000 and 5-second flush interval
    let db_writer = Arc::new(Mutex::new(DatabaseWriter::new("vessels.db", 1000)?));

    // Capture Ctrl+C signal to ensure final flush
    let db_writer_clone = Arc::clone(&db_writer);
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");

        info!("Received Ctrl+C, exiting...");
        // Try to flush remaining data before program exit
        if let Ok(mut writer) = db_writer_clone.lock() {
            if let Err(e) = writer.flush_all() {
                error!("Error flushing data on Ctrl+C: {}", e);
            }
        }

        std::process::exit(0);
    });

    while let Some((topic, payload)) = rx.recv().await {
        let writers = Arc::clone(&db_writer);

        task::spawn(async move {
            if let Err(e) = process_message(topic, payload, writers) {
                error!("Message processing error: {}", e);
            }
        });
    }

    Ok(())
}

fn process_message(
    topic: String,
    payload: Vec<u8>,
    db_writer: Arc<Mutex<DatabaseWriter>>,
) -> Result<()> {
    let parts: Vec<&str> = topic.split('/').collect();

    // Validate topic structure
    if parts.len() < 3 || parts[0] != "vessels-v2" {
        warn!("Invalid topic structure: {}", topic);
        return Ok(());
    }

    let mmsi = parts[1]
        .parse::<u32>()
        .with_context(|| format!("Failed to parse {} as MMSI", parts[1]))?;
    let message_type = parts[2];

    match message_type {
        "location" => {
            let location: VesselLocation = serde_json::from_slice(&payload)?;
            let mut writer = db_writer.lock().unwrap();
            writer.add_location(mmsi, location)?;
        }
        "metadata" => {
            let metadata: VesselMetadata = serde_json::from_slice(&payload)?;
            let mut writer = db_writer.lock().unwrap();
            writer.add_metadata(mmsi, metadata)?;
        }
        _ => warn!("Unknown message type '{}', ignoring.", message_type),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_sample_location() -> VesselLocation {
        VesselLocation {
            time: 1668075025,
            sog: 10.7,
            cog: 326.6,
            nav_stat: 0,
            rot: 0.0,
            pos_acc: true,
            raim: false,
            heading: 325,
            lon: 20.345818,
            lat: 60.03802,
        }
    }

    fn create_sample_metadata() -> VesselMetadata {
        VesselMetadata {
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
        }
    }

    #[test]
    fn test_database_creation_and_insertion() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir
            .path()
            .join("vessels.db")
            .to_str()
            .unwrap()
            .to_string();

        // Create DatabaseWriter
        let mut db_writer = DatabaseWriter::new(&db_path, 10)?;

        // Insert sample data
        let mmsi = 123456;
        let location = create_sample_location();
        let metadata = create_sample_metadata();

        // Add and flush locations
        db_writer.add_location(mmsi, location.clone())?;
        db_writer.flush_locations()?;

        // Add and flush metadata
        db_writer.add_metadata(mmsi, metadata.clone())?;
        db_writer.flush_metadata()?;

        // Verify insertions
        let conn = Connection::open(&db_path)?;

        // Check location insertion
        let location_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM locations WHERE mmsi = ?1",
            params![mmsi],
            |row| row.get(0),
        )?;
        assert_eq!(location_count, 1);

        // Check metadata insertion
        let metadata_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM metadata WHERE mmsi = ?1",
            params![mmsi],
            |row| row.get(0),
        )?;
        assert_eq!(metadata_count, 1);

        Ok(())
    }

    #[test]
    fn test_batch_insertion_performance() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir
            .path()
            .join("vessels_perf.db")
            .to_str()
            .unwrap()
            .to_string();

        let mut db_writer = DatabaseWriter::new(&db_path, 1000)?;

        let mmsi = 123456;
        let location = create_sample_location();
        let metadata = create_sample_metadata();

        // Performance test with batch insertions
        let start = Instant::now();
        let num_iterations = 10_000;

        for _ in 0..num_iterations {
            db_writer.add_location(mmsi, location.clone())?;
            db_writer.add_metadata(mmsi, metadata.clone())?;
        }

        // Ensure final flush
        db_writer.flush_all()?;

        let duration = start.elapsed();
        let total_messages = num_iterations * 2;
        let messages_per_second = total_messages as f64 / duration.as_secs_f64();

        println!("Processed {} messages in {:.2?}", total_messages, duration);
        println!(
            "Batch Insertion Throughput: {:.2} messages/second",
            messages_per_second
        );

        // Verify total inserted records
        let conn = Connection::open(&db_path)?;
        let location_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM locations", [], |row| row.get(0))?;
        let metadata_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM metadata", [], |row| row.get(0))?;

        assert_eq!(location_count, num_iterations as i64);
        assert_eq!(metadata_count, num_iterations as i64);

        Ok(())
    }
}
