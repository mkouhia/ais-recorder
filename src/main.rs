use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::{error, info};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use rusqlite::{params, Connection, OpenFlags, Transaction};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct VesselLocation {
    time: u64,
    sog: f64,
    cog: f64,
    #[serde(default)]
    navStat: u8,
    #[serde(default)]
    rot: f64,
    #[serde(default)]
    posAcc: bool,
    #[serde(default)]
    raim: bool,
    #[serde(default)]
    heading: u16,
    lon: f64,
    lat: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
struct VesselMetadata {
    timestamp: u64,
    destination: String,
    name: String,
    #[serde(default)]
    draught: u8,
    #[serde(default)]
    eta: u64,
    #[serde(default)]
    posType: u8,
    #[serde(default)]
    refA: u16,
    #[serde(default)]
    refB: u16,
    #[serde(default)]
    refC: u16,
    #[serde(default)]
    refD: u16,
    #[serde(default)]
    callSign: String,
    #[serde(default)]
    imo: u64,
    #[serde(rename = "type")]
    #[serde(default)]
    vessel_type: u8,
}

struct DatabaseWriter {
    connection: Connection,
    location_batch: VecDeque<(String, VesselLocation)>,
    metadata_batch: VecDeque<(String, VesselMetadata)>,
    batch_size: usize,
    last_flush: Instant,
}

impl DatabaseWriter {
    fn new(db_path: &str, batch_size: usize) -> Result<Self> {
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;

        // Create tables if not exists
        conn.execute(
            "CREATE TABLE IF NOT EXISTS vessel_locations (
                mmsi TEXT,
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
            "CREATE TABLE IF NOT EXISTS vessel_metadata (
                mmsi TEXT,
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

        Ok(Self {
            connection: conn,
            location_batch: VecDeque::with_capacity(batch_size),
            metadata_batch: VecDeque::with_capacity(batch_size),
            batch_size,
            last_flush: Instant::now(),
        })
    }

    fn add_location(&mut self, mmsi: String, location: VesselLocation) -> Result<()> {
        self.location_batch.push_back((mmsi, location));

        if self.location_batch.len() >= self.batch_size
            || self.last_flush.elapsed() >= Duration::from_secs(5)
        {
            self.flush_locations()?;
        }

        Ok(())
    }

    fn add_metadata(&mut self, mmsi: String, metadata: VesselMetadata) -> Result<()> {
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

        {
            let mut stmt = tx.prepare(
                "INSERT INTO vessel_locations (
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
                    location.navStat,
                    location.rot,
                    location.posAcc as i32,
                    location.raim as i32,
                    location.heading,
                    location.lon,
                    location.lat
                ])?;
            }
        }
        tx.commit()?;
        self.last_flush = Instant::now();
        Ok(())
    }

    fn flush_metadata(&mut self) -> Result<()> {
        if self.metadata_batch.is_empty() {
            return Ok(());
        }

        let tx = self.connection.transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT INTO vessel_metadata (
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
                    metadata.posType,
                    metadata.refA,
                    metadata.refB,
                    metadata.refC,
                    metadata.refD,
                    metadata.callSign,
                    metadata.imo,
                    metadata.vessel_type
                ])?;
            }
        }

        tx.commit()?;
        self.last_flush = Instant::now();
        Ok(())
    }

    // Ensure any remaining batched data is written on drop
    fn flush_all(&mut self) -> Result<()> {
        self.flush_locations()?;
        self.flush_metadata()?;
        Ok(())
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

    let mut mqttoptions = MqttOptions::new("vessel_logger", "mqtt.example.com", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    client.subscribe("vessels-v2/#", QoS::AtLeastOnce).await?;

    let (tx, mut rx) = mpsc::channel(100);

    // Spawn MQTT listener
    tokio::spawn(mqtt_listener(eventloop, tx));

    // Create database writer with batch size of 1000 and 5-second flush interval
    let db_writer = Arc::new(Mutex::new(DatabaseWriter::new("vessels.db", 1000)?));

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
        return Ok(());
    }

    let mmsi = parts[1].to_string();
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
        _ => {}
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
            navStat: 0,
            rot: 0.0,
            posAcc: true,
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
            posType: 15,
            refA: 160,
            refB: 33,
            refC: 20,
            refD: 12,
            callSign: "V7WW7".to_string(),
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
        let mmsi = "123456".to_string();
        let location = create_sample_location();
        let metadata = create_sample_metadata();

        // Add and flush locations
        db_writer.add_location(mmsi.clone(), location.clone())?;
        db_writer.flush_locations()?;

        // Add and flush metadata
        db_writer.add_metadata(mmsi.clone(), metadata.clone())?;
        db_writer.flush_metadata()?;

        // Verify insertions
        let conn = Connection::open(&db_path)?;

        // Check location insertion
        let location_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM vessel_locations WHERE mmsi = ?1",
            params![mmsi],
            |row| row.get(0),
        )?;
        assert_eq!(location_count, 1);

        // Check metadata insertion
        let metadata_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM vessel_metadata WHERE mmsi = ?1",
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

        let mmsi = "123456".to_string();
        let location = create_sample_location();
        let metadata = create_sample_metadata();

        // Performance test with batch insertions
        let start = Instant::now();
        let num_iterations = 10_000;

        for _ in 0..num_iterations {
            db_writer.add_location(mmsi.clone(), location.clone())?;
            db_writer.add_metadata(mmsi.clone(), metadata.clone())?;
        }

        // Ensure final flush
        db_writer.flush_locations()?;
        db_writer.flush_metadata()?;

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
            conn.query_row("SELECT COUNT(*) FROM vessel_locations", [], |row| {
                row.get(0)
            })?;
        let metadata_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM vessel_metadata", [], |row| row.get(0))?;

        assert_eq!(location_count, num_iterations as i64);
        assert_eq!(metadata_count, num_iterations as i64);

        Ok(())
    }
}
