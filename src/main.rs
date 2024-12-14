use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use chrono::{TimeZone, Utc};
use log::error;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct VesselLocation {
    time: u64,
    sog: f64,
    cog: f64,
    #[serde(rename = "navStat")]
    nav_stat: u8,
    rot: f64,
    #[serde(rename = "posAcc")]
    pos_acc: bool,
    raim: bool,
    heading: u16,
    lon: f64,
    lat: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct VesselMetadata {
    timestamp: u64,
    destination: String,
    name: String,
    draught: u8,
    eta: u64,
    #[serde(rename = "posType")]
    pos_type: u8,
    #[serde(rename = "refA")]
    ref_a: u16,
    #[serde(rename = "refB")]
    ref_b: u16,
    #[serde(rename = "refC")]
    ref_c: u16,
    #[serde(rename = "refD")]
    ref_d: u16,
    #[serde(rename = "callSign")]
    call_sign: String,
    imo: u64,
    #[serde(rename = "type")]
    vessel_type: u8,
}

struct FileWriters {
    location_writers: HashMap<String, BufWriter<File>>,
    metadata_writers: HashMap<String, BufWriter<File>>,
}

impl FileWriters {
    fn new() -> Self {
        Self {
            location_writers: HashMap::new(),
            metadata_writers: HashMap::new(),
        }
    }

    fn get_or_create_location_writer(
        &mut self,
        mmsi: &str,
        date: &str,
    ) -> Result<&mut BufWriter<File>> {
        let path = format!("data/location/{}/{}.ndjson", date, mmsi);

        if !self.location_writers.contains_key(&path) {
            fs::create_dir_all(Path::new(&path).parent().unwrap())?;
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            self.location_writers
                .insert(path.clone(), BufWriter::new(file));
        }

        Ok(self.location_writers.get_mut(&path).unwrap())
    }

    fn get_or_create_metadata_writer(
        &mut self,
        mmsi: &str,
        date: &str,
    ) -> Result<&mut BufWriter<File>> {
        let path = format!("data/metadata/{}/{}.ndjson", date, mmsi);

        if !self.metadata_writers.contains_key(&path) {
            fs::create_dir_all(Path::new(&path).parent().unwrap())?;
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            self.metadata_writers
                .insert(path.clone(), BufWriter::new(file));
        }

        Ok(self.metadata_writers.get_mut(&path).unwrap())
    }

    fn write_location(&mut self, mmsi: &str, location: &VesselLocation) -> Result<()> {
        let date = Utc
            .timestamp_opt(location.time as i64, 0)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
        let writer = self.get_or_create_location_writer(mmsi, &date)?;
        writeln!(writer, "{}", serde_json::to_string(location)?)?;
        Ok(())
    }

    fn write_metadata(&mut self, mmsi: &str, metadata: &VesselMetadata) -> Result<()> {
        let date = Utc
            .timestamp_millis_opt(metadata.timestamp as i64)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
        let writer = self.get_or_create_metadata_writer(mmsi, &date)?;
        writeln!(writer, "{}", serde_json::to_string(metadata)?)?;
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

    let file_writers = Arc::new(Mutex::new(FileWriters::new()));

    while let Some((topic, payload)) = rx.recv().await {
        let writers = Arc::clone(&file_writers);

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
    writers: Arc<Mutex<FileWriters>>,
) -> Result<()> {
    let parts: Vec<&str> = topic.split('/').collect();

    // Validate topic structure
    if parts.len() < 3 || parts[0] != "vessels-v2" {
        return Ok(());
    }

    let mmsi = parts[1];
    let message_type = parts[2];

    match message_type {
        "location" => {
            let location: VesselLocation = serde_json::from_slice(&payload)?;
            let mut file_writers = writers.lock().unwrap();
            file_writers.write_location(mmsi, &location)?;
        }
        "metadata" => {
            let metadata: VesselMetadata = serde_json::from_slice(&payload)?;
            let mut file_writers = writers.lock().unwrap();
            file_writers.write_metadata(mmsi, &metadata)?;
        }
        _ => {}
    }

    Ok(())
}
