use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use log::error;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;
use tokio::task;

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

    fn write_raw_message(&mut self, mmsi: &str, message_type: &str, payload: &[u8]) -> Result<()> {
        let date = Utc::now().format("%Y-%m-%d").to_string();

        let writer = match message_type {
            "location" => self.get_or_create_location_writer(mmsi, &date)?,
            "metadata" => self.get_or_create_metadata_writer(mmsi, &date)?,
            _ => return Ok(()),
        };

        // Write raw payload as a single line, converting UTF-8 bytes
        writeln!(writer, "{}", String::from_utf8_lossy(payload))?;
        writer.flush()?;
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

    let mut file_writers = writers.lock().unwrap();
    file_writers.write_raw_message(mmsi, message_type, &payload)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_file_writers_basic_functionality() -> Result<()> {
        let temp_dir = tempdir()?;
        std::env::set_current_dir(&temp_dir)?;

        let mut file_writers = FileWriters::new();

        // Test location writer
        let location_payload = r#"{"time":1668075025,"sog":10.7,"cog":326.6}"#.as_bytes();
        file_writers.write_raw_message("123456", "location", location_payload)?;

        // Test metadata writer
        let metadata_payload = r#"{"timestamp":1668075026035,"name":"ARUNA CIHAN"}"#.as_bytes();
        file_writers.write_raw_message("123456", "metadata", metadata_payload)?;

        // Verify location file created
        let location_path = "data/location/2024-02-20/123456.ndjson";
        assert!(Path::new(location_path).exists());

        // Verify metadata file created
        let metadata_path = "data/metadata/2024-02-20/123456.ndjson";
        assert!(Path::new(metadata_path).exists());

        Ok(())
    }

    #[test]
    fn test_raw_message_writing() -> Result<()> {
        let temp_dir = tempdir()?;
        std::env::set_current_dir(&temp_dir)?;

        let mut file_writers = FileWriters::new();

        let test_cases = vec![
            ("123456", "location", r#"{"lat":60.03802,"lon":20.345818}"#),
            (
                "789012",
                "metadata",
                r#"{"imo":9543756,"callSign":"V7WW7"}"#,
            ),
        ];

        for (mmsi, message_type, payload) in test_cases.iter() {
            file_writers.write_raw_message(mmsi, message_type, payload.as_bytes())?;
        }

        // Check files exist and content is correct
        for (mmsi, message_type, payload) in test_cases.iter() {
            let path = format!(
                "data/{}/{}/2024-02-20/{}.ndjson",
                message_type,
                Utc::now().format("%Y-%m-%d"),
                mmsi
            );

            let content = fs::read_to_string(&path)?;
            assert!(content.contains(payload));
        }

        Ok(())
    }

    #[test]
    fn test_topic_parsing() {
        // Valid topics
        let valid_topics = vec![
            ("vessels-v2/123456/location", ("123456", "location")),
            ("vessels-v2/789012/metadata", ("789012", "metadata")),
        ];

        for (topic, expected) in valid_topics {
            let parts: Vec<&str> = topic.split('/').collect();
            assert_eq!(parts[1], expected.0);
            assert_eq!(parts[2], expected.1);
        }

        // Invalid topics should not panic
        let invalid_topics = vec!["vessels/123456/location", "random/topic", "vessels-v2"];

        for topic in invalid_topics {
            let parts: Vec<&str> = topic.split('/').collect();
            assert!(parts.len() < 3 || parts[0] != "vessels-v2");
        }
    }
}
