//! MQTT client implementation

use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS, Transport};

use crate::{
    errors::AisLoggerError,
    models::{AisMessage, AisMessageType, VesselLocation, VesselMetadata},
};

/// MQTT client for receiving AIS data
pub struct MqttClientBuilder {
    client: AsyncClient,
    event_loop: EventLoop,
    tx: mpsc::Sender<Result<AisMessage, AisLoggerError>>,
    rx: mpsc::Receiver<Result<AisMessage, AisLoggerError>>,
}

pub struct MqttClient {
    _client: AsyncClient,
    rx: mpsc::Receiver<Result<AisMessage, AisLoggerError>>,
    _topics: Vec<String>,
    _handle: tokio::task::JoinHandle<Result<(), AisLoggerError>>,
}

impl MqttClientBuilder {
    /// Create a new MQTT client
    pub fn new(id: &str, host: &str) -> Result<Self, AisLoggerError> {
        let mut mqtt_options = MqttOptions::new(id, host, 443);

        mqtt_options.set_transport(Transport::wss_with_default_config());
        mqtt_options.set_keep_alive(Duration::from_secs(5));
        // mqtt_options.set_connection_timeout(Duration::from_secs(10));

        let (client, event_loop) = AsyncClient::new(mqtt_options, 100);

        // Create a channel for message passing
        let (tx, rx) = mpsc::channel(100);

        Ok(Self {
            client,
            event_loop,
            tx,
            rx,
        })
    }

    /// Connect to MQTT broker and subscribe to topics
    ///
    /// Note: Initial subscription needs not be done here, as it is done
    /// in the event loop.
    pub async fn connect(self, topics: &[String]) -> Result<MqttClient, AisLoggerError> {
        let topics = topics.to_vec();

        let _handle = tokio::spawn(Self::process_events(
            self.tx,
            self.event_loop,
            self.client.clone(), // Clone client for event loop
            topics.clone(),
        ));

        Ok(MqttClient {
            _client: self.client,
            rx: self.rx,
            _topics: topics,
            _handle,
        })
    }

    async fn subscribe(client: AsyncClient, topics: &[String]) -> Result<(), AisLoggerError> {
        for topic in topics.iter() {
            info!("Subscribing to topic: {}", topic);
            client.subscribe(topic, QoS::AtLeastOnce).await?;
        }
        Ok(())
    }

    /// Process MQTT events
    ///
    /// This function is responsible for handling incoming messages. The library
    /// `rumqttc` will automatically reconnect if connection is lost, but topic
    /// subscriptions need to be re-established. Therefore, also topic subscription
    /// is handled here.
    ///
    /// NOTE: If topic subscription fails, the loop will break and return an error.
    async fn process_events(
        tx: mpsc::Sender<Result<AisMessage, AisLoggerError>>,
        mut event_loop: EventLoop,
        client: AsyncClient,
        topics: Vec<String>,
    ) -> Result<(), AisLoggerError> {
        loop {
            match event_loop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("Connected to MQTT broker, subscribing to topics");
                    if let Err(e) = Self::subscribe(client.clone(), &topics).await {
                        error!("Failed to subscribe: {}", e);
                        break Err(e);
                    }
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    match Self::parse_message(&publish.topic, &publish.payload) {
                        Ok(message) => {
                            if let Err(e) = tx.send(Ok(message)).await {
                                error!("Failed to send message: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("MQTT Error: {}", e);
                    continue;
                }
                _ => continue,
            }
        }
    }

    /// Parse incoming message based on topic
    fn parse_message(topic: &str, payload: &[u8]) -> Result<AisMessage, AisLoggerError> {
        let parts: Vec<&str> = topic.split('/').collect();

        // Validate topic structure
        if parts.len() < 3 || parts[0] != "vessels-v2" {
            return Err(AisLoggerError::InvalidTopic(topic.to_string()));
        }

        let mmsi = parts[1].try_into()?;

        match parts[2] {
            "location" => {
                let location: VesselLocation = serde_json::from_slice(payload)?;
                Ok(AisMessage::new(mmsi, AisMessageType::Location(location)))
            }
            "metadata" => {
                let metadata: VesselMetadata = serde_json::from_slice(payload)?;
                Ok(AisMessage::new(mmsi, AisMessageType::Metadata(metadata)))
            }
            _ => Err(AisLoggerError::UnknownMessageType(parts[2].to_string())),
        }
    }
}

impl MqttClient {
    /// Receive next message
    pub async fn recv(&mut self) -> Result<Option<AisMessage>, AisLoggerError> {
        self.rx.recv().await.transpose()
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use crate::models::Mmsi;

    use super::*;

    #[test]
    fn parse_location_message() {
        let topic = "vessels-v2/123456/location";
        let payload = r#"{
            "time":1668075025,
            "sog":10.7,
            "cog":326.6,
            "navStat":0,
            "rot":0,
            "posAcc":true,
            "raim":false,
            "heading":325,
            "lon":20.345818,
            "lat":60.03802
        }"#
        .as_bytes();

        let message = MqttClientBuilder::parse_message(topic, payload).unwrap();

        let expected = AisMessage {
            mmsi: Mmsi::try_from(123456).unwrap(),
            message_type: AisMessageType::Location(VesselLocation {
                time: DateTime::from_timestamp(1668075025, 0).unwrap(),
                sog: Some(10.7),
                cog: Some(326.6),
                nav_stat: Some(0),
                rot: Some(0i8),
                pos_acc: true,
                raim: false,
                heading: Some(325),
                lon: 20.345818,
                lat: 60.03802,
            }),
        };

        assert_eq!(message, expected);
    }

    #[test]
    fn parse_metadata_message() {
        let topic = "vessels-v2/123456/metadata";
        let payload = r#"{
            "timestamp":1668075026035,
            "destination":"UST LUGA",
            "name":"ARUNA CIHAN",
            "draught":68,
            "eta":733376,
            "posType":15,
            "refA":160,
            "refB":33,
            "refC":20,
            "refD":12,
            "callSign":"V7WW7",
            "imo":9543756,
            "type":70
        }"#
        .as_bytes();

        let message = MqttClientBuilder::parse_message(topic, payload).unwrap();

        let expected = AisMessage {
            mmsi: Mmsi::try_from(123456).unwrap(),
            message_type: AisMessageType::Metadata(VesselMetadata {
                time: DateTime::from_timestamp_millis(1668075026035).unwrap(),
                destination: Some("UST LUGA".to_string()),
                name: Some("ARUNA CIHAN".to_string()),
                draught: Some(68),
                eta: 733376,
                pos_type: Some(15),
                ref_a: Some(160),
                ref_b: Some(33),
                ref_c: Some(20),
                ref_d: Some(12),
                call_sign: Some("V7WW7".to_string()),
                imo: Some(9543756),
                vessel_type: Some(70),
            }),
        };

        assert_eq!(message, expected);
    }
}
