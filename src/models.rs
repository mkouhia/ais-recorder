//! Data models.

use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize, Serializer};

use crate::errors::AisLoggerError;
use serde_helpers::*;

/// Maritime Mobile Service Identity (MMSI)
///
/// A unique nine-digit number for identifying vessels in AIS messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mmsi(u32);

impl TryFrom<u32> for Mmsi {
    type Error = AisLoggerError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value > 999_999_999 {
            return Err(AisLoggerError::InvalidMmsi(value.to_string()));
        }
        Ok(Self(value))
    }
}

impl TryFrom<&str> for Mmsi {
    type Error = AisLoggerError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed = value
            .parse::<u32>()
            .map_err(|_| AisLoggerError::InvalidMmsi(value.to_string()))?;
        Self::try_from(parsed)
    }
}

impl Mmsi {
    /// Get the raw MMSI value
    pub fn value(&self) -> u32 {
        self.0
    }
}

/// Vessel location
///
/// See: https://meri.digitraffic.fi/swagger/#/AIS%20V1/vesselLocationsByMssiAndTimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct VesselLocation {
    /// Location record timestamp in seconds from Unix epoch.
    pub time: u64,
    /// Speed over ground in knots, None if not available (=102.3)
    #[serde(deserialize_with = "deserialize_sog")]
    pub sog: Option<f32>,
    /// Course over ground in degrees, None if not available (360)
    #[serde(deserialize_with = "deserialize_cog")]
    pub cog: Option<f32>,
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
    /// - 15 = default -> None
    #[serde(rename = "navStat", deserialize_with = "deserialize_nav_stat")]
    pub nav_stat: Option<u8>,
    /// Rate of turn, degrees per minute. None if not available (=-128)
    ///
    /// Values range between -128 - 127. –128 indicates that value is not
    /// available (default). Coded by ROT[AIS] = 4.733 SQRT(ROT[IND]) where
    /// ROT[IND] is the Rate of Turn degrees per minute, as indicated by
    /// an external sensor.
    /// - +127 = turning right at 720 degrees per minute or higher
    /// - -127 = turning left at 720 degrees per minute or higher.
    #[serde(deserialize_with = "deserialize_rot")]
    pub rot: Option<i8>,
    /// Position accuracy, 1 = high, 0 = low
    #[serde(rename = "posAcc")]
    pub pos_acc: bool,
    /// Receiver autonomous integrity monitoring (RAIM) flag of electronic position fixing device
    pub raim: bool,
    /// Heading in Degrees (0-359), None if 511 = not available (default)
    #[serde(deserialize_with = "deserialize_heading")]
    pub heading: Option<u16>,
    /// Longitude in WGS84 format in decimal degrees:
    pub lon: f64,
    /// Latitude in WGS84 format in decimal degrees:
    pub lat: f64,
}

/// Vessel metadata
///
/// See: https://meri.digitraffic.fi/swagger/#/AIS%20V1/vesselMetadataByMssi
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct VesselMetadata {
    /// Name of the vessel, empty string if not available
    #[serde(deserialize_with = "deserialize_trimmed_string")]
    pub name: Option<String>,
    /// Record timestamp in milliseconds from Unix epoch
    pub timestamp: u64,
    /// Destination, empty string if not available
    #[serde(deserialize_with = "deserialize_trimmed_string")]
    pub destination: Option<String>,
    /// Vessel type, None if undefined (0)
    #[serde(rename = "type", deserialize_with = "deserialize_vessel_type")]
    pub vessel_type: Option<u8>,
    /// Call sign, empty string if not available
    #[serde(rename = "callSign", deserialize_with = "deserialize_trimmed_string")]
    pub call_sign: Option<String>,
    /// Vessel International Maritime Organization (IMO) number
    ///
    /// None if not available (0)
    #[serde(deserialize_with = "deserialize_imo")]
    pub imo: Option<u32>,
    /// Maximum present static draught in m, None if not available (0)
    #[serde(deserialize_with = "deserialize_draught")]
    pub draught: Option<f32>,
    /// Estimated time of arrival; MMDDHHMM UTC
    ///
    /// For SAR aircraft, the use of this field may be decided by the
    /// responsible administration.
    #[serde(deserialize_with = "deserialize_eta")]
    pub eta: Eta,
    /// Type of electronic position fixing device, None if undefined (0)
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
    #[serde(rename = "posType", deserialize_with = "deserialize_pos_type")]
    pub pos_type: Option<u8>,
    /// Reference point for reported position dimension A
    #[serde(rename = "refA", deserialize_with = "deserialize_ref_dim")]
    pub ref_a: Option<u16>,
    /// Reference point for reported position dimension B
    #[serde(rename = "refB", deserialize_with = "deserialize_ref_dim")]
    pub ref_b: Option<u16>,
    /// Reference point for reported position dimension C
    #[serde(rename = "refC", deserialize_with = "deserialize_ref_dim")]
    pub ref_c: Option<u16>,
    /// Reference point for reported position dimension D
    #[serde(rename = "refD", deserialize_with = "deserialize_ref_dim")]
    pub ref_d: Option<u16>,
}

/// Estimated time of arrival, in metadata message
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct Eta {
    pub month: Option<u8>,
    pub day: Option<u8>,
    pub hour: Option<u8>,
    pub minute: Option<u8>,
}

impl Eta {
    /// Convert ETA from 20-bit packed format
    ///
    /// - Bits 19-16: month; 1-12; 0 = not available = default
    /// - Bits 15-11: day; 1-31; 0 = not available = default
    /// - Bits 10-6: hour; 0-23; 24 = not available = default
    /// - Bits 5-0: minute; 0-59; 60 = not available = default
    pub(crate) fn from_bits(value: u32) -> Self {
        let month = (value >> 16 & 0xF) as u8;
        let day = (value >> 11 & 0x1F) as u8;
        let hour = (value >> 6 & 0x1F) as u8;
        let minute = (value & 0x3F) as u8;

        Eta {
            month: match month {
                0 | 13..=255 => None,
                m => Some(m),
            },
            day: match day {
                0 | 32..=255 => None,
                d => Some(d),
            },
            hour: match hour {
                24..=255 => None,
                h => Some(h),
            },
            minute: match minute {
                60..=255 => None,
                m => Some(m),
            },
        }
    }

    // Convert Eta fields to u32 representation
    pub(crate) fn to_bits(&self) -> u32 {
        let mut value: u32 = 0;

        if let Some(month) = self.month {
            value |= (month as u32) << 16;
        }
        if let Some(day) = self.day {
            value |= (day as u32) << 11;
        }
        if let Some(hour) = self.hour {
            value |= (hour as u32) << 6;
        }
        if let Some(minute) = self.minute {
            value |= minute as u32;
        }

        value
    }

    // Convert to DateTime<Utc> with reference timestamp
    #[allow(dead_code)]
    pub fn to_datetime(&self, reference: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // All fields must be present for a valid datetime
        let (month, day, hour, minute) = match (self.month, self.day, self.hour, self.minute) {
            (Some(m), Some(d), Some(h), Some(min)) => (m, d, h, min),
            _ => return None,
        };
        // Start with the reference year
        let year = reference.year();

        // Try to create a datetime with the current year
        let naive_dt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(year, month as u32, day as u32)?,
            chrono::NaiveTime::from_hms_opt(hour as u32, minute as u32, 0)?,
        );

        // Convert to UTC and check if it's before the reference time
        let mut result = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);
        if result < *reference {
            // If it's in the past, try next year
            let next_naive_dt = chrono::NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(year + 1, month as u32, day as u32)?,
                chrono::NaiveTime::from_hms_opt(hour as u32, minute as u32, 0)?,
            );
            result = DateTime::<Utc>::from_naive_utc_and_offset(next_naive_dt, Utc);
        }

        Some(result)
    }
}

// Serialize to u32 for storage
impl Serialize for Eta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.to_bits())
    }
}

/// Different AIS message types
#[derive(Debug, Clone, PartialEq)]
pub enum AisMessageType {
    Location(VesselLocation),
    Metadata(VesselMetadata),
}

/// Represents a complete AIS message with MMSI and type
#[derive(Debug, Clone, PartialEq)]
pub struct AisMessage {
    /// Maritime Mobile Service Identity
    pub mmsi: Mmsi,
    /// Type of AIS message
    pub message_type: AisMessageType,
}

impl AisMessage {
    /// Create a new AIS message
    pub fn new(mmsi: Mmsi, message_type: AisMessageType) -> Self {
        Self { mmsi, message_type }
    }
}

/// Custom deserializers
mod serde_helpers {
    use super::Eta;
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize_sog<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f32::deserialize(deserializer)?;
        Ok(if value == 102.3 { None } else { Some(value) })
    }

    pub fn deserialize_cog<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f32::deserialize(deserializer)?;
        Ok(if value == 360.0 { None } else { Some(value) })
    }

    pub fn deserialize_nav_stat<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        Ok(if value == 15 { None } else { Some(value) })
    }

    pub fn deserialize_rot<'de, D>(deserializer: D) -> Result<Option<i8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i8::deserialize(deserializer)?;
        Ok(if value == -128 { None } else { Some(value) })
    }

    pub fn deserialize_heading<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        Ok(if value == 511 { None } else { Some(value) })
    }

    pub fn deserialize_trimmed_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        let trimmed = s.trim();
        Ok(if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        })
    }

    pub fn deserialize_vessel_type<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        Ok(if value == 0 { None } else { Some(value) })
    }

    pub fn deserialize_imo<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Ok(if value == 0 { None } else { Some(value) })
    }

    pub fn deserialize_draught<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        Ok(if value == 0 {
            None
        } else {
            Some((value as f32) / 10f32)
        })
    }

    pub fn deserialize_eta<'de, D>(deserializer: D) -> Result<Eta, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Ok(Eta::from_bits(value))
    }

    pub fn deserialize_pos_type<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        Ok(if value == 0 { None } else { Some(value) })
    }

    pub fn deserialize_ref_dim<'de, D>(deserializer: D) -> Result<Option<u16>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        Ok(if value == 0 { None } else { Some(value) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Eta;
    use chrono::{TimeZone, Timelike};

    #[test]
    fn parse_location() {
        let s = r#"{
          "time" : 1734361116,
          "sog" : 0.0,
          "cog" : 229.6,
          "navStat" : 0,
          "rot" : -127,
          "posAcc" : false,
          "raim" : true,
          "heading" : 359,
          "lon" : 28.886522,
          "lat" : 61.866617
        }"#;
        let loc: VesselLocation = serde_json::from_str(s).unwrap();
        let expected = VesselLocation {
            time: 1734361116,
            sog: Some(0.0),
            cog: Some(229.6),
            nav_stat: Some(0),
            rot: Some(-127i8),
            pos_acc: false,
            raim: true,
            heading: Some(359u16),
            lon: 28.886522,
            lat: 61.866617,
        };

        assert_eq!(loc, expected);
    }

    #[test]
    fn parse_location_nones() {
        let s = r#"{
          "time" : 1734361116,
          "sog" : 102.3,
          "cog" : 360.0,
          "navStat" : 15,
          "rot" : -128,
          "posAcc" : false,
          "raim" : true,
          "heading" : 511,
          "lon" : 28.886522,
          "lat" : 61.866617
        }"#;
        let loc: VesselLocation = serde_json::from_str(s).unwrap();
        let expected = VesselLocation {
            time: 1734361116,
            sog: None,
            cog: None,
            nav_stat: None,
            rot: None,
            pos_acc: false,
            raim: true,
            heading: None,
            lon: 28.886522,
            lat: 61.866617,
        };

        assert_eq!(loc, expected);
    }

    #[test]
    fn parse_metadata() {
        let s = r#"{
            "timestamp" : 1734363992454,
            "destination" : "SEPIT",
            "name" : "SUULA",
            "draught" : 79,
            "eta" : 823872,
            "posType" : 3,
            "refA" : 111,
            "refB" : 29,
            "refC" : 14,
            "refD" : 8,
            "callSign" : "LAUY8",
            "imo" : 9267560,
            "type" : 80
        }"#;
        let loc: VesselMetadata = serde_json::from_str(s).unwrap();
        let expected = VesselMetadata {
            name: Some("SUULA".to_string()),
            timestamp: 1734363992454,
            destination: Some("SEPIT".to_string()),
            vessel_type: Some(80),
            call_sign: Some("LAUY8".to_string()),
            imo: Some(9267560),
            draught: Some(7.9),
            eta: Eta {
                month: Some(12),
                day: Some(18),
                hour: Some(9),
                minute: Some(0),
            },
            pos_type: Some(3),
            ref_a: Some(111),
            ref_b: Some(29),
            ref_c: Some(14),
            ref_d: Some(8),
        };

        assert_eq!(loc, expected);
    }

    #[test]
    fn test_eta_datetime_conversion() {
        let eta = Eta {
            month: Some(2),
            day: Some(25),
            hour: Some(14),
            minute: Some(30),
        };

        // Test with reference time in current year
        let reference = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let dt = eta.to_datetime(&reference).unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 2);
        assert_eq!(dt.day(), 25);
        assert_eq!(dt.hour(), 14);
        assert_eq!(dt.minute(), 30);

        // Test with reference month after the ETA (should roll to next year)
        let reference = Utc.with_ymd_and_hms(2024, 12, 26, 0, 0, 0).unwrap();
        let dt = eta.to_datetime(&reference).unwrap();
        assert_eq!(dt.year(), 2025);
        assert_eq!(dt.month(), 2);
        assert_eq!(dt.day(), 25);
    }
}
