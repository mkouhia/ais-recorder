//! Data models.

use serde::{Deserialize, Serialize};

/// Vessel location
///
/// See: https://meri.digitraffic.fi/swagger/#/AIS%20V1/vesselLocationsByMssiAndTimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct VesselLocation {
    /// Location record timestamp in milliseconds from Unix epoch.
    pub time: u64,
    /// Speed over ground in knots, 102.3 = not available
    pub sog: f32,
    /// Course over ground in degrees, 360 = not available (default)
    pub cog: f32,
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
    pub nav_stat: u8,
    /// Rate of turn, ROT[AIS].
    ///
    /// Values range between -128 - 127. –128 indicates that value is not
    /// available (default). Coded by ROT[AIS] = 4.733 SQRT(ROT[IND]) where
    /// ROT[IND] is the Rate of Turn degrees per minute, as indicated by
    /// an external sensor.
    /// - +127 = turning right at 720 degrees per minute or higher
    /// - -127 = turning left at 720 degrees per minute or higher.
    pub rot: i32,
    /// Position accuracy, 1 = high, 0 = low
    #[serde(rename = "posAcc")]
    pub pos_acc: bool,
    /// Receiver autonomous integrity monitoring (RAIM) flag of electronic position fixing device
    pub raim: bool,
    /// Degrees (0-359), 511 = not available (default)
    pub heading: u16,
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
    /// Name of the vessel, maximum 20 characters using 6-bit ASCII
    pub name: String,
    /// Record timestamp in milliseconds from Unix epoch
    pub timestamp: u64,
    /// Destination, maximum 20 characters using 6-bit ASCII
    pub destination: String,
    /// Vessel's AIS ship type
    #[serde(rename = "type")]
    pub vessel_type: u8,
    /// Call sign, maximum 7 6-bit ASCII characters
    #[serde(rename = "callSign")]
    pub call_sign: String,
    /// Vessel International Maritime Organization (IMO) number
    pub imo: u32,
    /// Maximum present static draught in 1/10m
    ///
    /// 255 = draught 25.5 m or greater, 0 = not available (default)
    pub draught: u8,
    /// Estimated time of arrival; MMDDHHMM UTC
    ///
    /// - Bits 19-16: month; 1-12; 0 = not available = default
    /// - Bits 15-11: day; 1-31; 0 = not available = default
    /// - Bits 10-6: hour; 0-23; 24 = not available = default
    /// - Bits 5-0: minute; 0-59; 60 = not available = default
    ///
    /// For SAR aircraft, the use of this field may be decided by the
    /// responsible administration.
    pub eta: u32,
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
    pub pos_type: u8,
    /// Reference point for reported position dimension A
    #[serde(rename = "refA")]
    pub ref_a: u16,
    /// Reference point for reported position dimension B
    #[serde(rename = "refB")]
    pub ref_b: u16,
    /// Reference point for reported position dimension C
    #[serde(rename = "refC")]
    pub ref_c: u16,
    /// Reference point for reported position dimension D
    #[serde(rename = "refD")]
    pub ref_d: u16,
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
    pub mmsi: u32,
    /// Type of AIS message
    pub message_type: AisMessageType,
}

impl AisMessage {
    /// Create a new AIS message
    pub fn new(mmsi: u32, message_type: AisMessageType) -> Self {
        Self { mmsi, message_type }
    }
}

#[cfg(test)]
mod tests {
    use super::{VesselLocation, VesselMetadata};

    #[test]
    fn parse_location() {
        let s = r#"{
          "time" : 1734361116,
          "sog" : 0.0,
          "cog" : 229.6,
          "navStat" : 0,
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
            sog: 0.0,
            cog: 229.6,
            nav_stat: 0,
            rot: -128,
            pos_acc: false,
            raim: true,
            heading: 511,
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
            name: "SUULA".to_string(),
            timestamp: 1734363992454,
            destination: "SEPIT".to_string(),
            vessel_type: 80,
            call_sign: "LAUY8".to_string(),
            imo: 9267560,
            draught: 79,
            eta: 823872,
            pos_type: 3,
            ref_a: 111,
            ref_b: 29,
            ref_c: 14,
            ref_d: 8,
        };

        assert_eq!(loc, expected);
    }
}
