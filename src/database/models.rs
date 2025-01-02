// src/database/models.rs
#[derive(Debug, sqlx::FromRow)]
struct LocationRow {
    mmsi: i32,
    timestamp: DateTime<Utc>,
    sog: Option<f32>,
    cog: Option<f32>,
    nav_stat: Option<i16>,
    rot: Option<i16>, // Store raw ROT value
    pos_acc: bool,
    raim: bool,
    heading: Option<i16>,
    lon: f64,
    lat: f64,
}

#[derive(Debug, sqlx::FromRow)]
struct MetadataRow {
    mmsi: i32,
    timestamp: DateTime<Utc>,
    name: Option<String>,
    destination: Option<String>,
    vessel_type: Option<i16>,
    call_sign: Option<String>,
    imo: Option<i32>,
    draught: Option<i16>, // Store raw draught value
    eta: Option<i32>,     // Store raw ETA bits
    pos_type: Option<i16>,
    ref_a: Option<i16>,
    ref_b: Option<i16>,
    ref_c: Option<i16>,
    ref_d: Option<i16>,
}
