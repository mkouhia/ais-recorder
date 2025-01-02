use chrono::{Timelike, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::env;

use ais_recorder::{
    database::Database,
    models::{AisMessage, AisMessageType, Mmsi, VesselLocation, VesselMetadata},
};

async fn setup_test_db() -> Result<(PgPool, Database), sqlx::Error> {
    dotenvy::dotenv().unwrap();
    let database_url =
        env::var("DATABASE_URL").expect("Environment variable DATABASE_URL required");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    let db = Database::new(pool.clone());
    db.run_migrations().await.expect("Failed to run migrations");

    Ok((pool, db))
}

#[sqlx::test]
async fn test_insert_location() {
    let (pool, db) = setup_test_db().await.unwrap();

    let time = Utc::now().with_nanosecond(0).unwrap();
    let location = VesselLocation {
        time,
        sog: Some(10.5),
        cog: Some(123.4),
        nav_stat: Some(0),
        rot: Some(34i8),
        pos_acc: true,
        raim: false,
        heading: Some(125),
        lon: 24.945831,
        lat: 60.192059,
    };

    let mmsi = Mmsi::try_from(230_123_456).unwrap();
    let message = AisMessage::new(mmsi, AisMessageType::Location(location.clone()));

    db.process_message(message)
        .await
        .expect("Failed to insert location");

    // Verify the insertion
    let stored: (f64, f64) =
        sqlx::query_as("SELECT lon, lat FROM locations WHERE mmsi = $1 AND time = $2")
            .bind(mmsi.value() as i32)
            .bind(time)
            .fetch_one(&pool)
            .await
            .expect("Failed to retrieve location");

    assert_eq!(stored.0, location.lon);
    assert_eq!(stored.1, location.lat);
}

#[sqlx::test]
async fn test_insert_metadata() {
    let (pool, db) = setup_test_db().await.unwrap();

    let time = Utc::now().with_nanosecond(0).unwrap();
    let metadata = VesselMetadata {
        time,
        name: Some("TEST VESSEL".to_string()),
        destination: Some("TEST PORT".to_string()),
        vessel_type: Some(70),
        call_sign: Some("ABC123".to_string()),
        imo: Some(1234567),
        draught: Some(45),
        eta: 733376,
        pos_type: Some(1),
        ref_a: Some(10),
        ref_b: Some(20),
        ref_c: Some(5),
        ref_d: Some(5),
    };

    let mmsi = Mmsi::try_from(230_123_456).unwrap();
    let message = AisMessage::new(mmsi, AisMessageType::Metadata(metadata.clone()));

    db.process_message(message)
        .await
        .expect("Failed to insert metadata");

    // Verify the insertion
    let stored: (String, String) =
        sqlx::query_as("SELECT name, destination FROM metadata WHERE mmsi = $1 AND time = $2")
            .bind(mmsi.value() as i32)
            .bind(time)
            .fetch_one(&pool)
            .await
            .expect("Failed to retrieve metadata");

    assert_eq!(stored.0, metadata.name.unwrap());
    assert_eq!(stored.1, metadata.destination.unwrap());
}
