use chrono::{Timelike, Utc};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::env;

use ais_recorder::{
    database::Database,
    models::{AisMessage, AisMessageType, Eta, Mmsi, VesselLocation, VesselMetadata},
};

async fn setup_test_db() -> Pool<Postgres> {
    dotenvy::dotenv().unwrap();
    let database_url =
        env::var("DATABASE_URL").expect("Environment variable DATABASE_URL required");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    // Run migrations
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    pool
}

#[sqlx::test]
async fn test_insert_location() {
    let pool = setup_test_db().await;
    let db = Database::new(pool.clone()).await.unwrap();

    let time_utc = Utc::now().with_nanosecond(0).unwrap();
    let time = time_utc.timestamp() as u64;
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
            .bind(time_utc)
            .fetch_one(&pool)
            .await
            .expect("Failed to retrieve location");

    assert_eq!(stored.0, location.lon);
    assert_eq!(stored.1, location.lat);
}

#[sqlx::test]
async fn test_insert_metadata() {
    let pool = setup_test_db().await;
    let db = Database::new(pool.clone()).await.unwrap();

    let time_utc = Utc::now().with_nanosecond(0).unwrap();
    let timestamp = time_utc.timestamp_millis() as u64;
    let metadata = VesselMetadata {
        timestamp,
        name: Some("TEST VESSEL".to_string()),
        destination: Some("TEST PORT".to_string()),
        vessel_type: Some(70),
        call_sign: Some("ABC123".to_string()),
        imo: Some(1234567),
        draught: Some(4.5),
        eta: Eta {
            month: Some(12),
            day: Some(25),
            hour: Some(14),
            minute: Some(30),
        },
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
            .bind(time_utc)
            .fetch_one(&pool)
            .await
            .expect("Failed to retrieve metadata");

    assert_eq!(stored.0, metadata.name.unwrap());
    assert_eq!(stored.1, metadata.destination.unwrap());
}

#[ignore]
#[sqlx::test]
async fn test_rot_conversion() {
    // let pool = setup_test_db().await;
    // let db = Database::new(pool);

    // let location = VesselLocation {
    //     timestamp: Utc::now(),
    //     rot: Some(-720.0), // Should be stored as -127
    //                        // ... other fields
    // };

    // let mmsi = Mmsi::try_from(230123456).unwrap();
    // db.insert_location(mmsi, &location).await.unwrap();

    // // Verify raw storage
    // let raw_rot: i16 = sqlx::query_scalar("SELECT rot FROM locations WHERE mmsi = $1")
    //     .bind(mmsi.value() as i32)
    //     .fetch_one(&db.pool)
    //     .await
    //     .unwrap();

    // assert_eq!(raw_rot, -127);

    // // Verify conversion back
    // let stored_location = db
    //     .get_vessel_location(mmsi, location.timestamp)
    //     .await
    //     .unwrap()
    //     .unwrap();

    // assert!((stored_location.rot.unwrap() - -720.0).abs() < 0.1);
}

#[ignore]
#[sqlx::test]
async fn test_eta_conversion() {
    // let pool = setup_test_db().await;
    // let db = Database::new(pool);

    // let eta = Eta {
    //     month: Some(12),
    //     day: Some(25),
    //     hour: Some(14),
    //     minute: Some(30),
    // };
    // let raw_bits = eta.to_bits();

    // // ... test storage and retrieval of raw bits
}
