// src/database/types.rs
#[derive(Debug, sqlx::Type)]
#[sqlx(transparent)]
struct RawEta(i32);

#[derive(Debug, sqlx::Type)]
#[sqlx(transparent)]
struct RawRot(i16);
