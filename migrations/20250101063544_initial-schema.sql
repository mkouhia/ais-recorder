-- Create tables for AIS data
CREATE TABLE locations (
    mmsi INTEGER NOT NULL, -- Raw MMSI (max 9 digits)
    time TIMESTAMPTZ NOT NULL, -- Note: microseconds (original data has time/seconds)
    sog REAL, -- Speed over ground in knots
    cog REAL, -- Course over ground in degrees
    nav_stat SMALLINT, -- Raw navigation status (0-15)
    rot SMALLINT, -- Raw ROT value (-128 to 127)
    pos_acc BOOLEAN NOT NULL, -- Position accuracy flag
    raim BOOLEAN NOT NULL, -- RAIM flag
    heading SMALLINT, -- Raw heading (0-359, 511)
    lon DOUBLE PRECISION NOT NULL, -- Longitude in decimal degrees
    lat DOUBLE PRECISION NOT NULL, -- Latitude in decimal degrees
    PRIMARY KEY (mmsi, time)
);

CREATE TABLE metadata (
    mmsi INTEGER NOT NULL, -- Raw MMSI
    time TIMESTAMPTZ NOT NULL, --  Unix timestamp in milliseconds
    name VARCHAR(20), -- Vessel name (max 20 chars)
    destination VARCHAR(20), -- Destination (max 20 chars)
    vessel_type SMALLINT, -- Raw vessel type
    call_sign VARCHAR(7), -- Call sign (max 7 chars)
    imo INTEGER, -- IMO number (7 digits)
    draught SMALLINT, -- Raw draught value (0-255)
    eta INTEGER, -- Raw ETA bits (20-bit packed format)
    pos_type SMALLINT, -- Raw position fixing device type
    ref_a SMALLINT, -- Raw dimension A
    ref_b SMALLINT, -- Raw dimension B
    ref_c SMALLINT, -- Raw dimension C
    ref_d SMALLINT, -- Raw dimension D
    PRIMARY KEY (mmsi, time)
);

-- Convert tables to hypertables
SELECT
    create_hypertable ('locations', by_range ('time', INTERVAL '1 day'));

SELECT
    create_hypertable ('metadata', by_range ('time', INTERVAL '1 day'));

-- Enable compression
ALTER TABLE locations
SET
    (
        timescaledb.compress,
        timescaledb.compress_orderby = 'time DESC',
        timescaledb.compress_segmentby = 'mmsi'
    );

ALTER TABLE metadata
SET
    (
        timescaledb.compress,
        timescaledb.compress_orderby = 'time DESC',
        timescaledb.compress_segmentby = 'mmsi'
    );

-- Enable compression with appropriate policies
SELECT
    add_compression_policy ('locations', INTERVAL '7 days');

SELECT
    add_compression_policy ('metadata', INTERVAL '7 days');

-- Create indices
CREATE INDEX idx_loca_mmsi_ts ON locations (mmsi, time DESC);

CREATE INDEX idx_meta_mmsi_ts ON metadata (mmsi, time DESC);
