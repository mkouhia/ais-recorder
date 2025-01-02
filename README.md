# AIS logger

## Configuration

The application can be configured through:
1. Environment variables
2. Configuration files (`config/default.toml` and `config.toml`)
3. `.env` file (development only)

### Development Setup

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Modify `.env` with your local settings

### Production Setup

In production, use environment variables or a `config.toml` file:

```toml
database_url = "postgres://user:pass@host/db"

[digitraffic_marine]
uri = "mqtt://broker:1883"
id = "ais-recorder-prod"
topics = ["vessels-v2/+/location", "vessels-v2/+/metadata"]
```

Environment variables take precedence over the config file. Environment
variables are in uppercase and prefixed with `AIS_RECORDER__`.

## Other environment variables

- `RUST_LOG` determines log level.
  - Use `RUST_LOG='ais_recorder=trace'` for more verbose output

## Database

Use the `sqlx-cli` to manage the database schema.

    $ cargo install sqlx-cli --no-default-features --features rustls,postgres

Provide the database URL in the `DATABASE_URL` environment variable, or in `.env` file.

    export DATABASE_URL=postgres://username:password@localhost/ais-recorder

Create the database schema:

      sqlx database create
      sqlx migrate run

Create and run migrations:

    sqlx migrate add <name>>
    sqlx migrate run

## Development environment

### PostgreSQL

1. Run the TimescaleDB Docker image

        docker-compose up -d

2. Connect to a database using the `psql` executable in the Docker container:

        docker exec -it db psql -U ais-recorder
