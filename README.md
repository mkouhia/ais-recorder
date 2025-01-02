# AIS logger

## Environment variables

- `RUST_LOG` determines log level.
  - Use `RUST_LOG='ais_recorder=trace'` for more verbose output

## Database

    # only for postgres
    $ cargo install sqlx-cli --no-default-features --features postgres

Provide the database URL in the `DATABASE_URL` environment variable.

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
