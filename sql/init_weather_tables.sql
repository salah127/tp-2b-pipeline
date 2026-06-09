-- Création des tables PostgreSQL utilisées par le pipeline météo

CREATE TABLE IF NOT EXISTS weather_data (
    city TEXT NOT NULL,
    date DATE NOT NULL,
    max_temperature_c DOUBLE PRECISION,
    min_temperature_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    timezone TEXT,
    ingestion_ts TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (city, date)
);

CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    execution_date TIMESTAMPTZ NOT NULL,
    city_count INTEGER,
    row_count INTEGER,
    status TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
