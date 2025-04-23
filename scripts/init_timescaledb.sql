-- Create weather_data table
CREATE TABLE IF NOT EXISTS weather_data (
    timestamp       TIMESTAMPTZ NOT NULL,
    city            TEXT,
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    wind_speed      DOUBLE PRECISION,
    PRIMARY KEY (timestamp, city)
);
SELECT create_hypertable('weather_data', 'timestamp', if_not_exists => TRUE);

-- Create caiso_load table
CREATE TABLE IF NOT EXISTS caiso_load (
    timestamp       TIMESTAMPTZ NOT NULL,
    region          TEXT,
    load_mw         DOUBLE PRECISION,
    PRIMARY KEY (timestamp, region)
);
SELECT create_hypertable('caiso_load', 'timestamp', if_not_exists => TRUE);

