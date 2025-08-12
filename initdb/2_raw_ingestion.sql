CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.predictions (
    id SERIAL PRIMARY KEY,
    route_id VARCHAR(10) NOT NULL,
    stop_id VARCHAR(25) NOT NULL,
    predicted_minutes INTEGER NOT NULL,
    predicted_time TIMESTAMP NOT NULL,
    poll_time TIMESTAMP NOT NULL,
    FOREIGN KEY (route_id) REFERENCES core.routes(id) ON DELETE CASCADE,
    FOREIGN KEY (stop_id) REFERENCES core.stops(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS raw.locations (
    id SERIAL PRIMARY KEY,
    route_id VARCHAR(10) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    poll_time TIMESTAMP NOT NULL,
    FOREIGN KEY (route_id) REFERENCES core.routes(id) ON DELETE CASCADE
);