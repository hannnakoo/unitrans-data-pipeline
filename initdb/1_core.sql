CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.routes (
    id VARCHAR(10) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS core.stops (
    id VARCHAR(25) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    tag VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS core.route_stop_times (
    id SERIAL PRIMARY KEY,
    route_id VARCHAR(10) NOT NqULL,
    stop_id VARCHAR(25) NOT NULL,
    schedule_class VARCHAR(255) NOT NULL,
    service_class VARCHAR(255) NOT NULL,
    scheduled_time TIME NOT NULL,
    FOREIGN KEY (route_id) REFERENCES core.routes(id) ON DELETE CASCADE,
    FOREIGN KEY (stop_id) REFERENCES core.stops(id) ON DELETE CASCADE
);