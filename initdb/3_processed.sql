CREATE SCHEMA IF NOT EXISTS processed;

CREATE TABLE IF NOT EXISTS processed.arrivals (
    id SERIAL PRIMARY KEY,
    route_id VARCHAR(10) NOT NULL,
    stop_id VARCHAR(25) NOT NULL,
    arrival_time TIMESTAMP NOT NULL,
    processed_date TIMESTAMP NOT NULL,
    FOREIGN KEY (route_id) REFERENCES core.routes(id) ON DELETE CASCADE,
    FOREIGN KEY (stop_id) REFERENCES core.stops(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS processed.predictions (
    id SERIAL PRIMARY KEY,
    arrival_id INTEGER NOT NULL,
    route_id VARCHAR(10) NOT NULL,
    stop_id VARCHAR(25) NOT NULL,
    predicted_minutes INTEGER NOT NULL,
    predicted_time TIMESTAMP NOT NULL,
    poll_time TIMESTAMP NOT NULL,
    FOREIGN KEY (route_id) REFERENCES core.routes(id) ON DELETE CASCADE,
    FOREIGN KEY (stop_id) REFERENCES core.stops(id) ON DELETE CASCADE,
    FOREIGN KEY (arrival_id) REFERENCES processed.arrivals(id) ON DELETE CASCADE
);
