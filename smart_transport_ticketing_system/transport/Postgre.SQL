CREATE DATABASE smart_ticketing_system;
\c smart_ticketing_system;


CREATE TABLE routes (
    route_id SERIAL PRIMARY KEY,
    route_name VARCHAR(100) NOT NULL,    
    origin VARCHAR(100) NOT NULL,
    destination VARCHAR(100) NOT NULL,
    distance_km DECIMAL(6,2),
    estimated_time INTERVAL,               
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE schedules (
    schedule_id SERIAL PRIMARY KEY,
    route_id INT NOT NULL REFERENCES routes(route_id) ON DELETE CASCADE,
    departure_time TIMESTAMP NOT NULL,
    arrival_time TIMESTAMP NOT NULL,
    driver_name VARCHAR(100),
    bus_number VARCHAR(50),
    status VARCHAR(20) DEFAULT 'Scheduled', 
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

