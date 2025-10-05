
CREATE DATABASE smart_ticketing_system;
\c smart_ticketing_system;


CREATE TABLE tickets (
    ticket_id SERIAL PRIMARY KEY,
    passenger_name VARCHAR(100) NOT NULL,
    route VARCHAR(100) NOT NULL,
    trip_date DATE NOT NULL,
    seat_number VARCHAR(10),
    price DECIMAL(10, 2) NOT NULL,
    payment_status VARCHAR(20) DEFAULT 'Pending',
    booking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE service_updates (
    update_id SERIAL PRIMARY KEY,
    update_message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);




