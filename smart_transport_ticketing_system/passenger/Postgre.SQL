CREATE DATABASE smart_ticketing_system;
\c smart_ticketing_system;


CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE tickets (
    ticket_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id) ON DELETE CASCADE,
    route VARCHAR(100) NOT NULL,
    trip_date DATE NOT NULL,
    seat_number VARCHAR(10),
    price DECIMAL(10, 2) NOT NULL,
    payment_status VARCHAR(20) DEFAULT 'Pending',
    booking_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



