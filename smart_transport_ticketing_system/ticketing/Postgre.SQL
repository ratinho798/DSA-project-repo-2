CREATE DATABASE smart_ticketing_system;
\c smart_ticketing_system;


CREATE TABLE tickets (
    ticket_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    origin VARCHAR(100) NOT NULL,         
    destination VARCHAR(100) NOT NULL,     
    travel_date DATE NOT NULL,
    seat_number VARCHAR(10),
    price DECIMAL(10, 2),
    status VARCHAR(20) DEFAULT 'Pending',  
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


