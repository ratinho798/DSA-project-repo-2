CREATE DATABASE smart_ticketing_system;
\c smart_ticketing_system;


CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    ticket_id INT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    method VARCHAR(50) NOT NULL,            
    status VARCHAR(20) DEFAULT 'Processed', 
    transaction_ref VARCHAR(100) UNIQUE,    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

