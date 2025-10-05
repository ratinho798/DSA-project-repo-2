-- Create database
CREATE DATABASE smart_ticketing_system;
\c smart_ticketing_system;


CREATE TABLE notifications (
    notification_id SERIAL PRIMARY KEY,
    message JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'sent',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
