-- Create subscribers_restaurants table (source metadata)
CREATE TABLE IF NOT EXISTS subscribers_restaurants (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    restaurant_id VARCHAR(255) NOT NULL
);

-- Create subscribers_feedback table (sink table)
CREATE TABLE IF NOT EXISTS subscribers_feedback (
    id SERIAL PRIMARY KEY,
    restaurant_id VARCHAR(255),
    adv_campaign_id VARCHAR(255),
    adv_campaign_content TEXT,
    adv_campaign_owner VARCHAR(255),
    adv_campaign_owner_contact VARCHAR(255),
    adv_campaign_datetime_start BIGINT,
    adv_campaign_datetime_end BIGINT,
    datetime_created BIGINT,
    client_id VARCHAR(255),
    trigger_datetime_created TIMESTAMP,
    feedback TEXT
);

-- Populate initial subscriber metadata for testing join
INSERT INTO subscribers_restaurants (client_id, restaurant_id) VALUES
('client_1', 'rest_1'),
('client_2', 'rest_1'),
('client_3', 'rest_2'),
('client_4', 'rest_3'),
('client_5', 'rest_1')
ON CONFLICT DO NOTHING;

-- Create student role and grant permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'student') THEN
        CREATE ROLE student WITH LOGIN PASSWORD 'de-student';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE de TO student;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO student;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO student;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO student;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO student;
