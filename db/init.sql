CREATE TABLE sensor_data (
    id BIGSERIAL PRIMARY KEY,

    device_id VARCHAR(50) NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,

    co DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    light BOOLEAN,
    smoke DOUBLE PRECISION,
    temperature DOUBLE PRECISION,

    received_at TIMESTAMPTZ DEFAULT NOW()


);

CREATE INDEX idx_device_time
ON sensor_data (device_id, timestamp);