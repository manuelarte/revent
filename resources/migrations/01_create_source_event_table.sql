CREATE TABLE IF NOT EXISTS source_event (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    monotonic_clock BIGINT UNIQUE NOT NULL,
    aggregate VARCHAR(255) NOT NULL CHECK (length(aggregate) >= 3 AND length(aggregate) < 255),
    aggregate_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX idx_aggregate ON source_event (aggregate);
CREATE INDEX idx_aggregate_id ON source_event (aggregate_id);
CREATE INDEX idx_monotonic_clock ON source_event (monotonic_clock);
