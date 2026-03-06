CREATE TABLE IF NOT EXISTS torii_introspect_schema_state (
    table_id BYTEA PRIMARY KEY,
    create_table_json TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
