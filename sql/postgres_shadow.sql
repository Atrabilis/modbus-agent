-- Target table for a `postgres_shadow` output: one row per plant, device,
-- slave and timestamp, with every register in a JSONB payload. Any device
-- fits without changing the table; use one table per device type or one for
-- everything. The agent does not create tables.
--
-- Replace edge.example_shadow with the schema and table of the output. The
-- primary key is the ON CONFLICT target of the writer (a key without
-- series_key is also accepted, for older tables).
-- Same layout as ansible-infra playbooks/ops/ensure_timescale_shadow_table.yml.

CREATE SCHEMA IF NOT EXISTS edge;

CREATE TABLE IF NOT EXISTS edge.example_shadow (
  plant       TEXT        NOT NULL,
  ts          TIMESTAMPTZ NOT NULL,
  device_name TEXT        NOT NULL,
  slave_name  TEXT        NOT NULL,
  payload     JSONB       NOT NULL,
  series_key  TEXT        NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (plant, device_name, slave_name, ts, series_key)
);

-- Reading a register back as a number, NaN and infinity included:
--   SELECT ts, (payload->'fields'->>'ac_voltage')::double precision
--   FROM edge.example_shadow WHERE device_name = 'inverter_block';
