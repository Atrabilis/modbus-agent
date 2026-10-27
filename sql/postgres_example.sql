-- Example target table for a `postgres` output: one column per register.
-- The writer inserts by column name; registers or tags without a matching
-- column are skipped with a warning, so the table decides what is stored.
-- The agent does not create tables.
--
-- Required: ts, device_name, slave_name, slave_id, and a unique key on
-- (ts, device_name, slave_name), or (ts, device_name, slave_name, series_key)
-- when the table has a series_key column. Other tag columns are optional
-- (plant, ip_address, ...). Register columns use the sanitized register name.

CREATE TABLE IF NOT EXISTS public.inverter_example (
  ts          TIMESTAMPTZ NOT NULL,
  device_name TEXT        NOT NULL,
  slave_name  TEXT        NOT NULL,
  slave_id    INTEGER     NOT NULL,
  plant       TEXT,
  ac_voltage  DOUBLE PRECISION,
  temperature DOUBLE PRECISION,
  energy      DOUBLE PRECISION,
  model       TEXT,
  PRIMARY KEY (ts, device_name, slave_name)
);
