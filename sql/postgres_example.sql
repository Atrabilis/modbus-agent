-- Example target table for a `postgres` output: one column per register.
-- The writer inserts by column name; registers without a matching column are
-- skipped with a warning, so the table decides which registers are stored.
-- The agent does not create tables.
--
-- Columns the writer fills:
--   ts, device_name, slave_name, slave_id   always (required)
--   series_key, flags                        when the table has them: flags is
--                                            a JSONB object with the other tags
--                                            (plant, register flags, ...) and
--                                            series_key a hash of them
--   <register name>                          one per register, sanitized name
-- Tags are never separate columns: a `plant` column would stay NULL; read it
-- from flags->>'plant'.
--
-- The ON CONFLICT target is (ts, device_name, slave_name), plus series_key when
-- the table has that column, so the table needs a unique key on those columns.

CREATE TABLE IF NOT EXISTS public.inverter_example (
  ts          TIMESTAMPTZ NOT NULL,
  device_name TEXT        NOT NULL,
  slave_name  TEXT        NOT NULL,
  slave_id    INTEGER     NOT NULL,
  series_key  TEXT        NOT NULL,
  flags       JSONB,
  ac_voltage  DOUBLE PRECISION,
  temperature DOUBLE PRECISION,
  energy      DOUBLE PRECISION,
  model       TEXT,
  PRIMARY KEY (ts, device_name, slave_name, series_key)
);
