-- Correr esto UNA sola vez contra el proyecto de Supabase (SQL Editor del
-- dashboard) antes del primer run del workflow.

CREATE TABLE IF NOT EXISTS vehicle_positions (
  vehicle_id TEXT NOT NULL,
  slot INTEGER NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  ts BIGINT NOT NULL,
  interno TEXT,
  PRIMARY KEY (vehicle_id, slot)
);
