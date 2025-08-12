-- migrations/003_add_advanced_statcast_metrics.sql
-- Add all missing advanced Statcast columns to games table

-- Expected stats (critical for betting analysis)
ALTER TABLE games ADD COLUMN IF NOT EXISTS estimated_ba_using_speedangle REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS estimated_woba_using_speedangle REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS estimated_slg_using_speedangle REAL;

-- Quality of contact metrics
ALTER TABLE games ADD COLUMN IF NOT EXISTS launch_speed_angle SMALLINT; -- Barrel classification (1-8)
ALTER TABLE games ADD COLUMN IF NOT EXISTS babip_value REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS iso_value REAL;

-- Advanced pitch data
ALTER TABLE games ADD COLUMN IF NOT EXISTS release_spin_rate REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS effective_speed REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS release_extension REAL;

-- Pitch movement
ALTER TABLE games ADD COLUMN IF NOT EXISTS pfx_x REAL; -- Horizontal movement
ALTER TABLE games ADD COLUMN IF NOT EXISTS pfx_z REAL; -- Vertical movement

-- Hit location
ALTER TABLE games ADD COLUMN IF NOT EXISTS hc_x REAL; -- Hit coordinate X
ALTER TABLE games ADD COLUMN IF NOT EXISTS hc_y REAL; -- Hit coordinate Y

-- Release point data
ALTER TABLE games ADD COLUMN IF NOT EXISTS release_pos_x REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS release_pos_z REAL;

-- Velocity components
ALTER TABLE games ADD COLUMN IF NOT EXISTS vx0 REAL; -- Initial velocity X
ALTER TABLE games ADD COLUMN IF NOT EXISTS vy0 REAL; -- Initial velocity Y
ALTER TABLE games ADD COLUMN IF NOT EXISTS vz0 REAL; -- Initial velocity Z

-- Acceleration components
ALTER TABLE games ADD COLUMN IF NOT EXISTS ax REAL; -- Acceleration X
ALTER TABLE games ADD COLUMN IF NOT EXISTS ay REAL; -- Acceleration Y
ALTER TABLE games ADD COLUMN IF NOT EXISTS az REAL; -- Acceleration Z

-- Create indexes on new advanced metrics for faster analysis
CREATE INDEX IF NOT EXISTS idx_games_xba ON games(estimated_ba_using_speedangle) WHERE estimated_ba_using_speedangle IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_games_barrels ON games(launch_speed_angle) WHERE launch_speed_angle = 6;
CREATE INDEX IF NOT EXISTS idx_games_spin_rate ON games(release_spin_rate) WHERE release_spin_rate IS NOT NULL;

SELECT 'Advanced Statcast columns added to games table' AS status;