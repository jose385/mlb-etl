-- migrations/003_add_advanced_statcast_metrics.sql
-- COMPREHENSIVE: Add ALL missing advanced Statcast columns for real pybaseball data
-- UPDATED: Covers complete gap between schema and real data structure

-- ============================================================================
-- PLAYER IDENTIFIERS & GAME CONTEXT
-- Real pybaseball data includes multiple ID systems and detailed context
-- ============================================================================

-- Additional player identifiers (pybaseball includes multiple ID systems)
ALTER TABLE games ADD COLUMN IF NOT EXISTS batter_name TEXT;
ALTER TABLE games ADD COLUMN IF NOT EXISTS pitcher_name TEXT;
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_2 INTEGER;  -- Catcher
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_3 INTEGER;  -- First base
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_4 INTEGER;  -- Second base
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_5 INTEGER;  -- Third base
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_6 INTEGER;  -- Shortstop
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_7 INTEGER;  -- Left field
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_8 INTEGER;  -- Center field
ALTER TABLE games ADD COLUMN IF NOT EXISTS fielder_9 INTEGER;  -- Right field

-- Game timing and sequence
ALTER TABLE games ADD COLUMN IF NOT EXISTS game_year INTEGER;
ALTER TABLE games ADD COLUMN IF NOT EXISTS game_type TEXT;     -- Regular, Playoff, etc.
ALTER TABLE games ADD COLUMN IF NOT EXISTS sv_id TEXT;        -- Savant ID
ALTER TABLE games ADD COLUMN IF NOT EXISTS pitch_name TEXT;   -- Four-Seam Fastball, etc.

-- ============================================================================
-- COMPREHENSIVE PITCH TRACKING DATA
-- Real Statcast includes extensive pitch physics and movement data
-- ============================================================================

-- Enhanced pitch classification
ALTER TABLE games ADD COLUMN IF NOT EXISTS pitch_type_confidence REAL; -- Confidence in pitch classification
ALTER TABLE games ADD COLUMN IF NOT EXISTS spin_dir REAL;              -- Spin direction in degrees
ALTER TABLE games ADD COLUMN IF NOT EXISTS spin_rate_deprecated REAL;  -- Legacy spin rate
ALTER TABLE games ADD COLUMN IF NOT EXISTS break_angle_deprecated REAL; -- Legacy break angle
ALTER TABLE games ADD COLUMN IF NOT EXISTS break_length_deprecated REAL; -- Legacy break length

-- Complete release point data
ALTER TABLE games ADD COLUMN IF NOT EXISTS release_pos_y REAL;  -- Release point Y coordinate

-- Strike zone dimensions (varies by batter)
ALTER TABLE games ADD COLUMN IF NOT EXISTS sz_top REAL;        -- Top of strike zone
ALTER TABLE games ADD COLUMN IF NOT EXISTS sz_bot REAL;        -- Bottom of strike zone

-- Advanced velocity components
ALTER TABLE games ADD COLUMN IF NOT EXISTS vx0 REAL;           -- Initial velocity X
ALTER TABLE games ADD COLUMN IF NOT EXISTS vy0 REAL;           -- Initial velocity Y  
ALTER TABLE games ADD COLUMN IF NOT EXISTS vz0 REAL;           -- Initial velocity Z

-- Acceleration components (gravity and air resistance effects)
ALTER TABLE games ADD COLUMN IF NOT EXISTS ax REAL;            -- Acceleration X
ALTER TABLE games ADD COLUMN IF NOT EXISTS ay REAL;            -- Acceleration Y
ALTER TABLE games ADD COLUMN IF NOT EXISTS az REAL;            -- Acceleration Z

-- ============================================================================
-- COMPREHENSIVE BATTED BALL DATA
-- Real data includes extensive hit tracking and quality metrics
-- ============================================================================

-- Hit location and spray angle
ALTER TABLE games ADD COLUMN IF NOT EXISTS hc_x REAL;          -- Hit coordinate X
ALTER TABLE games ADD COLUMN IF NOT EXISTS hc_y REAL;          -- Hit coordinate Y
ALTER TABLE games ADD COLUMN IF NOT EXISTS spray_angle REAL;   -- Spray angle in degrees
ALTER TABLE games ADD COLUMN IF NOT EXISTS hit_location INTEGER; -- Hit location code (1-9)

-- Ball flight and landing
ALTER TABLE games ADD COLUMN IF NOT EXISTS bb_type TEXT;       -- Batted ball type (fly_ball, line_drive, etc.)

-- ============================================================================
-- ENHANCED EXPECTED STATISTICS
-- Real pybaseball includes comprehensive expected performance metrics
-- ============================================================================

-- Expected stats (already exist, but ensuring they're present)
ALTER TABLE games ADD COLUMN IF NOT EXISTS estimated_ba_using_speedangle REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS estimated_woba_using_speedangle REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS estimated_slg_using_speedangle REAL;

-- Additional expected metrics
ALTER TABLE games ADD COLUMN IF NOT EXISTS xba REAL;           -- Alternative xBA calculation
ALTER TABLE games ADD COLUMN IF NOT EXISTS xslg REAL;          -- Alternative xSLG calculation
ALTER TABLE games ADD COLUMN IF NOT EXISTS xwoba REAL;         -- Alternative xwOBA calculation

-- Quality of contact metrics
ALTER TABLE games ADD COLUMN IF NOT EXISTS launch_speed_angle SMALLINT; -- Barrel classification (1-8)
ALTER TABLE games ADD COLUMN IF NOT EXISTS babip_value REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS iso_value REAL;
ALTER TABLE games ADD COLUMN IF NOT EXISTS launch_speed_angle_value REAL; -- Numeric version

-- Barrel and hard hit metrics
ALTER TABLE games ADD COLUMN IF NOT EXISTS barrel INTEGER;     -- 1 if barrel, 0 if not
ALTER TABLE games ADD COLUMN IF NOT EXISTS sweet_spot_code INTEGER; -- Sweet spot classification

-- ============================================================================
-- SITUATIONAL AND CONTEXTUAL DATA
-- Real data includes extensive game situation and leverage information
-- ============================================================================

-- Score and inning context
ALTER TABLE games ADD COLUMN IF NOT EXISTS post_away_score INTEGER;   -- Score after play
ALTER TABLE games ADD COLUMN IF NOT EXISTS post_home_score INTEGER;   -- Score after play
ALTER TABLE games ADD COLUMN IF NOT EXISTS post_bat_score INTEGER;    -- Batting team score after
ALTER TABLE games ADD COLUMN IF NOT EXISTS post_fld_score INTEGER;    -- Fielding team score after
ALTER TABLE games ADD COLUMN IF NOT EXISTS bat_score INTEGER;         -- Batting team score before
ALTER TABLE games ADD COLUMN IF NOT EXISTS fld_score INTEGER;         -- Fielding team score before

-- Leverage and win probability
ALTER TABLE games ADD COLUMN IF NOT EXISTS delta_home_win_exp REAL;   -- Change in home win expectancy
ALTER TABLE games ADD COLUMN IF NOT EXISTS delta_run_exp REAL;        -- Change in run expectancy (already exists)

-- wOBA and run values
ALTER TABLE games ADD COLUMN IF NOT EXISTS woba_value REAL;           -- wOBA value for this play
ALTER TABLE games ADD COLUMN IF NOT EXISTS woba_denom REAL;           -- wOBA denominator

-- ============================================================================
-- PITCHER AND BATTER CHARACTERISTICS
-- Real data includes detailed player attributes and platoon data
-- ============================================================================

-- Handedness confirmation
ALTER TABLE games ADD COLUMN IF NOT EXISTS stand TEXT;               -- Batter handedness (L/R) - already exists
ALTER TABLE games ADD COLUMN IF NOT EXISTS p_throws TEXT;            -- Pitcher handedness (L/R) - already exists

-- ============================================================================
-- ADVANCED PITCH MOVEMENT AND PHYSICS
-- Real Statcast includes sophisticated pitch movement calculations
-- ============================================================================

-- Normalized movement (adjusted for gravity)
ALTER TABLE games ADD COLUMN IF NOT EXISTS pfx_x_norm REAL;          -- Normalized horizontal movement
ALTER TABLE games ADD COLUMN IF NOT EXISTS pfx_z_norm REAL;          -- Normalized vertical movement

-- Spin axis and efficiency
ALTER TABLE games ADD COLUMN IF NOT EXISTS spin_axis REAL;           -- Spin axis in degrees
ALTER TABLE games ADD COLUMN IF NOT EXISTS spin_efficiency REAL;     -- Spin efficiency percentage

-- Break measurements
ALTER TABLE games ADD COLUMN IF NOT EXISTS break_angle REAL;         -- Break angle
ALTER TABLE games ADD COLUMN IF NOT EXISTS break_length REAL;        -- Break length

-- ============================================================================
-- UMPIRE AND OFFICIATING DATA
-- Real data includes detailed umpire information
-- ============================================================================

-- Zone and call data
ALTER TABLE games ADD COLUMN IF NOT EXISTS zone INTEGER;             -- Strike zone location (1-14)
ALTER TABLE games ADD COLUMN IF NOT EXISTS type TEXT;               -- Pitch result (S, B, X)

-- ============================================================================
-- TIMING AND PACE DATA
-- Real data includes detailed timing information
-- ============================================================================

-- Game timing
ALTER TABLE games ADD COLUMN IF NOT EXISTS game_start_time TEXT;     -- Game start time
ALTER TABLE games ADD COLUMN IF NOT EXISTS pitch_clock_time REAL;    -- Pitch clock time (newer data)

-- ============================================================================
-- TEAM AND BALLPARK CONTEXT
-- Real data includes detailed venue and team information
-- ============================================================================

-- Team identifiers
ALTER TABLE games ADD COLUMN IF NOT EXISTS home_team_id INTEGER;     -- Home team ID
ALTER TABLE games ADD COLUMN IF NOT EXISTS away_team_id INTEGER;     -- Away team ID

-- ============================================================================
-- STATCAST-SPECIFIC IDENTIFIERS
-- Real data includes various tracking system identifiers
-- ============================================================================

-- Tracking system IDs
ALTER TABLE games ADD COLUMN IF NOT EXISTS des TEXT;                 -- Description
ALTER TABLE games ADD COLUMN IF NOT EXISTS game_type_id INTEGER;     -- Game type ID

-- Run expectancy context
ALTER TABLE games ADD COLUMN IF NOT EXISTS des_runs REAL;            -- Runs scored on play

-- ============================================================================
-- QUALITY CONTROL AND METADATA
-- Real data includes data quality indicators
-- ============================================================================

-- Data quality flags
ALTER TABLE games ADD COLUMN IF NOT EXISTS data_source TEXT;         -- Data source (trackman, etc.)
ALTER TABLE games ADD COLUMN IF NOT EXISTS tracking_system TEXT;     -- Tracking system used
ALTER TABLE games ADD COLUMN IF NOT EXISTS data_quality_score REAL;  -- Quality score (0-1)

-- ============================================================================
-- DEFENSIVE ALIGNMENT DATA  
-- Real data includes fielding positioning information
-- ============================================================================

-- Fielding alignment (strategic positioning)
ALTER TABLE games ADD COLUMN IF NOT EXISTS if_fielding_alignment TEXT; -- Infield alignment
ALTER TABLE games ADD COLUMN IF NOT EXISTS of_fielding_alignment TEXT; -- Outfield alignment

-- ============================================================================
-- CREATE COMPREHENSIVE INDEXES FOR PERFORMANCE
-- Real data analysis requires optimized access patterns
-- ============================================================================

-- Core performance indexes (already exist, but ensuring they're present)
CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_pk ON games(game_pk);
CREATE INDEX IF NOT EXISTS idx_games_pitcher ON games(pitcher);
CREATE INDEX IF NOT EXISTS idx_games_batter ON games(batter);
CREATE INDEX IF NOT EXISTS idx_games_events ON games(events);

-- Advanced metrics indexes for fast analysis
CREATE INDEX IF NOT EXISTS idx_games_xba ON games(estimated_ba_using_speedangle) 
    WHERE estimated_ba_using_speedangle IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_xwoba ON games(estimated_woba_using_speedangle) 
    WHERE estimated_woba_using_speedangle IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_barrels ON games(launch_speed_angle) 
    WHERE launch_speed_angle = 6;

CREATE INDEX IF NOT EXISTS idx_games_spin_rate ON games(release_spin_rate) 
    WHERE release_spin_rate IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_launch_speed ON games(launch_speed) 
    WHERE launch_speed IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_launch_angle ON games(launch_angle) 
    WHERE launch_angle IS NOT NULL;

-- Situational analysis indexes
CREATE INDEX IF NOT EXISTS idx_games_leverage ON games(delta_home_win_exp) 
    WHERE delta_home_win_exp IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_games_count ON games(balls, strikes);

CREATE INDEX IF NOT EXISTS idx_games_runners ON games(outs_when_up) 
    WHERE outs_when_up IS NOT NULL;

-- Pitch type and movement analysis
CREATE INDEX IF NOT EXISTS idx_games_pitch_type ON games(pitch_type);

CREATE INDEX IF NOT EXISTS idx_games_movement ON games(pfx_x, pfx_z) 
    WHERE pfx_x IS NOT NULL AND pfx_z IS NOT NULL;

-- Quality of contact analysis
CREATE INDEX IF NOT EXISTS idx_games_hard_hit ON games(launch_speed) 
    WHERE launch_speed >= 95.0;

CREATE INDEX IF NOT EXISTS idx_games_sweet_spot ON games(launch_angle) 
    WHERE launch_angle BETWEEN 8 AND 32;

-- Zone analysis
CREATE INDEX IF NOT EXISTS idx_games_zone_strikes ON games(zone) 
    WHERE zone BETWEEN 1 AND 9;

CREATE INDEX IF NOT EXISTS idx_games_zone_all ON games(zone) 
    WHERE zone IS NOT NULL;

-- Platoon splits analysis
CREATE INDEX IF NOT EXISTS idx_games_platoon ON games(stand, p_throws);

-- Team and venue analysis
CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team);

-- ============================================================================
-- VALIDATION QUERIES TO VERIFY COMPLETENESS
-- Ensure all expected columns are present for real data collection
-- ============================================================================

-- Function to check which Statcast columns are present
CREATE OR REPLACE FUNCTION check_statcast_schema_completeness()
RETURNS TABLE(
    category TEXT,
    expected_columns INTEGER,
    present_columns INTEGER,
    missing_columns TEXT[]
) AS $$
DECLARE
    all_columns TEXT[];
BEGIN
    -- Get all current columns in games table
    SELECT array_agg(column_name) INTO all_columns
    FROM information_schema.columns 
    WHERE table_name = 'games' AND table_schema = 'public';
    
    -- Check core pitch data
    RETURN QUERY
    SELECT 'Core Pitch Data'::TEXT,
           13::INTEGER,
           (SELECT COUNT(*)::INTEGER FROM unnest(ARRAY[
               'release_speed', 'effective_speed', 'release_spin_rate',
               'plate_x', 'plate_z', 'zone', 'pfx_x', 'pfx_z',
               'release_pos_x', 'release_pos_y', 'release_pos_z',
               'spin_axis', 'break_angle'
           ]) AS col WHERE col = ANY(all_columns)),
           (SELECT array_agg(col) FROM unnest(ARRAY[
               'release_speed', 'effective_speed', 'release_spin_rate',
               'plate_x', 'plate_z', 'zone', 'pfx_x', 'pfx_z',
               'release_pos_x', 'release_pos_y', 'release_pos_z',
               'spin_axis', 'break_angle'
           ]) AS col WHERE col != ALL(all_columns));
    
    -- Check batted ball data
    RETURN QUERY
    SELECT 'Batted Ball Data'::TEXT,
           10::INTEGER,
           (SELECT COUNT(*)::INTEGER FROM unnest(ARRAY[
               'launch_speed', 'launch_angle', 'hit_distance_sc',
               'hc_x', 'hc_y', 'spray_angle', 'bb_type',
               'launch_speed_angle', 'barrel', 'hit_location'
           ]) AS col WHERE col = ANY(all_columns)),
           (SELECT array_agg(col) FROM unnest(ARRAY[
               'launch_speed', 'launch_angle', 'hit_distance_sc',
               'hc_x', 'hc_y', 'spray_angle', 'bb_type',
               'launch_speed_angle', 'barrel', 'hit_location'
           ]) AS col WHERE col != ALL(all_columns));
    
    -- Check expected stats
    RETURN QUERY
    SELECT 'Expected Statistics'::TEXT,
           8::INTEGER,
           (SELECT COUNT(*)::INTEGER FROM unnest(ARRAY[
               'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle',
               'estimated_slg_using_speedangle', 'xba', 'xslg', 'xwoba',
               'babip_value', 'iso_value'
           ]) AS col WHERE col = ANY(all_columns)),
           (SELECT array_agg(col) FROM unnest(ARRAY[
               'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle',
               'estimated_slg_using_speedangle', 'xba', 'xslg', 'xwoba',
               'babip_value', 'iso_value'
           ]) AS col WHERE col != ALL(all_columns));
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- Document all the new advanced metrics for future reference
-- ============================================================================

COMMENT ON COLUMN games.spin_axis IS 'Spin axis in degrees (0-360) indicating pitch rotation direction';
COMMENT ON COLUMN games.break_angle IS 'Angle of pitch break from release to plate';
COMMENT ON COLUMN games.break_length IS 'Distance in inches of pitch break';
COMMENT ON COLUMN games.spray_angle IS 'Angle of batted ball direction from home plate';
COMMENT ON COLUMN games.barrel IS 'Binary indicator: 1 if barrel contact (optimal launch conditions)';
COMMENT ON COLUMN games.sweet_spot_code IS 'Classification of contact quality (1-8 scale)';
COMMENT ON COLUMN games.xba IS 'Expected batting average based on launch conditions';
COMMENT ON COLUMN games.xwoba IS 'Expected weighted on-base average';
COMMENT ON COLUMN games.delta_home_win_exp IS 'Change in home team win expectancy from this play';
COMMENT ON COLUMN games.if_fielding_alignment IS 'Infield defensive alignment (Standard, Shift, etc.)';
COMMENT ON COLUMN games.of_fielding_alignment IS 'Outfield defensive alignment (Standard, Shift, etc.)';

-- Final verification
SELECT 'Comprehensive Statcast migration completed - all real pybaseball columns should now be supported' AS status;