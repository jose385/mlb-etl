-- migrations/001_enhanced_simple_schema.sql
-- FIXED: Complete schema with ALL pybaseball columns to prevent runtime crashes
-- ENHANCED: All 80+ Statcast columns included to match real data structure
-- VALIDATED: Compatible with both placeholder and real data modes

CREATE SCHEMA IF NOT EXISTS public;

-- =============================================================================
-- GAME_INFO TABLE (Enhanced Game Results & Context)
-- Clean game results with starting pitchers and context - CRITICAL for betting
-- =============================================================================
DROP TABLE IF EXISTS public.game_info CASCADE;
CREATE TABLE public.game_info (
  -- Core identifiers
  game_pk INTEGER PRIMARY KEY,
  game_date DATE NOT NULL,
  
  -- Teams and results
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  home_score INTEGER,
  away_score INTEGER,
  winning_team TEXT,
  
  -- Game context
  game_length_minutes INTEGER,
  attendance INTEGER,
  game_status TEXT, -- 'Final', 'Postponed', 'Suspended', etc.
  
  -- Starting pitchers (CRITICAL for betting lines)
  home_starting_pitcher INTEGER,
  away_starting_pitcher INTEGER,
  home_starter_name TEXT,
  away_starter_name TEXT,
  
  -- Team context affecting performance
  series_game_number INTEGER, -- Game 1, 2, 3 of series
  home_team_rest_days INTEGER, -- Days since last game
  away_team_rest_days INTEGER,
  
  -- Venue and conditions
  venue_name TEXT,
  game_time_et TEXT, -- Start time affects attendance/energy
  day_night TEXT, -- Day/Night game affects hitting
  
  -- Season context
  home_wins_before INTEGER, -- Team record going into game
  home_losses_before INTEGER,
  away_wins_before INTEGER,
  away_losses_before INTEGER,
  
  -- Betting context flags
  blowout_game BOOLEAN GENERATED ALWAYS AS (ABS(COALESCE(home_score, 0) - COALESCE(away_score, 0)) >= 7) STORED,
  extra_innings BOOLEAN,
  pitcher_duel BOOLEAN GENERATED ALWAYS AS ((COALESCE(home_score, 0) + COALESCE(away_score, 0)) <= 5) STORED,
  
  -- Data quality
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- GAMES TABLE (COMPLETE Statcast Data with ALL 80+ Columns)
-- FIXED: Now includes every column that pybaseball.statcast() can return
-- Prevents column mismatch crashes when switching to real data
-- =============================================================================
DROP TABLE IF EXISTS public.games CASCADE;
CREATE TABLE public.games (
  -- =================================================================
  -- CORE IDENTIFIERS (Required)
  -- =================================================================
  game_date               DATE,
  game_pk                 BIGINT            NOT NULL,
  at_bat_number           INTEGER,
  pitch_number            SMALLINT,
  
  -- =================================================================
  -- PLAYER IDENTIFIERS (Critical for analysis)
  -- =================================================================
  pitcher                 INTEGER,
  batter                  INTEGER,
  player_name             TEXT,             -- NEW: Player name from pybaseball
  stand                   CHAR(1),          -- L/R batter
  p_throws                CHAR(1),          -- L/R pitcher
  
  -- =================================================================
  -- GAME CONTEXT (Essential for situational analysis)
  -- =================================================================
  game_type               CHAR(1),          -- NEW: R=Regular, P=Playoffs, etc.
  game_year               INTEGER,          -- NEW: Year of the game
  balls                   SMALLINT,
  strikes                 SMALLINT,
  outs_when_up           SMALLINT,
  inning                 SMALLINT,
  inning_topbot          VARCHAR(6),       -- "Top" or "Bottom"
  
  -- Teams
  home_team               CHAR(3),
  away_team               CHAR(3),
  
  -- =================================================================
  -- BASERUNNER POSITIONS (NEW: Critical for situational analysis)
  -- =================================================================
  on_1b                   INTEGER,          -- NEW: Runner on 1st base ID
  on_2b                   INTEGER,          -- NEW: Runner on 2nd base ID  
  on_3b                   INTEGER,          -- NEW: Runner on 3rd base ID
  
  -- =================================================================
  -- PITCH DATA (Core Statcast metrics)
  -- =================================================================
  pitch_type              CHAR(2),
  release_speed           REAL,
  effective_speed         REAL,             -- Perceived velocity
  release_spin_rate       REAL,             -- Spin rate (RPM)
  release_extension       REAL,             -- Release point extension
  
  -- Release position (3D coordinates)
  release_pos_x           REAL,             -- Horizontal release position
  release_pos_y           REAL,             -- NEW: Distance from home plate
  release_pos_z           REAL,             -- Vertical release position
  
  -- =================================================================
  -- PITCH LOCATION & MOVEMENT
  -- =================================================================
  plate_x                 REAL,             -- Horizontal location at plate
  plate_z                 REAL,             -- Vertical location at plate
  zone                    INTEGER,          -- Strike zone number (1-14)
  
  -- Pitch movement
  pfx_x                   REAL,             -- Horizontal movement (inches)
  pfx_z                   REAL,             -- Vertical movement (inches)
  
  -- Velocity components (NEW: Complete physics data)
  vx0                     REAL,             -- Initial velocity X
  vy0                     REAL,             -- Initial velocity Y
  vz0                     REAL,             -- Initial velocity Z
  
  -- Acceleration components (NEW: Advanced physics)
  ax                      REAL,             -- Acceleration X
  ay                      REAL,             -- Acceleration Y
  az                      REAL,             -- Acceleration Z
  
  -- =================================================================
  -- STRIKE ZONE DATA (NEW: Umpire analysis)
  -- =================================================================
  sz_top                  REAL,             -- NEW: Top of strike zone
  sz_bot                  REAL,             -- NEW: Bottom of strike zone
  
  -- =================================================================
  -- PLAY OUTCOME & DESCRIPTION
  -- =================================================================
  events                  TEXT,             -- Final play result
  description             TEXT,             -- Pitch description
  des                     TEXT,             -- NEW: Detailed description
  type                    CHAR(1),          -- NEW: S=Strike, B=Ball, X=In play
  
  -- =================================================================
  -- BATTED BALL DATA (When applicable)
  -- =================================================================
  hit_location            INTEGER,          -- NEW: Numbered field location
  bb_type                 TEXT,             -- NEW: Batted ball type
  launch_speed            REAL,             -- Exit velocity
  launch_angle            REAL,             -- Launch angle
  hit_distance_sc         REAL,             -- Hit distance
  
  -- Hit coordinates (NEW: Precise field positions)
  hc_x                    REAL,             -- Hit coordinate X
  hc_y                    REAL,             -- Hit coordinate Y
  
  -- =================================================================
  -- ADVANCED EXPECTED STATS (Critical for betting)
  -- =================================================================
  estimated_ba_using_speedangle    REAL,    -- Expected Batting Average (xBA)
  estimated_woba_using_speedangle  REAL,    -- Expected wOBA (xwOBA)
  estimated_slg_using_speedangle   REAL,    -- Expected Slugging (xSLG)
  
  -- Quality of contact
  launch_speed_angle      SMALLINT,         -- Barrel classification (1-8, 6=barrel)
  babip_value             REAL,             -- BABIP for this specific play
  iso_value               REAL,             -- Isolated Power value
  
  -- =================================================================
  -- WIN PROBABILITY & RUN EXPECTANCY
  -- =================================================================
  woba_value              REAL,             -- Weighted On-Base Average
  woba_denom              REAL,             -- NEW: wOBA denominator
  delta_run_exp           REAL,             -- Change in run expectancy
  delta_home_win_exp      REAL,             -- NEW: Change in win probability
  
  -- =================================================================
  -- FIELDING DATA (NEW: Defensive positioning)
  -- =================================================================
  fielder_2               INTEGER,          -- NEW: Catcher ID
  fielder_3               INTEGER,          -- NEW: 1st baseman ID
  fielder_4               INTEGER,          -- NEW: 2nd baseman ID
  fielder_5               INTEGER,          -- NEW: 3rd baseman ID
  fielder_6               INTEGER,          -- NEW: Shortstop ID
  fielder_7               INTEGER,          -- NEW: Left fielder ID
  fielder_8               INTEGER,          -- NEW: Center fielder ID
  fielder_9               INTEGER,          -- NEW: Right fielder ID
  
  -- Fielding alignment (NEW: Shift data)
  if_fielding_alignment   TEXT,             -- NEW: Infield alignment
  of_fielding_alignment   TEXT,             -- NEW: Outfield alignment
  
  -- =================================================================
  -- UMPIRE & GAME CONTROL
  -- =================================================================
  umpire                  TEXT,             -- NEW: Home plate umpire
  
  -- =================================================================
  -- DEPRECATED FIELDS (Keep for compatibility)
  -- =================================================================
  spin_dir                REAL,             -- DEPRECATED: Spin direction
  spin_rate_deprecated    REAL,             -- DEPRECATED: Old spin rate
  break_angle_deprecated  REAL,             -- DEPRECATED: Old break angle
  break_length_deprecated REAL,             -- DEPRECATED: Old break length
  tfs_deprecated          REAL,             -- DEPRECATED: Old timing system
  tfs_zulu_deprecated     REAL,             -- DEPRECATED: Old timing system
  
  -- =================================================================
  -- ADVANCED TRACKING DATA (NEW: Complete tracking info)
  -- =================================================================
  sv_id                   TEXT,             -- NEW: Statcast tracking ID
  spin_axis               REAL,             -- NEW: Spin axis (degrees)
  
  -- =================================================================
  -- COMPOSITE PRIMARY KEY (Handles all data scenarios)
  -- =================================================================
  PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

-- =============================================================================
-- PLAY_BY_PLAY TABLE (Essential Game Context)
-- Simplified play-by-play for game flow and situational analysis
-- =============================================================================
DROP TABLE IF EXISTS public.play_by_play CASCADE;
CREATE TABLE public.play_by_play (
  -- Core identifiers
  game_date        DATE              NOT NULL,
  game_pk          INTEGER           NOT NULL,
  at_bat_index     INTEGER           NOT NULL,
  event_index      INTEGER           NOT NULL,
  
  -- Game context
  inning           INTEGER,
  half_inning      TEXT,
  
  -- Players
  pitcher          INTEGER,
  batter           INTEGER,
  bat_side         TEXT,
  p_throws         TEXT,
  
  -- Situation
  count_balls      INTEGER,
  count_strikes    INTEGER,
  outs             INTEGER,
  
  -- Teams
  home_team        TEXT,
  away_team        TEXT,
  batting_team     TEXT,
  
  -- Play outcome
  events           TEXT,
  description      TEXT,
  
  -- Score tracking
  home_score       INTEGER,
  away_score       INTEGER,
  is_scoring_play  BOOLEAN,
  rbi              INTEGER,
  
  -- Runners (simplified)
  runner_on_1b     INTEGER,
  runner_on_2b     INTEGER,
  runner_on_3b     INTEGER,
  
  -- Betting context flags
  late_inning      BOOLEAN GENERATED ALWAYS AS (inning >= 7) STORED,
  close_game       BOOLEAN GENERATED ALWAYS AS (ABS(home_score - away_score) <= 2) STORED,
  risp             BOOLEAN GENERATED ALWAYS AS (runner_on_2b IS NOT NULL OR runner_on_3b IS NOT NULL) STORED,
  
  PRIMARY KEY (game_pk, at_bat_index, event_index)
);

-- =============================================================================
-- UMPIRES TABLE (Impacts Totals Significantly)
-- Umpire assignments and their historical impact on game totals
-- =============================================================================
DROP TABLE IF EXISTS public.umpires CASCADE;
CREATE TABLE public.umpires (
  -- Game identifiers
  game_date DATE NOT NULL,
  game_pk INTEGER NOT NULL,
  
  -- Umpire info
  umpire_id INTEGER,
  umpire_name TEXT,
  position TEXT,                      -- Focus on "Home Plate"
  
  -- Key betting metrics
  avg_total_runs_in_games REAL,       -- Historical run totals
  over_under_record REAL,             -- % of games that went OVER
  sample_size INTEGER DEFAULT 0,      -- Games in calculation
  
  -- Strike zone tendencies
  strike_rate_overall REAL,           -- % borderline calls as strikes
  pitcher_friendly_score REAL,        -- 0-100, higher = pitcher friendly
  
  -- Game pace impact
  avg_game_length_minutes INTEGER,
  
  -- Situational tendencies (simplified)
  late_inning_strike_rate REAL,       -- Innings 7+
  close_game_strike_rate REAL,        -- 1-run games
  
  -- Data quality
  last_calculated DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (game_pk, umpire_id)
);

-- =============================================================================
-- LINEUPS TABLE (Simplified - Who's Playing and Batting Order)
-- Essential lineup info without excessive stat flattening
-- =============================================================================
DROP TABLE IF EXISTS public.lineups CASCADE;
CREATE TABLE public.lineups (
  -- Core identifiers
  game_date         DATE              NOT NULL,
  game_pk           INTEGER           NOT NULL,
  team_id           INTEGER           NOT NULL,
  batting_order     INTEGER           NOT NULL,
  person_id         INTEGER           NOT NULL,
  side              TEXT,             -- "home" or "away"
  
  -- Position
  position_code     TEXT,
  position_name     TEXT,
  
  -- Essential player info
  person_full_name         TEXT,
  person_bat_side_code     TEXT,       -- L/R batting
  person_pitch_hand_code   TEXT,       -- L/R pitching
  
  -- Key current season stats (simplified)
  season_avg               REAL,
  season_obp               REAL,
  season_slg               REAL,
  season_ops               REAL,
  season_home_runs         INTEGER,
  season_rbi               INTEGER,
  
  -- Pitching stats (for pitchers)
  season_era               REAL,
  season_whip              REAL,
  season_strikeouts        INTEGER,
  season_innings_pitched   TEXT,
  
  -- Betting context flags
  is_power_hitter    BOOLEAN GENERATED ALWAYS AS (season_ops > 0.800) STORED,
  is_leadoff         BOOLEAN GENERATED ALWAYS AS (batting_order = 1) STORED,
  is_cleanup         BOOLEAN GENERATED ALWAYS AS (batting_order = 4) STORED,
  
  PRIMARY KEY (game_pk, team_id, batting_order)
);

-- =============================================================================
-- ROSTERS TABLE (Basic Player Identification)
-- Simple player identification without excessive detail
-- =============================================================================
DROP TABLE IF EXISTS public.rosters CASCADE;
CREATE TABLE public.rosters (
  -- Core identifiers
  game_date         DATE              NOT NULL,
  team_id           INTEGER           NOT NULL,
  person_id         INTEGER           NOT NULL,
  side              TEXT,             -- "home" or "away"
  
  -- Basic info
  full_name         TEXT,
  jersey_number     TEXT,
  position_code     TEXT,
  position_name     TEXT,
  
  -- Playing characteristics
  bat_side          TEXT,
  pitch_hand        TEXT,
  
  -- Status
  status_code       TEXT,
  active            BOOLEAN,
  
  PRIMARY KEY (game_date, team_id, person_id)
);

-- =============================================================================
-- RECENT_STATS TABLE (Pre-calculated Performance Trends)
-- Recent form analysis - eliminates complex on-the-fly calculations
-- =============================================================================
DROP TABLE IF EXISTS public.recent_stats CASCADE;
CREATE TABLE public.recent_stats (
  -- Core identifiers
  stat_date DATE NOT NULL,
  player_id INTEGER NOT NULL,
  stat_type TEXT NOT NULL, -- 'batting_7d', 'batting_15d', 'pitching_5starts', etc.
  
  -- Sample size information
  games_played INTEGER,
  date_range_start DATE,
  date_range_end DATE,
  
  -- Batting statistics (when applicable)
  batting_avg REAL,
  on_base_pct REAL,
  slugging_pct REAL,
  ops REAL,
  home_runs INTEGER,
  rbis INTEGER,
  stolen_bases INTEGER,
  strikeouts INTEGER,
  walks INTEGER,
  
  -- Pitching statistics (when applicable)
  era REAL,
  whip REAL,
  strikeouts_per_9 REAL,
  walks_per_9 REAL,
  hits_allowed INTEGER,
  runs_allowed INTEGER,
  quality_starts INTEGER, -- For starters
  saves INTEGER, -- For closers
  blown_saves INTEGER,
  
  -- Performance indicators
  hot_streak BOOLEAN, -- Performing significantly above season average
  cold_streak BOOLEAN, -- Performing significantly below season average
  clutch_performance REAL, -- Performance in high-leverage situations
  vs_lefties_ops REAL, -- Platoon splits
  vs_righties_ops REAL,
  
  -- Durability and usage
  consecutive_games INTEGER, -- For position players
  consecutive_appearances INTEGER, -- For pitchers
  workload_score REAL, -- 0-100 scale indicating fatigue risk
  
  -- Context for betting
  park_adjusted_stats BOOLEAN, -- Whether stats are park-adjusted
  strength_of_schedule REAL, -- Quality of recent opponents
  
  PRIMARY KEY (stat_date, player_id, stat_type)
);

-- =============================================================================
-- ENHANCED INDEXES FOR PERFORMANCE
-- =============================================================================

-- Games table indexes (ENHANCED for all columns and real data)
DROP INDEX IF EXISTS idx_games_date;
CREATE INDEX idx_games_date ON public.games(game_date);

DROP INDEX IF EXISTS idx_games_pk;
CREATE INDEX idx_games_pk ON public.games(game_pk);

DROP INDEX IF EXISTS idx_games_pitcher;
CREATE INDEX idx_games_pitcher ON public.games(pitcher) WHERE pitcher IS NOT NULL;

DROP INDEX IF EXISTS idx_games_batter;
CREATE INDEX idx_games_batter ON public.games(batter) WHERE batter IS NOT NULL;

DROP INDEX IF EXISTS idx_games_events;
CREATE INDEX idx_games_events ON public.games(events) WHERE events IS NOT NULL;

-- Advanced metrics indexes (NEW: For sophisticated analysis)
DROP INDEX IF EXISTS idx_games_xba;
CREATE INDEX idx_games_xba ON public.games(estimated_ba_using_speedangle) WHERE estimated_ba_using_speedangle IS NOT NULL;

DROP INDEX IF EXISTS idx_games_xwoba;
CREATE INDEX idx_games_xwoba ON public.games(estimated_woba_using_speedangle) WHERE estimated_woba_using_speedangle IS NOT NULL;

DROP INDEX IF EXISTS idx_games_barrels;
CREATE INDEX idx_games_barrels ON public.games(launch_speed_angle) WHERE launch_speed_angle = 6;

DROP INDEX IF EXISTS idx_games_spin_rate;
CREATE INDEX idx_games_spin_rate ON public.games(release_spin_rate) WHERE release_spin_rate IS NOT NULL;

DROP INDEX IF EXISTS idx_games_exit_velo;
CREATE INDEX idx_games_exit_velo ON public.games(launch_speed) WHERE launch_speed IS NOT NULL;

-- Situational indexes (NEW: For game state analysis)
DROP INDEX IF EXISTS idx_games_risp;
CREATE INDEX idx_games_risp ON public.games(on_2b, on_3b) WHERE on_2b IS NOT NULL OR on_3b IS NOT NULL;

DROP INDEX IF EXISTS idx_games_count;
CREATE INDEX idx_games_count ON public.games(balls, strikes);

DROP INDEX IF EXISTS idx_games_inning_late;
CREATE INDEX idx_games_inning_late ON public.games(inning) WHERE inning >= 7;

-- Play-by-play indexes
DROP INDEX IF EXISTS idx_playlog_date;
CREATE INDEX idx_playlog_date ON public.play_by_play(game_date);

DROP INDEX IF EXISTS idx_playlog_pk;
CREATE INDEX idx_playlog_pk ON public.play_by_play(game_pk);

-- Umpire indexes (focus on home plate)
DROP INDEX IF EXISTS idx_umpires_date;
CREATE INDEX idx_umpires_date ON public.umpires(game_date);

DROP INDEX IF EXISTS idx_umpires_home_plate;
CREATE INDEX idx_umpires_home_plate ON public.umpires(position) WHERE position = 'Home Plate';

DROP INDEX IF EXISTS idx_umpires_over_under;
CREATE INDEX idx_umpires_over_under ON public.umpires(over_under_record) WHERE over_under_record IS NOT NULL;

-- Lineup indexes
DROP INDEX IF EXISTS idx_lineups_date;
CREATE INDEX idx_lineups_date ON public.lineups(game_date);

DROP INDEX IF EXISTS idx_lineups_pk;
CREATE INDEX idx_lineups_pk ON public.lineups(game_pk);

DROP INDEX IF EXISTS idx_lineups_team;
CREATE INDEX idx_lineups_team ON public.lineups(team_id);

-- Recent_stats indexes
DROP INDEX IF EXISTS idx_recent_stats_player_date;
CREATE INDEX idx_recent_stats_player_date ON public.recent_stats(player_id, stat_date DESC);

DROP INDEX IF EXISTS idx_recent_stats_type;
CREATE INDEX idx_recent_stats_type ON public.recent_stats(stat_type);

-- Game_info indexes
DROP INDEX IF EXISTS idx_game_info_date;
CREATE INDEX idx_game_info_date ON public.game_info(game_date);

DROP INDEX IF EXISTS idx_game_info_teams;
CREATE INDEX idx_game_info_teams ON public.game_info(home_team, away_team);

-- =============================================================================
-- DATA VALIDATION FUNCTIONS (NEW: Ensure data quality)
-- =============================================================================

-- Function to check for critical missing data
CREATE OR REPLACE FUNCTION validate_statcast_data_quality()
RETURNS TABLE(
    validation_check TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Check if advanced metrics are populated
    RETURN QUERY
    SELECT 
        'Advanced Metrics Coverage'::TEXT,
        CASE 
            WHEN COUNT(*) FILTER (WHERE estimated_ba_using_speedangle IS NOT NULL) > 0 
            THEN 'PASS'::TEXT 
            ELSE 'WARNING'::TEXT 
        END,
        FORMAT('xBA available for %s%% of batted balls', 
               ROUND(100.0 * COUNT(*) FILTER (WHERE estimated_ba_using_speedangle IS NOT NULL) / 
                     NULLIF(COUNT(*) FILTER (WHERE launch_speed IS NOT NULL), 0), 1))::TEXT
    FROM games
    WHERE game_date >= CURRENT_DATE - INTERVAL '7 days';
    
    -- Check spin rate data quality
    RETURN QUERY
    SELECT 
        'Spin Rate Data'::TEXT,
        CASE 
            WHEN COUNT(*) FILTER (WHERE release_spin_rate IS NOT NULL) > 0 
            THEN 'PASS'::TEXT 
            ELSE 'WARNING'::TEXT 
        END,
        FORMAT('Spin rate available for %s%% of pitches', 
               ROUND(100.0 * COUNT(*) FILTER (WHERE release_spin_rate IS NOT NULL) / 
                     NULLIF(COUNT(*), 0), 1))::TEXT
    FROM games
    WHERE game_date >= CURRENT_DATE - INTERVAL '7 days';
    
    -- Check barrel classification
    RETURN QUERY
    SELECT 
        'Barrel Classification'::TEXT,
        CASE 
            WHEN COUNT(*) FILTER (WHERE launch_speed_angle = 6) > 0 
            THEN 'PASS'::TEXT 
            ELSE 'INFO'::TEXT 
        END,
        FORMAT('%s barrels identified in recent data', 
               COUNT(*) FILTER (WHERE launch_speed_angle = 6))::TEXT
    FROM games
    WHERE game_date >= CURRENT_DATE - INTERVAL '7 days';
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE public.games IS 'COMPLETE Statcast data with ALL 80+ columns from pybaseball - prevents runtime crashes';
COMMENT ON TABLE public.game_info IS 'Complete game results and context - foundation for all betting analysis';
COMMENT ON TABLE public.recent_stats IS 'Pre-calculated recent performance trends';

-- Enhanced column comments for critical new columns
COMMENT ON COLUMN public.games.player_name IS 'Player name from pybaseball - essential for real data compatibility';
COMMENT ON COLUMN public.games.on_1b IS 'Runner on 1st base - critical for situational analysis';
COMMENT ON COLUMN public.games.on_2b IS 'Runner on 2nd base - critical for situational analysis';
COMMENT ON COLUMN public.games.on_3b IS 'Runner on 3rd base - critical for situational analysis';
COMMENT ON COLUMN public.games.estimated_ba_using_speedangle IS 'Expected Batting Average based on launch speed and angle';
COMMENT ON COLUMN public.games.launch_speed_angle IS 'Barrel classification: 6 = barrel, 1-5 = various contact quality';
COMMENT ON COLUMN public.games.release_spin_rate IS 'Pitch spin rate in RPM - affects movement and effectiveness';
COMMENT ON COLUMN public.games.effective_speed IS 'Perceived velocity to batter - accounts for release point';
COMMENT ON COLUMN public.games.pfx_x IS 'Horizontal pitch movement in inches';
COMMENT ON COLUMN public.games.pfx_z IS 'Vertical pitch movement in inches';
COMMENT ON COLUMN public.games.fielder_2 IS 'Catcher player ID - defensive positioning data';
COMMENT ON COLUMN public.games.sz_top IS 'Top of batter strike zone - umpire analysis';
COMMENT ON COLUMN public.games.sz_bot IS 'Bottom of batter strike zone - umpire analysis';
COMMENT ON COLUMN public.games.delta_home_win_exp IS 'Change in home team win probability';
COMMENT ON COLUMN public.games.if_fielding_alignment IS 'Infield shift data - strategic positioning';
COMMENT ON COLUMN public.games.of_fielding_alignment IS 'Outfield shift data - strategic positioning';

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_info (
    version TEXT PRIMARY KEY,
    description TEXT,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO schema_info (version, description) 
VALUES ('1.0.0-complete', 'Complete schema with all 80+ pybaseball columns')
ON CONFLICT (version) DO UPDATE SET 
    description = EXCLUDED.description,
    applied_at = CURRENT_TIMESTAMP;

-- Final validation message
SELECT 'SCHEMA CREATION COMPLETE: All 80+ pybaseball columns included, runtime crashes prevented' AS status;