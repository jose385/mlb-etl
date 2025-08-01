-- migrations/001_enhanced_simple_schema.sql
-- Enhanced consolidated schema for comprehensive MLB betting analysis
-- 9 tables optimized for betting decisions while staying streamlined
-- UPDATED: Made idempotent with proper DROP statements

CREATE SCHEMA IF NOT EXISTS public;

-- =============================================================================
-- GAMES TABLE (Simplified Statcast Data)
-- Core game data with essential columns for betting analysis
-- =============================================================================
DROP TABLE IF EXISTS public.games CASCADE;
CREATE TABLE public.games (
  -- Core identifiers
  game_date               DATE              NOT NULL,
  game_pk                 BIGINT            NOT NULL,
  at_bat_number           INTEGER           NOT NULL,
  pitch_number            SMALLINT          NOT NULL,
  
  -- Essential player info
  pitcher                 INTEGER           NOT NULL,
  batter                  INTEGER           NOT NULL,
  stand                   CHAR(1),          -- L/R batter
  p_throws                CHAR(1),          -- L/R pitcher
  
  -- Game situation (betting context)
  balls                   SMALLINT          NOT NULL,
  strikes                 SMALLINT          NOT NULL,
  outs_when_up           SMALLINT,
  inning                 SMALLINT,
  inning_topbot          VARCHAR(3),
  
  -- Teams
  home_team               CHAR(3),
  away_team               CHAR(3),
  
  -- Essential pitch data
  release_speed           REAL,
  
  -- Key location data
  plate_x                 REAL,
  plate_z                 REAL,
  zone                    INTEGER,
  
  -- Outcome data (critical for analysis)
  events                  TEXT,
  description             TEXT,
  
  -- Hit data when relevant
  launch_speed            REAL,
  launch_angle            REAL,
  hit_distance_sc         REAL,
  
  -- Essential metrics
  woba_value              REAL,
  delta_run_exp           REAL,
  
  -- Pitch classification
  pitch_type              CHAR(2),
  
  PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

-- =============================================================================
-- GAME_INFO TABLE (Enhanced Game Results & Context)
-- Clean game results with starting pitchers and context - CRITICAL for betting
-- MOVED BEFORE OTHER TABLES FOR FOREIGN KEY REFERENCES
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
-- WEATHER TABLE (Critical for Betting)
-- Weather conditions that significantly impact game outcomes
-- =============================================================================
DROP TABLE IF EXISTS public.weather CASCADE;
CREATE TABLE public.weather (
  -- Game identifiers
  game_date DATE NOT NULL,
  game_pk INTEGER NOT NULL,
  venue_name TEXT,
  home_team TEXT,
  away_team TEXT,
  
  -- Essential weather for betting
  temperature_f REAL,
  humidity_pct INTEGER,
  wind_speed_mph REAL,
  wind_direction_deg INTEGER,
  
  -- Calculated impact factors
  wind_x_component REAL,              -- Helps/hurts home runs
  wind_y_component REAL,              -- Helps/hurts home runs
  hr_distance_factor_ft REAL,         -- Estimated HR distance change
  
  -- Betting impact scores (0-100)
  over_under_lean TEXT,               -- "OVER", "UNDER", "NEUTRAL"
  weather_impact_score REAL,          -- Strength of impact
  
  -- Park-specific adjustments
  park_factor REAL DEFAULT 1.0,      -- Ballpark run factor
  
  -- Data tracking
  data_source TEXT DEFAULT 'openweather',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (game_pk)
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
-- VENUE_FACTORS TABLE (Enhanced Ballpark Information)
-- Comprehensive park factors beyond weather - essential for accurate totals
-- =============================================================================
DROP TABLE IF EXISTS public.venue_factors CASCADE;
CREATE TABLE public.venue_factors (
  -- Core identification
  venue_name TEXT PRIMARY KEY,
  home_team TEXT NOT NULL,
  city TEXT,
  state TEXT,
  
  -- Physical characteristics affecting ball flight
  elevation_feet INTEGER,
  foul_territory_rank INTEGER, -- 1-30 ranking (1 = most foul territory)
  
  -- Wall dimensions (affects home runs)
  wall_height_lf INTEGER,
  wall_height_cf INTEGER,
  wall_height_rf INTEGER,
  distance_lf_foul INTEGER,
  distance_cf INTEGER,
  distance_rf_foul INTEGER,
  
  -- Calculated performance factors (historical data)
  hr_factor REAL DEFAULT 1.0, -- Park factor for home runs (1.0 = neutral)
  run_factor REAL DEFAULT 1.0, -- Overall run scoring factor
  double_factor REAL DEFAULT 1.0, -- Park factor for doubles
  
  -- Pitcher/hitter friendliness
  pitcher_friendly_score INTEGER, -- 1-10 scale (10 = most pitcher friendly)
  left_handed_hitter_advantage REAL, -- Factor for lefty hitters
  right_handed_hitter_advantage REAL, -- Factor for righty hitters
  
  -- Environmental factors
  dome_stadium BOOLEAN DEFAULT FALSE,
  retractable_roof BOOLEAN DEFAULT FALSE,
  artificial_turf BOOLEAN DEFAULT FALSE,
  
  -- Special characteristics affecting play
  wind_patterns TEXT, -- 'Swirling', 'Consistent', 'Variable'
  sun_field_advantage TEXT, -- Which dugout has sun advantage
  crowd_noise_factor INTEGER, -- 1-10 scale for crowd impact
  
  -- Betting-specific factors
  over_under_tendency REAL, -- Historical percentage of games going OVER
  favorite_covering_rate REAL, -- How often home team covers spread
  average_game_length_minutes INTEGER,
  
  -- Special conditions
  altitude_affects_ball_flight BOOLEAN GENERATED ALWAYS AS (elevation_feet > 3000) STORED,
  extreme_foul_territory BOOLEAN GENERATED ALWAYS AS (foul_territory_rank <= 5) STORED,
  short_porch BOOLEAN, -- Any wall distance < 310 feet
  
  -- Data tracking
  last_updated DATE,
  season_year INTEGER -- Factors can change year to year
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Games table indexes
DROP INDEX IF EXISTS idx_games_date;
CREATE INDEX idx_games_date ON public.games(game_date);

DROP INDEX IF EXISTS idx_games_pk;
CREATE INDEX idx_games_pk ON public.games(game_pk);

DROP INDEX IF EXISTS idx_games_pitcher;
CREATE INDEX idx_games_pitcher ON public.games(pitcher);

DROP INDEX IF EXISTS idx_games_teams;
CREATE INDEX idx_games_teams ON public.games(home_team, away_team);

DROP INDEX IF EXISTS idx_games_events;
CREATE INDEX idx_games_events ON public.games(events);

-- Play-by-play indexes
DROP INDEX IF EXISTS idx_playlog_date;
CREATE INDEX idx_playlog_date ON public.play_by_play(game_date);

DROP INDEX IF EXISTS idx_playlog_pk;
CREATE INDEX idx_playlog_pk ON public.play_by_play(game_pk);

DROP INDEX IF EXISTS idx_playlog_late_inning;
CREATE INDEX idx_playlog_late_inning ON public.play_by_play(late_inning);

DROP INDEX IF EXISTS idx_playlog_close_game;
CREATE INDEX idx_playlog_close_game ON public.play_by_play(close_game);

-- Weather indexes
DROP INDEX IF EXISTS idx_weather_date;
CREATE INDEX idx_weather_date ON public.weather(game_date);

DROP INDEX IF EXISTS idx_weather_impact;
CREATE INDEX idx_weather_impact ON public.weather(weather_impact_score);

DROP INDEX IF EXISTS idx_weather_temperature;
CREATE INDEX idx_weather_temperature ON public.weather(temperature_f);

-- Umpire indexes (focus on home plate)
DROP INDEX IF EXISTS idx_umpires_date;
CREATE INDEX idx_umpires_date ON public.umpires(game_date);

DROP INDEX IF EXISTS idx_umpires_home_plate;
CREATE INDEX idx_umpires_home_plate ON public.umpires(position) WHERE position = 'Home Plate';

DROP INDEX IF EXISTS idx_umpires_over_under;
CREATE INDEX idx_umpires_over_under ON public.umpires(over_under_record);

DROP INDEX IF EXISTS idx_umpires_name;
CREATE INDEX idx_umpires_name ON public.umpires(umpire_name);

-- Lineup indexes
DROP INDEX IF EXISTS idx_lineups_date;
CREATE INDEX idx_lineups_date ON public.lineups(game_date);

DROP INDEX IF EXISTS idx_lineups_pk;
CREATE INDEX idx_lineups_pk ON public.lineups(game_pk);

DROP INDEX IF EXISTS idx_lineups_order;
CREATE INDEX idx_lineups_order ON public.lineups(batting_order);

DROP INDEX IF EXISTS idx_lineups_power;
CREATE INDEX idx_lineups_power ON public.lineups(is_power_hitter);

-- Roster indexes
DROP INDEX IF EXISTS idx_rosters_date;
CREATE INDEX idx_rosters_date ON public.rosters(game_date);

DROP INDEX IF EXISTS idx_rosters_team;
CREATE INDEX idx_rosters_team ON public.rosters(team_id);

DROP INDEX IF EXISTS idx_rosters_person;
CREATE INDEX idx_rosters_person ON public.rosters(person_id);

-- Game_info indexes
DROP INDEX IF EXISTS idx_game_info_date;
CREATE INDEX idx_game_info_date ON public.game_info(game_date);

DROP INDEX IF EXISTS idx_game_info_teams;
CREATE INDEX idx_game_info_teams ON public.game_info(home_team, away_team);

DROP INDEX IF EXISTS idx_game_info_starters;
CREATE INDEX idx_game_info_starters ON public.game_info(home_starting_pitcher, away_starting_pitcher);

DROP INDEX IF EXISTS idx_game_info_venue;
CREATE INDEX idx_game_info_venue ON public.game_info(venue_name);

DROP INDEX IF EXISTS idx_game_info_status;
CREATE INDEX idx_game_info_status ON public.game_info(game_status);

-- Recent_stats indexes
DROP INDEX IF EXISTS idx_recent_stats_player_date;
CREATE INDEX idx_recent_stats_player_date ON public.recent_stats(player_id, stat_date DESC);

DROP INDEX IF EXISTS idx_recent_stats_type;
CREATE INDEX idx_recent_stats_type ON public.recent_stats(stat_type);

DROP INDEX IF EXISTS idx_recent_stats_hot_streak;
CREATE INDEX idx_recent_stats_hot_streak ON public.recent_stats(hot_streak) WHERE hot_streak = TRUE;

DROP INDEX IF EXISTS idx_recent_stats_cold_streak;
CREATE INDEX idx_recent_stats_cold_streak ON public.recent_stats(cold_streak) WHERE cold_streak = TRUE;

DROP INDEX IF EXISTS idx_recent_stats_batting;
CREATE INDEX idx_recent_stats_batting ON public.recent_stats(stat_type, ops) WHERE stat_type LIKE 'batting%';

DROP INDEX IF EXISTS idx_recent_stats_pitching;
CREATE INDEX idx_recent_stats_pitching ON public.recent_stats(stat_type, era) WHERE stat_type LIKE 'pitching%';

-- Venue_factors indexes
DROP INDEX IF EXISTS idx_venue_factors_team;
CREATE INDEX idx_venue_factors_team ON public.venue_factors(home_team);

DROP INDEX IF EXISTS idx_venue_factors_run_factor;
CREATE INDEX idx_venue_factors_run_factor ON public.venue_factors(run_factor);

DROP INDEX IF EXISTS idx_venue_factors_hr_factor;
CREATE INDEX idx_venue_factors_hr_factor ON public.venue_factors(hr_factor);

DROP INDEX IF EXISTS idx_venue_factors_pitcher_friendly;
CREATE INDEX idx_venue_factors_pitcher_friendly ON public.venue_factors(pitcher_friendly_score);

-- Enhanced composite indexes for betting queries
DROP INDEX IF EXISTS idx_betting_starters_recent;
CREATE INDEX idx_betting_starters_recent ON public.recent_stats(player_id, stat_type, stat_date) 
  WHERE stat_type IN ('pitching_5starts', 'pitching_15d');

DROP INDEX IF EXISTS idx_betting_hitters_recent;
CREATE INDEX idx_betting_hitters_recent ON public.recent_stats(player_id, stat_type, ops) 
  WHERE stat_type IN ('batting_7d', 'batting_15d') AND ops IS NOT NULL;

-- =============================================================================
-- COMMENTS FOR DOCUMENTATION
-- =============================================================================
COMMENT ON TABLE public.games IS 'Simplified Statcast data focusing on essential betting factors';
COMMENT ON TABLE public.play_by_play IS 'Game flow and situational context for betting analysis';
COMMENT ON TABLE public.weather IS 'Weather conditions impacting game outcomes and totals';
COMMENT ON TABLE public.umpires IS 'Umpire tendencies affecting strike zones and game totals';
COMMENT ON TABLE public.lineups IS 'Starting lineups with key offensive stats';
COMMENT ON TABLE public.rosters IS 'Basic player identification and characteristics';

-- Enhanced table comments
COMMENT ON TABLE public.game_info IS 'Complete game results and context - foundation for all betting analysis';
COMMENT ON TABLE public.recent_stats IS 'Pre-calculated recent performance trends to eliminate complex on-the-fly calculations';
COMMENT ON TABLE public.venue_factors IS 'Comprehensive ballpark factors affecting game outcomes and betting totals';

-- Enhanced column comments
COMMENT ON COLUMN public.weather.over_under_lean IS 'Weather-based OVER/UNDER recommendation';
COMMENT ON COLUMN public.umpires.over_under_record IS 'Historical percentage of OVER outcomes with this umpire';
COMMENT ON COLUMN public.lineups.is_power_hitter IS 'Player with OPS > 0.800 (home run threat)';
COMMENT ON COLUMN public.play_by_play.late_inning IS 'Inning 7+ (clutch situations)';
COMMENT ON COLUMN public.play_by_play.close_game IS 'Score difference <= 2 runs';
COMMENT ON COLUMN public.play_by_play.risp IS 'Runner in scoring position (2nd or 3rd base)';

-- New table column comments
COMMENT ON COLUMN public.game_info.home_starting_pitcher IS 'Starting pitcher ID - drives most betting line movement';
COMMENT ON COLUMN public.recent_stats.hot_streak IS 'Player performing significantly above season average';
COMMENT ON COLUMN public.recent_stats.workload_score IS 'Fatigue risk score 0-100 (100 = highest risk)';
COMMENT ON COLUMN public.venue_factors.run_factor IS 'Park run factor vs league average (1.0 = neutral, >1.0 = hitter friendly)';
COMMENT ON COLUMN public.venue_factors.over_under_tendency IS 'Historical percentage of games at this venue going OVER the total';