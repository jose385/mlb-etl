-- migrations/003_create_statsapi_playlog_comprehensive.sql
-- Much more comprehensive play-by-play schema

CREATE TABLE IF NOT EXISTS public.statsapi_playlog (
  -- Core identifiers
  game_date        DATE,
  game_pk          INTEGER,
  at_bat_index     INTEGER,
  event_index      INTEGER,
  play_index       INTEGER,
  
  -- Game context
  inning           INTEGER,
  half_inning      TEXT,
  inning_state     TEXT,
  game_year        INTEGER,
  
  -- Players
  pitcher          INTEGER,
  batter           INTEGER,
  p_throws         TEXT,
  bat_side         TEXT,
  
  -- Count and situation
  count_balls      INTEGER,
  count_strikes    INTEGER,
  outs             INTEGER,
  
  -- Teams
  home_team        TEXT,
  away_team        TEXT,
  batting_team     TEXT,
  fielding_team    TEXT,
  
  -- Play outcome
  events           TEXT,
  description      TEXT,
  event_type       TEXT,
  
  -- Timing
  play_end_time    TIMESTAMP,
  start_time       TIMESTAMP,
  
  -- Hit data
  batted_ball_type TEXT,
  hit_location     INTEGER,
  bb_type          TEXT,
  
  -- Coordinates (if available)
  plate_x          FLOAT,
  plate_z          FLOAT,
  
  -- Score and game state
  home_score       INTEGER,
  away_score       INTEGER,
  is_scoring_play  BOOLEAN,
  rbi              INTEGER,
  
  -- Win probability (if available)
  home_win_expectancy REAL,
  away_win_expectancy REAL,
  win_probability_added REAL,
  
  -- Runner information (flattened)
  runner_on_1b     INTEGER,
  runner_on_2b     INTEGER,
  runner_on_3b     INTEGER,
  
  -- Additional nested data will be added dynamically
  
  PRIMARY KEY (game_pk, at_bat_index, event_index)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_playlog_game_date ON public.statsapi_playlog(game_date);
CREATE INDEX IF NOT EXISTS idx_playlog_game_pk ON public.statsapi_playlog(game_pk);
CREATE INDEX IF NOT EXISTS idx_playlog_pitcher ON public.statsapi_playlog(pitcher);
CREATE INDEX IF NOT EXISTS idx_playlog_batter ON public.statsapi_playlog(batter);
CREATE INDEX IF NOT EXISTS idx_playlog_events ON public.statsapi_playlog(events);