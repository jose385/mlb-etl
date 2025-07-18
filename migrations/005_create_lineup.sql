-- migrations/005_create_lineup_comprehensive.sql
-- Complete lineup with full player details

CREATE TABLE IF NOT EXISTS public.lineup (
  -- Core identifiers
  game_date         DATE,
  game_pk           INTEGER,
  team_id           INTEGER,
  batting_order     INTEGER,
  person_id         INTEGER,
  side              TEXT,       -- "home" or "away"
  
  -- Position information
  position_code     TEXT,
  position_name     TEXT,
  position_type     TEXT,
  position_abbreviation TEXT,
  
  -- Player information (denormalized for convenience)
  full_name         TEXT,
  first_name        TEXT,
  last_name         TEXT,
  jersey_number     TEXT,
  
  -- Playing characteristics
  bat_side          TEXT,
  pitch_hand        TEXT,
  
  -- Physical info
  height            TEXT,
  weight            INTEGER,
  age               INTEGER,
  
  -- Game-specific stats (if available)
  season_stats_games_played INTEGER,
  season_stats_at_bats INTEGER,
  season_stats_hits INTEGER,
  season_stats_home_runs INTEGER,
  season_stats_rbi INTEGER,
  season_stats_avg REAL,
  
  -- Additional fields will be added dynamically
  
  PRIMARY KEY (game_pk, team_id, batting_order)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_lineup_game_date ON public.lineup(game_date);
CREATE INDEX IF NOT EXISTS idx_lineup_game_pk ON public.lineup(game_pk);
CREATE INDEX IF NOT EXISTS idx_lineup_team_id ON public.lineup(team_id);
CREATE INDEX IF NOT EXISTS idx_lineup_person_id ON public.lineup(person_id);
CREATE INDEX IF NOT EXISTS idx_lineup_batting_order ON public.lineup(batting_order);
