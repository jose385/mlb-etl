-- migrations/004_create_roster_comprehensive.sql
-- Complete player roster information

CREATE TABLE IF NOT EXISTS public.roster (
  -- Core identifiers
  game_date         DATE,
  team_id           INTEGER,
  person_id         INTEGER,
  side              TEXT,       -- "home" or "away"
  
  -- Jersey and position
  jersey_number     TEXT,
  position_code     TEXT,
  position_name     TEXT,
  position_type     TEXT,
  position_abbreviation TEXT,
  
  -- Status
  status_code       TEXT,
  status_description TEXT,
  
  -- Personal information
  full_name         TEXT,
  first_name        TEXT,
  last_name         TEXT,
  use_name          TEXT,
  boxscore_name     TEXT,
  name_slug         TEXT,
  
  -- Physical characteristics
  height            TEXT,
  weight            INTEGER,
  birth_date        DATE,
  age               INTEGER,
  birth_city        TEXT,
  birth_state_province TEXT,
  birth_country     TEXT,
  
  -- Playing characteristics
  bat_side          TEXT,
  pitch_hand        TEXT,
  
  -- Career info
  mlb_debut_date    DATE,
  active            BOOLEAN,
  current_team_id   INTEGER,
  primary_position_code TEXT,
  primary_position_name TEXT,
  
  -- Additional fields will be added dynamically
  
  PRIMARY KEY (game_date, team_id, person_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_roster_game_date ON public.roster(game_date);
CREATE INDEX IF NOT EXISTS idx_roster_team_id ON public.roster(team_id);
CREATE INDEX IF NOT EXISTS idx_roster_person_id ON public.roster(person_id);
CREATE INDEX IF NOT EXISTS idx_roster_position ON public.roster(position_code);
