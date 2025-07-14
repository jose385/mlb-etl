-- migrations/004_create_roster.sql
CREATE TABLE IF NOT EXISTS public.roster (
  game_date      DATE,
  team_id        INTEGER,
  person_id      INTEGER,
  jersey_number  TEXT,
  position_code  TEXT,
  status_code    TEXT,
  side           TEXT,       -- “home” or “away”
  PRIMARY KEY (game_date, team_id, person_id)
);
