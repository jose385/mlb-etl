-- migrations/005_create_lineup.sql
CREATE TABLE IF NOT EXISTS public.lineup (
  game_date      DATE,
  game_pk        INTEGER,
  team_id        INTEGER,
  batting_order  INTEGER,
  person_id      INTEGER,
  position_code  TEXT,
  side           TEXT,       -- “home” or “away”
  PRIMARY KEY (game_pk, team_id, batting_order)
);
