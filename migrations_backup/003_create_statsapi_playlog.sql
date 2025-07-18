-- migrations/003_create_statsapi_playlog.sql
CREATE TABLE IF NOT EXISTS public.statsapi_playlog (
  game_date        DATE,
  game_pk          INTEGER,
  at_bat_index     INTEGER,
  event_index      INTEGER,
  inning           INTEGER,
  half_inning      TEXT,
  pitcher          INTEGER,
  batter           INTEGER,
  events           TEXT,
  description      TEXT,
  count_balls      INTEGER,
  count_strikes    INTEGER,
  play_end_time    TIMESTAMP,    -- adjust if your Parquet has it as string
  p_throws         TEXT,
  home_team        TEXT,
  away_team        TEXT,
  batted_ball_type TEXT,
  hit_location     INTEGER,
  bb_type          TEXT,
  game_year        INTEGER,
  plate_x          FLOAT,
  plate_z          FLOAT,
  -- add any other top-level columns here from your Parquet schema...
  PRIMARY KEY (game_pk, at_bat_index, event_index)
);
