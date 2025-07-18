-- migrations/001_create_statcast_table_comprehensive.sql
-- This replaces your current statcast migration with ALL major columns

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.statcast (
  -- Core game info
  game_date               DATE              NOT NULL,
  game_pk                 BIGINT            NOT NULL,
  at_bat_number           INTEGER           NOT NULL,
  pitch_number            SMALLINT          NOT NULL,
  
  -- Player info
  pitcher                 INTEGER           NOT NULL,
  batter                  INTEGER           NOT NULL,
  stand                   CHAR(1),
  p_throws                CHAR(1),
  
  -- Count and situation
  balls                   SMALLINT          NOT NULL,
  strikes                 SMALLINT          NOT NULL,
  outs_when_up           SMALLINT,
  inning                 SMALLINT,
  inning_topbot          VARCHAR(3),
  
  -- Teams
  home_team               CHAR(3),
  away_team               CHAR(3),
  bat_team               CHAR(3),
  fld_team               CHAR(3),
  
  -- Pitch characteristics
  release_speed           REAL,
  release_pos_x           REAL,
  release_pos_y           REAL,
  release_pos_z           REAL,
  release_extension       REAL,
  
  -- Spin data
  spin_rate               REAL,
  spin_axis               REAL,
  spin_dir                REAL,
  
  -- Velocity components
  vx0                     REAL,
  vy0                     REAL,
  vz0                     REAL,
  ax                      REAL,
  ay                      REAL,
  az                      REAL,
  
  -- Location data
  plate_x                 REAL,
  plate_z                 REAL,
  sz_top                  REAL,
  sz_bot                  REAL,
  zone                    INTEGER,
  
  -- Movement
  pfx_x                   REAL,
  pfx_z                   REAL,
  break_angle             REAL,
  break_length            REAL,
  
  -- Timing
  plate_time              REAL,
  effective_speed         REAL,
  perceived_velocity      REAL,
  
  -- Hit data
  events                  TEXT,
  description             TEXT,
  launch_speed            REAL,
  launch_angle            REAL,
  hit_distance_sc         REAL,
  hc_x                    REAL,
  hc_y                    REAL,
  hit_location           INTEGER,
  bb_type                TEXT,
  bearing                 REAL,
  
  -- Advanced metrics
  woba_value              REAL,
  woba_denom              INTEGER,
  babip_value             REAL,
  iso_value               REAL,
  launch_speed_angle      INTEGER,
  estimated_ba_using_speedangle  REAL,
  estimated_woba_using_speedangle REAL,
  
  -- Win probability
  delta_home_win_exp      REAL,
  delta_run_exp           REAL,
  
  -- Weather (when available)
  temperature             REAL,
  wind_speed              REAL,
  wind_direction          TEXT,
  humidity                REAL,
  
  -- Fielding
  fielder_2               INTEGER,
  fielder_3               INTEGER,
  fielder_4               INTEGER,
  fielder_5               INTEGER,
  fielder_6               INTEGER,
  fielder_7               INTEGER,
  fielder_8               INTEGER,
  fielder_9               INTEGER,
  if_fielding_alignment   TEXT,
  of_fielding_alignment   TEXT,
  
  -- Identifiers and metadata
  sv_id                   TEXT,
  pitch_name              TEXT,
  pitch_type              CHAR(2),
  game_type               CHAR(1),
  
  -- Any additional columns will be added dynamically
  
  PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_statcast_game_date ON public.statcast(game_date);
CREATE INDEX IF NOT EXISTS idx_statcast_game_pk ON public.statcast(game_pk);
CREATE INDEX IF NOT EXISTS idx_statcast_pitcher ON public.statcast(pitcher);
CREATE INDEX IF NOT EXISTS idx_statcast_batter ON public.statcast(batter);
CREATE INDEX IF NOT EXISTS idx_statcast_events ON public.statcast(events);
CREATE INDEX IF NOT EXISTS idx_statcast_teams ON public.statcast(home_team, away_team);