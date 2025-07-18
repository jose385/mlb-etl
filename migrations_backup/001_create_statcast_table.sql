-- migrations/001_create_statcast_table.sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.statcast (
  game_date               DATE              NOT NULL,
  game_pk                 BIGINT            NOT NULL,
  at_bat_number           INTEGER           NOT NULL,
  pitcher                 INTEGER           NOT NULL,
  batter                  INTEGER           NOT NULL,
  balls                   SMALLINT          NOT NULL,
  strikes                 SMALLINT          NOT NULL,
  plate_x                 REAL,
  plate_z                 REAL,
  release_speed           REAL,
  release_pos_x           REAL,
  release_pos_z           REAL,
  spin_rate               REAL,
  plate_time              REAL,
  pitch_number            SMALLINT          NOT NULL,
  events                  TEXT,
  description             TEXT,
  stand                   CHAR(1),
  p_throws                CHAR(1),
  home_team               CHAR(3),
  away_team               CHAR(3),
  PRIMARY KEY (game_pk, at_bat_number, pitch_number)
);
