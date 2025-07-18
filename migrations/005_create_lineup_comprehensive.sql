-- migrations/005_create_lineup_comprehensive.sql
-- Fixed lineup table with corrected indexes

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
  
  -- Player information (from players.ID{player_id} object)
  person_full_name         TEXT,
  person_link              TEXT,
  person_first_name        TEXT,
  person_last_name         TEXT,
  person_primary_number    TEXT,
  person_birth_date        DATE,
  person_current_age       INTEGER,
  person_birth_city        TEXT,
  person_birth_state_province TEXT,
  person_birth_country     TEXT,
  person_height            TEXT,
  person_weight            INTEGER,
  person_active            BOOLEAN,
  person_use_name          TEXT,
  person_boxscore_name     TEXT,
  person_name_slug         TEXT,
  
  -- Playing characteristics
  person_bat_side_code        TEXT,
  person_bat_side_description TEXT,
  person_pitch_hand_code      TEXT,
  person_pitch_hand_description TEXT,
  
  -- Career information
  person_mlb_debut_date    DATE,
  person_is_player         BOOLEAN,
  person_is_verified       BOOLEAN,
  
  -- Primary position
  person_primary_position_code TEXT,
  person_primary_position_name TEXT,
  person_primary_position_type TEXT,
  person_primary_position_abbreviation TEXT,
  
  -- Current team
  person_current_team_id   INTEGER,
  person_current_team_name TEXT,
  
  -- Strike zone
  person_strike_zone_top   REAL,
  person_strike_zone_bottom REAL,
  
  -- Game-specific batting stats (from stats object if available)
  stats_batting_games_played        INTEGER,
  stats_batting_ground_outs         INTEGER,
  stats_batting_air_outs            INTEGER,
  stats_batting_runs                INTEGER,
  stats_batting_doubles             INTEGER,
  stats_batting_triples             INTEGER,
  stats_batting_home_runs           INTEGER,
  stats_batting_strike_outs         INTEGER,
  stats_batting_base_on_balls       INTEGER,
  stats_batting_intentional_walks   INTEGER,
  stats_batting_hits                INTEGER,
  stats_batting_hit_by_pitch        INTEGER,
  stats_batting_avg                 REAL,
  stats_batting_at_bats             INTEGER,
  stats_batting_obp                 REAL,
  stats_batting_slg                 REAL,
  stats_batting_ops                 REAL,
  stats_batting_caught_stealing     INTEGER,
  stats_batting_stolen_bases        INTEGER,
  stats_batting_stolen_base_percentage REAL,
  stats_batting_ground_into_double_play INTEGER,
  stats_batting_number_of_pitches   INTEGER,
  stats_batting_plate_appearances   INTEGER,
  stats_batting_total_bases         INTEGER,
  stats_batting_rbi                 INTEGER,
  stats_batting_left_on_base        INTEGER,
  stats_batting_sac_bunts           INTEGER,
  stats_batting_sac_flies           INTEGER,
  stats_batting_babip               REAL,
  stats_batting_ground_outs_to_air_outs REAL,
  stats_batting_catchers_interference INTEGER,
  stats_batting_at_bats_per_home_run REAL,
  
  -- Pitching stats (for pitchers in lineup)
  stats_pitching_games_played       INTEGER,
  stats_pitching_games_started      INTEGER,
  stats_pitching_ground_outs        INTEGER,
  stats_pitching_air_outs           INTEGER,
  stats_pitching_runs               INTEGER,
  stats_pitching_doubles            INTEGER,
  stats_pitching_triples            INTEGER,
  stats_pitching_home_runs          INTEGER,
  stats_pitching_strike_outs        INTEGER,
  stats_pitching_base_on_balls      INTEGER,
  stats_pitching_intentional_walks  INTEGER,
  stats_pitching_hits               INTEGER,
  stats_pitching_hit_by_pitch       INTEGER,
  stats_pitching_avg                REAL,
  stats_pitching_at_bats            INTEGER,
  stats_pitching_obp                REAL,
  stats_pitching_slg                REAL,
  stats_pitching_ops                REAL,
  stats_pitching_caught_stealing    INTEGER,
  stats_pitching_stolen_bases       INTEGER,
  stats_pitching_stolen_base_percentage REAL,
  stats_pitching_ground_into_double_play INTEGER,
  stats_pitching_number_of_pitches  INTEGER,
  stats_pitching_era                REAL,
  stats_pitching_innings_pitched    TEXT,
  stats_pitching_wins               INTEGER,
  stats_pitching_losses             INTEGER,
  stats_pitching_saves              INTEGER,
  stats_pitching_save_opportunities INTEGER,
  stats_pitching_holds              INTEGER,
  stats_pitching_blown_saves        INTEGER,
  stats_pitching_earned_runs        INTEGER,
  stats_pitching_whip               REAL,
  stats_pitching_batters_faced      INTEGER,
  stats_pitching_outs_pitched       INTEGER,
  stats_pitching_games_pitched      INTEGER,
  stats_pitching_complete_games     INTEGER,
  stats_pitching_shutouts           INTEGER,
  stats_pitching_strikes            INTEGER,
  stats_pitching_strike_percentage  REAL,
  stats_pitching_hit_batsmen        INTEGER,
  stats_pitching_balks              INTEGER,
  stats_pitching_wild_pitches       INTEGER,
  stats_pitching_pickoffs           INTEGER,
  stats_pitching_total_bases        INTEGER,
  stats_pitching_ground_outs_to_air_outs REAL,
  stats_pitching_wins_above_replacement REAL,
  stats_pitching_pitches_per_inning REAL,
  stats_pitching_games_finished     INTEGER,
  stats_pitching_strikeout_walk_ratio REAL,
  stats_pitching_strikeouts_per_nine_innings REAL,
  stats_pitching_walks_per_nine_innings REAL,
  stats_pitching_hits_per_nine_innings REAL,
  stats_pitching_run_support_per_nine_innings REAL,
  stats_pitching_runs_per_nine_innings REAL,
  stats_pitching_home_runs_per_nine_innings REAL,
  
  -- Fielding stats
  stats_fielding_assists            INTEGER,
  stats_fielding_put_outs           INTEGER,
  stats_fielding_errors             INTEGER,
  stats_fielding_chances            INTEGER,
  stats_fielding_fielding           REAL,
  stats_fielding_position_code      TEXT,
  stats_fielding_position_name      TEXT,
  stats_fielding_position_abbreviation TEXT,
  
  -- Season info
  stats_season                      INTEGER,
  stats_team_id                     INTEGER,
  stats_team_name                   TEXT,
  
  PRIMARY KEY (game_pk, team_id, batting_order)
);

-- Basic indexes (no conditional WHERE clauses that could cause issues)
CREATE INDEX IF NOT EXISTS idx_lineup_game_date ON public.lineup(game_date);
CREATE INDEX IF NOT EXISTS idx_lineup_game_pk ON public.lineup(game_pk);
CREATE INDEX IF NOT EXISTS idx_lineup_team_id ON public.lineup(team_id);
CREATE INDEX IF NOT EXISTS idx_lineup_person_id ON public.lineup(person_id);
CREATE INDEX IF NOT EXISTS idx_lineup_batting_order ON public.lineup(batting_order);
CREATE INDEX IF NOT EXISTS idx_lineup_position ON public.lineup(position_code);
CREATE INDEX IF NOT EXISTS idx_lineup_side ON public.lineup(side);

-- Composite indexes for analysis
CREATE INDEX IF NOT EXISTS idx_lineup_team_order ON public.lineup(team_id, batting_order);
CREATE INDEX IF NOT EXISTS idx_lineup_game_team ON public.lineup(game_pk, team_id, side);