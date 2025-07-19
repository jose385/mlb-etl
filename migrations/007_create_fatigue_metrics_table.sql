-- migrations/007_create_fatigue_metrics_table.sql
-- Player fatigue and rest metrics for performance prediction

CREATE TABLE IF NOT EXISTS public.fatigue_metrics (
  game_date DATE NOT NULL,
  game_pk INTEGER,
  team_id INTEGER NOT NULL,
  team_type TEXT,
  player_id INTEGER NOT NULL,
  position_code TEXT,
  position_name TEXT,
  player_name TEXT,
  
  team_travel_distance REAL,
  team_timezone_changes REAL,
  team_travel_fatigue_score REAL,
  team_games_in_last_7 INTEGER,
  team_consecutive_road_games INTEGER,
  
  days_since_last_appearance INTEGER,
  appearances_last_7 INTEGER,
  appearances_last_15 INTEGER,
  total_pitches_last_7 INTEGER,
  total_pitches_last_15 INTEGER,
  consecutive_appearances INTEGER,
  workload_fatigue_score REAL,
  performance_risk_score REAL,
  
  days_since_last_game INTEGER,
  games_last_7 INTEGER,
  games_last_15 INTEGER,
  at_bats_last_7 INTEGER,
  consecutive_games INTEGER,
  fatigue_score REAL,
  rest_advantage_score REAL,
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (game_date, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_fatigue_game_date ON public.fatigue_metrics(game_date);
CREATE INDEX IF NOT EXISTS idx_fatigue_player_id ON public.fatigue_metrics(player_id);
CREATE INDEX IF NOT EXISTS idx_fatigue_team_id ON public.fatigue_metrics(team_id);
CREATE INDEX IF NOT EXISTS idx_fatigue_position ON public.fatigue_metrics(position_code);
CREATE INDEX IF NOT EXISTS idx_fatigue_game_pk ON public.fatigue_metrics(game_pk);