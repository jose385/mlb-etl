-- migrations/007_create_fatigue_metrics_table.sql
-- Player fatigue and rest metrics for performance prediction

CREATE TABLE IF NOT EXISTS public.fatigue_metrics (
  -- Game and player identifiers
  game_date DATE NOT NULL,
  game_pk INTEGER,
  team_id INTEGER NOT NULL,
  team_type TEXT, -- 'home' or 'away'
  player_id INTEGER NOT NULL,
  position_code TEXT,
  position_name TEXT,
  player_name TEXT,
  
  -- Team travel fatigue metrics
  team_travel_distance REAL,
  team_timezone_changes REAL,
  team_travel_fatigue_score REAL,
  team_games_in_last_7 INTEGER,
  team_consecutive_road_games INTEGER,
  
  -- Pitcher-specific fatigue metrics
  days_since_last_appearance INTEGER,
  appearances_last_7 INTEGER,
  appearances_last_15 INTEGER,
  total_pitches_last_7 INTEGER,
  total_pitches_last_15 INTEGER,
  consecutive_appearances INTEGER,
  workload_fatigue_score REAL,
  performance_risk_score REAL,
  
  -- Batter-specific fatigue metrics  
  days_since_last_game INTEGER,
  games_last_7 INTEGER,
  games_last_15 INTEGER,
  at_bats_last_7 INTEGER,
  consecutive_games INTEGER,
  fatigue_score REAL,
  rest_advantage_score REAL,
  
  -- Data quality and timestamps
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (game_date, team_id, player_id)
);

-- Indexes for analysis queries
CREATE INDEX IF NOT EXISTS idx_fatigue_game_date ON public.fatigue_metrics(game_date);
CREATE INDEX IF NOT EXISTS idx_fatigue_player_id ON public.fatigue_metrics(player_id);
CREATE INDEX IF NOT EXISTS idx_fatigue_team_id ON public.fatigue_metrics(team_id);
CREATE INDEX IF NOT EXISTS idx_fatigue_position ON public.fatigue_metrics(position_code);
CREATE INDEX IF NOT EXISTS idx_fatigue_game_pk ON public.fatigue_metrics(game_pk);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fatigue_pitcher_workload ON public.fatigue_metrics(position_code, workload_fatigue_score) 
WHERE position_code = '1';

CREATE INDEX IF NOT EXISTS idx_fatigue_batter_rest ON public.fatigue_metrics(position_code, rest_advantage_score) 
WHERE position_code != '1';

CREATE INDEX IF NOT EXISTS idx_fatigue_team_travel ON public.fatigue_metrics(team_id, team_travel_fatigue_score);

-- Comments for documentation
COMMENT ON TABLE public.fatigue_metrics IS 'Player fatigue and rest metrics for predicting performance degradation';
COMMENT ON COLUMN public.fatigue_metrics.workload_fatigue_score IS 'Pitcher fatigue score based on recent workload (0-100, higher = more fatigued)';
COMMENT ON COLUMN public.fatigue_metrics.performance_risk_score IS 'Risk of performance degradation due to fatigue (0-100, higher = higher risk)';
COMMENT ON COLUMN public.fatigue_metrics.rest_advantage_score IS 'Advantage from rest for position players (0-100, higher = better rested)';
COMMENT ON COLUMN public.fatigue_metrics.team_travel_fatigue_score IS 'Team fatigue from travel (0-100, higher = more fatigued)';