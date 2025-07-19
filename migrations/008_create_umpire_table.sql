-- migrations/008_create_umpire_table.sql
-- Umpire data table for game officials and their tendencies

CREATE TABLE IF NOT EXISTS public.umpires (
  -- Game identifiers
  game_date DATE NOT NULL,
  game_pk INTEGER NOT NULL,
  
  -- Umpire identification
  umpire_id INTEGER,
  umpire_name TEXT,
  position TEXT,  -- "Home Plate", "First Base", "Second Base", "Third Base"
  
  -- Basic umpire info
  first_name TEXT,
  last_name TEXT,
  full_name TEXT,
  
  -- Historical performance metrics (calculated from past games)
  total_games_officiated INTEGER DEFAULT 0,
  strikes_called_per_game REAL,
  balls_called_per_game REAL,
  total_pitches_per_game REAL,
  
  -- Strike zone tendencies (critical for betting)
  strike_rate_overall REAL,           -- % of borderline pitches called strikes
  strike_rate_low_zone REAL,          -- Bottom of zone
  strike_rate_high_zone REAL,         -- Top of zone
  strike_rate_outside_zone REAL,      -- Outside traditional zone
  strike_rate_inside_zone REAL,       -- Inside traditional zone
  
  -- Game pace and betting impact
  avg_game_length_minutes INTEGER,
  avg_total_runs_in_games REAL,       -- Historical run totals with this ump
  over_under_record REAL,             -- % of games that went OVER
  
  -- Advanced tendencies
  pitcher_friendly_score REAL,        -- 0-100 scale, higher = more pitcher friendly
  consistency_score REAL,             -- How consistent their zone is
  
  -- Situational tendencies
  late_inning_strike_rate REAL,       -- Strike rate in innings 7+
  close_game_strike_rate REAL,        -- Strike rate in 1-run games
  runners_on_strike_rate REAL,        -- Strike rate with RISP
  
  -- Data quality and tracking
  sample_size INTEGER DEFAULT 0,      -- Number of games in calculations
  last_calculated DATE,               -- When metrics were last updated
  data_source TEXT DEFAULT 'mlb_api',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (game_pk, umpire_id)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_umpires_game_date ON public.umpires(game_date);
CREATE INDEX IF NOT EXISTS idx_umpires_name ON public.umpires(umpire_name);
CREATE INDEX IF NOT EXISTS idx_umpires_position ON public.umpires(position);
CREATE INDEX IF NOT EXISTS idx_umpires_home_plate ON public.umpires(position) WHERE position = 'Home Plate';
CREATE INDEX IF NOT EXISTS idx_umpires_over_under ON public.umpires(over_under_record);
CREATE INDEX IF NOT EXISTS idx_umpires_pitcher_friendly ON public.umpires(pitcher_friendly_score);

-- Composite indexes for betting analysis
CREATE INDEX IF NOT EXISTS idx_umpires_betting_analysis ON public.umpires(position, over_under_record, avg_total_runs_in_games);
CREATE INDEX IF NOT EXISTS idx_umpires_recent_games ON public.umpires(umpire_name, game_date DESC);

-- Comments for documentation
COMMENT ON TABLE public.umpires IS 'MLB umpire assignments and historical tendencies for betting analysis';
COMMENT ON COLUMN public.umpires.over_under_record IS 'Percentage of games that went OVER the total with this umpire (critical for totals betting)';
COMMENT ON COLUMN public.umpires.pitcher_friendly_score IS 'Score 0-100 indicating how pitcher-friendly this umpire is (higher = more pitcher friendly)';
COMMENT ON COLUMN public.umpires.strike_rate_overall IS 'Percentage of borderline pitches called as strikes (major impact on game flow)';