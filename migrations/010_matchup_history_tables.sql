-- migrations/010_matchup_history_tables.sql
-- Player vs Pitcher matchup history and pitch tunneling tables

-- Batter vs Pitcher historical matchups
CREATE TABLE IF NOT EXISTS public.batter_pitcher_matchups (
    id SERIAL PRIMARY KEY,
    batter_id INTEGER NOT NULL,
    pitcher_id INTEGER NOT NULL,
    batter_name TEXT,
    pitcher_name TEXT,
    
    -- Time period for this analysis
    analysis_start_date DATE,
    analysis_end_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Basic counting stats
    plate_appearances INTEGER DEFAULT 0,
    at_bats INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    home_runs INTEGER DEFAULT 0,
    doubles INTEGER DEFAULT 0,
    triples INTEGER DEFAULT 0,
    walks INTEGER DEFAULT 0,
    hit_by_pitch INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    
    -- Advanced metrics
    batting_avg REAL,
    on_base_pct REAL,
    slugging_pct REAL,
    ops REAL,
    woba REAL,
    xwoba REAL,
    
    -- Expected stats
    expected_batting_avg REAL,
    expected_slugging REAL,
    
    -- Contact quality
    avg_exit_velocity REAL,
    max_exit_velocity REAL,
    avg_launch_angle REAL,
    barrel_rate REAL,
    hard_hit_rate REAL,
    
    -- Pitch-specific performance
    fastball_ops REAL,
    breaking_ball_ops REAL,
    offspeed_ops REAL,
    
    -- Situational performance
    risp_avg REAL,
    two_strike_ops REAL,
    late_count_ops REAL,
    
    -- Recency weighting (more recent games weighted higher)
    weighted_ops REAL,
    recent_form_factor REAL, -- 1.0 = normal, >1.0 = hot, <1.0 = cold
    
    -- Statistical significance
    sample_size_rating TEXT, -- 'LARGE', 'MEDIUM', 'SMALL', 'INSUFFICIENT'
    confidence_level REAL,
    
    -- Betting insights
    betting_edge_strength REAL, -- 0-100 scale
    betting_recommendation TEXT,
    edge_description TEXT,
    
    UNIQUE(batter_id, pitcher_id, analysis_end_date)
);

-- Pitch tunneling analysis table
CREATE TABLE IF NOT EXISTS public.pitch_tunneling (
    id SERIAL PRIMARY KEY,
    pitcher_id INTEGER NOT NULL,
    pitcher_name TEXT,
    game_date DATE,
    game_pk INTEGER,
    
    -- Pitch pair being analyzed for tunneling
    pitch_type_1 TEXT NOT NULL, -- e.g., 'FF' (4-seam fastball)
    pitch_type_2 TEXT NOT NULL, -- e.g., 'SL' (slider)
    
    -- Release point similarity (key for tunneling)
    release_point_diff_x REAL, -- Horizontal difference in inches
    release_point_diff_y REAL, -- Vertical difference in inches
    release_point_diff_z REAL, -- Depth difference in inches
    release_point_similarity REAL, -- 0-100 scale
    
    -- Tunnel point analysis (where pitches start to separate)
    tunnel_break_distance REAL, -- Distance from plate when pitches separate
    tunnel_quality_score REAL, -- 0-100, higher = better tunneling
    
    -- Movement differential at plate
    horizontal_break_diff REAL, -- Difference in horizontal movement
    vertical_break_diff REAL,   -- Difference in vertical movement
    movement_contrast REAL,     -- How different the movements are
    
    -- Velocity differential
    velocity_diff REAL,         -- Speed difference between pitches
    velocity_similarity REAL,   -- How similar speeds are out of hand
    
    -- Usage patterns
    pitch_1_usage_rate REAL,    -- How often pitch 1 is thrown
    pitch_2_usage_rate REAL,    -- How often pitch 2 is thrown
    sequence_frequency REAL,    -- How often thrown in sequence
    
    -- Effectiveness metrics
    whiff_rate_improvement REAL, -- How much tunneling improves whiff rate
    chase_rate_improvement REAL, -- How much it improves chase rate on pitch 2
    called_strike_rate_diff REAL,
    
    -- Opponent performance against tunnel
    opponent_avg_vs_tunnel REAL,
    opponent_slugging_vs_tunnel REAL,
    opponent_whiff_rate REAL,
    
    -- Sample size and confidence
    pitch_1_count INTEGER,
    pitch_2_count INTEGER,
    tunneling_sequences INTEGER,
    statistical_confidence TEXT, -- 'HIGH', 'MEDIUM', 'LOW'
    
    -- Betting implications
    strikeout_prop_impact REAL, -- How this affects K prop betting
    betting_insight TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pitcher_id, game_date, pitch_type_1, pitch_type_2)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_matchups_batter ON public.batter_pitcher_matchups(batter_id);
CREATE INDEX IF NOT EXISTS idx_matchups_pitcher ON public.batter_pitcher_matchups(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_matchups_updated ON public.batter_pitcher_matchups(last_updated);
CREATE INDEX IF NOT EXISTS idx_matchups_sample_size ON public.batter_pitcher_matchups(sample_size_rating);
CREATE INDEX IF NOT EXISTS idx_matchups_betting_edge ON public.batter_pitcher_matchups(betting_edge_strength) WHERE betting_edge_strength > 15;

CREATE INDEX IF NOT EXISTS idx_tunneling_pitcher ON public.pitch_tunneling(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_tunneling_date ON public.pitch_tunneling(game_date);
CREATE INDEX IF NOT EXISTS idx_tunneling_quality ON public.pitch_tunneling(tunnel_quality_score) WHERE tunnel_quality_score > 70;
CREATE INDEX IF NOT EXISTS idx_tunneling_pitches ON public.pitch_tunneling(pitch_type_1, pitch_type_2);

-- Comments for documentation
COMMENT ON TABLE public.batter_pitcher_matchups IS 'Historical performance data for specific batter vs pitcher matchups';
COMMENT ON TABLE public.pitch_tunneling IS 'Advanced pitch tunneling analysis for deception and effectiveness';
COMMENT ON COLUMN public.batter_pitcher_matchups.betting_edge_strength IS 'Strength of betting edge (0-100), higher values indicate stronger opportunities';
COMMENT ON COLUMN public.pitch_tunneling.tunnel_quality_score IS 'Quality of pitch tunneling (0-100), measures how well pitches tunnel together';