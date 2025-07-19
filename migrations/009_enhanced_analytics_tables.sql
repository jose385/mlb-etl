-- Add these tables to support enhanced analytics
-- Run as new migration: migrations/009_enhanced_analytics_tables.sql

-- Player trends tracking
CREATE TABLE IF NOT EXISTS player_trends (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_date DATE NOT NULL,
    trend_type TEXT NOT NULL, -- 'hot_streak', 'cold_streak', 'form_change'
    confidence TEXT, -- 'HIGH', 'MEDIUM', 'LOW'
    recent_avg REAL,
    season_avg REAL,
    trend_strength REAL,
    betting_insight TEXT,
    sample_size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, game_date, trend_type)
);

-- Team-level analytics cache
CREATE TABLE IF NOT EXISTS team_analytics_cache (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL,
    analysis_date DATE NOT NULL,
    analysis_type TEXT NOT NULL, -- 'handedness', 'lineup', 'late_innings'
    analysis_data JSONB,
    betting_insights TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(team_id, analysis_date, analysis_type)
);

-- Advanced statcast metrics
CREATE TABLE IF NOT EXISTS player_advanced_metrics (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    game_date DATE NOT NULL,
    expected_ba REAL,
    actual_ba REAL,
    ba_difference REAL,
    expected_woba REAL,
    actual_woba REAL,
    woba_difference REAL,
    barrel_rate REAL,
    hard_hit_rate REAL,
    luck_factor TEXT,
    contact_quality TEXT,
    betting_recommendation TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, game_date)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_player_trends_date ON player_trends(game_date);
CREATE INDEX IF NOT EXISTS idx_player_trends_player ON player_trends(player_id);
CREATE INDEX IF NOT EXISTS idx_team_analytics_team_date ON team_analytics_cache(team_id, analysis_date);
CREATE INDEX IF NOT EXISTS idx_advanced_metrics_player_date ON player_advanced_metrics(player_id, game_date);