-- migrations/002_foreign_keys.sql
-- Add optional foreign key relationships for data integrity
-- These are OPTIONAL - only add if you want strict referential integrity

-- Drop existing foreign keys if they exist
ALTER TABLE IF EXISTS public.games DROP CONSTRAINT IF EXISTS fk_games_game_info;
ALTER TABLE IF EXISTS public.play_by_play DROP CONSTRAINT IF EXISTS fk_pbp_game_info;
ALTER TABLE IF EXISTS public.weather DROP CONSTRAINT IF EXISTS fk_weather_game_info;
ALTER TABLE IF EXISTS public.umpires DROP CONSTRAINT IF EXISTS fk_umpires_game_info;
ALTER TABLE IF EXISTS public.lineups DROP CONSTRAINT IF EXISTS fk_lineups_game_info;

-- Add foreign key constraints (DEFERRED to handle loading order issues)
-- Games table references game_info
ALTER TABLE public.games 
ADD CONSTRAINT fk_games_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- Play by play references game_info
ALTER TABLE public.play_by_play 
ADD CONSTRAINT fk_pbp_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- Weather references game_info
ALTER TABLE public.weather 
ADD CONSTRAINT fk_weather_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- Umpires reference game_info
ALTER TABLE public.umpires 
ADD CONSTRAINT fk_umpires_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- Lineups reference game_info
ALTER TABLE public.lineups 
ADD CONSTRAINT fk_lineups_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;

-- Create constraint validation function
CREATE OR REPLACE FUNCTION validate_game_pk_references()
RETURNS TABLE(
    table_name TEXT,
    orphaned_records BIGINT,
    total_records BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'games'::TEXT, 
           COUNT(CASE WHEN gi.game_pk IS NULL THEN 1 END)::BIGINT,
           COUNT(*)::BIGINT
    FROM public.games g
    LEFT JOIN public.game_info gi ON g.game_pk = gi.game_pk
    
    UNION ALL
    
    SELECT 'play_by_play'::TEXT,
           COUNT(CASE WHEN gi.game_pk IS NULL THEN 1 END)::BIGINT,
           COUNT(*)::BIGINT
    FROM public.play_by_play pbp
    LEFT JOIN public.game_info gi ON pbp.game_pk = gi.game_pk
    
    UNION ALL
    
    SELECT 'weather'::TEXT,
           COUNT(CASE WHEN gi.game_pk IS NULL THEN 1 END)::BIGINT,
           COUNT(*)::BIGINT
    FROM public.weather w
    LEFT JOIN public.game_info gi ON w.game_pk = gi.game_pk
    
    UNION ALL
    
    SELECT 'umpires'::TEXT,
           COUNT(CASE WHEN gi.game_pk IS NULL THEN 1 END)::BIGINT,
           COUNT(*)::BIGINT
    FROM public.umpires u
    LEFT JOIN public.game_info gi ON u.game_pk = gi.game_pk
    
    UNION ALL
    
    SELECT 'lineups'::TEXT,
           COUNT(CASE WHEN gi.game_pk IS NULL THEN 1 END)::BIGINT,
           COUNT(*)::BIGINT
    FROM public.lineups l
    LEFT JOIN public.game_info gi ON l.game_pk = gi.game_pk;
END;
$$ LANGUAGE plpgsql;

-- Usage: SELECT * FROM validate_game_pk_references();
COMMENT ON FUNCTION validate_game_pk_references() IS 'Check for orphaned records that would violate foreign key constraints';