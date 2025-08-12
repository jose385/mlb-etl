-- migrations/002_foreign_keys.sql
-- STREAMLINED: Foreign keys for 7-table schema only
-- REMOVED: Weather and venue_factors constraints (tables removed)

-- Drop existing foreign keys if they exist
ALTER TABLE IF EXISTS public.games DROP CONSTRAINT IF EXISTS fk_games_game_info;
ALTER TABLE IF EXISTS public.play_by_play DROP CONSTRAINT IF EXISTS fk_pbp_game_info;
ALTER TABLE IF EXISTS public.umpires DROP CONSTRAINT IF EXISTS fk_umpires_game_info;
ALTER TABLE IF EXISTS public.lineups DROP CONSTRAINT IF EXISTS fk_lineups_game_info;

-- Set default constraint behavior for this session
SET CONSTRAINTS ALL DEFERRED;

-- Add foreign key constraints that are properly deferrable
-- These will NOT be checked until transaction commit

-- Games table references game_info (most common reference)
ALTER TABLE public.games 
ADD CONSTRAINT fk_games_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE 
DEFERRABLE INITIALLY DEFERRED;

-- Play by play references game_info  
ALTER TABLE public.play_by_play 
ADD CONSTRAINT fk_pbp_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE 
DEFERRABLE INITIALLY DEFERRED;

-- Umpires reference game_info
ALTER TABLE public.umpires 
ADD CONSTRAINT fk_umpires_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE 
DEFERRABLE INITIALLY DEFERRED;

-- Lineups reference game_info
ALTER TABLE public.lineups 
ADD CONSTRAINT fk_lineups_game_info 
FOREIGN KEY (game_pk) REFERENCES public.game_info(game_pk) 
ON DELETE CASCADE 
DEFERRABLE INITIALLY DEFERRED;

-- Create function to validate constraints manually (for debugging)
CREATE OR REPLACE FUNCTION check_foreign_key_violations()
RETURNS TABLE(
    table_name TEXT,
    constraint_name TEXT,
    violation_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'games'::TEXT as table_name, 
           'fk_games_game_info'::TEXT as constraint_name,
           COUNT(*)::BIGINT as violation_count
    FROM public.games g
    LEFT JOIN public.game_info gi ON g.game_pk = gi.game_pk
    WHERE gi.game_pk IS NULL
    
    UNION ALL
    
    SELECT 'play_by_play'::TEXT, 'fk_pbp_game_info'::TEXT, COUNT(*)::BIGINT
    FROM public.play_by_play pbp
    LEFT JOIN public.game_info gi ON pbp.game_pk = gi.game_pk
    WHERE gi.game_pk IS NULL
    
    UNION ALL
    
    SELECT 'umpires'::TEXT, 'fk_umpires_game_info'::TEXT, COUNT(*)::BIGINT
    FROM public.umpires u
    LEFT JOIN public.game_info gi ON u.game_pk = gi.game_pk
    WHERE gi.game_pk IS NULL
    
    UNION ALL
    
    SELECT 'lineups'::TEXT, 'fk_lineups_game_info'::TEXT, COUNT(*)::BIGINT
    FROM public.lineups l
    LEFT JOIN public.game_info gi ON l.game_pk = gi.game_pk
    WHERE gi.game_pk IS NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_foreign_key_violations() IS 'Check for foreign key violations in streamlined 7-table schema';