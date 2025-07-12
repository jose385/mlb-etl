-- Allow NULLs in mlb.roster.player_id so COPY never fails on missing IDs

ALTER TABLE mlb.roster

  ALTER COLUMN player_id DROP NOT NULL;


-- (optional) add an index for faster joins on non-null players

CREATE INDEX IF NOT EXISTS idx_roster_player_id

  ON mlb.roster(player_id)

  WHERE player_id IS NOT NULL;
  
