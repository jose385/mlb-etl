-- ============================================================
-- MLB BallDontLie Schema v3
-- All fields verified against live docs at mlb.balldontlie.io
-- Fixes: injuries PK, stats game_date removed, player team fields,
--        lineup full player fields, missing indexes
-- ============================================================

-- Teams
CREATE TABLE IF NOT EXISTS mlb_teams (
    team_id             INTEGER PRIMARY KEY,
    slug                TEXT,
    abbreviation        TEXT,
    display_name        TEXT,           -- "Los Angeles Dodgers"
    short_display_name  TEXT,           -- "Dodgers"
    name                TEXT,           -- "Dodgers"
    location            TEXT,           -- "Los Angeles"
    league              TEXT,           -- American / National
    division            TEXT,           -- East / Central / West
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Players
CREATE TABLE IF NOT EXISTS mlb_players (
    player_id           INTEGER PRIMARY KEY,
    first_name          TEXT,
    last_name           TEXT,
    full_name           TEXT,
    position            TEXT,
    bats_throws         TEXT,           -- e.g. "Left/Right"
    jersey              TEXT,
    college             TEXT,
    birth_place         TEXT,
    dob                 TEXT,           -- formatted as string by API e.g. "5/7/1994"
    age                 INTEGER,
    height              TEXT,
    weight              TEXT,
    draft               TEXT,           -- e.g. "2007: Rd 2, Pk 78 (ATL)"
    debut_year          INTEGER,
    active              BOOLEAN,
    -- Denormalized current team (join mlb_teams for full team data)
    team_id             INTEGER REFERENCES mlb_teams(team_id),
    team_name           TEXT,           -- display_name: "Los Angeles Dodgers"
    team_short_name     TEXT,           -- name: "Dodgers"
    team_abbr           TEXT,           -- "LAD"
    team_location       TEXT,           -- "Los Angeles"
    team_league         TEXT,           -- "National"
    team_division       TEXT,           -- "West"
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Games
CREATE TABLE IF NOT EXISTS mlb_games (
    game_id             INTEGER PRIMARY KEY,
    date                TIMESTAMP,
    season              INTEGER,
    season_type         TEXT,           -- spring_training / regular / postseason
    postseason          BOOLEAN,
    status              TEXT,           -- STATUS_FINAL, STATUS_IN_PROGRESS, etc.
    period              INTEGER,        -- current inning number
    clock               INTEGER,
    display_clock       TEXT,
    venue               TEXT,
    attendance          INTEGER,
    conference_play     BOOLEAN,
    -- Top-level name strings returned by API
    home_team_name_str  TEXT,           -- "New York Yankees"
    away_team_name_str  TEXT,           -- "Los Angeles Dodgers"
    -- Home team
    home_team_id        INTEGER REFERENCES mlb_teams(team_id),
    home_team_name      TEXT,
    home_team_abbr      TEXT,
    -- Away team
    away_team_id        INTEGER REFERENCES mlb_teams(team_id),
    away_team_name      TEXT,
    away_team_abbr      TEXT,
    -- Scores from home_team_data / away_team_data nested objects
    home_runs           INTEGER,
    home_hits           INTEGER,
    home_errors         INTEGER,
    away_runs           INTEGER,
    away_hits           INTEGER,
    away_errors         INTEGER,
    -- Inning-by-inning as JSON arrays e.g. "[0, 1, 4, 0, 0, 1, 0, 5]"
    home_inning_scores  TEXT,
    away_inning_scores  TEXT,
    -- Scoring summary as JSON array of play objects
    scoring_summary     TEXT,
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Player Game Stats
-- NOTE: API returns game_id but NOT game date — join to mlb_games for date
-- Composite PK: one row per player per game
CREATE TABLE IF NOT EXISTS mlb_stats (
    game_id                     INTEGER REFERENCES mlb_games(game_id),
    player_id                   INTEGER REFERENCES mlb_players(player_id),
    season                      INTEGER,
    player_name                 TEXT,
    player_position             TEXT,
    player_jersey               TEXT,
    player_bats_throws          TEXT,
    team_name                   TEXT,
    -- Batting
    at_bats                     INTEGER,
    runs                        INTEGER,
    hits                        INTEGER,
    doubles                     INTEGER,
    triples                     INTEGER,
    hr                          INTEGER,
    rbi                         INTEGER,
    bb                          INTEGER,
    k                           INTEGER,
    avg                         NUMERIC(6,4),
    obp                         NUMERIC(6,4),
    slg                         NUMERIC(6,4),
    intentional_walks           INTEGER,
    hit_by_pitch                INTEGER,
    stolen_bases                INTEGER,
    caught_stealing             INTEGER,
    plate_appearances           INTEGER,
    total_bases                 INTEGER,
    left_on_base                INTEGER,
    fly_outs                    INTEGER,
    ground_outs                 INTEGER,
    line_outs                   INTEGER,
    pop_outs                    INTEGER,
    air_outs                    INTEGER,
    gidp                        INTEGER,
    sac_bunts                   INTEGER,
    sac_flies                   INTEGER,
    -- Pitching
    ip                          NUMERIC(6,1),
    p_hits                      INTEGER,
    p_runs                      INTEGER,
    er                          INTEGER,
    p_bb                        INTEGER,
    p_k                         INTEGER,
    p_hr                        INTEGER,
    pitch_count                 INTEGER,
    strikes                     INTEGER,
    era                         NUMERIC(7,3),
    batters_faced               INTEGER,
    pitching_outs               INTEGER,
    wins                        INTEGER,
    losses                      INTEGER,
    saves                       INTEGER,
    holds                       INTEGER,
    blown_saves                 INTEGER,
    games_started               INTEGER,
    wild_pitches                INTEGER,
    balks                       INTEGER,
    pitching_hbp                INTEGER,
    inherited_runners           INTEGER,
    inherited_runners_scored    INTEGER,
    -- Fielding
    putouts                     INTEGER,
    assists                     INTEGER,
    errors                      INTEGER,
    fielding_chances            INTEGER,
    fielding_pct                NUMERIC(6,4),
    updated_at                  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (game_id, player_id)
);

-- Standings
-- NOTE: live API response has significantly more fields than the OpenAPI spec
CREATE TABLE IF NOT EXISTS mlb_standings (
    team_id                     INTEGER REFERENCES mlb_teams(team_id),
    season                      INTEGER,
    team_name                   TEXT,
    team_abbr                   TEXT,
    league_name                 TEXT,
    league_short_name           TEXT,
    division_name               TEXT,
    division_short_name         TEXT,
    -- Win/loss record
    wins                        INTEGER,
    losses                      INTEGER,
    ties                        INTEGER,
    win_percent                 NUMERIC(10,8),
    games_played                INTEGER,
    -- Games back
    games_behind                NUMERIC(5,1),
    division_games_behind       NUMERIC(5,1),
    league_games_back           NUMERIC(5,1),
    sport_games_back            NUMERIC(5,1),
    conference_games_back       NUMERIC(5,1),
    -- Win % context
    league_win_percent          NUMERIC(10,8),
    division_win_percent        NUMERIC(10,8),
    -- Playoff / seeding
    playoff_seed                INTEGER,
    playoff_percent             NUMERIC(6,2),
    wildcard_percent            NUMERIC(6,2),
    clincher                    TEXT,           -- "*" / "x" / "y" etc.
    clinch_indicator            TEXT,
    magic_number_division       INTEGER,
    magic_number_wildcard       INTEGER,
    elimination_number          INTEGER,
    wild_card_elimination_number INTEGER,
    -- Streak
    streak                      INTEGER,
    last_ten_games              TEXT,           -- "5-5"
    -- Runs / points
    points_for                  INTEGER,        -- runs scored
    points_against              INTEGER,        -- runs allowed
    avg_points_for              NUMERIC(10,7),
    avg_points_against          NUMERIC(10,7),
    point_differential          INTEGER,
    differential                NUMERIC(12,7),
    game_back_points            INTEGER,
    -- Legacy OpenAPI fields (may or may not appear in response)
    runs_scored                 INTEGER,
    runs_allowed                INTEGER,
    run_differential            INTEGER,
    -- Home / road / OT splits
    home_wins                   INTEGER,
    home_losses                 INTEGER,
    home_ties                   INTEGER,
    road_wins                   INTEGER,
    road_losses                 INTEGER,
    road_ties                   INTEGER,
    ot_wins                     INTEGER,
    ot_losses                   INTEGER,
    -- Record strings
    total                       TEXT,           -- "94-68"
    home                        TEXT,           -- "44-37"
    road                        TEXT,           -- "50-31"
    intra_division              TEXT,           -- "26-26"
    intra_league                TEXT,           -- "71-45"
    -- Division tracking
    division_percent            NUMERIC(6,2),
    division_tied               INTEGER,
    league_rank                 INTEGER,
    sport_rank                  INTEGER,
    division_rank               INTEGER,
    division_leader             BOOLEAN,
    division_champ              BOOLEAN,
    last_updated                TEXT,
    updated_at                  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (team_id, season)
);

-- Player Season Stats (batting + pitching + fielding aggregates)
CREATE TABLE IF NOT EXISTS mlb_season_stats (
    player_id           INTEGER REFERENCES mlb_players(player_id),
    season              INTEGER,
    season_type         TEXT,           -- regular / postseason
    postseason          BOOLEAN,
    player_name         TEXT,
    position            TEXT,
    team_name           TEXT,
    -- Batting
    batting_gp          INTEGER,
    batting_ab          INTEGER,
    batting_r           INTEGER,
    batting_h           INTEGER,
    batting_2b          INTEGER,
    batting_3b          INTEGER,
    batting_hr          INTEGER,
    batting_rbi         INTEGER,
    batting_tb          INTEGER,
    batting_bb          INTEGER,
    batting_so          INTEGER,
    batting_sb          INTEGER,
    batting_avg         NUMERIC(8,7),
    batting_obp         NUMERIC(8,7),
    batting_slg         NUMERIC(8,7),
    batting_ops         NUMERIC(8,7),
    batting_war         NUMERIC(6,2),
    -- Pitching
    pitching_gp         INTEGER,
    pitching_gs         INTEGER,
    pitching_qs         INTEGER,
    pitching_w          INTEGER,
    pitching_l          INTEGER,
    pitching_era        NUMERIC(7,3),
    pitching_sv         INTEGER,
    pitching_hld        INTEGER,
    pitching_ip         NUMERIC(7,1),
    pitching_h          INTEGER,
    pitching_er         INTEGER,
    pitching_hr         INTEGER,
    pitching_bb         INTEGER,
    pitching_whip       NUMERIC(7,4),
    pitching_k          INTEGER,
    pitching_k_per_9    NUMERIC(6,3),
    pitching_war        NUMERIC(6,2),
    -- Fielding
    fielding_gp         INTEGER,
    fielding_gs         INTEGER,
    fielding_fip        NUMERIC(6,3),
    fielding_tc         INTEGER,
    fielding_po         INTEGER,
    fielding_a          INTEGER,
    fielding_fp         NUMERIC(8,7),
    fielding_e          INTEGER,
    fielding_dp         INTEGER,
    fielding_rf         NUMERIC(6,3),
    fielding_dwar       NUMERIC(6,2),
    fielding_pb         INTEGER,
    fielding_cs         INTEGER,
    fielding_cs_percent NUMERIC(6,4),
    fielding_sba        INTEGER,
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (player_id, season, season_type)
);

-- Team Season Stats
CREATE TABLE IF NOT EXISTS mlb_team_season_stats (
    team_id         INTEGER REFERENCES mlb_teams(team_id),
    season          INTEGER,
    season_type     TEXT,
    postseason      BOOLEAN,
    team_name       TEXT,
    team_abbr       TEXT,
    gp              INTEGER,
    -- Batting
    batting_ab      INTEGER,
    batting_r       INTEGER,
    batting_h       INTEGER,
    batting_2b      INTEGER,
    batting_3b      INTEGER,
    batting_hr      INTEGER,
    batting_rbi     INTEGER,
    batting_tb      INTEGER,
    batting_bb      INTEGER,
    batting_so      INTEGER,
    batting_sb      INTEGER,
    batting_avg     NUMERIC(8,7),
    batting_obp     NUMERIC(8,7),
    batting_slg     NUMERIC(8,7),
    batting_ops     NUMERIC(8,7),
    -- Pitching
    pitching_w      INTEGER,
    pitching_l      INTEGER,
    pitching_era    NUMERIC(7,3),
    pitching_sv     INTEGER,
    pitching_cg     INTEGER,
    pitching_sho    INTEGER,
    pitching_qs     INTEGER,
    pitching_ip     NUMERIC(7,1),
    pitching_h      INTEGER,
    pitching_er     INTEGER,
    pitching_hr     INTEGER,
    pitching_bb     INTEGER,
    pitching_k      INTEGER,
    pitching_oba    NUMERIC(8,7),
    pitching_whip   NUMERIC(7,4),
    -- Fielding
    fielding_e      INTEGER,
    fielding_fp     NUMERIC(8,7),
    fielding_tc     INTEGER,
    fielding_po     INTEGER,
    fielding_a      INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (team_id, season, season_type)
);

-- Player Splits
-- split_category values observed: byArena, byOpponent, byDayMonth, bySituation, etc.
CREATE TABLE IF NOT EXISTS mlb_splits (
    player_id           INTEGER REFERENCES mlb_players(player_id),
    season              INTEGER,
    category            TEXT,           -- batting / pitching
    split_category      TEXT,           -- byArena / byOpponent / byDayMonth / etc.
    split_name          TEXT,           -- "vs RHP" / "April" / "Dodger Stadium" / etc.
    split_abbreviation  TEXT,
    player_name         TEXT,
    -- Batting splits
    at_bats             INTEGER,
    runs                INTEGER,
    hits                INTEGER,
    doubles             INTEGER,
    triples             INTEGER,
    home_runs           INTEGER,
    rbis                INTEGER,
    walks               INTEGER,
    hit_by_pitch        INTEGER,
    strikeouts          INTEGER,
    stolen_bases        INTEGER,
    caught_stealing     INTEGER,
    avg                 NUMERIC(6,4),
    obp                 NUMERIC(6,4),
    slg                 NUMERIC(6,4),
    ops                 NUMERIC(6,4),
    -- Pitching splits
    era                 NUMERIC(7,3),
    wins                INTEGER,
    losses              INTEGER,
    saves               INTEGER,
    save_opportunities  INTEGER,
    games_played        INTEGER,
    games_started       INTEGER,
    complete_games      INTEGER,
    innings_pitched     NUMERIC(7,1),
    hits_allowed        INTEGER,
    runs_allowed        INTEGER,
    earned_runs         INTEGER,
    home_runs_allowed   INTEGER,
    walks_allowed       INTEGER,
    strikeouts_pitched  INTEGER,
    opponent_avg        NUMERIC(6,4),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (player_id, season, category, split_category, split_name)
);

-- Batter vs Pitcher Matchups
CREATE TABLE IF NOT EXISTS mlb_versus (
    player_id           INTEGER REFERENCES mlb_players(player_id),
    opponent_id         INTEGER REFERENCES mlb_players(player_id),
    opponent_team_id    INTEGER REFERENCES mlb_teams(team_id),
    player_name         TEXT,
    opponent_name       TEXT,
    opponent_team       TEXT,
    at_bats             INTEGER,
    hits                INTEGER,
    doubles             INTEGER,
    triples             INTEGER,
    home_runs           INTEGER,
    rbi                 INTEGER,
    walks               INTEGER,
    strikeouts          INTEGER,
    avg                 NUMERIC(6,4),
    obp                 NUMERIC(6,4),
    slg                 NUMERIC(6,4),
    ops                 NUMERIC(6,4),
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (player_id, opponent_id, opponent_team_id)
);

-- Play-by-Play
-- Composite PK: one row per play per game (no play_id in API)
CREATE TABLE IF NOT EXISTS mlb_plays (
    game_id             INTEGER REFERENCES mlb_games(game_id),
    "order"             INTEGER,        -- sequential play number
    type                TEXT,           -- "Start Batter/Pitcher" / "Play Result" / "Fly Out" etc.
    text                TEXT,           -- human-readable description
    inning              INTEGER,
    inning_type         TEXT,           -- "Top" / "Bottom"
    outs                INTEGER,        -- number of outs AFTER this play
    balls               INTEGER,        -- ball count at time of play
    strikes             INTEGER,        -- strike count at time of play
    home_score          INTEGER,
    away_score          INTEGER,
    scoring_play        BOOLEAN,
    score_value         INTEGER,
    batter_id           INTEGER,
    pitcher_id          INTEGER,
    pitch_type          TEXT,
    pitch_velocity      NUMERIC(5,1),
    hit_coordinate_x    NUMERIC(8,3),
    hit_coordinate_y    NUMERIC(8,3),
    trajectory          TEXT,           -- "F" fly / "G" ground / "L" line drive
    updated_at          TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (game_id, "order")
);

-- Plate Appearances
-- pa_key = "{game_id}|{batter_id}|{pa_number}"
CREATE TABLE IF NOT EXISTS mlb_plate_appearances (
    pa_key              TEXT PRIMARY KEY,
    game_id             INTEGER REFERENCES mlb_games(game_id),
    batter_id           INTEGER,
    pitcher_id          INTEGER,
    inning              INTEGER,
    half_inning         TEXT,           -- "top" / "bottom"
    pa_number           INTEGER,        -- plate appearance number within the game
    outs                INTEGER,        -- number of outs AT THE START of this plate appearance (0, 1, or 2)
    batter_side         TEXT,           -- "L" / "R"
    pitcher_hand        TEXT,           -- "L" / "R"
    result              TEXT,           -- "Strikeout" / "Single" / "Fly Out" etc.
    is_ball_in_play_out BOOLEAN,
    runner_on_first     BOOLEAN,
    runner_on_second    BOOLEAN,
    runner_on_third     BOOLEAN,
    pitch_count         INTEGER,        -- derived: count of pitches in this PA
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Individual Pitches (Statcast data — nested under plate appearances)
CREATE TABLE IF NOT EXISTS mlb_pitches (
    pa_key                  TEXT REFERENCES mlb_plate_appearances(pa_key),
    pitch_number            INTEGER,    -- pitch number within the plate appearance
    balls                   INTEGER,    -- ball count before this pitch
    strikes                 INTEGER,    -- strike count before this pitch
    pitch_call              TEXT,       -- "B" ball / "S" strike / "X" in play
    pitch_type_code         TEXT,       -- "FF" / "SL" / "CU" / "CH" etc.
    pitch_type              TEXT,       -- "4-Seam Fastball" / "Slider" etc.
    release_speed           NUMERIC(5,1),   -- velocity at release (mph)
    plate_speed             NUMERIC(5,1),   -- velocity at plate (mph)
    spin_rate               NUMERIC(7,1),   -- spin rate (RPM)
    release_extension       NUMERIC(5,2),   -- pitcher extension (feet)
    plate_time              NUMERIC(5,3),   -- time to reach plate (seconds)
    plate_x                 NUMERIC(7,3),   -- horizontal plate location (feet from center)
    plate_z                 NUMERIC(7,3),   -- vertical plate location (feet above ground)
    strike_zone             INTEGER,        -- strike zone region (1-14)
    strike_zone_top         NUMERIC(5,2),
    strike_zone_bottom      NUMERIC(5,2),
    horizontal_movement     NUMERIC(7,2),   -- inches
    vertical_movement       NUMERIC(7,2),   -- inches
    horizontal_break        NUMERIC(7,2),   -- inches
    vertical_break          NUMERIC(7,2),   -- inches
    induced_vertical_break  NUMERIC(7,2),   -- gravity-corrected (inches)
    release_pos_x           NUMERIC(7,3),
    release_pos_y           NUMERIC(7,3),
    release_pos_z           NUMERIC(7,3),
    velocity_x              NUMERIC(8,3),   -- velocity X component at release
    velocity_y              NUMERIC(8,3),   -- velocity Y component at release
    velocity_z              NUMERIC(8,3),   -- velocity Z component at release
    acceleration_x          NUMERIC(8,3),   -- acceleration X component
    acceleration_y          NUMERIC(8,3),   -- acceleration Y component
    acceleration_z          NUMERIC(8,3),   -- acceleration Z component
    bat_speed               NUMERIC(5,1),   -- bat speed on contact (mph); null if no contact
    exit_velocity           NUMERIC(5,1),   -- exit velocity (mph); null if not in play
    launch_angle            NUMERIC(5,1),   -- launch angle (degrees); null if not in play
    hit_distance            INTEGER,        -- projected distance (feet); null if not in play
    expected_batting_average NUMERIC(6,4),  -- xBA based on EV + LA; null if not in play
    is_barrel               BOOLEAN,        -- barrel classification; null if not in play
    hit_coordinate_x        NUMERIC(8,3),   -- spray chart X; null if not in play
    hit_coordinate_y        NUMERIC(8,3),   -- spray chart Y; null if not in play
    game_pitch_count        INTEGER,        -- total pitches in game so far
    pitcher_pitch_count     INTEGER,        -- pitcher's pitch count so far
    updated_at              TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (pa_key, pitch_number)
);

-- Lineups (2026 season onward; appear 1-2 hours before first pitch)
CREATE TABLE IF NOT EXISTS mlb_lineups (
    lineup_id               INTEGER PRIMARY KEY,
    game_id                 INTEGER REFERENCES mlb_games(game_id),
    batting_order           INTEGER,        -- 1-9; null for probable pitchers not batting
    position                TEXT,           -- "RF" / "CF" / "SP" / "DH" etc.
    is_probable_pitcher     BOOLEAN,
    -- Player fields (full player object provided by API)
    player_id               INTEGER REFERENCES mlb_players(player_id),
    player_name             TEXT,
    player_first_name       TEXT,
    player_last_name        TEXT,
    player_position         TEXT,           -- player's primary position
    player_jersey           TEXT,
    player_bats_throws      TEXT,
    player_active           BOOLEAN,
    -- Team
    team_id                 INTEGER REFERENCES mlb_teams(team_id),
    team_name               TEXT,
    team_abbr               TEXT,
    updated_at              TIMESTAMP DEFAULT NOW()
);

-- Player Injuries (historical + current)
-- BUG FIX: previously used player_id as PK — a player can have many injuries over time
-- Now uses serial injury_id; index on player_id for lookups
CREATE TABLE IF NOT EXISTS mlb_injuries (
    injury_id       SERIAL PRIMARY KEY,
    player_id       INTEGER REFERENCES mlb_players(player_id),
    player_name     TEXT,
    team_id         INTEGER REFERENCES mlb_teams(team_id),
    team_name       TEXT,
    team_abbr       TEXT,
    date            TIMESTAMP,
    return_date     TIMESTAMP,
    type            TEXT,           -- "Shoulder" / "Hamstring" etc.
    detail          TEXT,           -- "Surgery" / "Strain" etc.
    side            TEXT,           -- "Left" / "Right"
    status          TEXT,           -- "Out" / "Day-to-Day" / "10-Day IL" etc.
    long_comment    TEXT,
    short_comment   TEXT,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Betting Odds (GOAT — available from 2026 season)
-- Live data; odds updated in real-time, may disappear near game end
-- Vendors: betmgm, draftkings, fanatics, fanduel
-- NOTE: betmgm appears in odds but NOT in player props
CREATE TABLE IF NOT EXISTS mlb_odds (
    odd_id              INTEGER PRIMARY KEY,
    game_id             INTEGER REFERENCES mlb_games(game_id),
    vendor              TEXT,               -- betmgm / draftkings / fanatics / fanduel
    spread_home_value   TEXT,               -- e.g. "-1.5"
    spread_home_odds    INTEGER,            -- e.g. -110 (American odds)
    spread_away_value   TEXT,
    spread_away_odds    INTEGER,
    moneyline_home_odds INTEGER,
    moneyline_away_odds INTEGER,
    total_value         TEXT,               -- e.g. "8.5"
    total_over_odds     INTEGER,
    total_under_odds    INTEGER,
    updated_at          TIMESTAMP
);

-- Player Props (GOAT — available from 2026 season)
-- LIVE ONLY — API does not store historical prop data
-- Snapshot captured at collection time only
-- Batter props: hits, home_runs, total_bases, rbis, stolen_bases,
--   singles, doubles, triples, walks, strikeouts, runs_scored,
--   hits_runs_rbis, first_home_run (milestone)
-- Pitcher props: pitcher_strikeouts, pitcher_outs, pitcher_hits_allowed,
--   pitcher_walks, pitcher_earned_runs, pitcher_record_a_win (milestone)
CREATE TABLE IF NOT EXISTS mlb_player_props (
    prop_id         INTEGER PRIMARY KEY,
    game_id         INTEGER REFERENCES mlb_games(game_id),
    player_id       INTEGER REFERENCES mlb_players(player_id),
    vendor          TEXT,               -- draftkings / fanatics / fanduel
    prop_type       TEXT,
    line_value      TEXT,               -- the line e.g. "0.5" / "1.5"
    market_type     TEXT,               -- over_under / milestone
    over_odds       INTEGER,            -- null for milestone markets
    under_odds      INTEGER,            -- null for milestone markets
    odds            INTEGER,            -- for milestone markets only
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ================================================================
-- INDEXES
-- ================================================================

-- Games
CREATE INDEX IF NOT EXISTS idx_mlb_games_date        ON mlb_games(date);
CREATE INDEX IF NOT EXISTS idx_mlb_games_season      ON mlb_games(season);
CREATE INDEX IF NOT EXISTS idx_mlb_games_season_type ON mlb_games(season_type);
CREATE INDEX IF NOT EXISTS idx_mlb_games_home_team   ON mlb_games(home_team_id);
CREATE INDEX IF NOT EXISTS idx_mlb_games_away_team   ON mlb_games(away_team_id);

-- Stats
CREATE INDEX IF NOT EXISTS idx_mlb_stats_game        ON mlb_stats(game_id);
CREATE INDEX IF NOT EXISTS idx_mlb_stats_player      ON mlb_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_stats_season      ON mlb_stats(season);

-- Standings
CREATE INDEX IF NOT EXISTS idx_mlb_standings_season  ON mlb_standings(season);

-- Season stats
CREATE INDEX IF NOT EXISTS idx_mlb_ss_season         ON mlb_season_stats(season);
CREATE INDEX IF NOT EXISTS idx_mlb_tss_season        ON mlb_team_season_stats(season);

-- Splits
CREATE INDEX IF NOT EXISTS idx_mlb_splits_player     ON mlb_splits(player_id, season);
CREATE INDEX IF NOT EXISTS idx_mlb_splits_category   ON mlb_splits(split_category);

-- Versus
CREATE INDEX IF NOT EXISTS idx_mlb_versus_player     ON mlb_versus(player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_versus_opponent   ON mlb_versus(opponent_id);

-- Plays
CREATE INDEX IF NOT EXISTS idx_mlb_plays_game        ON mlb_plays(game_id);

-- Plate appearances
CREATE INDEX IF NOT EXISTS idx_mlb_pa_game           ON mlb_plate_appearances(game_id);
CREATE INDEX IF NOT EXISTS idx_mlb_pa_batter         ON mlb_plate_appearances(batter_id);
CREATE INDEX IF NOT EXISTS idx_mlb_pa_pitcher        ON mlb_plate_appearances(pitcher_id);

-- Pitches
CREATE INDEX IF NOT EXISTS idx_mlb_pitches_pa        ON mlb_pitches(pa_key);

-- Lineups
CREATE INDEX IF NOT EXISTS idx_mlb_lineups_game      ON mlb_lineups(game_id);
CREATE INDEX IF NOT EXISTS idx_mlb_lineups_player    ON mlb_lineups(player_id);

-- Injuries
CREATE INDEX IF NOT EXISTS idx_mlb_injuries_player   ON mlb_injuries(player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_injuries_team     ON mlb_injuries(team_id);
CREATE INDEX IF NOT EXISTS idx_mlb_injuries_status   ON mlb_injuries(status);

-- Odds
CREATE INDEX IF NOT EXISTS idx_mlb_odds_game         ON mlb_odds(game_id);
CREATE INDEX IF NOT EXISTS idx_mlb_odds_vendor       ON mlb_odds(vendor);

-- Player props
CREATE INDEX IF NOT EXISTS idx_mlb_props_game        ON mlb_player_props(game_id);
CREATE INDEX IF NOT EXISTS idx_mlb_props_player      ON mlb_player_props(player_id);
CREATE INDEX IF NOT EXISTS idx_mlb_props_type        ON mlb_player_props(prop_type);
