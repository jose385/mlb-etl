#!/usr/bin/env python
"""
Read every *.parquet in ./stage with DuckDB and stream it into
Postgres using psycopg COPY -- fastest path:contentReference[oaicite:9]{index=9}:contentReference[oaicite:10]{index=10}.
"""
import os, glob, duckdb, psycopg2, datetime, textwrap  


PG = psycopg2.connect(os.environ["PG_DSN"])  

duck = duckdb.connect()  

cur = PG.cursor()  

for fp in glob.glob("stage/*.parquet"):  

    df = duck.execute(f"SELECT * FROM read_parquet('{fp}')").fetch_df()  # DuckDB read_parquet:contentReference[oaicite:11]{index=11}  

    if df.empty: continue  

    df.columns = [c.replace('.', '_').replace('-', '_') for c in df.columns]

    cols = ','.join(df.columns)  

    # ⬇⬇⬇ paste the pathlib-based loader here ⬇⬇⬇

    from pathlib import Path


    output_dir = Path(os.getenv("OUTPUT_DIR", "stage"))

    file_path  = Path(fp)


    if not file_path.exists():

        raise FileNotFoundError(f"No Parquet file found at {file_path.resolve()}")
    

    with file_path.open("rb") as f, PG.cursor() as cur:

    # Ensure table exists in public

         cur.execute(f"""
                
        CREATE TABLE IF NOT EXISTS public.statcast_pitchlog (
                
            ... column definitions ...
                
        )
                
    """)

    -- 1. Ensure the mlb schema exists

CREATE SCHEMA IF NOT EXISTS mlb;


-- 2. Create statcast_pitchlog with every Statcast CSV field

CREATE TABLE IF NOT EXISTS mlb.statcast_pitchlog (

    pitch_type             TEXT,       -- The type of pitch derived from Statcast :contentReference[oaicite:0]{index=0}

    game_date              DATE,       -- Date of the game :contentReference[oaicite:1]{index=1}

    release_speed          REAL,       -- Pitch velocity (mph) :contentReference[oaicite:2]{index=2}

    release_pos_x          REAL,       -- Horizontal release position (ft) :contentReference[oaicite:3]{index=3}

    release_pos_z          REAL,       -- Vertical release position (ft) :contentReference[oaicite:4]{index=4}

    player_name            TEXT,       -- Player’s name tied to event :contentReference[oaicite:5]{index=5}

    batter                 INTEGER,    -- Batter’s MLB player ID :contentReference[oaicite:6]{index=6}

    pitcher                INTEGER,    -- Pitcher’s MLB player ID :contentReference[oaicite:7]{index=7}

    events                 TEXT,       -- Resulting plate appearance event :contentReference[oaicite:8]{index=8}

    description            TEXT,       -- Play-by-play description :contentReference[oaicite:9]{index=9}

    spin_dir               TEXT,       -- Deprecated spin direction :contentReference[oaicite:10]{index=10}

    spin_rate_deprecated   REAL,       -- Deprecated spin rate :contentReference[oaicite:11]{index=11}

    break_angle_deprecated REAL,       -- Deprecated break angle :contentReference[oaicite:12]{index=12}

    break_length_deprecated REAL,      -- Deprecated break length :contentReference[oaicite:13]{index=13}

    zone                   INTEGER,    -- Plate zone 1–14 :contentReference[oaicite:14]{index=14}

    des                    TEXT,       -- Alternate plate appearance description :contentReference[oaicite:15]{index=15}

    game_type              TEXT,       -- Game type (R, F, etc.) :contentReference[oaicite:16]{index=16}

    stand                  TEXT,       -- Batter’s stance (L/R) :contentReference[oaicite:17]{index=17}

    p_throws               TEXT,       -- Pitcher’s throwing hand (L/R) :contentReference[oaicite:18]{index=18}

    home_team              TEXT,       -- Home team abbreviation :contentReference[oaicite:19]{index=19}

    away_team              TEXT,       -- Away team abbreviation :contentReference[oaicite:20]{index=20}

    type                   TEXT,       -- Short pitch result (B/S/X) :contentReference[oaicite:21]{index=21}

    hit_location           INTEGER,    -- First fielder to touch the ball :contentReference[oaicite:22]{index=22}

    bb_type                TEXT,       -- Batted-ball type (ground, fly, etc.) :contentReference[oaicite:23]{index=23}

    balls                  INTEGER,    -- Balls in count before pitch :contentReference[oaicite:24]{index=24}

    strikes                INTEGER,    -- Strikes in count before pitch :contentReference[oaicite:25]{index=25}

    game_year              INTEGER,    -- Year game took place :contentReference[oaicite:26]{index=26}

    pfx_x                  REAL,       -- Horizontal movement at 50 ft (in.) :contentReference[oaicite:27]{index=27}

    pfx_z                  REAL,       -- Vertical movement at 50 ft (in.) :contentReference[oaicite:28]{index=28}

    plate_x                REAL,       -- Horizontal location over plate (ft) :contentReference[oaicite:29]{index=29}

    plate_z                REAL,       -- Vertical location over plate (ft) :contentReference[oaicite:30]{index=30}

    on_3b                  INTEGER,    -- Runner on 3B before pitch :contentReference[oaicite:31]{index=31}

    on_2b                  INTEGER,    -- Runner on 2B before pitch :contentReference[oaicite:32]{index=32}

    on_1b                  INTEGER,    -- Runner on 1B before pitch :contentReference[oaicite:33]{index=33}

    outs_when_up           INTEGER,    -- Outs before pitch :contentReference[oaicite:34]{index=34}

    inning                 INTEGER,    -- Inning number :contentReference[oaicite:35]{index=35}

    inning_topbot          TEXT,       -- Top/bottom of inning :contentReference[oaicite:36]{index=36}

    hc_x                   REAL,       -- Batted-ball X coordinate :contentReference[oaicite:37]{index=37}

    hc_y                   REAL,       -- Batted-ball Y coordinate :contentReference[oaicite:38]{index=38}

    tfs_deprecated         TEXT,       -- Deprecated timing field :contentReference[oaicite:39]{index=39}

    tfs_zulu_deprecated    TEXT,       -- Deprecated Zulu timing :contentReference[oaicite:40]{index=40}

    fielder_2              INTEGER,    -- Catcher’s MLB ID :contentReference[oaicite:41]{index=41}

    umpire                 TEXT,       -- Umpire ID (deprecated) :contentReference[oaicite:42]{index=42}

    sv_id                  TEXT,       -- Non-unique play event ID :contentReference[oaicite:43]{index=43}

    vx0                    REAL,       -- X-velocity at 50 ft (ft/s) :contentReference[oaicite:44]{index=44}

    vy0                    REAL,       -- Y-velocity at 50 ft (ft/s) :contentReference[oaicite:45]{index=45}

    vz0                    REAL,       -- Z-velocity at 50 ft (ft/s) :contentReference[oaicite:46]{index=46}

    ax                     REAL,       -- X-acceleration at 50 ft :contentReference[oaicite:47]{index=47}

    ay                     REAL,       -- Y-acceleration at 50 ft :contentReference[oaicite:48]{index=48}

    az                     REAL,       -- Z-acceleration at 50 ft :contentReference[oaicite:49]{index=49}

    sz_top                 REAL,       -- Top of zone (operator-set) :contentReference[oaicite:50]{index=50}

    sz_bot                 REAL,       -- Bottom of zone (operator-set) :contentReference[oaicite:51]{index=51}

    hit_distance           REAL,       -- Projected hit distance (ft) :contentReference[oaicite:52]{index=52}

    launch_speed           REAL,       -- Exit velocity (mph) :contentReference[oaicite:53]{index=53}

    launch_angle           REAL,       -- Launch angle (°) :contentReference[oaicite:54]{index=54}
                                                        
    effective_speed        REAL,       -- Derived release speed (mph) :contentReference[oaicite:55]{index=55}

    release_spin           REAL,       -- Statcast spin rate (rpm) :contentReference[oaicite:56]{index=56}

    release_extension      REAL,       -- Release extension (ft) :contentReference[oaicite:57]{index=57}

    game_pk                INTEGER,    -- Game’s unique ID :contentReference[oaicite:58]{index=58}

    pitch_name             TEXT,       -- Name of pitch variant :contentReference[oaicite:59]{index=59}

    home_score             INTEGER,    -- Home score before pitch :contentReference[oaicite:60]{index=60}

    away_score             INTEGER,    -- Away score before pitch :contentReference[oaicite:61]{index=61}

    post_home_score        INTEGER,    -- Home score after pitch :contentReference[oaicite:62]{index=62}

    post_away_score        INTEGER,    -- Away score after pitch :contentReference[oaicite:63]{index=63}

    bat_score              INTEGER,    -- Batting team score before pitch :contentReference[oaicite:64]{index=64}

    fld_score              INTEGER,    -- Fielding team score before pitch :contentReference[oaicite:65]{index=65}

    if_fielding_alignment  TEXT,       -- Infield alignment :contentReference[oaicite:66]{index=66}

    of_fielding_alignment  TEXT,       -- Outfield alignment :contentReference[oaicite:67]{index=67}

    spin_axis              REAL,       -- Spin axis in X–Z plane (°) :contentReference[oaicite:68]{index=68}
                                                                  
    delta_home_win_exp     REAL,       -- Change in win expectancy :contentReference[oaicite:69]{index=69}

    delta_run_exp          REAL        -- Change in run expectancy :contentReference[oaicite:70]{index=70}

);



    # Load into public.statcast_pitchlog

    cur.copy_expert(

        f"COPY public.statcast_pitchlog ({cols}) FROM STDIN",

        f

    )
    

    # ⬆⬆⬆ end of pasted block ⬆⬆⬆


    PG.commit()  

    print(f"→ loaded {len(df)} rows from {fp}")  

cur.close(); PG.close()  


