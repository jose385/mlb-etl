#!/usr/bin/env python3
"""
MLB BallDontLie Backfill v1
Collects MLB data from the BallDontLie API and saves to CSV.

Usage:
    python py/mlb_balldontlie_backfill.py --start 2025-03-27 --end 2025-10-01 --season 2025 --full
    python py/mlb_balldontlie_backfill.py --daily
    python py/mlb_balldontlie_backfill.py --start 2025-04-01 --end 2025-04-07 --games --stats
    python py/mlb_balldontlie_backfill.py --season 2025 --full --team NYY
"""

import os
import sys
import json
import argparse
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))
from py.mlb_balldontlie_client import MLBBallDontLieClient

DATA_DIR = Path("data/mlb")
DATA_DIR.mkdir(parents=True, exist_ok=True)

TEAM_ABBR_MAP = {
    "ARI": "Arizona Diamondbacks", "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",         "CWS": "Chicago White Sox",
    "CIN": "Cincinnati Reds",      "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",     "DET": "Detroit Tigers",
    "HOU": "Houston Astros",       "KC":  "Kansas City Royals",
    "LAA": "Los Angeles Angels",   "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",        "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",      "NYM": "New York Mets",
    "NYY": "New York Yankees",     "OAK": "Athletics",
    "PHI": "Philadelphia Phillies","PIT": "Pittsburgh Pirates",
    "SD":  "San Diego Padres",     "SF":  "San Francisco Giants",
    "SEA": "Seattle Mariners",     "STL": "St. Louis Cardinals",
    "TB":  "Tampa Bay Rays",       "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",    "WSH": "Washington Nationals",
}


# Primary key columns — static files
_CSV_PKS_STATIC = {
    "teams.csv":        ["team_id"],
    "players.csv":      ["player_id"],
    "versus.csv":       ["player_id", "opponent_id", "opponent_team_id"],
    "lineups.csv":      ["lineup_id"],
    "injuries.csv":     ["player_id", "date"],
    "odds.csv":         ["odd_id"],
    "player_props.csv": ["prop_id"],
}

# Primary key columns — season-suffixed files (any season year)
_CSV_PKS_SEASON = {
    "games":               ["game_id"],
    "stats":               ["game_id", "player_id"],
    "standings":           ["team_id", "season"],
    "season_stats":        ["player_id", "season", "season_type"],
    "team_season_stats":   ["team_id", "season", "season_type"],
    "splits":              ["player_id", "season", "category", "split_category", "split_name"],
    "plays":               ["game_id", "order"],
    "plate_appearances":   ["pa_key"],
    "pitches":             ["pa_key", "pitch_number"],
}


def get_csv_pks(filename: str) -> list:
    """Return primary key columns for any CSV filename, handling any season year."""
    if filename in _CSV_PKS_STATIC:
        return _CSV_PKS_STATIC[filename]
    # Try season-suffixed: e.g. "games_2026.csv" → base "games"
    import re as _re
    m = _re.match(r'^(.+)_(\d{4})\.csv$', filename)
    if m:
        base = m.group(1)
        return _CSV_PKS_SEASON.get(base, [])
    return []


# Keep CSV_PKS as a compatibility alias (used by convert_to_csv.py)
CSV_PKS = {**_CSV_PKS_STATIC,
           **{f"{k}_2025.csv": v for k, v in _CSV_PKS_SEASON.items()},
           **{f"{k}_2026.csv": v for k, v in _CSV_PKS_SEASON.items()}}


def save_csv(df: pd.DataFrame, filename: str, label: str) -> None:
    """
    Save DataFrame to CSV with smart merge — if the file already exists,
    new records are merged with existing ones and deduplicated by primary key.
    This means running the backfill multiple times accumulates data safely
    without duplicating or losing records. Matches the NBA backfill pattern.
    """
    if df.empty:
        print(f"   ⚠️  No data to save for {label}")
        return
    path = DATA_DIR / filename
    pk_cols = get_csv_pks(filename)

    if path.exists() and pk_cols:
        try:
            existing = pd.read_csv(path, low_memory=False)
            # Align dtypes to avoid merge issues on numeric PK columns
            for col in pk_cols:
                if col in existing.columns and col in df.columns:
                    try:
                        existing[col] = existing[col].astype(str)
                        df[col] = df[col].astype(str)
                    except Exception:
                        pass
            combined = pd.concat([existing, df], ignore_index=True)
            before = len(combined)
            combined = combined.drop_duplicates(subset=pk_cols, keep="last")
            after = len(combined)
            new_rows = after - (len(existing))
            if new_rows > 0:
                print(f"   📥 Merged: {len(existing):,} existing + {new_rows:,} new = {after:,} total")
            df = combined
        except Exception as e:
            print(f"   ⚠️  Could not merge with existing {filename}: {e}")

    df.to_csv(path, index=False)
    print(f"   ✅ Saved {len(df):,} rows → {path}")


def date_range(start: str, end: str) -> list:
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(end,   "%Y-%m-%d").date()
    days, cur = [], start_dt
    while cur <= end_dt:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


# ------------------------------------------------------------------ #
# Flatten helpers — field names match OpenAPI spec exactly
# ------------------------------------------------------------------ #

def flatten_team(t: dict) -> dict:
    return {
        "team_id":            t.get("id"),
        "slug":               t.get("slug"),
        "abbreviation":       t.get("abbreviation"),
        "display_name":       t.get("display_name"),
        "short_display_name": t.get("short_display_name"),
        "name":               t.get("name"),
        "location":           t.get("location"),
        "league":             t.get("league"),
        "division":           t.get("division"),
    }


def flatten_player(p: dict) -> dict:
    team = p.get("team") or {}
    return {
        "player_id":    p.get("id"),
        "first_name":   p.get("first_name"),
        "last_name":    p.get("last_name"),
        "full_name":    p.get("full_name"),
        "position":     p.get("position"),
        "bats_throws":  p.get("bats_throws"),
        "jersey":       p.get("jersey"),
        "college":      p.get("college"),
        "birth_place":  p.get("birth_place"),
        "dob":          p.get("dob"),
        "age":          p.get("age"),
        "height":       p.get("height"),
        "weight":       p.get("weight"),
        "draft":        p.get("draft"),
        "debut_year":   p.get("debut_year"),
        "active":       p.get("active"),
        "team_id":           team.get("id"),
        "team_name":         team.get("display_name"),  # "Los Angeles Dodgers"
        "team_short_name":   team.get("name"),           # "Dodgers"
        "team_abbr":         team.get("abbreviation"),
        "team_location":     team.get("location"),
        "team_league":       team.get("league"),
        "team_division":     team.get("division"),
    }


def flatten_game(g: dict) -> dict:
    home      = g.get("home_team") or {}
    away      = g.get("away_team") or {}
    home_data = g.get("home_team_data") or {}
    away_data = g.get("away_team_data") or {}
    return {
        "game_id":              g.get("id"),
        "date":                 g.get("date"),
        "season":               g.get("season"),
        "season_type":          g.get("season_type"),
        "postseason":           g.get("postseason"),
        "status":               g.get("status"),
        "period":               g.get("period"),        # current inning number
        "clock":                g.get("clock"),
        "display_clock":        g.get("display_clock"),
        "venue":                g.get("venue"),
        "attendance":           g.get("attendance"),
        "conference_play":      g.get("conference_play"),
        # Direct name strings returned by API (separate from nested team objects)
        "home_team_name_str":   g.get("home_team_name"),
        "away_team_name_str":   g.get("away_team_name"),
        # Nested team objects
        "home_team_id":         home.get("id"),
        "home_team_name":       home.get("display_name"),
        "home_team_abbr":       home.get("abbreviation"),
        "away_team_id":         away.get("id"),
        "away_team_name":       away.get("display_name"),
        "away_team_abbr":       away.get("abbreviation"),
        # Scores from nested team_data
        "home_runs":            home_data.get("runs"),
        "home_hits":            home_data.get("hits"),
        "home_errors":          home_data.get("errors"),
        "away_runs":            away_data.get("runs"),
        "away_hits":            away_data.get("hits"),
        "away_errors":          away_data.get("errors"),
        # Inning scores as JSON string
        "home_inning_scores":   json.dumps(home_data.get("inning_scores", [])),
        "away_inning_scores":   json.dumps(away_data.get("inning_scores", [])),
        # Scoring summary as JSON string
        "scoring_summary":      json.dumps(g.get("scoring_summary", [])),
    }


def flatten_stat(s: dict, season: int = None) -> dict:
    player = s.get("player") or {}
    return {
        "game_id":                  s.get("game_id"),
        "player_id":                player.get("id"),
        "player_name":              player.get("full_name"),
        "player_position":          player.get("position"),
        "player_jersey":            player.get("jersey"),
        "player_bats_throws":       player.get("bats_throws"),
        "season":                   season,
        "team_name":                s.get("team_name"),
        # Batting
        "at_bats":                  s.get("at_bats"),
        "runs":                     s.get("runs"),
        "hits":                     s.get("hits"),
        "doubles":                  s.get("doubles"),
        "triples":                  s.get("triples"),
        "hr":                       s.get("hr"),
        "rbi":                      s.get("rbi"),
        "bb":                       s.get("bb"),
        "k":                        s.get("k"),
        "avg":                      s.get("avg"),
        "obp":                      s.get("obp"),
        "slg":                      s.get("slg"),
        "intentional_walks":        s.get("intentional_walks"),
        "hit_by_pitch":             s.get("hit_by_pitch"),
        "stolen_bases":             s.get("stolen_bases"),
        "caught_stealing":          s.get("caught_stealing"),
        "plate_appearances":        s.get("plate_appearances"),
        "total_bases":              s.get("total_bases"),
        "left_on_base":             s.get("left_on_base"),
        "fly_outs":                 s.get("fly_outs"),
        "ground_outs":              s.get("ground_outs"),
        "line_outs":                s.get("line_outs"),
        "pop_outs":                 s.get("pop_outs"),
        "air_outs":                 s.get("air_outs"),
        "gidp":                     s.get("gidp"),
        "sac_bunts":                s.get("sac_bunts"),
        "sac_flies":                s.get("sac_flies"),
        # Pitching
        "ip":                       s.get("ip"),
        "p_hits":                   s.get("p_hits"),
        "p_runs":                   s.get("p_runs"),
        "er":                       s.get("er"),
        "p_bb":                     s.get("p_bb"),
        "p_k":                      s.get("p_k"),
        "p_hr":                     s.get("p_hr"),
        "pitch_count":              s.get("pitch_count"),
        "strikes":                  s.get("strikes"),
        "era":                      s.get("era"),
        "batters_faced":            s.get("batters_faced"),
        "pitching_outs":            s.get("pitching_outs"),
        "wins":                     s.get("wins"),
        "losses":                   s.get("losses"),
        "saves":                    s.get("saves"),
        "holds":                    s.get("holds"),
        "blown_saves":              s.get("blown_saves"),
        "games_started":            s.get("games_started"),
        "wild_pitches":             s.get("wild_pitches"),
        "balks":                    s.get("balks"),
        "pitching_hbp":             s.get("pitching_hbp"),
        "inherited_runners":        s.get("inherited_runners"),
        "inherited_runners_scored": s.get("inherited_runners_scored"),
        # Fielding
        "putouts":                  s.get("putouts"),
        "assists":                  s.get("assists"),
        "errors":                   s.get("errors"),
        "fielding_chances":         s.get("fielding_chances"),
        "fielding_pct":             s.get("fielding_pct"),
    }


def flatten_standing(s: dict) -> dict:
    team = s.get("team") or {}
    return {
        "team_id":                      team.get("id"),
        "team_name":                    s.get("team_name") or team.get("display_name"),
        "team_abbr":                    team.get("abbreviation"),
        "season":                       s.get("season"),
        "league_name":                  s.get("league_name"),
        "league_short_name":            s.get("league_short_name"),
        "division_name":                s.get("division_name"),
        "division_short_name":          s.get("division_short_name"),
        "wins":                         s.get("wins"),
        "losses":                       s.get("losses"),
        "ties":                         s.get("ties"),
        "win_percent":                  s.get("win_percent"),
        "games_played":                 s.get("games_played"),
        "games_behind":                 s.get("games_behind"),
        "division_games_behind":        s.get("division_games_behind"),
        "league_win_percent":           s.get("league_win_percent"),
        "division_win_percent":         s.get("division_win_percent"),
        "playoff_seed":                 s.get("playoff_seed"),
        "playoff_percent":              s.get("playoff_percent"),
        "wildcard_percent":             s.get("wildcard_percent"),
        "clincher":                     s.get("clincher"),
        "magic_number_division":        s.get("magic_number_division"),
        "magic_number_wildcard":        s.get("magic_number_wildcard"),
        "streak":                       s.get("streak"),
        "last_ten_games":               s.get("last_ten_games"),
        # Points / run data
        "points_for":                   s.get("points_for"),
        "points_against":               s.get("points_against"),
        "avg_points_for":               s.get("avg_points_for"),
        "avg_points_against":           s.get("avg_points_against"),
        "point_differential":           s.get("point_differential"),
        "differential":                 s.get("differential"),
        "game_back_points":             s.get("game_back_points"),
        # Home / road / OT splits
        "home_wins":                    s.get("home_wins"),
        "home_losses":                  s.get("home_losses"),
        "home_ties":                    s.get("home_ties"),
        "road_wins":                    s.get("road_wins"),
        "road_losses":                  s.get("road_losses"),
        "road_ties":                    s.get("road_ties"),
        "ot_wins":                      s.get("ot_wins"),
        "ot_losses":                    s.get("ot_losses"),
        # Record strings
        "total":                        s.get("total"),          # "94-68"
        "home":                         s.get("home"),           # "44-37"
        "road":                         s.get("road"),           # "50-31"
        "intra_division":               s.get("intra_division"), # "26-26"
        "intra_league":                 s.get("intra_league"),   # "71-45"
        # Division/playoff tracking
        "division_percent":             s.get("division_percent"),
        "division_tied":                s.get("division_tied"),
        # Fields from OpenAPI spec also included
        "league_games_back":            s.get("league_games_back"),
        "sport_games_back":             s.get("sport_games_back"),
        "conference_games_back":        s.get("conference_games_back"),
        "league_rank":                  s.get("league_rank"),
        "sport_rank":                   s.get("sport_rank"),
        "division_rank":                s.get("division_rank"),
        "division_leader":              s.get("division_leader"),
        "division_champ":               s.get("division_champ"),
        "clinch_indicator":             s.get("clinch_indicator"),
        "elimination_number":           s.get("elimination_number"),
        "wild_card_elimination_number": s.get("wild_card_elimination_number"),
        "runs_scored":                  s.get("runs_scored"),
        "runs_allowed":                 s.get("runs_allowed"),
        "run_differential":             s.get("run_differential"),
        "last_updated":                 s.get("last_updated"),
    }


def flatten_season_stat(s: dict) -> dict:
    player = s.get("player") or {}
    return {
        "player_id":            player.get("id"),
        "player_name":          player.get("full_name"),
        "position":             player.get("position"),
        "team_name":            s.get("team_name"),
        "season":               s.get("season"),
        "season_type":          s.get("season_type"),
        "postseason":           s.get("postseason"),
        "batting_gp":           s.get("batting_gp"),
        "batting_ab":           s.get("batting_ab"),
        "batting_r":            s.get("batting_r"),
        "batting_h":            s.get("batting_h"),
        "batting_2b":           s.get("batting_2b"),
        "batting_3b":           s.get("batting_3b"),
        "batting_hr":           s.get("batting_hr"),
        "batting_rbi":          s.get("batting_rbi"),
        "batting_tb":           s.get("batting_tb"),
        "batting_bb":           s.get("batting_bb"),
        "batting_so":           s.get("batting_so"),
        "batting_sb":           s.get("batting_sb"),
        "batting_avg":          s.get("batting_avg"),
        "batting_obp":          s.get("batting_obp"),
        "batting_slg":          s.get("batting_slg"),
        "batting_ops":          s.get("batting_ops"),
        "batting_war":          s.get("batting_war"),
        "pitching_gp":          s.get("pitching_gp"),
        "pitching_gs":          s.get("pitching_gs"),
        "pitching_qs":          s.get("pitching_qs"),
        "pitching_w":           s.get("pitching_w"),
        "pitching_l":           s.get("pitching_l"),
        "pitching_era":         s.get("pitching_era"),
        "pitching_sv":          s.get("pitching_sv"),
        "pitching_hld":         s.get("pitching_hld"),
        "pitching_ip":          s.get("pitching_ip"),
        "pitching_h":           s.get("pitching_h"),
        "pitching_er":          s.get("pitching_er"),
        "pitching_hr":          s.get("pitching_hr"),
        "pitching_bb":          s.get("pitching_bb"),
        "pitching_whip":        s.get("pitching_whip"),
        "pitching_k":           s.get("pitching_k"),
        "pitching_k_per_9":     s.get("pitching_k_per_9"),
        "pitching_war":         s.get("pitching_war"),
        "fielding_gp":          s.get("fielding_gp"),
        "fielding_gs":          s.get("fielding_gs"),
        "fielding_fip":         s.get("fielding_fip"),
        "fielding_tc":          s.get("fielding_tc"),
        "fielding_po":          s.get("fielding_po"),
        "fielding_a":           s.get("fielding_a"),
        "fielding_fp":          s.get("fielding_fp"),
        "fielding_e":           s.get("fielding_e"),
        "fielding_dp":          s.get("fielding_dp"),
        "fielding_rf":          s.get("fielding_rf"),
        "fielding_dwar":        s.get("fielding_dwar"),
        "fielding_pb":          s.get("fielding_pb"),
        "fielding_cs":          s.get("fielding_cs"),
        "fielding_cs_percent":  s.get("fielding_cs_percent"),
        "fielding_sba":         s.get("fielding_sba"),
    }


def flatten_team_season_stat(s: dict) -> dict:
    team = s.get("team") or {}
    return {
        "team_id":          team.get("id"),
        "team_name":        team.get("display_name"),
        "team_abbr":        team.get("abbreviation"),
        "season":           s.get("season"),
        "season_type":      s.get("season_type"),
        "postseason":       s.get("postseason"),
        "gp":               s.get("gp"),
        "batting_ab":       s.get("batting_ab"),
        "batting_r":        s.get("batting_r"),
        "batting_h":        s.get("batting_h"),
        "batting_2b":       s.get("batting_2b"),
        "batting_3b":       s.get("batting_3b"),
        "batting_hr":       s.get("batting_hr"),
        "batting_rbi":      s.get("batting_rbi"),
        "batting_tb":       s.get("batting_tb"),
        "batting_bb":       s.get("batting_bb"),
        "batting_so":       s.get("batting_so"),
        "batting_sb":       s.get("batting_sb"),
        "batting_avg":      s.get("batting_avg"),
        "batting_obp":      s.get("batting_obp"),
        "batting_slg":      s.get("batting_slg"),
        "batting_ops":      s.get("batting_ops"),
        "pitching_w":       s.get("pitching_w"),
        "pitching_l":       s.get("pitching_l"),
        "pitching_era":     s.get("pitching_era"),
        "pitching_sv":      s.get("pitching_sv"),
        "pitching_cg":      s.get("pitching_cg"),
        "pitching_sho":     s.get("pitching_sho"),
        "pitching_qs":      s.get("pitching_qs"),
        "pitching_ip":      s.get("pitching_ip"),
        "pitching_h":       s.get("pitching_h"),
        "pitching_er":      s.get("pitching_er"),
        "pitching_hr":      s.get("pitching_hr"),
        "pitching_bb":      s.get("pitching_bb"),
        "pitching_k":       s.get("pitching_k"),
        "pitching_oba":     s.get("pitching_oba"),
        "pitching_whip":    s.get("pitching_whip"),
        "fielding_e":       s.get("fielding_e"),
        "fielding_fp":      s.get("fielding_fp"),
        "fielding_tc":      s.get("fielding_tc"),
        "fielding_po":      s.get("fielding_po"),
        "fielding_a":       s.get("fielding_a"),
    }


def flatten_split(s: dict) -> dict:
    player = s.get("player") or {}
    return {
        "player_id":            player.get("id"),
        "player_name":          player.get("full_name"),
        "season":               s.get("season"),
        "category":             s.get("category"),
        "split_category":       s.get("split_category"),
        "split_name":           s.get("split_name"),
        "split_abbreviation":   s.get("split_abbreviation"),
        "at_bats":              s.get("at_bats"),
        "runs":                 s.get("runs"),
        "hits":                 s.get("hits"),
        "doubles":              s.get("doubles"),
        "triples":              s.get("triples"),
        "home_runs":            s.get("home_runs"),
        "rbis":                 s.get("rbis"),
        "walks":                s.get("walks"),
        "hit_by_pitch":         s.get("hit_by_pitch"),
        "strikeouts":           s.get("strikeouts"),
        "stolen_bases":         s.get("stolen_bases"),
        "caught_stealing":      s.get("caught_stealing"),
        "avg":                  s.get("avg"),
        "obp":                  s.get("obp"),
        "slg":                  s.get("slg"),
        "ops":                  s.get("ops"),
        "era":                  s.get("era"),
        "wins":                 s.get("wins"),
        "losses":               s.get("losses"),
        "saves":                s.get("saves"),
        "save_opportunities":   s.get("save_opportunities"),
        "games_played":         s.get("games_played"),
        "games_started":        s.get("games_started"),
        "complete_games":       s.get("complete_games"),
        "innings_pitched":      s.get("innings_pitched"),
        "hits_allowed":         s.get("hits_allowed"),
        "runs_allowed":         s.get("runs_allowed"),
        "earned_runs":          s.get("earned_runs"),
        "home_runs_allowed":    s.get("home_runs_allowed"),
        "walks_allowed":        s.get("walks_allowed"),
        "strikeouts_pitched":   s.get("strikeouts_pitched"),
        "opponent_avg":         s.get("opponent_avg"),
    }


def flatten_versus(v: dict) -> dict:
    player   = v.get("player")          or {}
    opponent = v.get("opponent_player") or {}
    opp_team = v.get("opponent_team")   or {}
    return {
        "player_id":        player.get("id"),
        "player_name":      player.get("full_name"),
        "opponent_id":      opponent.get("id"),
        "opponent_name":    opponent.get("full_name"),
        "opponent_team_id": opp_team.get("id"),
        "opponent_team":    opp_team.get("display_name"),
        "at_bats":          v.get("at_bats"),
        "hits":             v.get("hits"),
        "doubles":          v.get("doubles"),
        "triples":          v.get("triples"),
        "home_runs":        v.get("home_runs"),
        "rbi":              v.get("rbi"),
        "walks":            v.get("walks"),
        "strikeouts":       v.get("strikeouts"),
        "avg":              v.get("avg"),
        "obp":              v.get("obp"),
        "slg":              v.get("slg"),
        "ops":              v.get("ops"),
    }


def flatten_play(p: dict) -> dict:
    return {
        "game_id":          p.get("game_id"),
        "order":            p.get("order"),
        "type":             p.get("type"),
        "text":             p.get("text"),
        "inning":           p.get("inning"),
        "inning_type":      p.get("inning_type"),
        "outs":             p.get("outs"),
        "balls":            p.get("balls"),
        "strikes":          p.get("strikes"),
        "home_score":       p.get("home_score"),
        "away_score":       p.get("away_score"),
        "scoring_play":     p.get("scoring_play"),
        "score_value":      p.get("score_value"),
        "batter_id":        p.get("batter_id"),
        "pitcher_id":       p.get("pitcher_id"),
        "pitch_type":       p.get("pitch_type"),
        "pitch_velocity":   p.get("pitch_velocity"),
        "hit_coordinate_x": p.get("hit_coordinate_x"),
        "hit_coordinate_y": p.get("hit_coordinate_y"),
        "trajectory":       p.get("trajectory"),
    }


def flatten_pitch(pitch: dict, pa_key: str) -> dict:
    return {
        "pa_key":                   pa_key,
        "pitch_number":             pitch.get("pitch_number"),
        "balls":                    pitch.get("balls"),
        "strikes":                  pitch.get("strikes"),
        "pitch_call":               pitch.get("pitch_call"),
        "pitch_type_code":          pitch.get("pitch_type_code"),
        "pitch_type":               pitch.get("pitch_type"),
        "release_speed":            pitch.get("release_speed"),
        "plate_speed":              pitch.get("plate_speed"),
        "spin_rate":                pitch.get("spin_rate"),
        "release_extension":        pitch.get("release_extension"),
        "plate_time":               pitch.get("plate_time"),
        "plate_x":                  pitch.get("plate_x"),
        "plate_z":                  pitch.get("plate_z"),
        "strike_zone":              pitch.get("strike_zone"),
        "strike_zone_top":          pitch.get("strike_zone_top"),
        "strike_zone_bottom":       pitch.get("strike_zone_bottom"),
        "horizontal_movement":      pitch.get("horizontal_movement"),
        "vertical_movement":        pitch.get("vertical_movement"),
        "horizontal_break":         pitch.get("horizontal_break"),
        "vertical_break":           pitch.get("vertical_break"),
        "induced_vertical_break":   pitch.get("induced_vertical_break"),
        "release_pos_x":            pitch.get("release_pos_x"),
        "release_pos_y":            pitch.get("release_pos_y"),
        "release_pos_z":            pitch.get("release_pos_z"),
        "velocity_x":               pitch.get("velocity_x"),
        "velocity_y":               pitch.get("velocity_y"),
        "velocity_z":               pitch.get("velocity_z"),
        "acceleration_x":           pitch.get("acceleration_x"),
        "acceleration_y":           pitch.get("acceleration_y"),
        "acceleration_z":           pitch.get("acceleration_z"),
        "bat_speed":                pitch.get("bat_speed"),
        "exit_velocity":            pitch.get("exit_velocity"),
        "launch_angle":             pitch.get("launch_angle"),
        "hit_distance":             pitch.get("hit_distance"),
        "expected_batting_average": pitch.get("expected_batting_average"),
        "is_barrel":                pitch.get("is_barrel"),
        "hit_coordinate_x":         pitch.get("hit_coordinate_x"),
        "hit_coordinate_y":         pitch.get("hit_coordinate_y"),
        "game_pitch_count":         pitch.get("game_pitch_count"),
        "pitcher_pitch_count":      pitch.get("pitcher_pitch_count"),
    }


def flatten_plate_appearance(pa: dict, game_id: int) -> tuple:
    pa_number = pa.get("pa_number", 0)
    batter_id = pa.get("batter_id")
    pa_key = f"{game_id}|{batter_id}|{pa_number}"
    pa_row = {
        "pa_key":               pa_key,
        "game_id":              game_id,
        "batter_id":            batter_id,
        "pitcher_id":           pa.get("pitcher_id"),
        "inning":               pa.get("inning"),
        "half_inning":          pa.get("half_inning"),
        "pa_number":            pa_number,
        "outs":                 pa.get("outs"),
        "batter_side":          pa.get("batter_side"),
        "pitcher_hand":         pa.get("pitcher_hand"),
        "result":               pa.get("result"),
        "is_ball_in_play_out":  pa.get("is_ball_in_play_out"),
        "runner_on_first":      pa.get("runner_on_first"),
        "runner_on_second":     pa.get("runner_on_second"),
        "runner_on_third":      pa.get("runner_on_third"),
        "pitch_count":          len(pa.get("pitches") or []),
    }
    pitch_rows = [flatten_pitch(p, pa_key) for p in (pa.get("pitches") or [])]
    return pa_row, pitch_rows


def flatten_lineup(lu: dict) -> dict:
    player = lu.get("player") or {}
    team   = lu.get("team")   or {}
    return {
        "lineup_id":            lu.get("id"),
        "game_id":              lu.get("game_id"),
        # Batting order / position info
        "batting_order":        lu.get("batting_order"),
        "position":             lu.get("position"),
        "is_probable_pitcher":  lu.get("is_probable_pitcher"),
        # Player info (full object provided by API)
        "player_id":            player.get("id"),
        "player_name":          player.get("full_name"),
        "player_first_name":    player.get("first_name"),
        "player_last_name":     player.get("last_name"),
        "player_position":      player.get("position"),
        "player_jersey":        player.get("jersey"),
        "player_bats_throws":   player.get("bats_throws"),
        "player_active":        player.get("active"),
        # Team info
        "team_id":              team.get("id"),
        "team_name":            team.get("display_name"),
        "team_abbr":            team.get("abbreviation"),
    }


def flatten_injury(i: dict) -> dict:
    player = i.get("player") or {}
    team   = (player.get("team") or {})
    return {
        "player_id":     player.get("id"),
        "player_name":   player.get("full_name"),
        "team_id":       team.get("id"),
        "team_name":     team.get("display_name"),
        "team_abbr":     team.get("abbreviation"),
        "date":          i.get("date"),
        "return_date":   i.get("return_date"),
        "type":          i.get("type"),
        "detail":        i.get("detail"),
        "side":          i.get("side"),
        "status":        i.get("status"),
        "long_comment":  i.get("long_comment"),
        "short_comment": i.get("short_comment"),
    }



def flatten_odd(o: dict) -> dict:
    return {
        "odd_id":               o.get("id"),
        "game_id":              o.get("game_id"),
        "vendor":               o.get("vendor"),
        "spread_home_value":    o.get("spread_home_value"),
        "spread_home_odds":     o.get("spread_home_odds"),
        "spread_away_value":    o.get("spread_away_value"),
        "spread_away_odds":     o.get("spread_away_odds"),
        "moneyline_home_odds":  o.get("moneyline_home_odds"),
        "moneyline_away_odds":  o.get("moneyline_away_odds"),
        "total_value":          o.get("total_value"),
        "total_over_odds":      o.get("total_over_odds"),
        "total_under_odds":     o.get("total_under_odds"),
        "updated_at":           o.get("updated_at"),
    }


def flatten_player_prop(p: dict) -> dict:
    market = p.get("market") or {}
    return {
        "prop_id":      p.get("id"),
        "game_id":      p.get("game_id"),
        "player_id":    p.get("player_id"),
        "vendor":       p.get("vendor"),
        "prop_type":    p.get("prop_type"),
        "line_value":   p.get("line_value"),
        "market_type":  market.get("type"),
        "over_odds":    market.get("over_odds"),
        "under_odds":   market.get("under_odds"),
        "odds":         market.get("odds"),
        "updated_at":   p.get("updated_at"),
    }


# ------------------------------------------------------------------ #
# Backfill functions
# ------------------------------------------------------------------ #

def backfill_teams(client):
    print("\n⚾ TEAMS")
    teams = client.get_teams()
    if not teams:
        print("   No teams found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_team(t) for t in teams])
    save_csv(df, "teams.csv", "teams")
    return df


def backfill_players(client, team_ids=None):
    print("\n👤 PLAYERS")
    players = client.get_active_players(team_ids=team_ids)
    if not players:
        print("   No players found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_player(p) for p in players])
    save_csv(df, "players.csv", "players")
    return df


def backfill_games(client, start, end, team_ids=None, season=2025):
    print(f"\n🗓️  GAMES  ({start} → {end})")
    dates = date_range(start, end)
    all_rows = []
    for i in range(0, len(dates), 7):
        batch = dates[i:i+7]
        print(f"   {batch[0]} → {batch[-1]} ...", end=" ", flush=True)
        games = client.get_games(dates=batch, team_ids=team_ids, season_type="regular")
        print(f"{len(games)} games")
        all_rows.extend([flatten_game(g) for g in games])
    if not all_rows:
        print("   No games found."); return pd.DataFrame()
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["game_id"])
    save_csv(df, f"games_{season}.csv", "games")
    return df


def backfill_stats(client, season, team_ids=None):
    print(f"\n📊 PLAYER GAME STATS  (season {season})")
    stats = client.get_stats(seasons=[season])
    if not stats:
        print("   No stats found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_stat(s, season=season) for s in stats])
    df = df.drop_duplicates(subset=["game_id", "player_id"])
    save_csv(df, f"stats_{season}.csv", "player game stats")
    return df


def backfill_standings(client, season):
    print(f"\n🏆 STANDINGS  (season {season})")
    standings = client.get_standings(season=season)
    if not standings:
        print("   No standings found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_standing(s) for s in standings])
    save_csv(df, f"standings_{season}.csv", "standings")
    return df


def backfill_season_stats(client, season):
    print(f"\n📈 SEASON STATS  (season {season})")
    stats = client.get_season_stats(season=season)
    if not stats:
        print("   No season stats found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_season_stat(s) for s in stats])
    save_csv(df, f"season_stats_{season}.csv", "season stats")
    return df


def backfill_team_season_stats(client, season):
    print(f"\n🏟️  TEAM SEASON STATS  (season {season})")
    stats = client.get_team_season_stats(season=season)
    if not stats:
        print("   No team season stats found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_team_season_stat(s) for s in stats])
    save_csv(df, f"team_season_stats_{season}.csv", "team season stats")
    return df


def backfill_splits(client, season, player_ids=None):
    print(f"\n🔀 PLAYER SPLITS  (season {season})")
    if not player_ids:
        p = DATA_DIR / "players.csv"
        if p.exists():
            player_ids = pd.read_csv(p)["player_id"].dropna().astype(int).tolist()
        else:
            print("   ⚠️  Run --players first."); return pd.DataFrame()
    all_rows = []
    print(f"   {len(player_ids)} players...")
    for pid in player_ids:
        try:
            data = client.get_splits(player_id=pid, season=season)
            for group in data.values():
                if isinstance(group, list):
                    all_rows.extend([flatten_split(item) for item in group])
        except Exception as e:
            print(f"   ⚠️  player {pid}: {e}")
    if not all_rows:
        print("   No splits found."); return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    save_csv(df, f"splits_{season}.csv", "player splits")
    return df


def backfill_versus(client, player_ids=None, team_ids=None):
    print("\n⚔️  BATTER VS PITCHER")
    if not player_ids:
        p = DATA_DIR / "players.csv"
        if p.exists():
            player_ids = pd.read_csv(p)["player_id"].dropna().astype(int).tolist()
        else:
            print("   ⚠️  Run --players first."); return pd.DataFrame()
    if not team_ids:
        t = DATA_DIR / "teams.csv"
        if t.exists():
            team_ids = pd.read_csv(t)["team_id"].dropna().astype(int).tolist()
        else:
            print("   ⚠️  Run --teams first."); return pd.DataFrame()
    all_rows = []
    print(f"   {len(player_ids)} players × {len(team_ids)} teams — this is rate-limit heavy.")
    for pid in player_ids:
        for tid in team_ids:
            try:
                rows = client.get_versus(player_id=pid, opponent_team_id=tid)
                all_rows.extend([flatten_versus(v) for v in rows])
            except Exception:
                pass
    if not all_rows:
        print("   No versus data found."); return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    save_csv(df, "versus.csv", "batter vs pitcher")
    return df


def backfill_plays(client, start, end, team_ids=None, season=2025):
    print(f"\n▶️  PLAY-BY-PLAY  ({start} → {end})")
    all_rows = []
    for d in date_range(start, end):
        games = client.get_games(dates=[d], team_ids=team_ids, season_type="regular")
        if not games:
            continue
        print(f"   {d}: {len(games)} games")
        for g in games:
            gid = g.get("id")
            try:
                plays = client.get_plays(game_id=gid)
                all_rows.extend([flatten_play(p) for p in plays])
            except Exception as e:
                print(f"   ⚠️  game {gid}: {e}")
    if not all_rows:
        print("   No play-by-play found."); return pd.DataFrame()
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["game_id", "order"])
    save_csv(df, f"plays_{season}.csv", "play-by-play")
    return df


def backfill_plate_appearances(client, start, end, team_ids=None, season=2025):
    print(f"\n🥎 PLATE APPEARANCES + STATCAST PITCHES  ({start} → {end})")
    pa_rows, pitch_rows = [], []
    for d in date_range(start, end):
        games = client.get_games(dates=[d], team_ids=team_ids, season_type="regular")
        if not games:
            continue
        print(f"   {d}: {len(games)} games")
        for g in games:
            gid = g.get("id")
            try:
                pas = client.get_plate_appearances(game_id=gid)
                for pa in pas:
                    pa_row, pitches = flatten_plate_appearance(pa, gid)
                    pa_rows.append(pa_row)
                    pitch_rows.extend(pitches)
            except Exception as e:
                print(f"   ⚠️  game {gid}: {e}")
    pa_df    = pd.DataFrame(pa_rows).drop_duplicates(subset=["pa_key"])    if pa_rows    else pd.DataFrame()
    pitch_df = pd.DataFrame(pitch_rows)                                     if pitch_rows else pd.DataFrame()
    save_csv(pa_df,    f"plate_appearances_{season}.csv", "plate appearances")
    save_csv(pitch_df, f"pitches_{season}.csv",           "Statcast pitches")
    return pa_df, pitch_df


def backfill_lineups(client, start, end, team_ids=None):
    print(f"\n📋 LINEUPS  ({start} → {end})")
    all_rows = []
    for d in date_range(start, end):
        games = client.get_games(dates=[d], team_ids=team_ids)
        if not games:
            continue
        game_ids = [g["id"] for g in games]
        try:
            lineups = client.get_lineups(game_ids=game_ids)
            all_rows.extend([flatten_lineup(lu) for lu in lineups])
        except Exception as e:
            print(f"   ⚠️  {d}: {e}")
    if not all_rows:
        print("   No lineups found."); return pd.DataFrame()
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["lineup_id"])
    save_csv(df, "lineups.csv", "lineups")
    return df


def backfill_injuries(client, team_ids=None):
    print("\n🏥 INJURIES")
    injuries = client.get_injuries(team_ids=team_ids)
    if not injuries:
        print("   No injuries found."); return pd.DataFrame()
    df = pd.DataFrame([flatten_injury(i) for i in injuries])
    save_csv(df, "injuries.csv", "injuries")
    return df


# ------------------------------------------------------------------ #
# Orchestrators
# ------------------------------------------------------------------ #

def run_full_backfill(client, start, end, season, team_ids=None):
    print(f"\n{'='*60}")
    print(f"  MLB BALLDONTLIE FULL BACKFILL")
    print(f"  Range: {start} → {end}  |  Season: {season}")
    print(f"  Team filter: {team_ids or 'All'}")
    print(f"{'='*60}")
    backfill_teams(client)
    players_df = backfill_players(client, team_ids=team_ids)
    player_ids = players_df["player_id"].tolist() if not players_df.empty else None
    backfill_games(client, start, end, team_ids=team_ids, season=season)
    backfill_stats(client, season=season, team_ids=team_ids)
    backfill_standings(client, season=season)
    backfill_season_stats(client, season=season)
    backfill_team_season_stats(client, season=season)
    # backfill_splits — disabled: data not available early season; re-enable in May+
    # backfill_injuries — disabled: re-enable when needed
    # backfill_odds — disabled: re-enable for live game days
    # backfill_player_props — disabled: re-enable for live game days
    backfill_plays(client, start, end, team_ids=team_ids, season=season)
    backfill_plate_appearances(client, start, end, team_ids=team_ids, season=season)
    backfill_lineups(client, start, end, team_ids=team_ids)
    print(f"\n{'='*60}")
    print(f"  ✅ Full backfill complete! → {DATA_DIR.resolve()}")
    print(f"{'='*60}\n")


def run_daily(client, season, team_ids=None):
    today = date.today().strftime("%Y-%m-%d")
    print(f"\n📅 DAILY — {today}")
    backfill_games(client, today, today, team_ids=team_ids, season=season)
    backfill_stats(client, season=season, team_ids=team_ids)
    backfill_plays(client, today, today, team_ids=team_ids, season=season)
    backfill_plate_appearances(client, today, today, team_ids=team_ids, season=season)
    backfill_lineups(client, today, today, team_ids=team_ids)
    backfill_standings(client, season=season)
    # backfill_injuries — disabled
    # backfill_odds — disabled: re-enable for live game days
    # backfill_player_props — disabled: re-enable for live game days
    print("\n✅ Daily complete.\n")


def resolve_team_ids(client, abbr):
    for t in client.get_teams():
        if t.get("abbreviation", "").upper() == abbr.upper():
            return [t["id"]]
    raise ValueError(f"Team '{abbr}' not found.")


def main():
    parser = argparse.ArgumentParser(description="MLB BallDontLie Backfill")
    parser.add_argument("--start",              default=None)
    parser.add_argument("--end",                default=None)
    parser.add_argument("--season",             type=int, default=2026,
                        help="MLB season year (default: 2026 — current season)")
    parser.add_argument("--full",               action="store_true")
    parser.add_argument("--daily",              action="store_true")
    parser.add_argument("--team",               default=None)
    parser.add_argument("--teams",              action="store_true")
    parser.add_argument("--players",            action="store_true")
    parser.add_argument("--games",              action="store_true")
    parser.add_argument("--stats",              action="store_true")
    parser.add_argument("--standings",          action="store_true")
    parser.add_argument("--season-stats",       action="store_true")
    parser.add_argument("--team-season-stats",  action="store_true")
    parser.add_argument("--splits",             action="store_true")
    parser.add_argument("--versus",             action="store_true")
    parser.add_argument("--plays",              action="store_true")
    parser.add_argument("--plate-appearances",  action="store_true")
    parser.add_argument("--lineups",            action="store_true")
    parser.add_argument("--injuries",           action="store_true")
    parser.add_argument("--odds",               action="store_true")
    parser.add_argument("--player-props",       action="store_true")
    args = parser.parse_args()

    today = date.today().strftime("%Y-%m-%d")
    # Default start = Opening Day of the default season
    OPENING_DAYS = {2025: "2025-03-27", 2026: "2026-03-27"}
    start = args.start or OPENING_DAYS.get(args.season, f"{args.season}-03-27")
    end   = args.end   or today

    client = MLBBallDontLieClient()
    team_ids = None
    if args.team:
        team_ids = resolve_team_ids(client, args.team)
        print(f"Team filter: {args.team} → id {team_ids}")

    if args.daily:   run_daily(client, args.season, team_ids); return
    if args.full:    run_full_backfill(client, start, end, args.season, team_ids); return

    ran = False
    if args.teams:              backfill_teams(client); ran = True
    if args.players:            backfill_players(client, team_ids); ran = True
    if args.games:              backfill_games(client, start, end, team_ids, season=args.season); ran = True
    if args.stats:              backfill_stats(client, args.season, team_ids); ran = True
    if args.standings:          backfill_standings(client, args.season); ran = True
    if args.season_stats:       backfill_season_stats(client, args.season); ran = True
    if args.team_season_stats:  backfill_team_season_stats(client, args.season); ran = True
    if args.splits:             backfill_splits(client, args.season); ran = True
    if args.versus:             backfill_versus(client, team_ids=team_ids); ran = True
    if args.plays:              backfill_plays(client, start, end, team_ids, season=args.season); ran = True
    if args.plate_appearances:  backfill_plate_appearances(client, start, end, team_ids, season=args.season); ran = True
    if args.lineups:            backfill_lineups(client, start, end, team_ids); ran = True
    if args.injuries:           backfill_injuries(client, team_ids); ran = True
    if args.odds:               backfill_odds(client, start, end, team_ids); ran = True
    if args.player_props:       backfill_player_props(client, start, end, team_ids); ran = True
    if not ran:
        parser.print_help()


if __name__ == "__main__":
    main()

# ------------------------------------------------------------------ #
# Betting Odds (GOAT — available from 2026 season)
# Live data: odds updated in real-time, may disappear near game end
# Either dates or game_ids required — we loop by date fetching game_ids first
# ------------------------------------------------------------------ #
def backfill_odds(client, start, end, team_ids=None):
    print(f"\n💰 BETTING ODDS  ({start} → {end})")
    all_rows = []
    for d in date_range(start, end):
        games = client.get_games(dates=[d], team_ids=team_ids)
        if not games:
            continue
        game_ids = [g["id"] for g in games]
        try:
            odds = client.get_odds(game_ids=game_ids)
            if odds:
                print(f"   {d}: {len(odds)} odds records")
            all_rows.extend([flatten_odd(o) for o in odds])
        except Exception as e:
            print(f"   ⚠️  {d}: {e}")
    if not all_rows:
        print("   No odds data found (only available from 2026 season).")
        return pd.DataFrame()
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["odd_id"])
    save_csv(df, "odds.csv", "betting odds")
    return df


# ------------------------------------------------------------------ #
# Player Props (GOAT — available from 2026 season)
# LIVE only — no historical data stored; snapshots only
# Loop per game_id since game_id is required
# ------------------------------------------------------------------ #
def backfill_player_props(client, start, end, team_ids=None):
    print(f"\n🎯 PLAYER PROPS  ({start} → {end})")
    all_rows = []
    for d in date_range(start, end):
        games = client.get_games(dates=[d], team_ids=team_ids)
        if not games:
            continue
        print(f"   {d}: {len(games)} games")
        for g in games:
            gid = g.get("id")
            try:
                props = client.get_player_props(game_id=gid)
                all_rows.extend([flatten_player_prop(p) for p in props])
            except Exception as e:
                print(f"   ⚠️  game {gid}: {e}")
    if not all_rows:
        print("   No player props found (only available from 2026 season).")
        return pd.DataFrame()
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["prop_id"])
    save_csv(df, "player_props.csv", "player props")
    return df
