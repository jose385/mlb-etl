#!/usr/bin/env python3
"""
MLB BallDontLie API Client
All endpoints verified against live docs at mlb.balldontlie.io
GOAT tier required for all GOAT-only endpoints.
"""

import os
import time
import requests
from typing import Optional, Generator


class MLBBallDontLieClient:
    BASE_URL = "https://api.balldontlie.io"
    DEFAULT_PER_PAGE = 100
    REQUEST_DELAY = 0.12  # ~500 req/min, safely under 600/min GOAT limit

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("BALLDONTLIE_API_KEY")
        if not self.api_key:
            raise ValueError("BALLDONTLIE_API_KEY not set.")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": self.api_key})

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {}, timeout=30)
        response.raise_for_status()
        time.sleep(self.REQUEST_DELAY)
        return response.json()

    def _paginate(self, endpoint: str, params: dict = None) -> Generator[dict, None, None]:
        """Cursor-based pagination — yields one record at a time."""
        params = dict(params or {})
        params["per_page"] = params.get("per_page", self.DEFAULT_PER_PAGE)
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self._get(endpoint, params)
            for record in data.get("data", []):
                yield record
            cursor = (data.get("meta") or {}).get("next_cursor")
            if not cursor:
                break

    # ------------------------------------------------------------------ #
    # Teams — Free tier
    # ------------------------------------------------------------------ #
    def get_teams(self, division: str = None, league: str = None) -> list:
        params = {}
        if division:
            params["division"] = division
        if league:
            params["league"] = league
        return list(self._paginate("/mlb/v1/teams", params))

    def get_team(self, team_id: int) -> dict:
        return self._get(f"/mlb/v1/teams/{team_id}").get("data", {})

    # ------------------------------------------------------------------ #
    # Players — Free tier
    # ------------------------------------------------------------------ #
    def get_players(self, search: str = None, first_name: str = None,
                    last_name: str = None, team_ids: list = None,
                    player_ids: list = None) -> list:
        params = {}
        if search:        params["search"] = search
        if first_name:    params["first_name"] = first_name
        if last_name:     params["last_name"] = last_name
        if team_ids:      params["team_ids[]"] = team_ids
        if player_ids:    params["player_ids[]"] = player_ids
        return list(self._paginate("/mlb/v1/players", params))

    def get_player(self, player_id: int) -> dict:
        return self._get(f"/mlb/v1/players/{player_id}").get("data", {})

    # ------------------------------------------------------------------ #
    # Active Players — ALL-STAR+
    # ------------------------------------------------------------------ #
    def get_active_players(self, search: str = None, team_ids: list = None,
                            player_ids: list = None) -> list:
        params = {}
        if search:     params["search"] = search
        if team_ids:   params["team_ids[]"] = team_ids
        if player_ids: params["player_ids[]"] = player_ids
        return list(self._paginate("/mlb/v1/players/active", params))

    # ------------------------------------------------------------------ #
    # Player Injuries — ALL-STAR+
    # ------------------------------------------------------------------ #
    def get_injuries(self, player_ids: list = None, team_ids: list = None) -> list:
        params = {}
        if player_ids: params["player_ids[]"] = player_ids
        if team_ids:   params["team_ids[]"] = team_ids
        return list(self._paginate("/mlb/v1/player_injuries", params))

    # ------------------------------------------------------------------ #
    # Games — Free tier
    # ------------------------------------------------------------------ #
    def get_games(self, dates: list = None, seasons: list = None,
                  team_ids: list = None, season_type: str = None,
                  postseason: bool = None) -> list:
        params = {}
        if dates:       params["dates[]"] = dates
        if seasons:     params["seasons[]"] = seasons
        if team_ids:    params["team_ids[]"] = team_ids
        if season_type:
            params["season_type"] = season_type
        elif postseason is not None:
            params["postseason"] = str(postseason).lower()
        return list(self._paginate("/mlb/v1/games", params))

    def get_game(self, game_id: int) -> dict:
        return self._get(f"/mlb/v1/games/{game_id}").get("data", {})

    # ------------------------------------------------------------------ #
    # Player Game Stats — ALL-STAR+
    # NOTE: filter by game_ids[], player_ids[], or seasons[] (no dates filter)
    # ------------------------------------------------------------------ #
    def get_stats(self, game_ids: list = None, player_ids: list = None,
                  seasons: list = None) -> list:
        params = {}
        if game_ids:   params["game_ids[]"] = game_ids
        if player_ids: params["player_ids[]"] = player_ids
        if seasons:    params["seasons[]"] = seasons
        return list(self._paginate("/mlb/v1/stats", params))

    # ------------------------------------------------------------------ #
    # Team Standings — ALL-STAR+
    # NOTE: no pagination on this endpoint per live docs
    # ------------------------------------------------------------------ #
    def get_standings(self, season: int) -> list:
        return self._get("/mlb/v1/standings", {"season": season}).get("data", [])

    # ------------------------------------------------------------------ #
    # Player Season Stats — GOAT
    # ------------------------------------------------------------------ #
    def get_season_stats(self, season: int, player_ids: list = None,
                         team_id: int = None, season_type: str = None,
                         sort_by: str = None, sort_order: str = None) -> list:
        params = {"season": season}
        if player_ids:  params["player_ids[]"] = player_ids
        if team_id:     params["team_id"] = team_id
        if season_type: params["season_type"] = season_type
        if sort_by:     params["sort_by"] = sort_by
        if sort_order:  params["sort_order"] = sort_order
        return list(self._paginate("/mlb/v1/season_stats", params))

    # ------------------------------------------------------------------ #
    # Team Season Stats — GOAT
    # NOTE: no cursor pagination on this endpoint
    # ------------------------------------------------------------------ #
    def get_team_season_stats(self, season: int, team_id: int = None,
                               season_type: str = None) -> list:
        params = {"season": season}
        if team_id:     params["team_id"] = team_id
        if season_type: params["season_type"] = season_type
        return self._get("/mlb/v1/teams/season_stats", params).get("data", [])

    # ------------------------------------------------------------------ #
    # Player Splits — GOAT
    # NOTE: player_id AND season both REQUIRED
    # Returns a dict grouped by split_category (byArena, byOpponent, etc.)
    # ------------------------------------------------------------------ #
    def get_splits(self, player_id: int, season: int) -> dict:
        return self._get("/mlb/v1/players/splits",
                         {"player_id": player_id, "season": season}).get("data", {})

    # ------------------------------------------------------------------ #
    # Player vs Player — GOAT
    # NOTE: player_id AND opponent_team_id both REQUIRED
    # ------------------------------------------------------------------ #
    def get_versus(self, player_id: int, opponent_team_id: int) -> list:
        return self._get("/mlb/v1/players/versus",
                         {"player_id": player_id,
                          "opponent_team_id": opponent_team_id}).get("data", [])

    # ------------------------------------------------------------------ #
    # Play-by-Play — GOAT
    # NOTE: game_id is REQUIRED — must loop per game
    # ------------------------------------------------------------------ #
    def get_plays(self, game_id: int) -> list:
        return list(self._paginate("/mlb/v1/plays", {"game_id": game_id}))

    # ------------------------------------------------------------------ #
    # Plate Appearances + Statcast Pitches — GOAT
    # NOTE: game_id is REQUIRED; ALL plate appearances returned at once (no pagination)
    # ------------------------------------------------------------------ #
    def get_plate_appearances(self, game_id: int) -> list:
        return self._get("/mlb/v1/plate_appearances",
                         {"game_id": game_id}).get("data", [])

    # ------------------------------------------------------------------ #
    # Betting Odds — GOAT (available from 2026 season)
    # NOTE: either dates[] or game_ids[] is required
    # Odds are LIVE and updated in real-time; some may disappear near game end
    # Vendors: betmgm, draftkings, fanatics, fanduel
    # NOTE: all 4 vendors available for odds; betmgm NOT available for player props
    # ------------------------------------------------------------------ #
    def get_odds(self, dates: list = None, game_ids: list = None) -> list:
        params = {}
        if dates:    params["dates[]"] = dates
        if game_ids: params["game_ids[]"] = game_ids
        return list(self._paginate("/mlb/v1/odds", params))

    # ------------------------------------------------------------------ #
    # Player Props — GOAT (available from 2026 season)
    # NOTE: game_id is REQUIRED; data is LIVE only (no historical storage)
    # All props returned in single response — no pagination
    # Batter prop types: hits, home_runs, total_bases, rbis, stolen_bases,
    #   singles, doubles, triples, walks, strikeouts, runs_scored,
    #   hits_runs_rbis, first_home_run (milestone)
    # Pitcher prop types: pitcher_strikeouts, pitcher_outs,
    #   pitcher_hits_allowed, pitcher_walks, pitcher_earned_runs,
    #   pitcher_record_a_win (milestone)
    # Vendors: draftkings, fanatics, fanduel
    # NOTE: betmgm is NOT available for player props (only for odds)
    # ------------------------------------------------------------------ #
    def get_player_props(self, game_id: int, player_id: int = None,
                          prop_type: str = None, vendors: list = None) -> list:
        params = {"game_id": game_id}
        if player_id: params["player_id"] = player_id
        if prop_type: params["prop_type"] = prop_type
        if vendors:   params["vendors[]"] = vendors
        return self._get("/mlb/v1/odds/player_props", params).get("data", [])

    # ------------------------------------------------------------------ #
    # Lineups — GOAT (available from 2026 season)
    # NOTE: game_ids[] is REQUIRED; lineups appear 1-2 hours before first pitch
    # ------------------------------------------------------------------ #
    def get_lineups(self, game_ids: list) -> list:
        return list(self._paginate("/mlb/v1/lineups", {"game_ids[]": game_ids}))
