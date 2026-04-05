# MLB BallDontLie Data Scraper

MLB data collection system built on the BallDontLie API (GOAT tier).
Modeled after the NBA backfill pipeline.

---

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file:
```
BALLDONTLIE_API_KEY=your_key_here
DB_HOST=your-rds-host.amazonaws.com
DB_PORT=5432
DB_NAME=sports
DB_USER=postgres
DB_PASSWORD=your_password
```

Initialize the database:
```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f migrations/001_mlb_schema.sql
```

---

## Quick Start

```bash
# Full 2025 season backfill
python py/mlb_balldontlie_backfill.py --start 2025-03-27 --end 2025-10-01 --season 2025 --full

# Daily collection (run this every day)
python py/mlb_balldontlie_backfill.py --daily

# Filter to one team
python py/mlb_balldontlie_backfill.py --start 2025-04-01 --end 2025-04-30 --season 2025 --full --team NYY

# Specific endpoints only
python py/mlb_balldontlie_backfill.py --start 2025-04-01 --end 2025-04-07 --games --stats

# Load all CSVs to PostgreSQL
python loader/mlb_load_data.py --all --season 2025
```

---

## Endpoints Collected

| Endpoint              | Flag                    | Output File                  |
|-----------------------|-------------------------|------------------------------|
| Teams                 | `--teams`               | `teams.csv`                  |
| Players               | `--players`             | `players.csv`                |
| Games                 | `--games`               | `games_2025.csv`             |
| Player Game Stats     | `--stats`               | `stats_2025.csv`             |
| Standings             | `--standings`           | `standings_2025.csv`         |
| Player Season Stats   | `--season-stats`        | `season_stats_2025.csv`      |
| Team Season Stats     | `--team-season-stats`   | `team_season_stats_2025.csv` |
| Player Splits         | `--splits`              | `splits_2025.csv`            |
| Batter vs Pitcher     | `--versus`              | `versus_2025.csv`            |
| Play-by-Play          | `--plays`               | `plays_2025.csv`             |
| Plate Appearances     | `--plate-appearances`   | `plate_appearances_2025.csv` |
| Injuries              | `--injuries`            | `injuries.csv`               |
| Betting Odds          | `--odds`                | `odds.csv`                   |
| Player Props          | `--player-props`        | `player_props.csv`           |

All output goes to `data/mlb/`.

> **Note:** Betting Odds and Player Props are only available from the **2026 season** onward.
> Player Props are **live only** — the API does not store historical prop data.
> `--versus` is intentionally excluded from `--full` due to high request volume (players × teams); run separately.

---

## File Structure

```
mlb-data-scraper/
├── py/
│   ├── mlb_balldontlie_client.py   # API client (all endpoints)
│   └── mlb_balldontlie_backfill.py # Main backfill — saves direct to CSV
├── loader/
│   └── mlb_load_data.py            # PostgreSQL loader
├── migrations/
│   └── 001_mlb_schema.sql          # DB schema (16 tables)
├── convert_to_csv.py               # CSV consolidation tool (like NBA's convert_parquet_to_csv.py)
├── data/
│   └── mlb/                        # CSV output (auto-created)
├── requirements.txt
└── README.md
```

## CSV Consolidation Tool

Mirrors the NBA backfill's `convert_parquet_to_csv.py` pattern. Use after running backfills:

```bash
# See all collected files with row counts and sizes
python convert_to_csv.py --status

# Validate all files (null PKs, duplicates)
python convert_to_csv.py --validate

# Merge 2024 + 2025 data into combined files
python convert_to_csv.py --merge --seasons 2024 2025

# Export clean copy to target directory
python convert_to_csv.py --export --output ~/exports/mlb

# Full workflow: validate + merge + export
python convert_to_csv.py --all --output ~/exports/mlb
```

## Smart CSV Merge

The backfill uses **smart merge** — running it multiple times accumulates records safely.
If `games_2025.csv` already exists, new games are merged in and deduplicated by primary key.
Old records are never lost. This is the same incremental pattern as the NBA backfill.

---

## Team Abbreviations

| Abbr | Team                    |
|------|-------------------------|
| ARI  | Arizona Diamondbacks    |
| ATL  | Atlanta Braves          |
| BAL  | Baltimore Orioles       |
| BOS  | Boston Red Sox          |
| CHC  | Chicago Cubs            |
| CWS  | Chicago White Sox       |
| CIN  | Cincinnati Reds         |
| CLE  | Cleveland Guardians     |
| COL  | Colorado Rockies        |
| DET  | Detroit Tigers          |
| HOU  | Houston Astros          |
| KC   | Kansas City Royals      |
| LAA  | Los Angeles Angels      |
| LAD  | Los Angeles Dodgers     |
| MIA  | Miami Marlins           |
| MIL  | Milwaukee Brewers       |
| MIN  | Minnesota Twins         |
| NYM  | New York Mets           |
| NYY  | New York Yankees        |
| OAK  | Athletics               |
| PHI  | Philadelphia Phillies   |
| PIT  | Pittsburgh Pirates      |
| SD   | San Diego Padres        |
| SF   | San Francisco Giants    |
| SEA  | Seattle Mariners        |
| STL  | St. Louis Cardinals     |
| TB   | Tampa Bay Rays          |
| TEX  | Texas Rangers           |
| TOR  | Toronto Blue Jays       |
| WSH  | Washington Nationals    |
