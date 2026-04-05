#!/usr/bin/env python3
"""
MLB CSV Consolidation Tool
Mirrors the NBA backfill's convert_parquet_to_csv.py pattern.

Since the MLB system writes directly to CSV (no parquet intermediate),
this script handles:
  1. Listing all collected CSV files with row counts and sizes
  2. Merging season-year CSV files (e.g., merging 2024 + 2025 data)
  3. Validating CSV integrity (null PKs, duplicate checks)
  4. Exporting a clean copy to a target directory

Usage:
    # Show status of all collected CSV files
    python convert_to_csv.py --status

    # Merge multiple seasons into combined files
    python convert_to_csv.py --merge --seasons 2024 2025

    # Validate all CSV files
    python convert_to_csv.py --validate

    # Export clean CSVs to a target directory
    python convert_to_csv.py --export --output ~/exports/mlb

    # Full workflow: validate + merge + export
    python convert_to_csv.py --all --output ~/exports/mlb
"""

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data/mlb")

# ------------------------------------------------------------------ #
# Primary key definitions (must match backfill script)
# ------------------------------------------------------------------ #
CSV_PKS = {
    "teams.csv":                    ["team_id"],
    "players.csv":                  ["player_id"],
    "games_2025.csv":               ["game_id"],
    "games_2024.csv":               ["game_id"],
    "games_2023.csv":               ["game_id"],
    "stats_2025.csv":               ["game_id", "player_id"],
    "stats_2024.csv":               ["game_id", "player_id"],
    "stats_2023.csv":               ["game_id", "player_id"],
    "standings_2025.csv":           ["team_id", "season"],
    "standings_2024.csv":           ["team_id", "season"],
    "season_stats_2025.csv":        ["player_id", "season", "season_type"],
    "season_stats_2024.csv":        ["player_id", "season", "season_type"],
    "team_season_stats_2025.csv":   ["team_id", "season", "season_type"],
    "team_season_stats_2024.csv":   ["team_id", "season", "season_type"],
    "splits_2025.csv":              ["player_id", "season", "category", "split_category", "split_name"],
    "splits_2024.csv":              ["player_id", "season", "category", "split_category", "split_name"],
    "versus.csv":                   ["player_id", "opponent_id", "opponent_team_id"],
    "plays_2025.csv":               ["game_id", "order"],
    "plays_2024.csv":               ["game_id", "order"],
    "plate_appearances_2025.csv":   ["pa_key"],
    "plate_appearances_2024.csv":   ["pa_key"],
    "pitches_2025.csv":             ["pa_key", "pitch_number"],
    "pitches_2024.csv":             ["pa_key", "pitch_number"],
    "lineups.csv":                  ["lineup_id"],
    "injuries.csv":                 ["player_id", "date"],
    "odds.csv":                     ["odd_id"],
    "player_props.csv":             ["prop_id"],
}

# Season-aware files (suffixed with _YYYY.csv)
SEASON_FILES = [
    "games",
    "stats",
    "standings",
    "season_stats",
    "team_season_stats",
    "splits",
    "plays",
    "plate_appearances",
    "pitches",
]

# Non-season files (no year suffix)
STATIC_FILES = [
    "teams.csv",
    "players.csv",
    "versus.csv",
    "lineups.csv",
    "injuries.csv",
    "odds.csv",
    "player_props.csv",
]


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def human_size(path: Path) -> str:
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def dedup(df: pd.DataFrame, pk_cols: list) -> pd.DataFrame:
    if not pk_cols:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=pk_cols, keep="last")
    after = len(df)
    if before != after:
        print(f"     ⚠️  Removed {before - after:,} duplicates")
    return df


# ------------------------------------------------------------------ #
# Commands
# ------------------------------------------------------------------ #

def cmd_status() -> None:
    """Print a summary table of all collected CSV files."""
    print(f"\n{'='*70}")
    print(f"  MLB DATA STATUS  —  {DATA_DIR.resolve()}")
    print(f"{'='*70}")
    print(f"{'File':<42} {'Rows':>10} {'Size':>9}  PKs")
    print(f"{'-'*70}")

    all_files = sorted(DATA_DIR.glob("*.csv"))
    if not all_files:
        print("  (no CSV files found — run backfill first)")
        return

    total_rows = 0
    for path in all_files:
        try:
            df = load_csv(path)
            rows = len(df)
            total_rows += rows
            pk = ", ".join(CSV_PKS.get(path.name, ["—"]))
            print(f"  {path.name:<40} {rows:>10,} {human_size(path):>9}  {pk}")
        except Exception as e:
            print(f"  {path.name:<40} {'ERROR':>10}            {e}")

    print(f"{'-'*70}")
    print(f"  {'TOTAL':<40} {total_rows:>10,}")
    print()


def cmd_validate() -> None:
    """Validate all CSV files — check for null PKs and duplicates."""
    print(f"\n{'='*60}")
    print("  VALIDATION")
    print(f"{'='*60}")

    all_files = sorted(DATA_DIR.glob("*.csv"))
    issues = 0

    for path in all_files:
        pk_cols = CSV_PKS.get(path.name)
        try:
            df = load_csv(path)
            file_ok = True

            # Check for null PKs
            if pk_cols:
                for col in pk_cols:
                    if col in df.columns:
                        nulls = df[col].isna().sum()
                        if nulls > 0:
                            print(f"  ❌ {path.name}: {nulls:,} null values in PK column '{col}'")
                            issues += 1
                            file_ok = False

                # Check for duplicates
                dupes = df.duplicated(subset=[c for c in pk_cols if c in df.columns]).sum()
                if dupes > 0:
                    print(f"  ❌ {path.name}: {dupes:,} duplicate rows on PKs {pk_cols}")
                    issues += 1
                    file_ok = False

            if file_ok:
                print(f"  ✅ {path.name}: {len(df):,} rows — OK")

        except Exception as e:
            print(f"  ❌ {path.name}: could not read — {e}")
            issues += 1

    print()
    if issues == 0:
        print("  ✅ All files passed validation!")
    else:
        print(f"  ⚠️  {issues} issue(s) found")
    print()


def cmd_merge(seasons: list) -> None:
    """
    Merge multiple seasons of season-suffixed files into combined CSVs.
    E.g., games_2024.csv + games_2025.csv → games_all.csv
    """
    print(f"\n{'='*60}")
    print(f"  MERGE SEASONS: {seasons}")
    print(f"{'='*60}")

    for base in SEASON_FILES:
        frames = []
        found = []
        for season in seasons:
            fname = f"{base}_{season}.csv"
            path = DATA_DIR / fname
            if path.exists():
                df = load_csv(path)
                frames.append(df)
                found.append(f"{fname} ({len(df):,} rows)")

        if not frames:
            continue

        print(f"\n  {base}:")
        for f in found:
            print(f"    + {f}")

        combined = pd.concat(frames, ignore_index=True)

        # Determine PK for dedup — try any season suffix
        pk_key = f"{base}_{seasons[0]}.csv"
        pk_cols = CSV_PKS.get(pk_key, [])
        if pk_cols:
            combined = dedup(combined, pk_cols)

        out_path = DATA_DIR / f"{base}_all.csv"
        combined.to_csv(out_path, index=False)
        print(f"    → {out_path.name}: {len(combined):,} rows")

    print()


def cmd_export(output_dir: str) -> None:
    """Export all CSV files (clean copy) to output_dir."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  EXPORT  →  {out.resolve()}")
    print(f"{'='*60}")

    all_files = sorted(DATA_DIR.glob("*.csv"))
    if not all_files:
        print("  (no CSV files to export)")
        return

    for path in all_files:
        try:
            df = load_csv(path)
            pk_cols = CSV_PKS.get(path.name)
            if pk_cols:
                df = dedup(df, pk_cols)
            dest = out / path.name
            df.to_csv(dest, index=False)
            print(f"  ✅ {path.name}: {len(df):,} rows → {dest}")
        except Exception as e:
            print(f"  ❌ {path.name}: {e}")

    print()
    print(f"  Export complete → {out.resolve()}")
    print()


def cmd_all(seasons: list, output_dir: str) -> None:
    cmd_status()
    cmd_validate()
    if seasons:
        cmd_merge(seasons)
    if output_dir:
        cmd_export(output_dir)


# ------------------------------------------------------------------ #
# CLI
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(
        description="MLB CSV consolidation tool — mirrors NBA convert_parquet_to_csv.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument("--status",   action="store_true",
                        help="Show all CSV files with row counts and sizes")
    parser.add_argument("--validate", action="store_true",
                        help="Validate all CSV files for null PKs and duplicates")
    parser.add_argument("--merge",    action="store_true",
                        help="Merge multiple seasons into combined files")
    parser.add_argument("--export",   action="store_true",
                        help="Export clean CSVs to output directory")
    parser.add_argument("--all",      action="store_true",
                        help="Run status + validate + merge + export")
    parser.add_argument("--seasons",  nargs="+", type=int, default=[2025],
                        help="Seasons to merge (default: 2025)")
    parser.add_argument("--output",   default="exports/mlb",
                        help="Output directory for export (default: exports/mlb)")
    parser.add_argument("--data-dir", default="data/mlb",
                        help="Data directory (default: data/mlb)")

    args = parser.parse_args()

    global DATA_DIR
    DATA_DIR = Path(args.data_dir)

    if not DATA_DIR.exists():
        print(f"⚠️  Data directory not found: {DATA_DIR}")
        print("    Run the backfill first: python py/mlb_balldontlie_backfill.py --full")
        return

    if args.all:
        cmd_all(args.seasons, args.output)
    else:
        ran = False
        if args.status:   cmd_status();             ran = True
        if args.validate: cmd_validate();           ran = True
        if args.merge:    cmd_merge(args.seasons);  ran = True
        if args.export:   cmd_export(args.output);  ran = True
        if not ran:
            parser.print_help()
            print("\n  Quick start:")
            print("    python convert_to_csv.py --status")
            print("    python convert_to_csv.py --validate")
            print("    python convert_to_csv.py --all --output exports/mlb")


if __name__ == "__main__":
    main()
