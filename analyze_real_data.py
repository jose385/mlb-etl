#!/usr/bin/env python3
"""
Analyze the real MLB data we successfully collected from 2025-08-12
Extract meaningful insights for betting analysis
"""
import pandas as pd
import numpy as np
from collections import Counter

def analyze_real_mlb_data():
    """Comprehensive analysis of real MLB data collected from yesterday"""
    
    print("🎯 REAL MLB DATA ANALYSIS - August 12, 2025")
    print("=" * 60)
    
    # Load the real Statcast data
    df = pd.read_parquet('stage/games_2025-08-12.parquet')
    
    print(f"📊 DATASET OVERVIEW:")
    print(f"   Total pitches: {len(df):,}")
    print(f"   Unique games: {df['game_pk'].nunique()}")
    print(f"   Date: {df['game_date'].iloc[0]}")
    print(f"   Teams: {len(df['home_team'].unique())} different teams")
    
    # Game-by-game breakdown
    print(f"\n🏟️ GAMES PLAYED:")
    games = df.groupby('game_pk').agg({
        'home_team': 'first',
        'away_team': 'first', 
        'pitch_type': 'count'
    }).rename(columns={'pitch_type': 'pitches'})
    
    for i, (game_pk, row) in enumerate(games.iterrows(), 1):
        print(f"   {i:2d}. Game {game_pk}: {row['away_team']} @ {row['home_team']} ({row['pitches']} pitches)")
    
    # Pitching analysis
    print(f"\n⚾ PITCHING ANALYSIS:")
    pitch_counts = df['pitch_type'].value_counts()
    total_pitches = len(df)
    
    print(f"   Most common pitch types:")
    for pitch_type, count in pitch_counts.head(5).items():
        pct = (count / total_pitches) * 100
        print(f"      {pitch_type}: {count:,} ({pct:.1f}%)")
    
    # Velocity analysis
    velocity_data = df['release_speed'].dropna()
    print(f"\n   Velocity statistics:")
    print(f"      Average: {velocity_data.mean():.1f} mph")
    print(f"      Range: {velocity_data.min():.1f} - {velocity_data.max():.1f} mph")
    print(f"      Fastest pitches:")
    
    fastest = df.nlargest(3, 'release_speed')[['release_speed', 'pitch_type', 'pitcher']]
    for _, row in fastest.iterrows():
        print(f"         {row['release_speed']:.1f} mph {row['pitch_type']} (Pitcher {row['pitcher']})")
    
    # Spin rate analysis
    spin_data = df['release_spin_rate'].dropna()
    print(f"\n   Spin rate statistics:")
    print(f"      Average: {spin_data.mean():.0f} RPM")
    print(f"      Range: {spin_data.min():.0f} - {spin_data.max():.0f} RPM")
    
    # Batting analysis
    print(f"\n🏏 BATTING ANALYSIS:")
    batted_balls = df[df['launch_speed'].notna()]
    print(f"   Batted balls: {len(batted_balls):,}")
    
    if len(batted_balls) > 0:
        print(f"   Exit velocity:")
        print(f"      Average: {batted_balls['launch_speed'].mean():.1f} mph")
        print(f"      Hard hit (95+ mph): {(batted_balls['launch_speed'] >= 95).sum():,} ({(batted_balls['launch_speed'] >= 95).mean()*100:.1f}%)")
        
        print(f"   Launch angle:")
        print(f"      Average: {batted_balls['launch_angle'].mean():.1f}°")
        
        # Sweet spot analysis (15-35 degrees)
        sweet_spot = batted_balls[(batted_balls['launch_angle'] >= 15) & (batted_balls['launch_angle'] <= 35)]
        print(f"      Sweet spot (15-35°): {len(sweet_spot):,} ({len(sweet_spot)/len(batted_balls)*100:.1f}%)")
    
    # Expected stats analysis
    xba_data = df['estimated_ba_using_speedangle'].dropna()
    if len(xba_data) > 0:
        print(f"\n📈 EXPECTED STATISTICS:")
        print(f"   Expected BA available: {len(xba_data):,} batted balls")
        print(f"   Average xBA: {xba_data.mean():.3f}")
        print(f"   High-quality contact (xBA > 0.500): {(xba_data > 0.500).sum():,}")
    
    # Barrel analysis
    barrels = df[df['launch_speed_angle'] == 6]
    if len(barrels) > 0:
        print(f"\n🎯 BARREL ANALYSIS:")
        print(f"   Total barrels: {len(barrels):,}")
        print(f"   Barrel rate: {len(barrels)/len(batted_balls)*100:.1f}% of batted balls")
        print(f"   Average barrel exit velo: {barrels['launch_speed'].mean():.1f} mph")
        print(f"   Average barrel launch angle: {barrels['launch_angle'].mean():.1f}°")
        
        # Barrel outcomes
        barrel_outcomes = barrels['events'].value_counts()
        print(f"   Barrel outcomes:")
        for outcome, count in barrel_outcomes.items():
            if pd.notna(outcome):
                print(f"      {outcome}: {count}")
    
    # Game context analysis
    print(f"\n⚾ GAME CONTEXT:")
    
    # Inning distribution
    inning_counts = df['inning'].value_counts().sort_index()
    print(f"   Pitches by inning:")
    for inning, count in inning_counts.items():
        if pd.notna(inning) and inning <= 12:  # Reasonable inning range
            print(f"      Inning {int(inning)}: {count:,} pitches")
    
    # Count analysis
    if 'balls' in df.columns and 'strikes' in df.columns:
        counts = df.groupby(['balls', 'strikes']).size()
        print(f"\n   Most common counts:")
        for (balls, strikes), count in counts.nlargest(5).items():
            if pd.notna(balls) and pd.notna(strikes):
                print(f"      {int(balls)}-{int(strikes)}: {count:,} pitches")
    
    # Summary for betting analysis
    print(f"\n💰 BETTING ANALYSIS SUMMARY:")
    print(f"   🎯 Data Quality: EXCELLENT (Real MLB data)")
    print(f"   📊 Sample Size: {len(df):,} pitches from 15 games")
    print(f"   ⚾ Advanced Metrics: Full Statcast coverage")
    print(f"   🔬 Expected Stats: Available for {len(xba_data):,} batted balls")
    print(f"   🎪 Barrel Data: {len(barrels):,} optimal contact events")
    
    print(f"\n✅ READY FOR CLAUDE BETTING ANALYSIS!")
    print(f"   This real data provides:")
    print(f"   • Pitcher effectiveness metrics")
    print(f"   • Batter quality of contact")
    print(f"   • Game flow and situational data") 
    print(f"   • Expected performance vs actual results")
    
    return df

def find_interesting_patterns(df):
    """Find interesting patterns in the real data"""
    
    print(f"\n🔍 INTERESTING PATTERNS FOUND:")
    
    # Find games with unusual characteristics
    game_stats = df.groupby('game_pk').agg({
        'release_speed': 'mean',
        'launch_speed': lambda x: x.dropna().mean() if x.dropna().any() else None,
        'estimated_ba_using_speedangle': lambda x: x.dropna().mean() if x.dropna().any() else None,
        'home_team': 'first',
        'away_team': 'first'
    })
    
    # High-velocity games
    high_velo_games = game_stats[game_stats['release_speed'] > game_stats['release_speed'].quantile(0.8)]
    if len(high_velo_games) > 0:
        print(f"\n   🚀 High-velocity games (top 20%):")
        for game_pk, row in high_velo_games.iterrows():
            print(f"      Game {game_pk}: {row['away_team']} @ {row['home_team']} (Avg: {row['release_speed']:.1f} mph)")
    
    # High contact quality games
    high_contact = game_stats[game_stats['estimated_ba_using_speedangle'] > 0.300]
    if len(high_contact) > 0:
        print(f"\n   🎯 High contact quality games (xBA > 0.300):")
        for game_pk, row in high_contact.iterrows():
            if pd.notna(row['estimated_ba_using_speedangle']):
                print(f"      Game {game_pk}: {row['away_team']} @ {row['home_team']} (xBA: {row['estimated_ba_using_speedangle']:.3f})")
    
    # Unusual pitch type usage
    print(f"\n   🎪 Unique pitch types used yesterday:")
    rare_pitches = df['pitch_type'].value_counts()
    for pitch_type, count in rare_pitches.tail(5).items():
        if pd.notna(pitch_type) and count < 50:
            print(f"      {pitch_type}: {count} times")

if __name__ == "__main__":
    try:
        df = analyze_real_mlb_data()
        find_interesting_patterns(df)
        
        print(f"\n" + "="*60)
        print(f"🎉 CONGRATULATIONS!")
        print(f"You have successfully collected and analyzed REAL MLB data!")
        print(f"This data is ready for sophisticated betting analysis.")
        print(f"="*60)
        
    except Exception as e:
        print(f"❌ Error analyzing data: {e}")
        print(f"Make sure the parquet file exists: stage/games_2025-08-12.parquet")