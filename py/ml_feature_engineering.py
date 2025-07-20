# ==============================================================================
# FILE: py/ml_feature_engineering.py (NEW - Add this)
# ==============================================================================
#!/usr/bin/env python3
"""
Advanced feature engineering for ML models
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
import psycopg2

class MLFeatureEngineer:
    """Advanced feature engineering for betting models"""
    
    def __init__(self, conn):
        self.conn = conn
    
    def create_rolling_features(self, player_id: int, stat_type: str = 'batting', 
                               windows: List[int] = [5, 10, 20]) -> pd.DataFrame:
        """Create rolling statistical features"""
        
        if stat_type == 'batting':
            query = """
            SELECT 
                game_date,
                batter as player_id,
                CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 ELSE 0 END as hit,
                CASE WHEN events = 'home_run' THEN 1 ELSE 0 END as hr,
                CASE WHEN events IN ('walk', 'hit_by_pitch') THEN 1 ELSE 0 END as bb_hbp,
                CASE WHEN events LIKE '%strikeout%' THEN 1 ELSE 0 END as k,
                CASE WHEN events IS NOT NULL AND events != '' THEN 1 ELSE 0 END as ab,
                woba_value,
                launch_speed,
                launch_angle,
                estimated_woba_using_speedangle as xwoba
            FROM statcast 
            WHERE batter = %s
            AND game_date >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY game_date, at_bat_number
            """
        else:  # pitching
            query = """
            SELECT 
                game_date,
                pitcher as player_id,
                CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 ELSE 0 END as hit_allowed,
                CASE WHEN events = 'home_run' THEN 1 ELSE 0 END as hr_allowed,
                CASE WHEN events LIKE '%strikeout%' THEN 1 ELSE 0 END as k,
                CASE WHEN events IN ('walk', 'hit_by_pitch') THEN 1 ELSE 0 END as bb_hbp_allowed,
                release_speed,
                spin_rate,
                estimated_woba_using_speedangle as xwoba_against
            FROM statcast 
            WHERE pitcher = %s
            AND game_date >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY game_date, at_bat_number
            """
        
        df = pd.read_sql(query, self.conn, params=[player_id])
        
        if df.empty:
            return pd.DataFrame()
        
        # Group by game date and calculate game-level stats
        daily_stats = df.groupby('game_date').agg({
            'hit' if stat_type == 'batting' else 'hit_allowed': 'sum',
            'hr' if stat_type == 'batting' else 'hr_allowed': 'sum',
            'k': 'sum',
            'ab': 'sum',
            'woba_value' if stat_type == 'batting' else 'xwoba_against': 'mean',
            'launch_speed': 'mean'
        }).reset_index()
        
        daily_stats = daily_stats.sort_values('game_date')
        
        # Create rolling features
        for window in windows:
            daily_stats[f'avg_{window}d'] = daily_stats['hit'].rolling(window, min_periods=1).mean()
            daily_stats[f'hr_rate_{window}d'] = daily_stats['hr'].rolling(window, min_periods=1).sum() / daily_stats['ab'].rolling(window, min_periods=1).sum()
            daily_stats[f'k_rate_{window}d'] = daily_stats['k'].rolling(window, min_periods=1).sum() / daily_stats['ab'].rolling(window, min_periods=1).sum()
            daily_stats[f'woba_{window}d'] = daily_stats['woba_value'].rolling(window, min_periods=1).mean()
            
            # Trend features (slope of performance)
            daily_stats[f'trend_{window}d'] = daily_stats['avg_{window}d'].diff(5)  # 5-day change
        
        return daily_stats
    
    def create_matchup_features(self, batter_id: int, pitcher_id: int) -> Dict:
        """Create head-to-head matchup features"""
        
        query = """
        SELECT 
            COUNT(*) as total_pa,
            SUM(CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 ELSE 0 END) as hits,
            SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) as hrs,
            AVG(launch_speed) as avg_exit_velo,
            AVG(woba_value) as avg_woba
        FROM statcast 
        WHERE batter = %s AND pitcher = %s
        AND game_date >= CURRENT_DATE - INTERVAL '3 years'
        """
        
        df = pd.read_sql(query, self.conn, params=[batter_id, pitcher_id])
        
        if df.empty or df.iloc[0]['total_pa'] == 0:
            return {"no_history": True}
        
        row = df.iloc[0]
        return {
            "total_pa": int(row['total_pa']),
            "avg": row['hits'] / row['total_pa'] if row['total_pa'] > 0 else 0,
            "hr_rate": row['hrs'] / row['total_pa'] if row['total_pa'] > 0 else 0,
            "avg_exit_velo": float(row['avg_exit_velo']) if row['avg_exit_velo'] else 0,
            "avg_woba": float(row['avg_woba']) if row['avg_woba'] else 0,
            "sample_size": "large" if row['total_pa'] >= 20 else "medium" if row['total_pa'] >= 10 else "small"
        }
    
    def create_park_features(self, ballpark_name: str, weather_data: Dict) -> Dict:
        """Create ballpark and weather-adjusted features"""
        
        # Base park factors (could be stored in database)
        park_factors = {
            "Coors Field": {"run_factor": 1.25, "hr_factor": 1.35, "elevation": 5200},
            "Yankee Stadium": {"run_factor": 1.08, "hr_factor": 1.15, "elevation": 55},
            "Petco Park": {"run_factor": 0.95, "hr_factor": 0.90, "elevation": 62},
            # Add more as needed
        }
        
        base_factors = park_factors.get(ballpark_name, {"run_factor": 1.0, "hr_factor": 1.0, "elevation": 100})
        
        # Weather adjustments
        temp_f = weather_data.get("temperature_f", 70)
        wind_speed = weather_data.get("wind_speed_mph", 0)
        humidity = weather_data.get("humidity_pct", 50)
        
        # Temperature effect (every 10°F = ~4 feet of carry)
        temp_adjustment = (temp_f - 70) * 0.004  # 0.4% per degree
        
        # Wind effect (simplified)
        wind_adjustment = wind_speed * 0.002 if wind_speed > 0 else 0
        
        # Humidity effect (higher humidity = less carry)
        humidity_adjustment = -(humidity - 50) * 0.001
        
        final_run_factor = base_factors["run_factor"] * (1 + temp_adjustment + wind_adjustment + humidity_adjustment)
        final_hr_factor = base_factors["hr_factor"] * (1 + temp_adjustment * 1.5 + wind_adjustment * 2 + humidity_adjustment)
        
        return {
            "base_run_factor": base_factors["run_factor"],
            "base_hr_factor": base_factors["hr_factor"],
            "weather_adjusted_run_factor": round(final_run_factor, 3),
            "weather_adjusted_hr_factor": round(final_hr_factor, 3),
            "temp_adjustment": round(temp_adjustment, 3),
            "wind_adjustment": round(wind_adjustment, 3),
            "humidity_adjustment": round(humidity_adjustment, 3),
            "elevation": base_factors["elevation"]
        }
    
    def create_situational_features(self, player_id: int, position: str = "batter") -> Dict:
        """Create situational performance features"""
        
        situations = {
            "risp": "runner_on_2b IS NOT NULL OR runner_on_3b IS NOT NULL",
            "late_close": "inning >= 7 AND ABS(home_score - away_score) <= 2",
            "high_leverage": "inning >= 8",
            "day_game": "EXTRACT(hour FROM start_time) < 17",
            "vs_lefty": "p_throws = 'L'",
            "vs_righty": "p_throws = 'R'"
        }
        
        col_name = "batter" if position == "batter" else "pitcher"
        
        results = {}
        
        for situation, condition in situations.items():
            query = f"""
            WITH situational AS (
                SELECT 
                    COUNT(CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
                    COUNT(CASE WHEN events IS NOT NULL AND events != '' THEN 1 END) as abs
                FROM statcast s
                LEFT JOIN statsapi_playlog p ON s.game_pk = p.game_pk AND s.at_bat_number = p.at_bat_index
                WHERE s.{col_name} = %s
                AND s.game_date >= CURRENT_DATE - INTERVAL '60 days'
                AND ({condition})
            ),
            overall AS (
                SELECT 
                    COUNT(CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
                    COUNT(CASE WHEN events IS NOT NULL AND events != '' THEN 1 END) as abs
                FROM statcast s
                WHERE s.{col_name} = %s
                AND s.game_date >= CURRENT_DATE - INTERVAL '60 days'
            )
            SELECT 
                s.hits as sit_hits,
                s.abs as sit_abs,
                o.hits as total_hits,
                o.abs as total_abs
            FROM situational s, overall o
            """
            
            df = pd.read_sql(query, self.conn, params=[player_id, player_id])
            
            if not df.empty and df.iloc[0]['sit_abs'] > 0:
                row = df.iloc[0]
                sit_avg = row['sit_hits'] / row['sit_abs']
                overall_avg = row['total_hits'] / row['total_abs'] if row['total_abs'] > 0 else 0
                
                results[situation] = {
                    "avg": round(sit_avg, 3),
                    "vs_overall": round(sit_avg - overall_avg, 3),
                    "sample_size": int(row['sit_abs'])
                }
            else:
                results[situation] = {"avg": 0, "vs_overall": 0, "sample_size": 0}
        
        return results

# ==============================================================================
# FILE: py/betting_model.py (NEW - Add this)
# ==============================================================================
#!/usr/bin/env python3
"""
ML Models for betting predictions
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, accuracy_score, classification_report
import joblib
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

class BettingPredictor:
    """ML models for betting predictions"""
    
    def __init__(self):
        self.models = {}
        self.scalers = {}
        self.feature_importance = {}
    
    def prepare_player_prop_features(self, player_data: Dict, game_context: Dict, 
                                   matchup_data: Dict) -> pd.DataFrame:
        """Prepare features for player prop predictions"""
        
        features = []
        
        # Rolling performance features
        for window in [5, 10, 20]:
            if f'avg_{window}d' in player_data:
                features.extend([
                    player_data[f'avg_{window}d'],
                    player_data[f'hr_rate_{window}d'],
                    player_data[f'k_rate_{window}d'],
                    player_data[f'woba_{window}d'],
                    player_data[f'trend_{window}d']
                ])
        
        # Game context features
        features.extend([
            game_context.get('weather_adjusted_run_factor', 1.0),
            game_context.get('weather_adjusted_hr_factor', 1.0),
            game_context.get('temp_adjustment', 0),
            game_context.get('wind_adjustment', 0),
            game_context.get('umpire_pitcher_friendly_score', 50) / 100
        ])
        
        # Matchup features
        if not matchup_data.get('no_history', True):
            features.extend([
                matchup_data.get('avg', 0),
                matchup_data.get('hr_rate', 0),
                matchup_data.get('avg_woba', 0),
                1 if matchup_data.get('sample_size') == 'large' else 0.5 if matchup_data.get('sample_size') == 'medium' else 0
            ])
        else:
            features.extend([0, 0, 0, 0])  # No history
        
        return pd.DataFrame([features])
    
    def train_hits_model(self, training_data: pd.DataFrame) -> float:
        """Train model to predict hits"""
        
        # Prepare features and target
        feature_cols = [col for col in training_data.columns if col.startswith(('avg_', 'hr_rate_', 'woba_', 'trend_', 'weather_', 'umpire_', 'matchup_'))]
        X = training_data[feature_cols].fillna(0)
        y = training_data['actual_hits']  # This would need to be in your training data
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
        model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test_scaled)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        # Store model and scaler
        self.models['hits'] = model
        self.scalers['hits'] = scaler
        self.feature_importance['hits'] = dict(zip(feature_cols, model.feature_importances_))
        
        print(f"✅ Hits model trained. RMSE: {rmse:.3f}")
        return rmse
    
    def predict_player_hits(self, features: pd.DataFrame) -> Dict[str, float]:
        """Predict player hits probability"""
        
        if 'hits' not in self.models:
            return {"error": "Hits model not trained"}
        
        # Scale features
        features_scaled = self.scalers['hits'].transform(features.fillna(0))
        
        # Predict
        hits_prediction = self.models['hits'].predict(features_scaled)[0]
        
        # Convert to probabilities for different hit totals
        predictions = {}
        for hits in [0, 1, 2, 3]:
            # Using Poisson distribution approximation
            prob = np.exp(-hits_prediction) * (hits_prediction ** hits) / np.math.factorial(hits)
            predictions[f'exactly_{hits}_hits'] = round(prob, 3)
        
        predictions['over_0.5_hits'] = round(1 - predictions['exactly_0_hits'], 3)
        predictions['over_1.5_hits'] = round(1 - predictions['exactly_0_hits'] - predictions['exactly_1_hits'], 3)
        predictions['expected_hits'] = round(hits_prediction, 2)
        
        return predictions
    
    def save_models(self, filepath: str = "models/"):
        """Save trained models"""
        import os
        os.makedirs(filepath, exist_ok=True)
        
        for model_name, model in self.models.items():
            joblib.dump(model, f"{filepath}/{model_name}_model.pkl")
            joblib.dump(self.scalers[model_name], f"{filepath}/{model_name}_scaler.pkl")
        
        print(f"✅ Models saved to {filepath}")
    
    def load_models(self, filepath: str = "models/"):
        """Load trained models"""
        import os
        
        model_files = [f for f in os.listdir(filepath) if f.endswith('_model.pkl')]
        
        for model_file in model_files:
            model_name = model_file.replace('_model.pkl', '')
            self.models[model_name] = joblib.load(f"{filepath}/{model_file}")
            self.scalers[model_name] = joblib.load(f"{filepath}/{model_name}_scaler.pkl")
        
        print(f"✅ Loaded {len(self.models)} models from {filepath}")

# ==============================================================================
# FILE: py/betting_optimizer.py (NEW - Add this)
# ==============================================================================
#!/usr/bin/env python3
"""
Betting optimization and bankroll management
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from scipy.optimize import minimize

class BettingOptimizer:
    """Optimize betting strategies and bankroll management"""
    
    def __init__(self, bankroll: float = 1000, max_risk_per_bet: float = 0.05):
        self.bankroll = bankroll
        self.max_risk_per_bet = max_risk_per_bet
        
    def kelly_criterion(self, win_probability: float, odds: float) -> float:
        """Calculate optimal bet size using Kelly Criterion"""
        
        # Convert American odds to decimal
        if odds > 0:
            decimal_odds = (odds / 100) + 1
        else:
            decimal_odds = (100 / abs(odds)) + 1
        
        # Kelly formula: f = (bp - q) / b
        # where b = odds-1, p = win probability, q = lose probability
        b = decimal_odds - 1
        p = win_probability
        q = 1 - p
        
        kelly_fraction = (b * p - q) / b
        
        # Apply maximum risk constraint
        recommended_fraction = min(kelly_fraction, self.max_risk_per_bet)
        recommended_fraction = max(recommended_fraction, 0)  # Never negative
        
        return recommended_fraction
    
    def optimize_parlay(self, bets: List[Dict]) -> Dict:
        """Optimize parlay construction"""
        
        # Simple parlay optimization - pick highest confidence bets
        sorted_bets = sorted(bets, key=lambda x: x.get('confidence', 0), reverse=True)
        
        # Calculate parlay probability and payout
        parlay_prob = 1.0
        parlay_odds = 1.0
        
        recommended_parlay = []
        
        for bet in sorted_bets[:3]:  # Max 3-leg parlay
            if bet.get('confidence', 0) >= 0.6:  # High confidence only
                recommended_parlay.append(bet)
                parlay_prob *= bet.get('win_probability', 0.5)
                
                # Convert odds to decimal and multiply
                odds = bet.get('odds', -110)
                if odds > 0:
                    decimal_odds = (odds / 100) + 1
                else:
                    decimal_odds = (100 / abs(odds)) + 1
                
                parlay_odds *= decimal_odds
        
        # Calculate kelly bet size for parlay
        if len(recommended_parlay) >= 2:
            kelly_size = self.kelly_criterion(parlay_prob, (parlay_odds - 1) * 100)
            
            return {
                "recommended_parlay": recommended_parlay,
                "parlay_probability": round(parlay_prob, 3),
                "parlay_odds": round(parlay_odds, 2),
                "kelly_bet_size": round(kelly_size * self.bankroll, 2),
                "expected_value": round((parlay_odds * parlay_prob - 1) * kelly_size * self.bankroll, 2)
            }
        
        return {"message": "No suitable parlay found"}
    
    def calculate_bet_sizing(self, opportunities: List[Dict]) -> List[Dict]:
        """Calculate optimal bet sizes for multiple opportunities"""
        
        sized_bets = []
        total_risk = 0
        
        # Sort by expected value
        sorted_opportunities = sorted(opportunities, 
                                    key=lambda x: x.get('expected_value', 0), 
                                    reverse=True)
        
        for opp in sorted_opportunities:
            win_prob = opp.get('win_probability', 0.5)
            odds = opp.get('odds', -110)
            
            # Calculate Kelly size
            kelly_fraction = self.kelly_criterion(win_prob, odds)
            
            # Adjust for portfolio risk
            if total_risk + kelly_fraction > self.max_risk_per_bet * 5:  # Max 25% total risk
                kelly_fraction = max(0, self.max_risk_per_bet * 5 - total_risk)
            
            bet_size = kelly_fraction * self.bankroll
            
            if bet_size >= 10:  # Minimum bet size
                sized_bets.append({
                    **opp,
                    'kelly_fraction': round(kelly_fraction, 4),
                    'bet_size': round(bet_size, 2),
                    'risk_amount': round(bet_size, 2),
                    'potential_profit': round(bet_size * ((odds / 100) if odds > 0 else (100 / abs(odds))), 2)
                })
                
                total_risk += kelly_fraction
        
        return sized_bets