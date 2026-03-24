import pandas as pd
import numpy as np
import joblib
from sqlalchemy import create_engine
from config import DB_URL
from models.data_prep import FEATURE_COLS
from processing.team_features import get_team_form_at_date
from processing.player_features import get_player_features_at_date
from processing.h2h_features import (
    get_h2h_at_date,
    get_venue_stats,
    get_toss_impact
)

engine = create_engine(DB_URL)


def load_model():
    """Load saved XGBoost model. Falls back to baseline if not found."""
    try:
        model  = joblib.load("models/xgboost_model.pkl")
        print("Loaded XGBoost model.")
        return model, "xgboost"
    except FileNotFoundError:
        model = joblib.load("models/baseline_lr.pkl")
        scaler = joblib.load("models/scaler.pkl")
        print("Loaded baseline logistic regression model.")
        return (model, scaler), "baseline"


def build_live_features(
    team1: str,
    team2: str,
    venue: str,
    toss_winner: str,
    date: str
) -> pd.DataFrame:
    """
    Build a single-row feature dataframe for a live match.
    Uses the same feature engineering as the training pipeline.
    """

    # Team form
    form          = get_team_form_at_date(date)
    venue_stats   = get_venue_stats()
    toss_stats    = get_toss_impact()
    player_feats  = get_player_features_at_date(date)

    def get_form(team):
        if form.empty:
            return {"rolling_win_rate": 0.5, "streak": 0}
        row = form[form["team"] == team]
        return row.iloc[0].to_dict() if not row.empty else {
            "rolling_win_rate": 0.5, "streak": 0
        }

    def get_venue_wr(team):
        if venue_stats.empty:
            return 0.5
        row = venue_stats[
            (venue_stats["team"] == team) &
            (venue_stats["venue"] == venue)
        ]
        return float(row["venue_win_rate"].iloc[0]) if not row.empty else 0.5

    def get_toss_adv(team):
        if toss_stats.empty:
            return 0.0
        row = toss_stats[toss_stats["team"] == team]
        return float(row["toss_advantage"].iloc[0]) if not row.empty else 0.0

    def get_bat_avg(team):
        if player_feats.empty:
            return 0.0
        tp = player_feats[player_feats["team"] == team]
        return round(float(tp["rolling_avg_runs"].mean()), 2) if not tp.empty else 0.0

    def get_bowl_eco(team):
        if player_feats.empty:
            return 0.0
        tp = player_feats[
            (player_feats["team"] == team) &
            (player_feats["rolling_avg_economy"] > 0)
        ]
        return round(float(tp["rolling_avg_economy"].mean()), 2) if not tp.empty else 0.0

    # H2H
    h2h = get_h2h_at_date(team1, team2, date)

    team1_form = get_form(team1)
    team2_form = get_form(team2)

    features = {
        "team1_win_rate":        team1_form.get("rolling_win_rate", 0.5),
        "team2_win_rate":        team2_form.get("rolling_win_rate", 0.5),
        "win_rate_diff":         round(
            team1_form.get("rolling_win_rate", 0.5) -
            team2_form.get("rolling_win_rate", 0.5), 3
        ),
        "team1_streak":          team1_form.get("streak", 0),
        "team2_streak":          team2_form.get("streak", 0),
        "team1_h2h_win_rate":    h2h["team_a_win_rate"],
        "team2_h2h_win_rate":    h2h["team_b_win_rate"],
        "h2h_total":             h2h["h2h_total"],
        "team1_venue_win_rate":  get_venue_wr(team1),
        "team2_venue_win_rate":  get_venue_wr(team2),
        "venue_advantage_diff":  round(
            get_venue_wr(team1) - get_venue_wr(team2), 3
        ),
        "toss_won_by_team1":     int(toss_winner == team1),
        "team1_toss_advantage":  get_toss_adv(team1),
        "team2_toss_advantage":  get_toss_adv(team2),
        "team1_bat_avg":         get_bat_avg(team1),
        "team2_bat_avg":         get_bat_avg(team2),
        "bat_avg_diff":          round(get_bat_avg(team1) - get_bat_avg(team2), 2),
        "team1_bowl_eco":        get_bowl_eco(team1),
        "team2_bowl_eco":        get_bowl_eco(team2),
        "bowl_eco_diff":         round(get_bowl_eco(team1) - get_bowl_eco(team2), 2),
    }

    return pd.DataFrame([features])[FEATURE_COLS]


def predict_win_probability(
    team1: str,
    team2: str,
    venue: str,
    toss_winner: str,
    date: str
) -> dict:
    """
    Main function called by Block 5 dashboard.
    Returns win probabilities for both teams.
    """

    model_obj, model_type = load_model()

    # Build features
    X = build_live_features(team1, team2, venue, toss_winner, date)

    # Predict
    if model_type == "xgboost":
        proba = model_obj.predict_proba(X)[0]
    else:
        model, scaler = model_obj
        X_scaled = scaler.transform(X)
        proba = model.predict_proba(X_scaled)[0]

    team1_win_prob = round(float(proba[1]) * 100, 1)
    team2_win_prob = round(float(proba[0]) * 100, 1)

    result = {
        "team1":             team1,
        "team2":             team2,
        "venue":             venue,
        "toss_winner":       toss_winner,
        "team1_win_prob":    team1_win_prob,
        "team2_win_prob":    team2_win_prob,
        "model_used":        model_type,
        "features":          X.iloc[0].to_dict(),
    }

    return result


def predict_all_live_matches() -> list:
    """
    Fetch all current matches from DB and predict
    win probability for each. Called by the scheduler.
    """
    from datetime import date as dt

    matches = pd.read_sql("""
        SELECT match_id, name, team1, team2, venue, toss_winner, match_date
        FROM matches
        WHERE status NOT IN ('completed', 'Completed', 'Result')
        ORDER BY match_date DESC
        LIMIT 10
    """, engine)

    if matches.empty:
        print("No live matches found for prediction.")
        return []

    predictions = []
    for _, match in matches.iterrows():
        try:
            pred = predict_win_probability(
                team1       = match["team1"],
                team2       = match["team2"],
                venue       = match["venue"] or "Unknown",
                toss_winner = match["toss_winner"] or "",
                date        = str(match["match_date"])
            )
            pred["match_id"] = match["match_id"]
            pred["match_name"] = match["name"]
            predictions.append(pred)

            print(f"\n  {match['name']}")
            print(f"    {pred['team1']}: {pred['team1_win_prob']}%")
            print(f"    {pred['team2']}: {pred['team2_win_prob']}%")

        except Exception as e:
            print(f"  Error predicting {match['name']}: {e}")

    return predictions