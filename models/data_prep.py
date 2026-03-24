import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import joblib
import os
from config import DB_URL

engine = create_engine(DB_URL)

# These are the exact columns the model will train on
# Keeping this list in one place means Block 5 uses the same features
FEATURE_COLS = [
    "team1_win_rate",
    "team2_win_rate",
    "win_rate_diff",
    "team1_streak",
    "team2_streak",
    "team1_h2h_win_rate",
    "team2_h2h_win_rate",
    "h2h_total",
    "team1_venue_win_rate",
    "team2_venue_win_rate",
    "venue_advantage_diff",
    "toss_won_by_team1",
    "team1_toss_advantage",
    "team2_toss_advantage",
    "team1_bat_avg",
    "team2_bat_avg",
    "bat_avg_diff",
    "team1_bowl_eco",
    "team2_bowl_eco",
    "bowl_eco_diff",
]

TARGET_COL = "team1_won"


def load_features() -> pd.DataFrame:
    """Load feature matrix from PostgreSQL."""
    matrix = pd.read_sql("SELECT * FROM feature_matrix", engine)
    print(f"Loaded feature matrix: {matrix.shape}")
    return matrix


def check_data_quality(matrix: pd.DataFrame):
    """Print a quality report before training."""
    print("\n=== Data Quality Report ===")
    print(f"Total rows       : {len(matrix)}")
    print(f"Total features   : {len(FEATURE_COLS)}")

    # Null check
    nulls = matrix[FEATURE_COLS].isnull().sum()
    if nulls.sum() > 0:
        print(f"\nNulls found:")
        print(nulls[nulls > 0])
    else:
        print("Nulls            : None ✓")

    # Target balance
    balance = matrix[TARGET_COL].value_counts(normalize=True).round(3)
    print(f"\nTarget balance:")
    print(f"  team1 won  : {balance.get(1, 0)*100:.1f}%")
    print(f"  team1 lost : {balance.get(0, 0)*100:.1f}%")

    imbalance = abs(balance.get(1, 0) - balance.get(0, 0))
    if imbalance > 0.1:
        print(f"  WARNING: imbalance detected ({imbalance:.2f}) — will apply class weights")
    else:
        print(f"  Balance is good ✓")


def prepare_data():
    """
    Full preparation pipeline:
    1. Load features
    2. Quality check
    3. Fill nulls
    4. Train/test split (time-based, not random)
    5. Scale features
    6. Return everything needed for training
    """

    matrix = load_features()
    check_data_quality(matrix)

    # Fill any remaining nulls with column median
    matrix[FEATURE_COLS] = matrix[FEATURE_COLS].fillna(
        matrix[FEATURE_COLS].median()
    )

    X = matrix[FEATURE_COLS].values
    y = matrix[TARGET_COL].values

    # Time-based split — use last 20% of matches as test set
    # This is more realistic than random split for time series data
    split_idx = int(len(matrix) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    print(f"\nTrain size : {len(X_train)} matches")
    print(f"Test size  : {len(X_test)} matches")

    # Scale features — important for logistic regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    # Save scaler — Block 5 needs this for live inference
    os.makedirs("models", exist_ok=True)
    joblib.dump(scaler, "models/scaler.pkl")
    print("Scaler saved to models/scaler.pkl")

    # Compute class weight for imbalanced data
    n_neg = (y_train == 0).sum()
    n_pos = (y_train == 1).sum()
    class_weight = {0: 1.0, 1: round(n_neg / n_pos, 3)}
    print(f"Class weights: {class_weight}")

    return (
        X_train, X_test,
        X_train_scaled, X_test_scaled,
        y_train, y_test,
        class_weight,
        matrix
    )

