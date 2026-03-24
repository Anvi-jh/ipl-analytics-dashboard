import numpy as np
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import (
    accuracy_score, roc_auc_score,
    precision_score, recall_score,
    f1_score, confusion_matrix,
    classification_report
)
import joblib
import os
from models.data_prep import prepare_data, FEATURE_COLS
from models.baseline import _evaluate


def train_xgboost():
    """
    Train XGBoost with hyperparameter tuning.
    Compare against baseline and save best model.
    """

    (
        X_train, X_test,
        X_train_scaled, X_test_scaled,
        y_train, y_test,
        class_weight, matrix
    ) = prepare_data()

    # XGBoost uses scale_pos_weight instead of class_weight dict
    scale_pos_weight = class_weight.get(1, 1.0)

    print("\n=== Step 1: Hyperparameter Tuning (Grid Search) ===")

    # Parameter grid — kept small so it runs fast
    param_grid = {
        "n_estimators":     [50, 100, 200],
        "max_depth":        [3, 4, 5],
        "learning_rate":    [0.05, 0.1, 0.2],
        "subsample":        [0.8, 1.0],
        "colsample_bytree": [0.8, 1.0],
    }

    base_xgb = XGBClassifier(
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )

    # Stratified K-Fold keeps class balance in each fold
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    grid_search = GridSearchCV(
        estimator=base_xgb,
        param_grid=param_grid,
        cv=cv,
        scoring="roc_auc",
        n_jobs=-1,          # use all CPU cores
        verbose=1,
    )

    # XGBoost doesn't need scaled features — use raw X
    grid_search.fit(X_train, y_train)

    print(f"\nBest parameters found:")
    for param, val in grid_search.best_params_.items():
        print(f"  {param:20s}: {val}")
    print(f"\nBest CV AUC: {grid_search.best_score_:.3f}")

    # ── Train final model with best params ──────────────────────────
    print("\n=== Step 2: Training Final XGBoost Model ===")

    best_model = XGBClassifier(
        **grid_search.best_params_,
        scale_pos_weight=scale_pos_weight,
        use_label_encoder=False,
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )

    # Fit with early stopping on a validation set
    split = int(len(X_train) * 0.85)
    X_tr, X_val = X_train[:split], X_train[split:]
    y_tr, y_val = y_train[:split], y_train[split:]

    best_model.fit(
        X_tr, y_tr,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    # ── Evaluate ────────────────────────────────────────────────────
    y_pred       = best_model.predict(X_test)
    y_pred_proba = best_model.predict_proba(X_test)[:, 1]

    metrics = _evaluate(y_test, y_pred, y_pred_proba, model_name="XGBoost")

    # ── Feature importance ──────────────────────────────────────────
    importance_df = pd.DataFrame({
        "feature":    FEATURE_COLS,
        "importance": best_model.feature_importances_.round(4)
    }).sort_values("importance", ascending=False)

    print("\nTop 10 most important features:")
    print(importance_df.head(10).to_string(index=False))

    # ── Compare vs baseline ─────────────────────────────────────────
    print("\n=== Model Comparison ===")
    baseline = joblib.load("models/baseline_lr.pkl")
    scaler   = joblib.load("models/scaler.pkl")

    bl_pred       = baseline.predict(scaler.transform(X_test))
    bl_pred_proba = baseline.predict_proba(scaler.transform(X_test))[:, 1]
    bl_auc        = roc_auc_score(y_test, bl_pred_proba)
    bl_acc        = accuracy_score(y_test, bl_pred)

    xgb_auc = roc_auc_score(y_test, y_pred_proba)
    xgb_acc = accuracy_score(y_test, y_pred)

    print(f"\n{'Model':<25} {'Accuracy':>10} {'ROC-AUC':>10}")
    print("-" * 47)
    print(f"{'Logistic Regression':<25} {bl_acc:>10.3f} {bl_auc:>10.3f}")
    print(f"{'XGBoost':<25} {xgb_acc:>10.3f} {xgb_auc:>10.3f}")

    winner = "XGBoost" if xgb_auc > bl_auc else "Logistic Regression"
    print(f"\nBetter model: {winner}")

    # ── Save best model ─────────────────────────────────────────────
    os.makedirs("models", exist_ok=True)
    joblib.dump(best_model,    "models/xgboost_model.pkl")
    joblib.dump(importance_df, "models/feature_importance.pkl")
    print("\nXGBoost model saved to models/xgboost_model.pkl")
    print("Feature importance saved to models/feature_importance.pkl")

    return best_model, metrics, importance_df


if __name__ == "__main__":
    train_xgboost()