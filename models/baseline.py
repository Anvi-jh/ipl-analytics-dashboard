import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, roc_auc_score,
    precision_score, recall_score,
    f1_score, confusion_matrix,
    classification_report
)
import joblib
import os
from models.data_prep import prepare_data, FEATURE_COLS


def train_baseline():
    """
    Train logistic regression baseline model.
    Returns trained model and evaluation metrics.
    """

    (
        X_train, X_test,
        X_train_scaled, X_test_scaled,
        y_train, y_test,
        class_weight, matrix
    ) = prepare_data()

    print("\n=== Training Baseline (Logistic Regression) ===")

    model = LogisticRegression(
        class_weight=class_weight,
        max_iter=1000,
        random_state=42,
        solver="lbfgs"
    )

    model.fit(X_train_scaled, y_train)

    # Predictions
    y_pred       = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

    # Metrics
    metrics = _evaluate(y_test, y_pred, y_pred_proba, model_name="Logistic Regression")

    # Feature coefficients — shows which features matter most
    coef_df = pd.DataFrame({
        "feature":     FEATURE_COLS,
        "coefficient": model.coef_[0].round(4)
    }).sort_values("coefficient", ascending=False)

    print("\nTop 5 positive features (push toward team1 winning):")
    print(coef_df.head(5).to_string(index=False))
    print("\nTop 5 negative features (push toward team2 winning):")
    print(coef_df.tail(5).to_string(index=False))

    # Save model
    os.makedirs("models", exist_ok=True)
    joblib.dump(model, "models/baseline_lr.pkl")
    print("\nBaseline model saved to models/baseline_lr.pkl")

    return model, metrics, coef_df


def _evaluate(y_test, y_pred, y_pred_proba, model_name: str) -> dict:
    """Compute and print all evaluation metrics."""

    accuracy  = accuracy_score(y_test, y_pred)
    auc       = roc_auc_score(y_test, y_pred_proba)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall    = recall_score(y_test, y_pred, zero_division=0)
    f1        = f1_score(y_test, y_pred, zero_division=0)
    cm        = confusion_matrix(y_test, y_pred)

    print(f"\n{'='*40}")
    print(f"  {model_name} Results")
    print(f"{'='*40}")
    print(f"  Accuracy  : {accuracy:.3f}")
    print(f"  ROC-AUC   : {auc:.3f}")
    print(f"  Precision : {precision:.3f}")
    print(f"  Recall    : {recall:.3f}")
    print(f"  F1 Score  : {f1:.3f}")
    print(f"\n  Confusion Matrix:")
    print(f"              Predicted 0   Predicted 1")
    print(f"  Actual 0  :     {cm[0][0]}             {cm[0][1]}")
    print(f"  Actual 1  :     {cm[1][0]}             {cm[1][1]}")
    print(f"\n  Classification Report:")
    print(classification_report(y_test, y_pred,
          target_names=["team2 won", "team1 won"]))

    return {
        "model":     model_name,
        "accuracy":  round(accuracy, 3),
        "auc":       round(auc, 3),
        "precision": round(precision, 3),
        "recall":    round(recall, 3),
        "f1":        round(f1, 3),
    }


if __name__ == "__main__":
    train_baseline()