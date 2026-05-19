"""
Train a LightGBM fraud detection model on synthetic data and log it to MLflow.

Features match what the Spark streaming pipeline computes per user per 1-minute window:
  - txn_cnt       : number of transactions in the window
  - amt_sum       : total transaction amount in the window
  - ip_risk_avg   : average IP risk score (0-1) across transactions
  - chargeback_cnt: number of transactions flagged with prior chargeback history

Run:
    python -m src.ml.train_model
"""

import os
import pickle
import pathlib

import numpy as np
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score, classification_report

from src.utils.logging import get_logger

logger = get_logger("train_model")

MLRUNS_DIR = "mlruns"
MODEL_PATH = os.path.join(MLRUNS_DIR, "fraud_model.pkl")
MODEL_URI_PATH = os.path.join(MLRUNS_DIR, "latest_model_uri.txt")
EXPERIMENT_NAME = "payguard_fraud_detection"
RANDOM_SEED = 42


def generate_synthetic_data(n_samples: int = 50_000, fraud_rate: float = 0.05) -> tuple:
    """
    Generate statistically realistic windowed fraud features.

    Patterns encoded:
    - Legitimate: low velocity (1-5 txns/min), low amounts, clean IPs
    - Velocity fraud: burst of 15-50 txns/min from risky IPs
    - High-value fraud: 1-3 large transactions ($500-$5000) from risky IPs
    - Account takeover: moderate velocity + moderate amounts + chargebacks
    """
    rng = np.random.default_rng(RANDOM_SEED)
    n_fraud = int(n_samples * fraud_rate)
    n_legit = n_samples - n_fraud

    # --- Legitimate users ---
    legit_txn_cnt = rng.integers(1, 6, size=n_legit).astype(float)
    legit_amt_sum = rng.exponential(scale=60.0, size=n_legit) + 5.0   # $5 - ~$300
    legit_ip_risk = rng.beta(a=1.5, b=8.0, size=n_legit)              # mostly 0.0-0.3
    legit_cb_cnt = rng.choice([0, 1], size=n_legit, p=[0.97, 0.03]).astype(float)
    legit_labels = np.zeros(n_legit)

    # --- Fraudulent users (3 archetypes) ---
    n_vel = n_fraud // 3          # velocity fraud
    n_hv = n_fraud // 3           # high-value fraud
    n_ato = n_fraud - n_vel - n_hv  # account takeover

    # Velocity fraud: high txn count, moderate amounts, high IP risk
    vel_txn_cnt = rng.integers(15, 51, size=n_vel).astype(float)
    vel_amt_sum = rng.uniform(100.0, 800.0, size=n_vel)
    vel_ip_risk = rng.beta(a=6.0, b=2.0, size=n_vel)    # mostly 0.6-0.95
    vel_cb_cnt = rng.choice([0, 1, 2], size=n_vel, p=[0.5, 0.3, 0.2]).astype(float)

    # High-value fraud: low txn count, very high amounts, high IP risk
    hv_txn_cnt = rng.integers(1, 4, size=n_hv).astype(float)
    hv_amt_sum = rng.uniform(500.0, 5000.0, size=n_hv)
    hv_ip_risk = rng.beta(a=5.0, b=2.0, size=n_hv)      # mostly 0.5-0.95
    hv_cb_cnt = rng.choice([0, 1], size=n_hv, p=[0.6, 0.4]).astype(float)

    # Account takeover: moderate velocity, moderate amounts, moderate IP risk, chargebacks
    ato_txn_cnt = rng.integers(5, 16, size=n_ato).astype(float)
    ato_amt_sum = rng.uniform(200.0, 1500.0, size=n_ato)
    ato_ip_risk = rng.beta(a=3.0, b=3.0, size=n_ato)    # 0.3-0.7
    ato_cb_cnt = rng.choice([1, 2, 3], size=n_ato, p=[0.5, 0.35, 0.15]).astype(float)

    fraud_txn_cnt = np.concatenate([vel_txn_cnt, hv_txn_cnt, ato_txn_cnt])
    fraud_amt_sum = np.concatenate([vel_amt_sum, hv_amt_sum, ato_amt_sum])
    fraud_ip_risk = np.concatenate([vel_ip_risk, hv_ip_risk, ato_ip_risk])
    fraud_cb_cnt = np.concatenate([vel_cb_cnt, hv_cb_cnt, ato_cb_cnt])
    fraud_labels = np.ones(n_fraud)

    # Stack all
    X = np.column_stack([
        np.concatenate([legit_txn_cnt, fraud_txn_cnt]),
        np.concatenate([legit_amt_sum, fraud_amt_sum]),
        np.concatenate([legit_ip_risk, fraud_ip_risk]),
        np.concatenate([legit_cb_cnt, fraud_cb_cnt]),
    ])
    y = np.concatenate([legit_labels, fraud_labels])

    # Shuffle
    idx = rng.permutation(len(y))
    return X[idx], y[idx]


def train():
    pathlib.Path(MLRUNS_DIR).mkdir(parents=True, exist_ok=True)

    mlflow.set_tracking_uri(f"file://{os.path.abspath(MLRUNS_DIR)}")
    mlflow.set_experiment(EXPERIMENT_NAME)

    logger.info("Generating synthetic fraud training data...")
    X, y = generate_synthetic_data(n_samples=50_000, fraud_rate=0.05)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=RANDOM_SEED
    )

    feature_names = ["txn_cnt", "amt_sum", "ip_risk_avg", "chargeback_cnt"]

    params = {
        "objective": "binary",
        "metric": "auc",
        "n_estimators": 200,
        "learning_rate": 0.05,
        "max_depth": 6,
        "num_leaves": 31,
        "min_child_samples": 20,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": (len(y_train) - y_train.sum()) / y_train.sum(),
        "random_state": RANDOM_SEED,
        "n_jobs": -1,
        "verbose": -1,
    }

    logger.info("Training LightGBM model...")

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.log_params(params)
        mlflow.log_param("n_train", len(X_train))
        mlflow.log_param("n_test", len(X_test))
        mlflow.log_param("fraud_rate_train", round(y_train.mean(), 4))

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            feature_name=feature_names,
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(period=-1)],
        )

        # Evaluate
        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= 0.70).astype(int)

        auc = roc_auc_score(y_test, y_prob)
        ap = average_precision_score(y_test, y_prob)
        report = classification_report(y_test, y_pred, output_dict=True)
        precision = report["1.0"]["precision"] if "1.0" in report else report.get("1", {}).get("precision", 0)
        recall = report["1.0"]["recall"] if "1.0" in report else report.get("1", {}).get("recall", 0)
        f1 = report["1.0"]["f1-score"] if "1.0" in report else report.get("1", {}).get("f1-score", 0)

        mlflow.log_metrics({
            "test_auc_roc": round(auc, 4),
            "test_avg_precision": round(ap, 4),
            "test_precision_at_0.70": round(precision, 4),
            "test_recall_at_0.70": round(recall, 4),
            "test_f1_at_0.70": round(f1, 4),
        })

        logger.info(f"AUC-ROC: {auc:.4f}  |  Avg Precision: {ap:.4f}")
        logger.info(f"@threshold=0.70 — Precision: {precision:.4f}  Recall: {recall:.4f}  F1: {f1:.4f}")

        # Save model artifact (pickle for fast loading in Spark pandas_udf)
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)

        mlflow.log_artifact(MODEL_PATH, artifact_path="artifacts")

        # Log with mlflow.lightgbm for model registry compatibility
        mlflow.lightgbm.log_model(
            lgb_model=model.booster_,
            artifact_path="lgb_booster",
            registered_model_name="payguard_fraud",
        )

        model_uri = f"runs:/{run_id}/lgb_booster"

        # Write model URI so the pipeline can locate it
        with open(MODEL_URI_PATH, "w") as f:
            f.write(model_uri)

        logger.info(f"Model saved to {MODEL_PATH}")
        logger.info(f"MLflow run ID: {run_id}")
        logger.info(f"Model URI: {model_uri}")
        logger.info(f"URI written to {MODEL_URI_PATH}")

    return MODEL_PATH


if __name__ == "__main__":
    train()
