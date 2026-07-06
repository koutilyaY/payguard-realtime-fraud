"""
Feedback-driven retraining on the REAL data.

Reads analyst-labeled cases from PostgreSQL (each keyed by the transaction's
event_id), looks up those transactions' real feature vectors (V1..V28 + Amount)
from the gold Delta table the streaming job wrote, up-weights them, blends them
into the base real ULB training set, and retrains the LightGBM model.

This keeps the whole feedback loop on real data: the base data is real, and the
analyst-confirmed labels are real decisions on real (replayed) transactions.

Run:
    make retrain      # after labeling cases via POST /label/case/{case_id}
"""

import os
import json
import pickle
import pathlib
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    precision_recall_fscore_support,
)

from src.utils.config import load_config
from src.utils.logging import get_logger
from src.ml.train_model import (
    FEATURE_COLS,
    load_real_data,
    time_ordered_split,
    MLRUNS_DIR,
    MODEL_PATH,
    MODEL_URI_PATH,
    EXPERIMENT_NAME,
    RANDOM_SEED,
    DECISION_THRESHOLD,
)

logger = get_logger("retrain")

MIN_LABELED_CASES = 20   # require at least this many labels before retraining
LABEL_WEIGHT = 5         # up-weight each analyst-labeled real example


def _pg_conn(pg: dict):
    dbname = pg.get("db") or pg.get("dbname") or pg.get("database") or "postgres"
    return psycopg2.connect(
        host=pg["host"], port=int(pg["port"]), dbname=dbname,
        user=pg["user"], password=pg["password"],
    )


def fetch_labeled_cases(pg: dict) -> pd.DataFrame:
    """Analyst-labeled cases (FRAUD/LEGIT only), keyed by event_id."""
    conn = _pg_conn(pg)
    df = pd.read_sql(
        """
        SELECT event_id, analyst_label
          FROM cases
         WHERE analyst_label IN ('FRAUD', 'LEGIT')
           AND event_id IS NOT NULL
        """,
        conn,
    )
    conn.close()
    logger.info(
        f"Fetched {len(df)} labeled cases "
        f"(FRAUD={int(df['analyst_label'].eq('FRAUD').sum())}, "
        f"LEGIT={int(df['analyst_label'].eq('LEGIT').sum())})"
    )
    return df


def fetch_gold_features(gold_features_path: str) -> pd.DataFrame:
    """Read scored gold rows (event_id + V1..V28 + Amount) from Delta."""
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("PayGuard-Retrain")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.format("delta").load(gold_features_path)
    pdf = df.select("event_id", *FEATURE_COLS).toPandas()
    spark.stop()
    logger.info(f"Loaded {len(pdf)} gold rows from Delta at {gold_features_path}")
    return pdf


def build_labeled_training_data(labeled: pd.DataFrame, features: pd.DataFrame):
    """Join analyst labels to the real feature vectors by event_id."""
    merged = labeled.merge(features, on="event_id", how="inner")
    if merged.empty:
        logger.warning("No labeled cases matched a gold feature row by event_id.")
        return None, None
    X = merged[FEATURE_COLS].to_numpy()
    y = (merged["analyst_label"] == "FRAUD").astype(int).to_numpy()
    logger.info(f"Matched {len(merged)} labeled real transactions "
                f"(fraud={int(y.sum())}, legit={int((y == 0).sum())}).")
    return X, y


def retrain(X_new: np.ndarray, y_new: np.ndarray):
    """Blend up-weighted analyst-labeled real data into the base real training
    set and retrain. Evaluate on the same time-ordered held-out test set."""
    os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")
    pathlib.Path(MLRUNS_DIR).mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(f"file://{os.path.abspath(MLRUNS_DIR)}")
    mlflow.set_experiment(EXPERIMENT_NAME)

    df = load_real_data()
    X_fit, y_fit, X_val, y_val, X_test, y_test, _ = time_ordered_split(df)

    # Base real training data + up-weighted analyst-labeled real examples.
    X_combined = np.vstack([X_fit, np.repeat(X_new, LABEL_WEIGHT, axis=0)])
    y_combined = np.concatenate([y_fit, np.repeat(y_new, LABEL_WEIGHT)])

    n_pos = int(y_combined.sum())
    n_neg = int(len(y_combined) - n_pos)
    scale_pos_weight = float(np.sqrt(n_neg / max(n_pos, 1)))

    params = {
        "objective": "binary", "metric": "average_precision",
        "n_estimators": 400, "learning_rate": 0.05, "max_depth": 6,
        "num_leaves": 31, "min_child_samples": 40, "subsample": 0.8,
        "colsample_bytree": 0.8, "reg_lambda": 1.0,
        "scale_pos_weight": scale_pos_weight,
        "random_state": RANDOM_SEED, "n_jobs": -1, "verbose": -1,
    }

    logger.info("Retraining LightGBM on real base data + analyst feedback ...")
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.log_params(params)
        mlflow.set_tag("data_source", "ULB_creditcard_real + analyst_feedback")
        mlflow.log_param("n_labeled_cases", len(X_new))
        mlflow.log_param("retrain_timestamp", datetime.now(timezone.utc).isoformat())

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_combined, y_combined,
            eval_set=[(X_val, y_val)], eval_metric="average_precision",
            feature_name=FEATURE_COLS,
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

        y_prob = model.predict_proba(X_test)[:, 1]
        pr_auc = average_precision_score(y_test, y_prob)
        roc_auc = roc_auc_score(y_test, y_prob)
        y_pred = (y_prob >= DECISION_THRESHOLD).astype(int)
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_test, y_pred, labels=[1], average="binary", zero_division=0
        )
        mlflow.log_metrics({
            "test_pr_auc": round(float(pr_auc), 4),
            "test_roc_auc": round(float(roc_auc), 4),
            "test_precision": round(float(precision), 4),
            "test_recall": round(float(recall), 4),
            "test_f1": round(float(f1), 4),
        })
        logger.info(f"Retrained model — PR-AUC {pr_auc:.4f} | ROC-AUC {roc_auc:.4f} | "
                    f"P {precision:.4f} R {recall:.4f}")

        payload = {"model": model, "feature_cols": FEATURE_COLS,
                   "decision_threshold": DECISION_THRESHOLD}
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(payload, f)
        mlflow.log_artifact(MODEL_PATH, artifact_path="artifacts")
        mlflow.lightgbm.log_model(
            lgb_model=model.booster_, artifact_path="lgb_booster",
            registered_model_name="payguard_fraud",
        )
        model_uri = f"runs:/{run_id}/lgb_booster"
        with open(MODEL_URI_PATH, "w") as f:
            f.write(model_uri)
        logger.info(f"Retrained model saved to {MODEL_PATH} (run {run_id}). "
                    "Restart 'make stream' to pick it up.")
    return model_uri


def main():
    cfg = load_config()
    logger.info("=== PayGuard Retraining (real data + analyst feedback) ===")

    labeled = fetch_labeled_cases(cfg["postgres"])
    if len(labeled) < MIN_LABELED_CASES:
        logger.warning(
            f"Only {len(labeled)} labeled cases (minimum {MIN_LABELED_CASES}). "
            "Label more via POST /label/case/{case_id} before retraining."
        )
        return

    gold_path = cfg.get("paths", {}).get("gold_features", "delta/gold/user_risk_features_v1")
    features = fetch_gold_features(gold_path)

    X, y = build_labeled_training_data(labeled, features)
    if X is None or len(X) < MIN_LABELED_CASES:
        logger.warning("Insufficient matched training examples. Cannot retrain.")
        return

    model_uri = retrain(X, y)
    logger.info(f"Retraining complete. New model URI: {model_uri}")


if __name__ == "__main__":
    main()
