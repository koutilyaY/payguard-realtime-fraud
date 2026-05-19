"""
Feedback-driven model retraining pipeline.

Reads analyst-labeled cases from PostgreSQL, joins with gold feature windows
from Delta Lake, then retrains the LightGBM model and logs a new MLflow run.

Run:
    python -m src.ml.retrain

Typically triggered after analysts have labeled enough cases:
    make retrain
"""

import os
import pickle
import pathlib
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score, classification_report

from src.utils.config import load_config
from src.utils.logging import get_logger

logger = get_logger("retrain")

MLRUNS_DIR = "mlruns"
MODEL_PATH = os.path.join(MLRUNS_DIR, "fraud_model.pkl")
MODEL_URI_PATH = os.path.join(MLRUNS_DIR, "latest_model_uri.txt")
EXPERIMENT_NAME = "payguard_fraud_detection"
RANDOM_SEED = 42
MIN_LABELED_CASES = 50   # require at least this many labels before retraining
FEATURE_COLS = ["txn_cnt", "amt_sum", "ip_risk_avg", "chargeback_cnt"]


def _pg_conn(pg: dict):
    dbname = pg.get("db") or pg.get("dbname") or pg.get("database") or "postgres"
    return psycopg2.connect(
        host=pg["host"],
        port=int(pg["port"]),
        dbname=dbname,
        user=pg["user"],
        password=pg["password"],
    )


def fetch_labeled_cases(pg: dict) -> pd.DataFrame:
    """
    Pull analyst-labeled cases from PostgreSQL.
    Returns a DataFrame with columns: user_id, risk_score, analyst_label, created_at.
    Only FRAUD and LEGIT labels are included (UNKNOWN is excluded).
    """
    conn = _pg_conn(pg)
    query = """
        SELECT user_id,
               risk_score,
               analyst_label,
               created_at
          FROM cases
         WHERE analyst_label IN ('FRAUD', 'LEGIT')
         ORDER BY created_at DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    logger.info(f"Fetched {len(df)} labeled cases (FRAUD={df['analyst_label'].eq('FRAUD').sum()}, LEGIT={df['analyst_label'].eq('LEGIT').sum()})")
    return df


def fetch_gold_features(gold_features_path: str) -> pd.DataFrame:
    """
    Read the gold feature log from Delta Lake using PySpark,
    then return as a pandas DataFrame.
    Falls back to parquet snapshot if Delta read fails.
    """
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder.appName("PayGuard-Retrain")
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        df = spark.read.format("delta").load(gold_features_path)
        pdf = df.select("user_id", "window_end", *FEATURE_COLS).toPandas()
        spark.stop()
        logger.info(f"Loaded {len(pdf)} gold feature rows from Delta at {gold_features_path}")
        return pdf
    except Exception as e:
        logger.warning(f"Delta read failed ({e}), falling back to parquet snapshot if available.")
        snap = "data/silver_snapshot_parquet"
        if os.path.exists(snap):
            pdf = pd.read_parquet(snap)
            logger.info(f"Loaded {len(pdf)} rows from parquet snapshot.")
            return pdf
        raise RuntimeError(
            "Cannot read gold features from Delta or parquet snapshot. "
            "Ensure the streaming pipeline has run and produced gold features."
        ) from e


def build_training_data(labeled: pd.DataFrame, features: pd.DataFrame) -> tuple:
    """
    Join analyst labels to gold feature windows.

    Strategy: for each labeled case (user_id, created_at), find the gold feature
    window whose window_end is closest to created_at within a ±10-minute tolerance.
    This matches the case to the feature context at the time of scoring.
    """
    labeled = labeled.copy()
    features = features.copy()

    labeled["created_at"] = pd.to_datetime(labeled["created_at"], utc=True)
    features["window_end"] = pd.to_datetime(features["window_end"], utc=True)

    # Sort both for merge_asof
    labeled = labeled.sort_values("created_at")
    features = features.sort_values("window_end")

    matched_rows = []
    tolerance = pd.Timedelta("10 minutes")

    for _, case in labeled.iterrows():
        user_features = features[features["user_id"] == case["user_id"]].copy()
        if user_features.empty:
            continue
        # Find closest window_end to case created_at
        time_diffs = (user_features["window_end"] - case["created_at"]).abs()
        closest_idx = time_diffs.idxmin()
        if time_diffs[closest_idx] <= tolerance:
            row = user_features.loc[closest_idx, FEATURE_COLS].to_dict()
            row["label"] = 1 if case["analyst_label"] == "FRAUD" else 0
            matched_rows.append(row)

    if not matched_rows:
        logger.warning("No labeled cases could be matched to feature windows. Check timestamps.")
        return None, None

    df = pd.DataFrame(matched_rows)
    X = df[FEATURE_COLS].values
    y = df["label"].values
    logger.info(f"Built training set: {len(df)} rows (fraud={y.sum()}, legit={(y==0).sum()})")
    return X, y


def retrain(X_new: np.ndarray, y_new: np.ndarray, cfg: dict) -> str:
    """
    Combine new labeled data with bootstrap synthetic data,
    retrain the LightGBM model, log to MLflow.

    Returns the new model URI.
    """
    from src.ml.train_model import generate_synthetic_data

    pathlib.Path(MLRUNS_DIR).mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(f"file://{os.path.abspath(MLRUNS_DIR)}")
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Bootstrap: blend labeled data with synthetic to prevent overfitting to a small label set
    logger.info("Generating bootstrap synthetic data for blending...")
    X_syn, y_syn = generate_synthetic_data(n_samples=20_000, fraud_rate=0.05)

    # Give analyst labels 3x weight by repeating them
    X_combined = np.vstack([X_syn, np.repeat(X_new, 3, axis=0)])
    y_combined = np.concatenate([y_syn, np.repeat(y_new, 3)])

    X_train, X_test, y_train, y_test = train_test_split(
        X_combined, y_combined, test_size=0.2, stratify=y_combined, random_state=RANDOM_SEED
    )

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
        "scale_pos_weight": max(1.0, (len(y_train) - y_train.sum()) / max(y_train.sum(), 1)),
        "random_state": RANDOM_SEED,
        "n_jobs": -1,
        "verbose": -1,
    }

    logger.info("Retraining LightGBM with analyst feedback + synthetic bootstrap...")

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.log_params(params)
        mlflow.log_param("retrain_trigger", "analyst_feedback")
        mlflow.log_param("n_labeled_cases", len(X_new))
        mlflow.log_param("n_train_total", len(X_train))
        mlflow.log_param("retrain_timestamp", datetime.now(timezone.utc).isoformat())

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            feature_name=FEATURE_COLS,
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(period=-1)],
        )

        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= 0.70).astype(int)

        auc = roc_auc_score(y_test, y_prob)
        ap = average_precision_score(y_test, y_prob)
        report = classification_report(y_test, y_pred, output_dict=True)
        label_key = "1.0" if "1.0" in report else "1"
        precision = report.get(label_key, {}).get("precision", 0)
        recall = report.get(label_key, {}).get("recall", 0)
        f1 = report.get(label_key, {}).get("f1-score", 0)

        mlflow.log_metrics({
            "test_auc_roc": round(auc, 4),
            "test_avg_precision": round(ap, 4),
            "test_precision_at_0.70": round(precision, 4),
            "test_recall_at_0.70": round(recall, 4),
            "test_f1_at_0.70": round(f1, 4),
        })

        logger.info(f"Retrained model — AUC-ROC: {auc:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f}")

        # Save pickle for Spark UDF
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)

        mlflow.log_artifact(MODEL_PATH, artifact_path="artifacts")
        mlflow.lightgbm.log_model(
            lgb_model=model.booster_,
            artifact_path="lgb_booster",
            registered_model_name="payguard_fraud",
        )

        model_uri = f"runs:/{run_id}/lgb_booster"

        with open(MODEL_URI_PATH, "w") as f:
            f.write(model_uri)

        logger.info(f"Retrained model saved to {MODEL_PATH}")
        logger.info(f"MLflow run ID: {run_id}")
        logger.info(
            "Restart the streaming pipeline ('make stream') to pick up the new model."
        )

    return model_uri


def main():
    cfg = load_config()

    logger.info("=== PayGuard Model Retraining Pipeline ===")

    # 1. Fetch labeled cases
    labeled = fetch_labeled_cases(cfg["postgres"])

    if len(labeled) < MIN_LABELED_CASES:
        logger.warning(
            f"Only {len(labeled)} labeled cases found (minimum: {MIN_LABELED_CASES}). "
            "Label more cases via POST /label/case/{case_id} before retraining."
        )
        return

    # 2. Fetch gold features
    gold_path = cfg.get("paths", {}).get("gold_features", "delta/gold/user_risk_features_v1")
    features = fetch_gold_features(gold_path)

    # 3. Join labels to features
    X, y = build_training_data(labeled, features)

    if X is None or len(X) < MIN_LABELED_CASES:
        logger.warning("Insufficient matched training examples. Cannot retrain.")
        return

    # 4. Retrain and log
    model_uri = retrain(X, y, cfg)
    logger.info(f"Retraining complete. New model URI: {model_uri}")


if __name__ == "__main__":
    main()
