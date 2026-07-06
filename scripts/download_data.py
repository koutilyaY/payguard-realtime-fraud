"""
Download the real ULB Credit-Card Fraud Detection dataset and cache it under data/.

Source: Machine Learning Group, Université Libre de Bruxelles (ULB).
284,807 real (PCA-anonymized) European card-holder transactions from
September 2013, 492 of them confirmed frauds (0.172%). Columns: Time, V1..V28,
Amount, Class (1 = fraud).

We pull it from OpenML (no paid API key required). The dataset is mirrored there
under a couple of names/ids; we try each until one works.

Usage:
    python scripts/download_data.py
    # writes data/creditcard.csv (git-ignored)
"""

import os
import sys

import pandas as pd

DATA_DIR = "data"
OUT_CSV = os.path.join(DATA_DIR, "creditcard.csv")

# OpenML entry points to try, most specific first. data_id 42175 is the full
# upload that keeps the `Time` column (needed for the time-ordered split);
# data_id 1597 is the same transactions but drops Time, so it's only a fallback.
OPENML_ATTEMPTS = [
    {"data_id": 42175},
    {"data_id": 1597},
    {"name": "CreditCardFraudDetection", "version": 1},
    {"name": "creditcard", "version": 1},
]

EXPECTED_ROWS = 284_807
EXPECTED_FRAUD = 492


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """OpenML sometimes names the target 'Class' and sometimes returns it as a
    separate target series already merged in. Make sure Time/Amount/Class exist
    and Class is an int 0/1."""
    # Class may come back as a categorical/string ("0"/"1") — coerce to int.
    if "Class" not in df.columns:
        # Some mirrors label the target column differently.
        for cand in ("class", "target", "Class_"):
            if cand in df.columns:
                df = df.rename(columns={cand: "Class"})
                break
    if "Class" not in df.columns:
        raise ValueError(f"No 'Class' target column found. Columns: {list(df.columns)}")

    df["Class"] = df["Class"].astype(int)
    return df


def download() -> str:
    from sklearn.datasets import fetch_openml

    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(OUT_CSV):
        print(f"Already cached at {OUT_CSV} — skipping download.")
        return OUT_CSV

    last_err = None
    for attempt in OPENML_ATTEMPTS:
        try:
            print(f"Fetching from OpenML with {attempt} ...")
            ds = fetch_openml(as_frame=True, parser="auto", **attempt)
            df = ds.frame
            df = _normalize(df)
            break
        except Exception as e:  # noqa: BLE001 - we genuinely want to try the next id
            print(f"  attempt {attempt} failed: {e}")
            last_err = e
            df = None
    if df is None:
        print("All OpenML attempts failed.", file=sys.stderr)
        raise SystemExit(1) if last_err is None else last_err

    n_rows = len(df)
    n_fraud = int(df["Class"].sum())
    print(f"Downloaded {n_rows:,} rows, {n_fraud:,} frauds "
          f"({100 * n_fraud / n_rows:.3f}%).")

    # Sanity check against the known ground truth. Warn but don't hard-fail if a
    # mirror differs slightly.
    if n_rows != EXPECTED_ROWS or n_fraud != EXPECTED_FRAUD:
        print(f"WARNING: expected {EXPECTED_ROWS:,} rows / {EXPECTED_FRAUD} frauds; "
              f"got {n_rows:,} / {n_fraud}. Proceeding anyway.")

    df.to_csv(OUT_CSV, index=False)
    print(f"Cached to {OUT_CSV}")
    return OUT_CSV


if __name__ == "__main__":
    download()
