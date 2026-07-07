"""
Build the CI fixture: a small, committed slice of the real ULB data so the MLOps
pipeline can run end-to-end in CI without the ~150 MB download.

It keeps ALL 492 real frauds plus a time-spread sample of legit transactions. That
deliberately inflates the fraud rate (~4% vs the real 0.17%), so the fixture is a
*smoke test that the pipeline runs and gates*, NOT a benchmark — headline metrics
must come from `make train` on the full dataset. Frauds land on both sides of the
time-ordered split, so training, evaluation and the promotion gate all exercise.

Run: python scripts/make_ci_fixture.py   (needs data/creditcard.csv present)
"""

import os
import pandas as pd

SRC = os.path.join("data", "creditcard.csv")
OUT = os.path.join("fixtures", "creditcard_sample.csv")
N_LEGIT = 12000
SEED = 42


def main():
    if not os.path.exists(SRC):
        raise SystemExit(f"{SRC} not found — run scripts/download_data.py first.")
    df = pd.read_csv(SRC).sort_values("Time", kind="mergesort").reset_index(drop=True)
    frauds = df[df["Class"] == 1]
    legit = df[df["Class"] == 0].sample(n=N_LEGIT, random_state=SEED)
    fixture = pd.concat([frauds, legit]).sort_values("Time", kind="mergesort").reset_index(drop=True)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    fixture.to_csv(OUT, index=False)
    print(f"wrote {OUT}: {len(fixture)} rows, {int(fixture['Class'].sum())} frauds "
          f"({100 * fixture['Class'].mean():.2f}% — inflated on purpose)")


if __name__ == "__main__":
    main()
