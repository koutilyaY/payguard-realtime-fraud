# Model card — PayGuard fraud detector

## Overview

A LightGBM binary classifier that scores a card transaction's probability of being
fraud, `P(fraud) ∈ [0, 1]`. It is the model served as the champion (`@production`)
in the MLOps loop described in `docs/MLOPS_RUNBOOK.md`.

- **Registered name:** `payguard_fraud` (MLflow Model Registry)
- **Type:** gradient-boosted decision trees (LightGBM, binary objective)
- **Input:** 29 features per transaction — the 28 PCA components `V1..V28` plus
  `Amount`. `Time` is used only to split data, never as a feature.
- **Output:** a calibrated-ish probability and, at the fixed 0.5 operating
  threshold, a decision: `REVIEW` (`P >= 0.5`) or `ALLOW`.

## Training data

ULB Credit-Card Fraud Detection dataset — 284,807 real European card transactions
from September 2013, 492 confirmed frauds (0.172% positive). Retrieved via OpenML
(`data_id=42175`, which keeps `Time`). See the README's data-source section.

**Split is time-ordered, not random.** Sort by `Time`, train on the earlier ~70%,
evaluate on the later ~30%. A random split would leak frauds from the test period
into training and inflate the numbers; the time-ordered split mimics deploying
forward in time. Full details in `src/ml/train_model.py`.

## Metrics (real, held-out test set — full dataset)

From `make train` on the full data, written to `mlruns/real_data_metrics.json`.
Test set = the later 85,443 transactions, 108 of them fraud.

| Metric | Value |
|---|---|
| PR-AUC (average precision) | 0.733 |
| ROC-AUC | 0.976 |
| Brier score | 0.0009 |
| Precision @ 0.5 | 0.58 |
| Recall @ 0.5 | 0.80 |
| F1 @ 0.5 | 0.67 |
| Confusion @ 0.5 | TP 86 · FP 62 · FN 22 · TN 85,273 |

PR-AUC is the headline because the problem is ~0.17% positive; ROC-AUC looks
flattering on any imbalanced set. At 0.5 the model catches 86 of 108 held-out
frauds (80% recall) at 0.58 precision. A real team would tune the threshold to
their review capacity.

> The CI pipeline trains on a small committed fixture (`fixtures/creditcard_sample.csv`)
> that keeps all 492 frauds and inflates the fraud rate to ~4%. Its metrics
> (~0.88 PR-AUC) are **not** comparable to the table above — the fixture exists to
> exercise the pipeline and the promotion gate quickly, not to benchmark quality.

## Key modeling choice: imbalance handling

`scale_pos_weight` is set to `sqrt(#neg/#pos)` (~21), not the naive `#neg/#pos`
(~500). The naive weight wrecks probability calibration and drops PR-AUC to ~0.24;
the sqrt version keeps it usable (~0.73) with a Brier score of 0.0009. This single
choice is the difference between a useless and a usable model here.

## Promotion gate (how a version becomes production)

A newly trained version is registered, then gated on PR-AUC before it can serve:

- must clear an absolute floor (`PROMOTION_FLOOR = 0.55`), and
- must beat the current production model by at least `PROMOTION_MARGIN = 0.005`
  PR-AUC (or, with no incumbent, just clear the floor).

Logic lives in `src/mlops/registry.py::evaluate_gate`. A rejected challenger is
kept under the `@staging` alias for shadow scoring and review, never promoted.

## Intended use and limitations

- **Intended:** score replayed / batch transactions with the same 29-feature
  schema as the ULB dataset; power an analyst review queue.
- **Not intended:** production authorization decisions on live traffic without
  recalibration. The training data is a static 2-day 2013 sample.
- **PCA features are not interpretable.** The ULB features are anonymized, so the
  model gives a score, not an explanation. No per-feature reason codes are possible
  beyond the coarse `HIGH_MODEL_RISK` / `HIGH_AMOUNT` tags in the stream.
- **Drift is real and measurable.** On the full data, input distributions shift
  substantially between the early and late halves (several features at CRITICAL
  PSI). See the runbook. The model's *output* distribution is comparatively stable
  on that same split, but this is exactly why monitoring is wired in.
- **No fairness analysis** is possible — the dataset carries no demographic
  attributes (all features are PCA-anonymized).

## Provenance

Every version traces to an MLflow run with its params, metrics, and the exact
data-split fractions. The registry (`mlruns/`, file store) records which version is
`@production` and `@staging` and the full version history.
