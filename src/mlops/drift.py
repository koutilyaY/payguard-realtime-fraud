"""
Drift monitoring for the PayGuard fraud model.

Two questions this answers, on real feature distributions:

  1. Feature drift   — has the *input* distribution moved? We compute Population
                       Stability Index (PSI) and the two-sample Kolmogorov-Smirnov
                       statistic per feature between a reference window and a
                       current window.
  2. Prediction drift — has the *output* moved? We PSI the model's predicted
                       P(fraud) distribution between the two windows, and compare
                       mean scores and alert rates.

PSI is the standard credit-risk drift metric; the conventional bands are:

    PSI < 0.10           no meaningful shift
    0.10 <= PSI < 0.25   moderate shift  -> WARN
    PSI >= 0.25          major shift      -> CRITICAL

KS is reported alongside as a distribution-shape cross-check (0 = identical,
1 = disjoint). We bin PSI on the reference quantiles so the metric is comparable
across features regardless of scale.

Alerting DESIGN (not wired to a pager in this repo — see the runbook): a run that
returns overall severity WARN or CRITICAL is what a scheduler would act on. WARN
posts to the team channel; CRITICAL pages on-call AND fires the retraining trigger
in src/mlops/retrain_trigger.py. The severity + the drifted-feature list here are
exactly the payload that alert would carry.

Runnable on the real ULB data via `python -m src.mlops.drift` (early vs late time
windows), which is a genuine covariate-shift test: the dataset spans ~2 days and
the fraud mix shifts across it.
"""

import os
import json
import argparse
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Severity bands on PSI (see module docstring).
PSI_WARN = 0.10
PSI_CRITICAL = 0.25

SEVERITY_OK = "OK"
SEVERITY_WARN = "WARN"
SEVERITY_CRITICAL = "CRITICAL"
_SEVERITY_ORDER = {SEVERITY_OK: 0, SEVERITY_WARN: 1, SEVERITY_CRITICAL: 2}


def _severity_from_psi(psi: float) -> str:
    if psi >= PSI_CRITICAL:
        return SEVERITY_CRITICAL
    if psi >= PSI_WARN:
        return SEVERITY_WARN
    return SEVERITY_OK


def _max_severity(sevs: List[str]) -> str:
    if not sevs:
        return SEVERITY_OK
    return max(sevs, key=lambda s: _SEVERITY_ORDER[s])


def psi(reference: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index between two 1-D samples.

    Bins are the quantile edges of the reference sample, so each reference bin
    holds ~equal mass. A small epsilon floors empty bins to keep the log finite.
    """
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    current = np.asarray(current, dtype=float)
    current = current[~np.isnan(current)]
    if reference.size == 0 or current.size == 0:
        return 0.0

    quantiles = np.linspace(0, 1, bins + 1)
    edges = np.unique(np.quantile(reference, quantiles))
    if edges.size < 2:
        # Reference is (near) constant; fall back to a tiny spread so bins exist.
        edges = np.array([reference.min() - 1e-6, reference.max() + 1e-6])
    edges[0], edges[-1] = -np.inf, np.inf

    ref_counts, _ = np.histogram(reference, bins=edges)
    cur_counts, _ = np.histogram(current, bins=edges)

    eps = 1e-6
    ref_pct = ref_counts / max(ref_counts.sum(), 1) + eps
    cur_pct = cur_counts / max(cur_counts.sum(), 1) + eps

    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def ks_statistic(reference: np.ndarray, current: np.ndarray) -> float:
    """Two-sample Kolmogorov-Smirnov statistic (max CDF gap). SciPy if available,
    otherwise a NumPy fallback so the module has no hard SciPy dependency."""
    reference = np.asarray(reference, dtype=float)
    reference = reference[~np.isnan(reference)]
    current = np.asarray(current, dtype=float)
    current = current[~np.isnan(current)]
    if reference.size == 0 or current.size == 0:
        return 0.0
    try:
        from scipy.stats import ks_2samp
        return float(ks_2samp(reference, current).statistic)
    except Exception:
        grid = np.sort(np.concatenate([reference, current]))
        cdf_ref = np.searchsorted(np.sort(reference), grid, side="right") / reference.size
        cdf_cur = np.searchsorted(np.sort(current), grid, side="right") / current.size
        return float(np.max(np.abs(cdf_ref - cdf_cur)))


@dataclass
class FeatureDrift:
    feature: str
    psi: float
    ks: float
    severity: str


@dataclass
class DriftReport:
    n_reference: int
    n_current: int
    features: List[FeatureDrift] = field(default_factory=list)
    prediction_psi: Optional[float] = None
    prediction_severity: Optional[str] = None
    ref_mean_score: Optional[float] = None
    cur_mean_score: Optional[float] = None
    ref_alert_rate: Optional[float] = None
    cur_alert_rate: Optional[float] = None
    overall_severity: str = SEVERITY_OK

    def drifted_features(self, min_severity: str = SEVERITY_WARN) -> List[FeatureDrift]:
        floor = _SEVERITY_ORDER[min_severity]
        return sorted(
            [f for f in self.features if _SEVERITY_ORDER[f.severity] >= floor],
            key=lambda f: f.psi,
            reverse=True,
        )

    def as_dict(self) -> dict:
        d = asdict(self)
        d["psi_bands"] = {"warn": PSI_WARN, "critical": PSI_CRITICAL}
        return d


def compute_feature_drift(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    feature_cols: List[str],
) -> List[FeatureDrift]:
    out = []
    for col in feature_cols:
        if col not in reference.columns or col not in current.columns:
            continue
        p = psi(reference[col].to_numpy(), current[col].to_numpy())
        k = ks_statistic(reference[col].to_numpy(), current[col].to_numpy())
        out.append(FeatureDrift(feature=col, psi=round(p, 4), ks=round(k, 4),
                                severity=_severity_from_psi(p)))
    return out


def compute_prediction_drift(
    ref_scores: np.ndarray,
    cur_scores: np.ndarray,
    alert_threshold: float = 0.5,
):
    p = psi(ref_scores, cur_scores)
    return {
        "prediction_psi": round(p, 4),
        "prediction_severity": _severity_from_psi(p),
        "ref_mean_score": round(float(np.mean(ref_scores)), 6),
        "cur_mean_score": round(float(np.mean(cur_scores)), 6),
        "ref_alert_rate": round(float(np.mean(ref_scores >= alert_threshold)), 6),
        "cur_alert_rate": round(float(np.mean(cur_scores >= alert_threshold)), 6),
    }


def run_drift(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    feature_cols: List[str],
    model=None,
    alert_threshold: float = 0.5,
) -> DriftReport:
    """Full drift report. If a model is passed, prediction drift is included by
    scoring both windows; the model must expose predict_proba."""
    feats = compute_feature_drift(reference, current, feature_cols)
    report = DriftReport(
        n_reference=len(reference),
        n_current=len(current),
        features=feats,
    )

    severities = [f.severity for f in feats]

    if model is not None:
        ref_scores = model.predict_proba(reference[feature_cols].to_numpy())[:, 1]
        cur_scores = model.predict_proba(current[feature_cols].to_numpy())[:, 1]
        pred = compute_prediction_drift(ref_scores, cur_scores, alert_threshold)
        report.prediction_psi = pred["prediction_psi"]
        report.prediction_severity = pred["prediction_severity"]
        report.ref_mean_score = pred["ref_mean_score"]
        report.cur_mean_score = pred["cur_mean_score"]
        report.ref_alert_rate = pred["ref_alert_rate"]
        report.cur_alert_rate = pred["cur_alert_rate"]
        severities.append(pred["prediction_severity"])

    report.overall_severity = _max_severity(severities)
    return report


# --------------------------------------------------------------------------- #
# CLI: run drift on the real ULB data, early vs late time windows.
# --------------------------------------------------------------------------- #

def _load_model():
    """Load the persisted model bundle (fraud_model.pkl) if present."""
    import pickle
    path = os.path.join("mlruns", "fraud_model.pkl")
    if not os.path.exists(path):
        return None, None
    with open(path, "rb") as f:
        bundle = pickle.load(f)
    return bundle["model"], bundle.get("feature_cols")


def main():
    parser = argparse.ArgumentParser(description="Run drift detection on the real ULB data.")
    parser.add_argument("--data", default=os.path.join("data", "creditcard.csv"))
    parser.add_argument("--ref-frac", type=float, default=0.5,
                        help="Reference = earliest this fraction of transactions (by Time).")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON only.")
    args = parser.parse_args()

    from src.ml.train_model import FEATURE_COLS

    df = pd.read_csv(args.data)
    df = df.sort_values("Time", kind="mergesort").reset_index(drop=True)
    cut = int(len(df) * args.ref_frac)
    reference = df.iloc[:cut]
    current = df.iloc[cut:]

    model, model_features = _load_model()
    feature_cols = model_features or FEATURE_COLS

    report = run_drift(reference, current, feature_cols, model=model)

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
        return

    print("=" * 64)
    print("PayGuard drift report — real ULB data, early vs late time windows")
    print("=" * 64)
    print(f"reference window : {report.n_reference:,} txns (earliest {args.ref_frac:.0%})")
    print(f"current window   : {report.n_current:,} txns (latest {1 - args.ref_frac:.0%})")
    print(f"overall severity : {report.overall_severity}")
    print("-" * 64)
    print(f"{'feature':<10}{'PSI':>10}{'KS':>10}   severity")
    for f in sorted(report.features, key=lambda x: x.psi, reverse=True):
        print(f"{f.feature:<10}{f.psi:>10.4f}{f.ks:>10.4f}   {f.severity}")
    if report.prediction_psi is not None:
        print("-" * 64)
        print("prediction drift (model P(fraud)):")
        print(f"  PSI={report.prediction_psi:.4f}  severity={report.prediction_severity}")
        print(f"  mean score  ref={report.ref_mean_score:.6f}  cur={report.cur_mean_score:.6f}")
        print(f"  alert rate  ref={report.ref_alert_rate:.6f}  cur={report.cur_alert_rate:.6f}")
    drifted = report.drifted_features()
    print("-" * 64)
    print(f"features at WARN+ : {[f.feature for f in drifted]}")
    print("=" * 64)


if __name__ == "__main__":
    main()
