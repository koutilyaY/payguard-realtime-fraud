"""
Automated retraining trigger.

This is the decision layer that sits between drift monitoring and the training
pipeline. It answers one question — "should we retrain now?" — from two inputs:

  * a DriftReport (from src.mlops.drift), and
  * the age of the current production model.

Firing policy (all real code):

  * CRITICAL drift            -> retrain now (a major input/output shift)
  * WARN drift on >= MIN_WARN_FEATURES features -> retrain (broad moderate shift)
  * model older than MAX_MODEL_AGE_DAYS          -> scheduled refresh
  * otherwise                                    -> do not retrain

What's real vs. what's the deployment's job: `should_retrain()` and the reasons
it emits are real, tested code. `fire()` invokes the actual training entry point
in-process. The *scheduler* that calls this on a cadence (cron / Airflow / the CI
schedule) is configuration, not code in this repo — the CI workflow
(.github/workflows/mlops-train.yml) is one such caller, on a weekly schedule and
on demand. See docs/MLOPS_RUNBOOK.md.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.mlops.drift import (
    DriftReport,
    SEVERITY_CRITICAL,
    SEVERITY_WARN,
    _SEVERITY_ORDER,
)

# Policy knobs.
MIN_WARN_FEATURES = 3        # this many WARN+ features counts as a broad shift
MAX_MODEL_AGE_DAYS = 30      # scheduled refresh cadence


@dataclass
class TriggerDecision:
    should_retrain: bool
    reason: str
    trigger_type: str  # "drift_critical" | "drift_broad" | "model_age" | "none"

    def as_dict(self) -> dict:
        return {
            "should_retrain": self.should_retrain,
            "reason": self.reason,
            "trigger_type": self.trigger_type,
        }


def should_retrain(
    report: Optional[DriftReport] = None,
    model_age_days: Optional[float] = None,
) -> TriggerDecision:
    """Evaluate the firing policy. Drift breach wins over age (it's more urgent)."""
    if report is not None:
        if report.overall_severity == SEVERITY_CRITICAL:
            crit = [f.feature for f in report.features
                    if f.severity == SEVERITY_CRITICAL]
            pred_crit = report.prediction_severity == SEVERITY_CRITICAL
            detail = f"critical feature drift on {crit}" if crit else ""
            if pred_crit:
                detail = (detail + "; " if detail else "") + "critical prediction drift"
            return TriggerDecision(True, f"CRITICAL drift: {detail}", "drift_critical")

        warn_plus = [
            f for f in report.features
            if _SEVERITY_ORDER[f.severity] >= _SEVERITY_ORDER[SEVERITY_WARN]
        ]
        if len(warn_plus) >= MIN_WARN_FEATURES:
            return TriggerDecision(
                True,
                f"WARN drift on {len(warn_plus)} features (>= {MIN_WARN_FEATURES}): "
                f"{[f.feature for f in warn_plus]}",
                "drift_broad",
            )

    if model_age_days is not None and model_age_days >= MAX_MODEL_AGE_DAYS:
        return TriggerDecision(
            True,
            f"model age {model_age_days:.1f}d >= {MAX_MODEL_AGE_DAYS}d scheduled refresh",
            "model_age",
        )

    return TriggerDecision(False, "no drift breach and model within age budget", "none")


def production_model_age_days(client=None) -> Optional[float]:
    """Age (days) of the run behind the @production model, or None if unknown."""
    from src.mlops.registry import get_client, production_version, REGISTERED_MODEL
    client = client or get_client()
    ver = production_version(client)
    if ver is None:
        return None
    mv = client.get_model_version(REGISTERED_MODEL, ver)
    if not mv.run_id:
        return None
    run = client.get_run(mv.run_id)
    start_ms = run.info.start_time
    if not start_ms:
        return None
    started = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    return (datetime.now(timezone.utc) - started).total_seconds() / 86400.0


def fire() -> str:
    """Invoke the real training entry point. Returns the produced run_id.

    Kept thin on purpose: the trigger decides, train_model owns training. In a
    deployment the scheduler would call `should_retrain()` and, on True, run this
    (or dispatch the CI workflow, which does the same thing in a clean env).
    """
    from src.ml.train_model import train
    _, metrics = train()
    return metrics.get("run_id", "unknown")


if __name__ == "__main__":
    # Evaluate the trigger against real drift + the live production model age.
    import json
    import pandas as pd
    from src.ml.train_model import FEATURE_COLS
    from src.mlops.drift import run_drift, _load_model

    df = pd.read_csv("data/creditcard.csv").sort_values("Time", kind="mergesort").reset_index(drop=True)
    cut = len(df) // 2
    model, feats = _load_model()
    report = run_drift(df.iloc[:cut], df.iloc[cut:], feats or FEATURE_COLS, model=model)

    age = production_model_age_days()
    decision = should_retrain(report, model_age_days=age)
    print(json.dumps({
        "drift_overall_severity": report.overall_severity,
        "production_model_age_days": round(age, 2) if age is not None else None,
        "decision": decision.as_dict(),
    }, indent=2))
