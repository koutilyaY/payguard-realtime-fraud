"""
Model registry + gated promotion for the PayGuard fraud model.

This wraps MLflow's Model Registry with the promotion policy the rest of the loop
depends on. One registered model, `payguard_fraud`, accumulates versions (each
training or retraining run registers one). Two aliases mark the ones that matter:

    @production  -> the champion currently served
    @staging     -> a challenger being validated / shadow-scored

Why aliases and not stages: MLflow 3 removed the old `Staging`/`Production`
*stage* transitions (`transition_model_version_stage` is gone in 3.x). Aliases are
the supported replacement and carry the same meaning — `@production` is the model
in production, `@staging` is the one on deck. The words "Staging" and "Production"
below refer to these alias slots.

Promotion is gated. A challenger only becomes production if it beats the current
production model's PR-AUC by at least PROMOTION_MARGIN, and clears an absolute
floor (PROMOTION_FLOOR). If there is no production model yet, the floor alone
applies. PR-AUC (average precision) is the gate metric because the problem is
~0.17% positive — see train_model.py for why ROC-AUC would mislead here.

Everything here is real, runnable code against the local MLflow file store the
rest of the project already uses (mlruns/). No server required.
"""

import os
from dataclasses import dataclass
from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient

REGISTERED_MODEL = "payguard_fraud"
GATE_METRIC = "test_pr_auc"

# Promotion policy. A challenger must beat production by this margin AND clear the
# floor. These are deliberately conservative: a 0.5% absolute PR-AUC improvement
# is the smallest lift worth the risk of a swap; the floor stops a badly regressed
# model from ever going live even if it happens to beat a worse production model.
PROMOTION_MARGIN = 0.005
PROMOTION_FLOOR = 0.55

ALIAS_PRODUCTION = "production"
ALIAS_STAGING = "staging"


def mlruns_dir() -> str:
    return os.environ.get("PAYGUARD_MLRUNS", "mlruns")


def get_client() -> MlflowClient:
    """MLflow client wired to the local file store, matching train_model.py."""
    os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")
    uri = f"file://{os.path.abspath(mlruns_dir())}"
    mlflow.set_tracking_uri(uri)
    return MlflowClient(tracking_uri=uri)


@dataclass
class GateResult:
    promoted: bool
    reason: str
    candidate_version: str
    candidate_metric: Optional[float]
    incumbent_version: Optional[str]
    incumbent_metric: Optional[float]

    def as_dict(self) -> dict:
        return {
            "promoted": self.promoted,
            "reason": self.reason,
            "candidate_version": self.candidate_version,
            "candidate_metric": self.candidate_metric,
            "incumbent_version": self.incumbent_version,
            "incumbent_metric": self.incumbent_metric,
            "gate_metric": GATE_METRIC,
            "promotion_margin": PROMOTION_MARGIN,
            "promotion_floor": PROMOTION_FLOOR,
        }


def _metric_for_version(client: MlflowClient, version: str) -> Optional[float]:
    """Read the gate metric off the run that produced a given model version."""
    mv = client.get_model_version(REGISTERED_MODEL, version)
    if not mv.run_id:
        return None
    run = client.get_run(mv.run_id)
    val = run.data.metrics.get(GATE_METRIC)
    return float(val) if val is not None else None


def _version_by_alias(client: MlflowClient, alias: str) -> Optional[str]:
    # MLflow returns .version as an int here; normalize to str so all version
    # comparisons across this module are string-vs-string.
    try:
        return str(client.get_model_version_by_alias(REGISTERED_MODEL, alias).version)
    except Exception:
        return None


def latest_version(client: MlflowClient) -> Optional[str]:
    versions = client.search_model_versions(f"name='{REGISTERED_MODEL}'")
    if not versions:
        return None
    return str(max(int(v.version) for v in versions))


def production_version(client: Optional[MlflowClient] = None) -> Optional[str]:
    client = client or get_client()
    return _version_by_alias(client, ALIAS_PRODUCTION)


def staging_version(client: Optional[MlflowClient] = None) -> Optional[str]:
    client = client or get_client()
    return _version_by_alias(client, ALIAS_STAGING)


def evaluate_gate(candidate_version: str, client: Optional[MlflowClient] = None) -> GateResult:
    """Decide whether `candidate_version` should be promoted to production.

    Pure decision function: reads metrics, applies the policy, returns a verdict.
    It does NOT move any alias — call promote() to act on a positive verdict.
    """
    client = client or get_client()
    cand_metric = _metric_for_version(client, candidate_version)
    incumbent = _version_by_alias(client, ALIAS_PRODUCTION)
    inc_metric = _metric_for_version(client, incumbent) if incumbent else None

    def result(promoted, reason):
        return GateResult(
            promoted=promoted,
            reason=reason,
            candidate_version=str(candidate_version),
            candidate_metric=cand_metric,
            incumbent_version=incumbent,
            incumbent_metric=inc_metric,
        )

    if cand_metric is None:
        return result(False, f"candidate has no logged {GATE_METRIC}; cannot gate")

    if cand_metric < PROMOTION_FLOOR:
        return result(
            False,
            f"candidate {GATE_METRIC}={cand_metric:.4f} below floor {PROMOTION_FLOOR}",
        )

    if incumbent is None:
        return result(
            True,
            f"no production model yet; candidate clears floor {PROMOTION_FLOOR} "
            f"({GATE_METRIC}={cand_metric:.4f})",
        )

    if str(candidate_version) == str(incumbent):
        return result(False, "candidate is already the production model")

    if inc_metric is None:
        # Incumbent metric unreadable — be conservative and require the floor only.
        return result(
            True,
            f"incumbent {GATE_METRIC} unavailable; candidate clears floor "
            f"({GATE_METRIC}={cand_metric:.4f})",
        )

    lift = cand_metric - inc_metric
    if lift >= PROMOTION_MARGIN:
        return result(
            True,
            f"candidate {GATE_METRIC}={cand_metric:.4f} beats production "
            f"{inc_metric:.4f} by {lift:+.4f} (>= margin {PROMOTION_MARGIN})",
        )
    return result(
        False,
        f"candidate {GATE_METRIC}={cand_metric:.4f} does not beat production "
        f"{inc_metric:.4f} by margin {PROMOTION_MARGIN} (lift {lift:+.4f})",
    )


def stage_challenger(candidate_version: str, client: Optional[MlflowClient] = None) -> None:
    """Mark a version as the @staging challenger (for shadow scoring / review)."""
    client = client or get_client()
    client.set_registered_model_alias(REGISTERED_MODEL, ALIAS_STAGING, str(candidate_version))


def promote(candidate_version: str, client: Optional[MlflowClient] = None) -> None:
    """Move @production to `candidate_version` and clear it from @staging.

    Caller is responsible for having passed evaluate_gate() first. This is the
    only function that changes what production serves.
    """
    client = client or get_client()
    client.set_registered_model_alias(REGISTERED_MODEL, ALIAS_PRODUCTION, str(candidate_version))
    # If the promoted version was the staging challenger, retire the staging alias.
    if _version_by_alias(client, ALIAS_STAGING) == str(candidate_version):
        try:
            client.delete_registered_model_alias(REGISTERED_MODEL, ALIAS_STAGING)
        except Exception:
            pass


def rollback(to_version: Optional[str] = None, client: Optional[MlflowClient] = None) -> str:
    """One-command rollback: point @production at a prior registered version.

    With no argument, rolls back to the highest registered version that is NOT the
    current production one (the most recent previous model). Returns the version
    that is now production. This is what the serving layer reads on next reload.
    """
    client = client or get_client()
    current = _version_by_alias(client, ALIAS_PRODUCTION)

    if to_version is None:
        versions = sorted(
            (int(v.version) for v in client.search_model_versions(f"name='{REGISTERED_MODEL}'")),
            reverse=True,
        )
        prior = [str(v) for v in versions if str(v) != str(current)]
        if not prior:
            raise RuntimeError("No prior version to roll back to.")
        to_version = prior[0]

    client.set_registered_model_alias(REGISTERED_MODEL, ALIAS_PRODUCTION, str(to_version))
    return str(to_version)


def register_and_gate(run_id: str, client: Optional[MlflowClient] = None) -> GateResult:
    """Register the model logged under `run_id`, stage it, evaluate the gate, and
    promote if it passes. This is the entry point the CI/CD pipeline calls after a
    training run. Returns the gate verdict.
    """
    client = client or get_client()
    model_uri = f"runs:/{run_id}/lgb_booster"
    mv = mlflow.register_model(model_uri, REGISTERED_MODEL)
    version = mv.version

    stage_challenger(version, client)
    verdict = evaluate_gate(version, client)
    if verdict.promoted:
        promote(version, client)
    return verdict


if __name__ == "__main__":
    # Quick status dump against the current store.
    c = get_client()
    prod = production_version(c)
    stg = staging_version(c)
    print(f"registered model : {REGISTERED_MODEL}")
    print(f"latest version   : {latest_version(c)}")
    print(f"@production       : {prod}  ({GATE_METRIC}={_metric_for_version(c, prod) if prod else None})")
    print(f"@staging          : {stg}  ({GATE_METRIC}={_metric_for_version(c, stg) if stg else None})")
