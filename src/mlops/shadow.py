"""
Champion / challenger (shadow) scoring.

The production model (@production, the "champion") makes the real decision. When a
challenger (@staging) is registered, this module scores it *in shadow* on the same
transaction: the challenger's score is recorded and compared, but it never affects
the served decision. That lets you watch a candidate's behaviour on live-shaped
traffic before promoting it — the safe half of champion/challenger.

What it tracks per request and in aggregate:
  * champion score + decision   (the one returned to the caller)
  * challenger score + decision (shadow only)
  * agreement (do they make the same ALLOW/REVIEW call?)
  * score delta

Loading: both models come from the MLflow registry by alias, so a promotion or a
rollback (moving @production) changes what the champion is on the next reload —
no code change, no redeploy of a pickle. Falls back gracefully: if there is no
challenger registered, shadow scoring is simply off and only the champion runs.

This is real, runnable code. It's exercised over the FastAPI /score/shadow
endpoint (see src/serving/api.py) via TestClient in the tests.
"""

import os
import threading
from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np

DEFAULT_THRESHOLD = 0.5


def _load_model_by_alias(alias: str):
    """Load a registered model version by alias as a predict-capable object.

    Returns (model, version) or (None, None) if the alias isn't set. Uses the
    LightGBM flavour that train_model.py logs.
    """
    from src.mlops.registry import get_client, REGISTERED_MODEL
    import mlflow.pyfunc

    client = get_client()
    try:
        mv = client.get_model_version_by_alias(REGISTERED_MODEL, alias)
    except Exception:
        return None, None
    model = mlflow.pyfunc.load_model(f"models:/{REGISTERED_MODEL}@{alias}")
    return model, str(mv.version)


def _predict_proba(model, X: np.ndarray) -> np.ndarray:
    """Uniform P(fraud) extraction across sklearn-style and pyfunc/booster models."""
    if hasattr(model, "predict_proba"):
        return np.asarray(model.predict_proba(X))[:, 1]
    # mlflow pyfunc wrapping a LightGBM booster returns the positive prob directly.
    pred = np.asarray(model.predict(X))
    if pred.ndim == 2 and pred.shape[1] == 2:
        return pred[:, 1]
    return pred.ravel()


@dataclass
class ShadowStats:
    """Running agreement/behaviour comparison between champion and challenger."""
    n: int = 0
    agree: int = 0
    champion_alerts: int = 0
    challenger_alerts: int = 0
    sum_abs_delta: float = 0.0
    champion_version: Optional[str] = None
    challenger_version: Optional[str] = None

    def update(self, champ_score, chal_score, champ_alert, chal_alert):
        self.n += 1
        self.agree += int(champ_alert == chal_alert)
        self.champion_alerts += int(champ_alert)
        self.challenger_alerts += int(chal_alert)
        self.sum_abs_delta += abs(champ_score - chal_score)

    def as_dict(self) -> dict:
        return {
            "n_scored": self.n,
            "champion_version": self.champion_version,
            "challenger_version": self.challenger_version,
            "agreement_rate": round(self.agree / self.n, 4) if self.n else None,
            "champion_alert_rate": round(self.champion_alerts / self.n, 4) if self.n else None,
            "challenger_alert_rate": round(self.challenger_alerts / self.n, 4) if self.n else None,
            "mean_abs_score_delta": round(self.sum_abs_delta / self.n, 6) if self.n else None,
        }


class ShadowScorer:
    """Holds champion + (optional) challenger and scores transactions through both.

    Thread-safe enough for a single FastAPI worker: stats are guarded by a lock.
    Call reload() after a promotion/rollback to pick up the new aliases.
    """

    def __init__(self, feature_cols: List[str], threshold: float = DEFAULT_THRESHOLD):
        self.feature_cols = feature_cols
        self.threshold = threshold
        self._lock = threading.Lock()
        self.champion = None
        self.challenger = None
        self.stats = ShadowStats()
        self.reload()

    def reload(self):
        champ, champ_v = _load_model_by_alias("production")
        chal, chal_v = _load_model_by_alias("staging")
        with self._lock:
            self.champion = champ
            self.challenger = chal
            self.stats = ShadowStats(champion_version=champ_v, challenger_version=chal_v)

    def _row_to_X(self, features: dict) -> np.ndarray:
        return np.array([[float(features[c]) for c in self.feature_cols]])

    def score(self, features: dict) -> dict:
        """Score one transaction. Champion decides; challenger runs in shadow.

        The returned decision (`decision`, `risk_score`) is always the champion's.
        The `shadow` block reports the challenger and their agreement, or is null
        when no challenger is registered.
        """
        if self.champion is None:
            raise RuntimeError("No @production (champion) model registered.")

        X = self._row_to_X(features)
        champ_score = float(_predict_proba(self.champion, X)[0])
        champ_alert = champ_score >= self.threshold

        result = {
            "risk_score": round(champ_score, 6),
            "decision": "REVIEW" if champ_alert else "ALLOW",
            "champion_version": self.stats.champion_version,
            "shadow": None,
        }

        if self.challenger is not None:
            chal_score = float(_predict_proba(self.challenger, X)[0])
            chal_alert = chal_score >= self.threshold
            with self._lock:
                self.stats.update(champ_score, chal_score, champ_alert, chal_alert)
            result["shadow"] = {
                "challenger_version": self.stats.challenger_version,
                "challenger_risk_score": round(chal_score, 6),
                "challenger_decision": "REVIEW" if chal_alert else "ALLOW",
                "agree": champ_alert == chal_alert,
                "score_delta": round(champ_score - chal_score, 6),
            }
        return result

    def summary(self) -> dict:
        with self._lock:
            return self.stats.as_dict()


def build_default_scorer() -> Optional["ShadowScorer"]:
    """Construct a scorer using the trained bundle's feature contract, if present.

    Returns None if neither a persisted bundle nor a registered production model is
    available (so the API can start without models and report that honestly).
    """
    import pickle
    feature_cols = None
    bundle_path = os.path.join("mlruns", "fraud_model.pkl")
    if os.path.exists(bundle_path):
        with open(bundle_path, "rb") as f:
            feature_cols = pickle.load(f).get("feature_cols")
    if feature_cols is None:
        try:
            from src.ml.train_model import FEATURE_COLS
            feature_cols = FEATURE_COLS
        except Exception:
            return None
    try:
        return ShadowScorer(feature_cols)
    except Exception:
        return None
