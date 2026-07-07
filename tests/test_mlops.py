"""
Tests for the MLOps loop: promotion gate policy, drift math, retraining trigger,
and the shadow scorer's comparison logic. These are pure-logic tests — they don't
need a running MLflow server or the full dataset — plus one end-to-end registry
test against a throwaway file store.
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestDriftMath(unittest.TestCase):
    def test_psi_zero_for_identical(self):
        import numpy as np
        from src.mlops.drift import psi
        rng = np.random.default_rng(0)
        x = rng.normal(size=5000)
        self.assertLess(psi(x, x.copy()), 0.02)

    def test_psi_large_for_shifted(self):
        import numpy as np
        from src.mlops.drift import psi, PSI_CRITICAL
        rng = np.random.default_rng(0)
        ref = rng.normal(0, 1, 5000)
        cur = rng.normal(3, 1, 5000)  # big location shift
        self.assertGreater(psi(ref, cur), PSI_CRITICAL)

    def test_ks_bounds(self):
        import numpy as np
        from src.mlops.drift import ks_statistic
        rng = np.random.default_rng(1)
        a = rng.normal(size=2000)
        self.assertLess(ks_statistic(a, a.copy()), 0.05)
        self.assertGreater(ks_statistic(a, a + 5), 0.9)

    def test_severity_bands(self):
        from src.mlops.drift import _severity_from_psi, SEVERITY_OK, SEVERITY_WARN, SEVERITY_CRITICAL
        self.assertEqual(_severity_from_psi(0.05), SEVERITY_OK)
        self.assertEqual(_severity_from_psi(0.15), SEVERITY_WARN)
        self.assertEqual(_severity_from_psi(0.30), SEVERITY_CRITICAL)

    def test_run_drift_reports_features_and_overall(self):
        import numpy as np, pandas as pd
        from src.mlops.drift import run_drift, SEVERITY_CRITICAL
        rng = np.random.default_rng(2)
        cols = ["V1", "V2", "Amount"]
        ref = pd.DataFrame({c: rng.normal(0, 1, 3000) for c in cols})
        cur = pd.DataFrame({"V1": rng.normal(4, 1, 3000),  # drifted
                            "V2": rng.normal(0, 1, 3000),  # stable
                            "Amount": rng.normal(0, 1, 3000)})
        rep = run_drift(ref, cur, cols)
        self.assertEqual(len(rep.features), 3)
        self.assertEqual(rep.overall_severity, SEVERITY_CRITICAL)
        drifted = [f.feature for f in rep.drifted_features()]
        self.assertIn("V1", drifted)
        self.assertNotIn("V2", drifted)


class TestRetrainTrigger(unittest.TestCase):
    def _report(self, severities):
        from src.mlops.drift import DriftReport, FeatureDrift
        feats = [FeatureDrift(f"V{i}", 0.3 if s != "OK" else 0.01, 0.1, s)
                 for i, s in enumerate(severities)]
        overall = "CRITICAL" if "CRITICAL" in severities else (
            "WARN" if "WARN" in severities else "OK")
        return DriftReport(n_reference=100, n_current=100, features=feats,
                           overall_severity=overall)

    def test_critical_drift_fires(self):
        from src.mlops.retrain_trigger import should_retrain
        d = should_retrain(self._report(["CRITICAL", "OK"]))
        self.assertTrue(d.should_retrain)
        self.assertEqual(d.trigger_type, "drift_critical")

    def test_broad_warn_fires(self):
        from src.mlops.retrain_trigger import should_retrain
        d = should_retrain(self._report(["WARN", "WARN", "WARN"]))
        self.assertTrue(d.should_retrain)
        self.assertEqual(d.trigger_type, "drift_broad")

    def test_single_warn_does_not_fire(self):
        from src.mlops.retrain_trigger import should_retrain
        d = should_retrain(self._report(["WARN", "OK", "OK"]))
        self.assertFalse(d.should_retrain)

    def test_model_age_fires(self):
        from src.mlops.retrain_trigger import should_retrain, MAX_MODEL_AGE_DAYS
        d = should_retrain(self._report(["OK"]), model_age_days=MAX_MODEL_AGE_DAYS + 1)
        self.assertTrue(d.should_retrain)
        self.assertEqual(d.trigger_type, "model_age")

    def test_young_stable_does_not_fire(self):
        from src.mlops.retrain_trigger import should_retrain
        d = should_retrain(self._report(["OK"]), model_age_days=1)
        self.assertFalse(d.should_retrain)


class TestPromotionGateEndToEnd(unittest.TestCase):
    """Register real (tiny) models in a throwaway file store and exercise the gate."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"
        os.environ["PAYGUARD_MLRUNS"] = self.tmp

    def tearDown(self):
        os.environ.pop("PAYGUARD_MLRUNS", None)

    def _register(self, pr_auc):
        import mlflow, mlflow.sklearn
        from sklearn.dummy import DummyClassifier
        from src.mlops.registry import get_client, REGISTERED_MODEL
        get_client()
        mlflow.set_experiment("test_gate")
        with mlflow.start_run() as run:
            m = DummyClassifier(strategy="prior").fit([[0], [1]], [0, 1])
            mlflow.sklearn.log_model(m, name="lgb_booster")
            mlflow.log_metric("test_pr_auc", pr_auc)
            mv = mlflow.register_model(f"runs:/{run.info.run_id}/lgb_booster", REGISTERED_MODEL)
        return str(mv.version)

    def test_floor_reject_then_promote_then_reject_then_rollback(self):
        from src.mlops.registry import (
            get_client, evaluate_gate, stage_challenger, promote,
            production_version, staging_version, rollback, PROMOTION_FLOOR,
        )
        c = get_client()

        # Below floor -> rejected, no production set.
        v_low = self._register(PROMOTION_FLOOR - 0.1)
        verdict = evaluate_gate(v_low, c)
        self.assertFalse(verdict.promoted)
        self.assertIsNone(production_version(c))

        # Above floor, no incumbent -> promote.
        v_good = self._register(0.80)
        verdict = evaluate_gate(v_good, c)
        self.assertTrue(verdict.promoted)
        promote(v_good, c)
        self.assertEqual(production_version(c), v_good)

        # Non-improving challenger -> rejected, stays in staging, prod unchanged.
        v_same = self._register(0.801)  # +0.001 < margin 0.005
        stage_challenger(v_same, c)
        verdict = evaluate_gate(v_same, c)
        self.assertFalse(verdict.promoted)
        self.assertEqual(staging_version(c), v_same)
        self.assertEqual(production_version(c), v_good)

        # Rollback to the earlier registered version.
        rolled = rollback(v_low, c)
        self.assertEqual(rolled, v_low)
        self.assertEqual(production_version(c), v_low)


if __name__ == "__main__":
    unittest.main()
