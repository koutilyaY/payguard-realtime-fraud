"""
Unit tests for PayGuard core logic.
Run with: pytest tests/ -v
"""
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Config ────────────────────────────────────────────────────────────────────

class TestLoadConfig(unittest.TestCase):
    def test_loads_config_yaml(self):
        from src.utils.config import load_config
        cfg = load_config()
        self.assertIn("kafka", cfg)
        self.assertIn("paths", cfg)
        self.assertIn("redis", cfg)
        self.assertIn("postgres", cfg)
        self.assertIn("rules", cfg)
        self.assertIn("producer", cfg)

    def test_kafka_has_required_keys(self):
        from src.utils.config import load_config
        cfg = load_config()
        kafka = cfg["kafka"]
        self.assertIn("bootstrap_servers", kafka)
        self.assertIn("topic_raw", kafka)
        self.assertIn("topic_dlq", kafka)
        self.assertIn("topic_alerts", kafka)

    def test_paths_has_required_keys(self):
        from src.utils.config import load_config
        cfg = load_config()
        paths = cfg["paths"]
        for key in ("bronze", "silver", "gold_features", "dlq_delta", "checkpoints", "dq_results"):
            self.assertIn(key, paths, f"Missing path key: {key}")

    def test_stream_metrics_removed(self):
        from src.utils.config import load_config
        cfg = load_config()
        self.assertNotIn("stream_metrics", cfg.get("paths", {}))

    def test_rules_has_required_fields(self):
        from src.utils.config import load_config
        cfg = load_config()
        rules = cfg["rules"]
        self.assertIn("risk_threshold_alert", rules)
        self.assertIn("max_amount", rules)


# ── Logger ────────────────────────────────────────────────────────────────────

class TestLogger(unittest.TestCase):
    def test_get_logger_returns_logger(self):
        import logging
        from src.utils.logging import get_logger
        lg = get_logger("test")
        self.assertIsInstance(lg, logging.Logger)

    def test_logger_alias_matches_canonical(self):
        from src.utils.logger import get_logger as alias
        from src.utils.logging import get_logger as canonical
        self.assertIs(alias, canonical)

    def test_logger_respects_log_level_env(self):
        import logging
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}):
            from src.utils.logging import get_logger
            lg = get_logger("test_debug")
        self.assertEqual(lg.level, logging.DEBUG)


# ── Producer: real-transaction replay event shape ─────────────────────────────

def _mock_confluent_kafka():
    """Return a sys.modules patch dict for confluent_kafka (not installed locally)."""
    mock_ck = MagicMock()
    mock_ck.Producer = MagicMock(return_value=MagicMock())
    mock_ck.KafkaException = Exception
    return {"confluent_kafka": mock_ck}


class TestReplayEvent(unittest.TestCase):
    """The producer replays REAL ULB rows; test the row->event conversion."""

    def setUp(self):
        self._mods_patcher = patch.dict("sys.modules", _mock_confluent_kafka())
        self._mods_patcher.start()
        for key in list(sys.modules):
            if "produce_txns" in key:
                del sys.modules[key]

    def tearDown(self):
        self._mods_patcher.stop()

    def _fake_row(self):
        row = {"Time": 123.0, "Amount": 42.5, "Class": 1}
        for i in range(1, 29):
            row[f"V{i}"] = float(i) * 0.1
        return row

    def _get_mod(self):
        import src.producer.produce_txns as mod
        return mod

    def test_event_has_pca_features_and_amount(self):
        mod = self._get_mod()
        evt = mod.row_to_event(self._fake_row())
        for i in range(1, 29):
            self.assertIn(f"V{i}", evt)
        self.assertEqual(evt["amount"], 42.5)

    def test_event_carries_ground_truth_label(self):
        mod = self._get_mod()
        evt = mod.row_to_event(self._fake_row())
        # Label rides along for offline measurement; the model never sees it.
        self.assertEqual(evt["label_fraud"], 1)

    def test_event_id_is_uuid(self):
        import uuid
        mod = self._get_mod()
        evt = mod.row_to_event(self._fake_row())
        uuid.UUID(evt["event_id"])

    def test_event_has_timestamp(self):
        mod = self._get_mod()
        evt = mod.row_to_event(self._fake_row())
        self.assertIn("ts", evt)
        self.assertEqual(evt["time_offset"], 123.0)


# ── Streaming helpers ─────────────────────────────────────────────────────────

class TestStreamingHelpers(unittest.TestCase):
    def setUp(self):
        # patch PySpark and Delta imports so module loads without Spark
        self.patches = [
            patch.dict("sys.modules", {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock(),
                "pyspark.sql.functions": MagicMock(),
                "pyspark.sql.types": MagicMock(),
                "pyspark.sql.window": MagicMock(),
                "delta": MagicMock(),
                "delta.tables": MagicMock(),
            })
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def test_pick_returns_first_existing_key(self):
        from src.streaming.stream_fraud_pipeline import _pick
        self.assertEqual(_pick({"a": 1, "b": 2}, "a", "b"), 1)
        self.assertEqual(_pick({"b": 2}, "a", "b"), 2)
        self.assertIsNone(_pick({}, "a", "b"))

    def test_pick_skips_empty_string(self):
        from src.streaming.stream_fraud_pipeline import _pick
        self.assertEqual(_pick({"a": "", "b": "ok"}, "a", "b"), "ok")

    def test_resolve_kafka_from_config(self):
        from src.streaming.stream_fraud_pipeline import _resolve_kafka
        cfg = {
            "kafka": {
                "bootstrap_servers": "localhost:9092",
                "topic_raw": "txns",
                "topic_dlq": "txns_dead_letter",
                "topic_alerts": "txns_alerts",
            }
        }
        result = _resolve_kafka(cfg)
        self.assertEqual(result["bootstrap_servers"], "localhost:9092")
        self.assertEqual(result["topic_raw"], "txns")
        self.assertEqual(result["topic_dlq"], "txns_dead_letter")
        self.assertEqual(result["topic_alerts"], "txns_alerts")

    def test_resolve_kafka_raises_on_missing(self):
        from src.streaming.stream_fraud_pipeline import _resolve_kafka
        with self.assertRaises(KeyError):
            _resolve_kafka({})

    def test_resolve_streaming_uses_defaults(self):
        from src.streaming.stream_fraud_pipeline import _resolve_streaming
        result = _resolve_streaming({})
        self.assertEqual(result["watermark"], "10 minutes")
        self.assertEqual(result["window"], "5 minutes")
        self.assertEqual(result["trigger"], "10 seconds")

    def test_no_duplicate_gold_kpi_path(self):
        """Regression: GOLD_KPI_DAILY_PATH must be defined exactly once."""
        import ast
        path = os.path.join(os.path.dirname(__file__), "..", "src", "streaming", "stream_fraud_pipeline.py")
        source = open(path).read()
        count = source.count("GOLD_KPI_DAILY_PATH =")
        self.assertEqual(count, 1, f"GOLD_KPI_DAILY_PATH defined {count} times, expected 1")


# ── Real-data training core ───────────────────────────────────────────────────

class TestTimeOrderedSplit(unittest.TestCase):
    """The core must split by Time (earlier -> train, later -> test), no leakage."""

    def _frame(self):
        import pandas as pd
        rows = []
        for t in range(100):
            row = {"Time": float(t), "Amount": 10.0, "Class": t % 5 == 0}
            for i in range(1, 29):
                row[f"V{i}"] = 0.0
            rows.append(row)
        return pd.DataFrame(rows)

    def test_split_is_time_ordered_no_overlap(self):
        try:
            from src.ml.train_model import time_ordered_split, TRAIN_FRACTION
        except ImportError:
            self.skipTest("training deps (numpy/pandas/lightgbm) not installed")
        df = self._frame()
        X_fit, y_fit, X_val, y_val, X_test, y_test, t_split = time_ordered_split(df)
        # Test set must be strictly the later portion.
        expected_train = int(len(df) * TRAIN_FRACTION)
        self.assertEqual(len(y_fit) + len(y_val), expected_train)
        self.assertEqual(len(y_test), len(df) - expected_train)

    def test_split_requires_time_column(self):
        try:
            from src.ml.train_model import time_ordered_split
        except ImportError:
            self.skipTest("training deps not installed")
        df = self._frame().drop(columns=["Time"])
        with self.assertRaises(ValueError):
            time_ordered_split(df)

    def test_feature_cols_are_pca_plus_amount(self):
        try:
            from src.ml.train_model import FEATURE_COLS
        except ImportError:
            self.skipTest("training deps not installed")
        self.assertEqual(len(FEATURE_COLS), 29)  # V1..V28 + Amount
        self.assertEqual(FEATURE_COLS[-1], "Amount")
        self.assertNotIn("Time", FEATURE_COLS)  # Time is a split key, not a feature


# ── DQ results writer ─────────────────────────────────────────────────────────

class TestWriteDQ(unittest.TestCase):
    def test_write_dq_schema_columns(self):
        from src.quality.write_dq_results import DQ_SCHEMA
        col_names = [f.name for f in DQ_SCHEMA.fields]
        for expected in ("run_ts", "suite_name", "layer", "success", "success_percent",
                         "evaluated_expectations", "successful_expectations",
                         "unsuccessful_expectations", "validation_id"):
            self.assertIn(expected, col_names)

    def test_write_dq_builds_correct_row(self):
        """write_dq should call spark.createDataFrame with the right row values."""
        spark_mock = MagicMock()
        df_mock = MagicMock()
        spark_mock.createDataFrame.return_value = df_mock
        df_mock.write.format.return_value.mode.return_value.save = MagicMock()

        from src.quality.write_dq_results import write_dq
        results = {
            "success": True,
            "statistics": {
                "success_percent": 100.0,
                "evaluated_expectations": 4,
                "successful_expectations": 4,
                "unsuccessful_expectations": 0,
            },
            "meta": {"validation_id": "test-run-123"},
        }
        write_dq(spark_mock, "delta/test", "my_suite", "silver", results)

        spark_mock.createDataFrame.assert_called_once()
        row = spark_mock.createDataFrame.call_args[0][0][0]
        self.assertEqual(row["suite_name"], "my_suite")
        self.assertEqual(row["layer"], "silver")
        self.assertEqual(row["success"], 1)
        self.assertEqual(row["evaluated_expectations"], 4)
        self.assertEqual(row["validation_id"], "test-run-123")


# ── API ───────────────────────────────────────────────────────────────────────

def _mock_redis_modules():
    """Build a sys.modules patch for redis (not installed locally)."""
    mock_redis_mod = MagicMock()
    mock_exceptions = MagicMock()

    class _RedisError(Exception):
        pass

    mock_exceptions.RedisError = _RedisError
    mock_exceptions.ConnectionError = _RedisError
    mock_redis_mod.exceptions = mock_exceptions
    mock_redis_mod.Redis = MagicMock(return_value=MagicMock())
    return {
        "redis": mock_redis_mod,
        "redis.exceptions": mock_exceptions,
    }


class TestAPI(unittest.TestCase):
    def setUp(self):
        self._mods_patcher = patch.dict("sys.modules", _mock_redis_modules())
        self._mods_patcher.start()
        for key in list(sys.modules):
            if "serving.api" in key or "src.serving" in key:
                del sys.modules[key]

    def tearDown(self):
        self._mods_patcher.stop()

    def _get_app(self, redis_data=None, redis_raises=None):
        """Return a TestClient for the FastAPI app with mocked Redis."""
        from fastapi.testclient import TestClient
        import src.serving.api as api_mod

        mock_r = MagicMock()
        mock_r.ping.return_value = True
        if redis_raises:
            mock_r.get.side_effect = redis_raises
        else:
            mock_r.get.return_value = json.dumps(redis_data) if redis_data else None

        api_mod.r = mock_r
        return TestClient(api_mod.app)

    def test_health_returns_ok(self):
        client = self._get_app()
        resp = client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["ok"])

    def test_decision_not_found(self):
        client = self._get_app(redis_data=None)
        resp = client.get("/decision/txn/does-not-exist")
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.json()["found"])

    def test_decision_found(self):
        payload = {"event_id": "abc-123", "risk_score": 0.9, "decision": "REVIEW",
                   "reasons": "HIGH_MODEL_RISK", "updated_at": "2026-01-01T00:00:00Z"}
        client = self._get_app(redis_data=payload)
        resp = client.get("/decision/txn/abc-123")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["found"])
        self.assertEqual(body["data"]["risk_score"], 0.9)

    def test_decision_redis_error_returns_503(self):
        import sys
        redis_exc = sys.modules["redis"].exceptions.RedisError
        client = self._get_app(redis_raises=redis_exc("conn refused"))
        resp = client.get("/decision/txn/x")
        self.assertEqual(resp.status_code, 503)

    def test_api_no_uvicorn_entry_present(self):
        """Regression: api.py must have a uvicorn __main__ entry point."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "serving", "api.py")
        source = open(path).read()
        self.assertIn('if __name__ == "__main__"', source)
        self.assertIn("uvicorn", source)


if __name__ == "__main__":
    unittest.main()
