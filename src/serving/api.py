import json
import logging
from enum import Enum
from typing import Dict, Optional

import psycopg2
import redis
import redis.exceptions
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.utils.config import load_config

app = FastAPI()
cfg = load_config()
_log = logging.getLogger("api")

# Champion/challenger shadow scorer. Built lazily so the API still starts when no
# model is trained yet; endpoints that need it return 503 in that case.
try:
    from src.mlops.shadow import build_default_scorer
    _scorer = build_default_scorer()
except Exception as e:  # pragma: no cover - defensive at import time
    _log.warning("Shadow scorer unavailable at startup: %s", e)
    _scorer = None


class ScoreRequest(BaseModel):
    """One transaction's feature vector: {"V1": .., ..., "V28": .., "Amount": ..}."""
    features: Dict[str, float]


class AnalystLabel(str, Enum):
    FRAUD = "FRAUD"
    LEGIT = "LEGIT"
    UNKNOWN = "UNKNOWN"


class LabelRequest(BaseModel):
    label: AnalystLabel
    analyst_id: str = "anonymous"


def _pg_conn():
    pg = cfg["postgres"]
    dbname = pg.get("db") or pg.get("dbname") or pg.get("database") or "postgres"
    return psycopg2.connect(
        host=pg["host"],
        port=int(pg["port"]),
        dbname=dbname,
        user=pg["user"],
        password=pg["password"],
    )

try:
    r = redis.Redis(host=cfg["redis"]["host"], port=int(cfg["redis"]["port"]), decode_responses=True)
    r.ping()
except redis.exceptions.ConnectionError as e:
    _log.warning("Redis not available at startup: %s", e)
    r = None


@app.get("/health")
def health():
    redis_ok = False
    if r is not None:
        try:
            redis_ok = bool(r.ping())
        except Exception:
            pass
    return {"ok": True, "redis": redis_ok}


@app.get("/decision/txn/{event_id}")
def decision(event_id: str):
    """Latest risk snapshot for a scored transaction (keyed by event_id).

    The stream scores real ULB transactions per-transaction, so the online store
    is keyed on event_id — the ULB dataset has no user identifier.
    """
    if r is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    try:
        key = f"txn:{event_id}"
        val = r.get(key)
    except redis.exceptions.RedisError as e:
        raise HTTPException(status_code=503, detail=f"Redis error: {e}")
    if not val:
        return {"event_id": event_id, "found": False}
    try:
        return {"found": True, "data": json.loads(val)}
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid data in Redis: {e}")


@app.post("/label/case/{case_id}")
def label_case(case_id: str, body: LabelRequest):
    """
    Submit an analyst label for a fraud case.

    - FRAUD  : confirmed fraud — feeds retraining as a positive example
    - LEGIT  : false positive — feeds retraining as a negative example
    - UNKNOWN: clears a previous label (resets to default)

    After labeling, run 'make retrain' to incorporate new labels into the model.
    """
    try:
        conn = _pg_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE cases
               SET analyst_label = %s,
                   status        = CASE WHEN %s = 'FRAUD' THEN 'CONFIRMED'
                                        WHEN %s = 'LEGIT'  THEN 'FALSE_POSITIVE'
                                        ELSE 'OPEN' END
             WHERE case_id = %s
            RETURNING case_id, event_id, risk_score, analyst_label, status
            """,
            (body.label.value, body.label.value, body.label.value, case_id),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")

    if row is None:
        raise HTTPException(status_code=404, detail=f"Case '{case_id}' not found")

    _log.info("Case %s labeled %s by %s", case_id, body.label.value, body.analyst_id)
    return {
        "case_id": row[0],
        "event_id": row[1],
        "risk_score": row[2],
        "analyst_label": row[3],
        "status": row[4],
        "message": f"Labeled as {body.label.value}. Run 'make retrain' to update the model.",
    }


@app.get("/cases/labeled")
def get_labeled_cases(limit: int = 100):
    """Return cases that have been labeled by analysts (for retraining review)."""
    try:
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT case_id, event_id, risk_score, decision, analyst_label, status, created_at
              FROM cases
             WHERE analyst_label IN ('FRAUD', 'LEGIT')
             ORDER BY created_at DESC
             LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except psycopg2.Error as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}")

    return {
        "count": len(rows),
        "cases": [
            {
                "case_id": r[0],
                "event_id": r[1],
                "risk_score": r[2],
                "decision": r[3],
                "analyst_label": r[4],
                "status": r[5],
                "created_at": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ],
    }


# ── Champion/challenger shadow scoring + rollback ─────────────────────────────
# Unlike /decision (which reads the Spark stream's Redis snapshot), these score a
# transaction synchronously through the registry-backed models. The champion
# (@production) decides; the challenger (@staging) is scored in shadow only.

class RollbackRequest(BaseModel):
    to_version: Optional[str] = None  # None = roll back to the most recent prior


@app.post("/score/shadow")
def score_shadow(body: ScoreRequest):
    """Score a transaction. Returns the champion's decision plus, if a challenger
    is registered, its shadow score and whether the two agree."""
    if _scorer is None:
        raise HTTPException(status_code=503, detail="No model registered; train + register first.")
    try:
        return _scorer.score(body.features)
    except KeyError as e:
        raise HTTPException(status_code=422, detail=f"Missing feature: {e}")
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/shadow/summary")
def shadow_summary():
    """Running champion-vs-challenger agreement / alert-rate comparison."""
    if _scorer is None:
        raise HTTPException(status_code=503, detail="No model registered.")
    return _scorer.summary()


@app.post("/admin/rollback")
def admin_rollback(body: RollbackRequest):
    """One call to point @production back at a prior registered version. The
    scorer reloads so the change takes effect immediately for /score/shadow."""
    from src.mlops.registry import rollback
    try:
        now_production = rollback(body.to_version)
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if _scorer is not None:
        _scorer.reload()
    return {"rolled_back": True, "production_version": now_production}


@app.post("/admin/reload")
def admin_reload():
    """Reload champion/challenger from the registry (after a promotion elsewhere)."""
    if _scorer is None:
        raise HTTPException(status_code=503, detail="No model registered.")
    _scorer.reload()
    return {"reloaded": True, **_scorer.summary()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.serving.api:app", host="0.0.0.0", port=8000, reload=False)
