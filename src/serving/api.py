import json
import logging
from enum import Enum

import psycopg2
import redis
import redis.exceptions
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.utils.config import load_config

app = FastAPI()
cfg = load_config()
_log = logging.getLogger("api")


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


@app.get("/decision/user/{user_id}")
def decision(user_id: int):
    if r is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    try:
        key = f"user:{user_id}"
        val = r.get(key)
    except redis.exceptions.RedisError as e:
        raise HTTPException(status_code=503, detail=f"Redis error: {e}")
    if not val:
        return {"user_id": user_id, "found": False}
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
            RETURNING case_id, user_id, risk_score, analyst_label, status
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
        "user_id": row[1],
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
            SELECT case_id, user_id, risk_score, decision, analyst_label, status, created_at
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
                "user_id": r[1],
                "risk_score": r[2],
                "decision": r[3],
                "analyst_label": r[4],
                "status": r[5],
                "created_at": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.serving.api:app", host="0.0.0.0", port=8000, reload=False)
