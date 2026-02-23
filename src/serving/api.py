from fastapi import FastAPI
import redis
import json
from src.utils.config import load_config

app = FastAPI()
cfg = load_config()

r = redis.Redis(host=cfg["redis"]["host"], port=int(cfg["redis"]["port"]), decode_responses=True)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/decision/user/{user_id}")
def decision(user_id: int):
    key = f"user:{user_id}"
    val = r.get(key)
    if not val:
        return {"user_id": user_id, "found": False}
    return {"found": True, "data": json.loads(val)}
