# MLOps runbook — PayGuard

This describes the full training-to-serving loop around the fraud model, and is
blunt about what runs today versus what is a documented design that a deployment
would wire to its own scheduler and pager.

## The loop at a glance

```
                 ┌──────────────────────────────────────────────┐
                 │  train_model.py  (real ULB data, time split)  │
                 │  logs run + metrics to MLflow                 │
                 └───────────────┬──────────────────────────────┘
                                 │  PAYGUARD_GATE=1
                                 ▼
     ┌─────────────────────────────────────────────────────────────┐
     │  registry.register_and_gate                                 │
     │   register version → set @staging → evaluate_gate           │
     │   PR-AUC beats @production by margin (or clears floor)?     │
     │        yes → promote() sets @production                     │
     │        no  → stays @staging (challenger)                    │
     └───────────────┬───────────────────────────┬────────────────┘
                     │ @production               │ @staging
                     ▼                           ▼
     ┌──────────────────────────────┐   shadow-scored alongside
     │ serving (FastAPI)            │   the champion, never decides
     │  champion decides            │◄──────────────────────────────
     │  challenger shadow-scored    │
     │  /admin/rollback → prior ver │
     └──────────────────────────────┘
                     ▲
                     │ fires retraining
     ┌───────────────┴──────────────┐        ┌────────────────────────┐
     │ retrain_trigger.should_retrain│◄───────│ drift.run_drift        │
     │  CRITICAL / broad WARN / age  │ report │  PSI + KS per feature  │
     └──────────────────────────────┘        │  prediction drift      │
                                              └────────────────────────┘
```

## What runs end-to-end today

All of the following are real code, verified in a fresh venv (`pytest tests/` +
the module CLIs):

- **Training + versioning.** `python -m src.ml.train_model` trains on the real
  data and logs a run to MLflow (`mlruns/`, local file store).
- **Registry + gated promotion.** `src/mlops/registry.py`. With `PAYGUARD_GATE=1`,
  training registers the version, stages it, evaluates the gate, and promotes only
  if PR-AUC clears the floor / beats production by the margin. `register`, `stage`,
  `evaluate_gate`, `promote`, and `rollback` are all exercised by tests against a
  real (throwaway) file store.
- **Drift detection.** `python -m src.mlops.drift` computes real PSI + KS per
  feature and prediction drift on the real data (early vs late time windows).
- **Retraining trigger.** `src/mlops/retrain_trigger.py::should_retrain` turns a
  drift report + model age into a fire/don't-fire decision. `fire()` invokes the
  real training entry point.
- **Champion/challenger shadow serving.** `src/mlops/shadow.py` + the FastAPI
  endpoints `/score/shadow`, `/shadow/summary`, `/admin/rollback`, `/admin/reload`.
  The champion decides; the challenger is scored in shadow; rollback repoints
  `@production` and the scorer reloads. Verified via `TestClient`.
- **CI pipeline.** `.github/workflows/mlops-train.yml` runs tests → train+gate →
  registry status → drift on the committed fixture, and uploads `mlruns/` as an
  artifact. Validated locally by replaying its steps in a clean venv.

## What is design, not running here

Honest labels — these are deliberately not built into this repo:

- **The scheduler / cron.** The trigger and CI are callers-ready, but nothing in
  this repo runs on a timer. In a deployment, cron / Airflow / the CI `schedule:`
  block calls `should_retrain()` (or dispatches the workflow) on a cadence.
- **The pager.** Drift severity WARN/CRITICAL is the payload an alert would carry.
  Where it would page is described below; there is no PagerDuty/Slack integration
  wired. `run_drift` returns the severity and the drifted-feature list — that is
  the alert body, ready to POST to a webhook.
- **A model server.** Serving is the FastAPI app run locally; there is no
  autoscaled inference service, canary router, or traffic-splitting proxy. The
  shadow comparison happens in-process.
- **A tracking database.** MLflow uses the local file store (`mlruns/`), fine for a
  single node and this demo. Production would use a database backend + artifact
  store (the file store is deprecated as of Feb 2026).

## Drift results on the real data (measured)

`python -m src.mlops.drift` on the full ULB dataset, reference = earliest 50% by
`Time`, current = latest 50% (142,403 vs 142,404 transactions):

- **Overall severity: CRITICAL.** 8 features breach the CRITICAL PSI band (>= 0.25)
  and 7 more are at WARN (>= 0.10).
- Worst offenders: **V1 PSI 1.43** (KS 0.42), **V3 PSI 1.38** (KS 0.51), **V28 PSI
  1.01** (KS 0.36), V25 0.43, V15 0.38. These are genuine covariate shifts across
  the dataset's ~2 days.
- **Prediction drift is OK: PSI 0.06.** The model's `P(fraud)` distribution barely
  moves and the alert rate is nearly flat (0.00189 → 0.00198) despite the large
  input shift. That is a real, non-obvious finding: heavy input drift here does not
  translate into output drift — the drifted components are ones the model leans on
  little. It is exactly why you monitor *both* input and prediction drift rather
  than assuming one implies the other.

Given `overall = CRITICAL`, `should_retrain()` returns
`trigger_type = "drift_critical"` and fires — verified.

## Alerting design (where it would page)

`run_drift` returns a `DriftReport` with `overall_severity` and
`drifted_features()`. A monitor calling it on a schedule would route on severity:

| Severity | Action |
|---|---|
| OK | record metrics, no alert |
| WARN | post to the team channel (Slack/Teams) with the drifted-feature list |
| CRITICAL | page on-call **and** call `retrain_trigger.should_retrain` → `fire()` (or dispatch the CI workflow) |

The report `.as_dict()` is the alert payload — feature names, PSI, KS, severity,
prediction drift — ready to serialize to a webhook.

## Runbook procedures

### Train + gate a new model (the CI path, locally)

```bash
export MLFLOW_ALLOW_FILE_STORE=true PYTHONPATH=.
PAYGUARD_GATE=1 python -m src.ml.train_model      # full data
# or fast, on the fixture:
PAYGUARD_GATE=1 PAYGUARD_DATA_CSV=fixtures/creditcard_sample.csv python -m src.ml.train_model
python -m src.mlops.registry                      # show @production / @staging
```

The gate log line says whether it promoted and why (`promoted=True/False` + reason).

### Check for drift and whether to retrain

```bash
python -m src.mlops.drift                          # human-readable table
python -m src.mlops.retrain_trigger                # drift + model age → decision
```

### Shadow-score a challenger and compare

Register a candidate under `@staging` (the gate does this for a rejected
challenger; you can also stage manually via `registry.stage_challenger`). Then:

```bash
make api
curl -s -X POST localhost:8000/score/shadow \
  -H 'content-type: application/json' \
  -d '{"features": {"V1": -1.3, ... , "V28": 0.02, "Amount": 149.62}}'
curl -s localhost:8000/shadow/summary   # agreement rate, alert rates, mean delta
```

The response carries the champion's decision plus the challenger's shadow score and
whether they agree.

### Roll back (one command)

If a promotion misbehaves, repoint `@production` at a prior version. Via the API
(reloads the live scorer):

```bash
curl -s -X POST localhost:8000/admin/rollback \
  -H 'content-type: application/json' -d '{"to_version": null}'   # most recent prior
# or target a specific version: -d '{"to_version": "6"}'
```

Or directly against the registry:

```bash
python -c "from src.mlops.registry import rollback; print('now production:', rollback())"
```

Rollback is instant — it only moves an alias; no retraining, no redeploy of a
pickle. The serving layer reads the alias on its next `reload()`.

### Feedback-driven retraining (existing path)

Analyst labels flow through `POST /label/case/{id}` into Postgres; `make retrain`
blends up-weighted confirmed cases into the real base training set. See the README.
That retrained model can then be run through the same gate.
