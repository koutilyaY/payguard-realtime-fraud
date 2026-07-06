.PHONY: up down up-all down-all producer stream dq_silver kafka-init api train retrain download

# Use the same compose files everywhere (Kafka + Monitoring)
COMPOSE = docker compose -f docker/docker-compose.yml -f docker/monitoring/docker-compose.monitoring.yml

up:
	cd docker && docker compose up -d

down:
	cd docker && docker compose down

up-all:
	$(COMPOSE) up -d
	@echo "Waiting for Kafka to be ready..."
	sleep 10
	$(MAKE) kafka-init

down-all:
	$(COMPOSE) down

kafka-init:
	$(COMPOSE) run --rm kafka-init

# Download the real ULB credit-card fraud dataset (via OpenML, no paid key).
download:
	. .venv/bin/activate && python scripts/download_data.py

producer:
	. .venv/bin/activate && PYTHONPATH=. python -m src.producer.produce_txns

stream:
	. .venv/bin/activate && \
	PYTHONPATH=. \
	PYSPARK_SUBMIT_ARGS="--driver-memory 2g \
	--conf spark.executor.memory=2g \
	--conf spark.local.dir=$(PWD)/.spark_tmp \
	--conf spark.sql.shuffle.partitions=4 \
	--conf spark.default.parallelism=4 \
	pyspark-shell" \
	python -m src.streaming.stream_fraud_pipeline

dq_silver:
	. .venv/bin/activate && python -m src.quality.validate_silver

api:
	. .venv/bin/activate && PYTHONPATH=. uvicorn src.serving.api:app --host 0.0.0.0 --port 8000

# Train the LightGBM model on the REAL ULB dataset (time-ordered split) and log
# to MLflow. Downloads the data first if it isn't cached. Run before 'make stream'.
train:
	. .venv/bin/activate && PYTHONPATH=. python -m src.ml.train_model

# Retrain using analyst feedback from the cases table + gold features.
# Run after labeling cases via POST /label/case/{case_id}, then restart 'make stream'.
retrain:
	. .venv/bin/activate && PYTHONPATH=. python -m src.ml.retrain
