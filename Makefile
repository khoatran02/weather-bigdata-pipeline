.DEFAULT_GOAL := help
DC := docker compose
COUNTRIES ?= VN
DOCKER_RUN_CACHE := docker run --rm --user $(shell id -u):$(shell id -g) -v $(CURDIR)/data:/data ktxldll-weather/app:1.0

.PHONY: help env build up down status logs urls archive backfill gold train forecast check demo test e2e bench dashboards

help: ## Show this help
	@awk 'BEGIN{FS=":.*## "} /^[a-z0-9-]+:.*## /{printf "  %-12s %s\n",$$1,$$2}' $(MAKEFILE_LIST)

env: ## Create .env with random secrets
	@python3 scripts/init_env.py

build: ## Build the Spark and Airflow images
	docker build --target app -t ktxldll-weather/app:1.0 .
	docker build --target airflow -t ktxldll-weather/airflow:1.0 .

up: env ## Start the whole platform
	$(DC) up -d

down: ## Stop the platform (data volumes are kept)
	$(DC) down

status: ## Show service status
	$(DC) ps --format 'table {{.Service}}\t{{.Status}}'

logs: ## Follow the streaming and collector logs
	$(DC) logs -f --tail=50 streaming collector

urls: ## Show the web interfaces
	@echo "Grafana     http://localhost:13000  admin / GRAFANA_PASSWORD"
	@echo "Airflow     http://localhost:18083  admin / AIRFLOW_PASSWORD"
	@echo "InfluxDB    http://localhost:18086  admin / INFLUX_PASSWORD"
	@echo "Spark       http://localhost:18081"
	@echo "Streaming   http://localhost:14040"
	@echo "Kafka UI    http://localhost:18082"
	@echo "Prometheus  http://localhost:19090"
	@echo "Passwords are in .env"

archive: ## Download the METAR archive (resumable), COUNTRIES=VN,TH,LA,KH for more
	@mkdir -p data/cache
	$(DOCKER_RUN_CACHE) python3 -m weather_pipeline.ingest.archive --directory /data/cache --countries $(COUNTRIES)

backfill: ## Replay the archive through Kafka and wait for silver to catch up
	$(DC) run --rm job python3 -m weather_pipeline.ingest.replay
	python3 scripts/wait_for_stream.py

gold: ## Rebuild gold
	$(DC) run --rm job submit weather_pipeline.batch.gold

train: ## Train and evaluate the forecast models
	$(DC) run --rm job submit weather_pipeline.ml.train

forecast: ## Score the latest observations
	$(DC) run --rm job submit weather_pipeline.ml.forecast

check: ## Run the data-quality gate
	$(DC) run --rm job submit weather_pipeline.batch.checks

demo: build up archive backfill gold check train forecast urls ## Everything from zero

test: ## Lint and unit tests
	docker build -q --target test -t ktxldll-weather/test:1.0 .
	docker run --rm ktxldll-weather/test:1.0

e2e: ## Failure-injection test in an isolated namespace (restarts a broker and a worker)
	python3 scripts/fault_tolerance.py

bench: ## Scalability and storage-format benchmark (pauses streaming)
	python3 scripts/benchmark.py

dashboards: ## Regenerate the Grafana dashboards
	python3 scripts/build_dashboards.py
