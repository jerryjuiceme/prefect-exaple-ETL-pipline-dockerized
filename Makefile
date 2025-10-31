# Makefile

# Load environment variables
include .env
export $(shell sed 's/=.*//' .env)

# Start all docker services
up:
	docker network create prefect-network || true
	docker compose -f docker-compose.yaml -f docker-compose-worker.yaml up -d --build

# Stop all containers
down:
	docker compose -f docker-compose.yaml -f docker-compose-worker.yaml down

# Run credentials seeding
seed:
	uv run seed_credentials.py

# Run Prefect deployment
deploy:
	uv run deploy.py

# Start Prefect worker locally
worker:
	prefect worker start --pool "basic-pipe"

# Build worker image
build:
	docker build -t prefect-worker .

# Logs
logs:
	docker compose logs -f
