
This repository provides a small FastAPI app that stores a running sum in Redis.
The default `docker-compose.yml` now defines two app services (`app1` and `app2`) that both connect to the same Redis instance. Each app binds to a unique host port so you can call them directly from your host.

Prerequisites

- Docker Desktop (Windows) or Docker Engine + Compose (Linux/macOS)
- Project checked out to a local directory

Quick start (two named app services)

1. Build and start services (from the repository root):

```powershell
docker compose up --build -d
```


By default the compose file may not bind host ports for the app services (they run on the Docker network). There are two common demo modes below:

1) Scaled / Docker-network mode (no host ports)

This is the recommended demo for showing many replicas that share Redis. App containers are accessible on the Docker network; to call an endpoint inside a container use `docker compose exec`:

```powershell
# Build and start (no host ports bound)
docker compose up --build -d

# List containers to find the running service names
docker compose ps

# Call /abacus/sum inside the first running app container (runs curl inside the container)
docker compose exec app1 curl -sS -H "X-API-Key: local-dev-key" http://localhost:8000/abacus/sum
```

2) Named-services with host ports (easy host access)

If you'd like to call the services directly from your host (PowerShell), add `ports` mappings for each app in `docker-compose.yml` (for example `8001:8000` and `8002:8000`) and then start:

```powershell
docker compose up --build -d
```

Example PowerShell host calls when ports are mapped:

```powershell
# Get sum from app1 (if you mapped 8001:8000)
Invoke-RestMethod -Uri "http://localhost:8001/abacus/sum" -Method Get -Headers @{"X-API-Key"="local-dev-key"}

# Add a number to app2 (if you mapped 8002:8000)
Invoke-RestMethod -Uri "http://localhost:8002/abacus/number" -Method Post -Body '{"number": 5}' -ContentType "application/json" -Headers @{"X-API-Key"="local-dev-key"; "Idempotency-Key"="unique-key-123"}
```

Load testing and metrics capture

We provide a small Python load testing script at `tools/load_test.py` that uses `httpx` to send concurrent POSTs to the app endpoints and polls `/metrics` while the test runs.

Example (named-services mode with host ports):

```powershell
# Run a load test of 1000 requests with concurrency 50 against app1/app2
python tools/load_test.py --target1 http://localhost:8001 --target2 http://localhost:8002 --requests 1000 --concurrency 50 --metrics-url http://localhost:8001/metrics
```

Example (scaled/docker-network mode):

```powershell
# Start without host ports bound
docker compose up --build -d

# Run load test against one of the containers using the host-mapped metrics port (if you expose it)
# or run the load script from inside a helper container. For local quick tests it's easiest to use named-services mode.
```

The script writes metrics snapshots to `metrics_snapshots.txt`. You can inspect the Prometheus metrics lines for `abacus_request_total`, `abacus_request_duration_seconds_bucket`, and `abacus_redis_operations_total` to calculate requests/sec, latency buckets and Redis error counts.

Environment (REDIS_URL)

Both app services are configured to use the same Redis connection string:

`REDIS_URL=redis://redis:6379/0`

Security and API key

The services enable API key auth with the environment variable `ABACUS_API_KEYS=local-dev-key`. Include the header `X-API-Key: local-dev-key` in your requests. POST requests to `/abacus/number` additionally require an `Idempotency-Key` header.

Example POST (PowerShell):

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/abacus/number" -Method Post -Body '{"number": 42}' -ContentType "application/json" -Headers @{"X-API-Key"="local-dev-key"; "Idempotency-Key"="unique-key-1"}
```

If you prefer to scale multiple identical replicas instead of named services, remove the `ports` mapping for the app services and use:

```powershell
docker compose up --build --detach --scale app=2
```

Cleanup

```powershell
docker compose down
```

## Run the project (load the environment)

These steps show how to run the project locally and how to prepare the Python environment used by some helper scripts and tests.

1. Create and activate a virtual environment (PowerShell):

```powershell
python -m venv .abacusenv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
. .\.abacusenv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

2. Run the FastAPI app locally (without Docker) for quick dev/debug:

```powershell
# from repo root
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

3. Or run the full stack (recommended) with Docker Compose:

```powershell
docker compose up --build -d
# check services
docker compose ps
```

Notes:
- The apps expect Redis at `redis:6379` when run under compose. The compose file sets `REDIS_URL=redis://redis:6379/0` by default.
- If you run the app locally (uvicorn), set `REDIS_URL` in your shell (for example `setx REDIS_URL "redis://localhost:6379/0"` on Windows) and ensure a Redis instance is reachable.

## Running tests

There are three kinds of tests in this repo: unit (storage), integration, and load tests.

1) Storage unit test (fast)

```powershell
# run the unit test for storage
pytest Tests/test_storage.py -q
```

2) Integration tests (require Docker or testcontainers)

These tests validate multi-node correctness and may start containers. They are marked with `integration`.

```powershell
# run all integration tests
pytest -m integration -q

# or run the container-based integration test that uses docker compose
pytest Tests/integration/test_multinode_containers.py -q
```

3) Load test (external load-runner)

The load testing helper is `tools/load_test.py`. Example usage (named-services mode with host ports mapped):

```powershell
# short smoke run (includes API key and idempotency)
python .\tools\load_test.py --target1 http://localhost:8001 --target2 http://localhost:8002 --requests 100 --concurrency 10 --metrics-url http://localhost:8001/metrics --idempotency --api-key local-dev-key --rate 10 --out-csv results.csv
```

Outputs:
- `metrics_snapshots.txt` — Prometheus-format metrics collected during the run.
- `results.csv` — per-request CSV with timestamp, worker id, status, latency and error (if any).

Tip: if you see many 429 responses, the service enforces rate limiting (env var `ABACUS_RATE_LIMIT_REQUESTS`, default in compose: 600). Either lower the load or raise the limit in your compose override.

## Adding more app instances

There are two common approaches to add more app instances for demo or scale testing.

1) Named services (explicit entries in `docker-compose.yml`)

Copy one of the `app1`/`app2` service blocks and add `app3` with its own `ports` mapping if you want to call it directly from the host. Example `docker-compose.override.yml` snippet:

```yaml
services:
	app3:
		build: .
		environment:
			- REDIS_URL=redis://redis:6379/0
			- ABACUS_AUTH_ENABLED=true
			- ABACUS_API_KEYS=local-dev-key
		depends_on:
			redis:
				condition: service_healthy
		ports:
			- "8003:8000"

# then start with the override
docker compose -f docker-compose.yml -f docker-compose.override.yml up --build -d
```

2) Replica scaling (recommended for many identical replicas)

Change `docker-compose.yml` to define a single service named `app` (instead of `app1`/`app2`) that uses the same image and env. Then use `--scale` to start N replicas. Example:

```powershell
# build and start 4 replicas
docker compose up --build --detach --scale app=4

# find the container names and call one via docker exec
docker compose ps
docker compose exec app sh -c "curl -sS -H 'X-API-Key: local-dev-key' http://localhost:8000/abacus/sum"
```

Notes about networking and ports:
- In scaled mode replicas do not have host ports by default. They are reachable to each other over the Docker network by service name (`app`) and to the host only if you explicitly map ports.
- For ease of host-based testing, use named-services with `ports` mapped (8001, 8002, ...). For realistic multi-replica behavior, prefer scaled mode and use `docker compose exec` or run tests from an in-network helper container.

If you want, I can add a small `docker-compose.override.yml` template to the repo that demonstrates both named host ports and a scale-friendly `app` service—tell me which you prefer and I'll add it.
