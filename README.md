# mini-k8ts

Distributed mini-kubernetes style scheduler prototype based on the provided HLD/LLD.

## Goals (from HLD/LLD)

- API Gateway for task and cluster interactions.
- Control plane scheduler with filter/score/bind pipeline.
- Worker node heartbeat and task execution flow.
- Postgres-backed state store for nodes/tasks/allocations.
- Metrics and reliability-aware scheduling for unstable nodes.

## Current MVP Status

- LLD-aligned Go project structure created (`cmd`, `internal`, `pkg`, `tests`, `deployments`).
- Config loader with runtime knobs (`HTTP_ADDR`, log level, heartbeat and scheduler intervals).
- Structured JSON logger with levels (`debug`, `info`, `warn`, `error`).
- Request tracing includes `request_id`, HTTP status, response size, and latency in middleware logs.
- API server with required MVP endpoints implemented:
	- `POST /tasks`
	- `GET /cluster`
	- `GET /metrics`
	- `GET /metrics/prometheus`
	- `POST /nodes/register`
	- `POST /nodes/heartbeat`
	- `GET /nodes/{nodeID}/tasks/running`
	- `POST /tasks/{taskID}/status`
	- `GET /tasks/{taskID}/logs`
	- `GET /healthz`
- In-memory repositories for tasks/nodes with thread-safe state updates.
- Postgres repository implementation added in internal/db/postgres.go with SQL migration bootstrap in internal/db/migrations/001_init.sql.
- Scheduler loop running in control plane process:
	- fetch pending tasks
	- filter + score nodes
	- bind task by updating task status/node assignment
- Scheduler now prioritizes higher-priority tasks and uses a local resource shadow map to avoid over-scheduling before the next heartbeat.
- Failure detector and rescheduler loop:
	- mark inactive nodes as `DOWN` when heartbeats are missed
	- requeue `RUNNING` tasks from `DOWN` nodes back to `PENDING`
	- exponential backoff + `max_attempts` before marking `FAILED`
- Scheduler now reconciles inconsistent task/allocation state before scheduling, improving crash/restart recovery.
- Self-healing retries for failed tasks (requeue with backoff until `max_attempts`).
- Reliability is dynamic:
	- successful heartbeats increase node reliability gradually
	- inactivity/down transitions reduce reliability, lowering future scheduling preference
- Node health now tracks heartbeat count, task success/failure counts, uptime %, and success ratio for scoring and metrics.
- Postgres-backed scheduler leader election provides a control-plane HA baseline when multiple scheduler replicas are deployed.
- Worker process now registers, sends periodic heartbeats with live CPU/memory usage, and executes real Docker containers.
- Worker shutdown now best-effort stops active tasks and reports interrupted execution back to the control plane.
- Worker exposes an internal logs API and the scheduler proxies `GET /tasks/{taskID}/logs` to the right node.
- Tasks accept a command override (`command`) in `POST /tasks` and in the `m8s run` CLI.
- `POST /tasks` supports idempotency with `X-Idempotency-Key`.
- Namespaced secret storage is available through the API.
- Prometheus-compatible metrics added at `GET /metrics/prometheus` with a custom collector.
- Service endpoint selection filters unhealthy backends, and pending tasks are scheduled with namespace-aware fair ordering.
- Docker Compose adds Prometheus and Grafana, plus worker container access to the Docker socket for real task execution.
- Interactive CLI (`bin/m8s`) for `get`, `describe`, `run`, `top`, `logs`, completion, and namespace/service/job operations.
- Unit tests expanded for API flow, repository behavior, scheduler loop, client, config, and logging.
- Integration and scale smoke tests added under `tests/`.

## Run

```bash
go run ./cmd/scheduler
```

```bash
go run ./cmd/worker
```

CLI (requires scheduler running):

```bash
./bin/m8s --url http://localhost:8080 get nodes
```

Docker Compose stack (scheduler, workers, Prometheus, Grafana):

```bash
docker compose -f deployments/docker-compose.yml up
```

Optional worker/runtime env vars:

- `SCHEDULER_API_BASE_URL` (default `http://127.0.0.1:8080`)
- `STORE_BACKEND` (default `memory`, set to `postgres` for DB-backed mode)
- `POSTGRES_DSN` (used when `STORE_BACKEND=postgres`)
- `WORKER_NODE_ID` (default `worker-1`)
- `WORKER_TOTAL_CPU` (default `4`)
- `WORKER_TOTAL_MEMORY` (default `8192`)
- `HEARTBEAT_INTERVAL` (default `5s`)
- `TASK_EXECUTION_TIMEOUT` (default `5m`)
- `API_AUTH_TOKEN` (optional; enables API key auth)
- `API_RATE_LIMIT_RPM` (optional; per-client rate limit)
- `API_RATE_LIMIT_BURST` (optional; burst capacity)
- `SCHEDULER_AUTH_TOKEN` (optional; worker/CLI token for scheduler API)
- `M8S_AUTH_TOKEN` (optional; CLI token if API auth is enabled)

## Test

```bash
go test ./...
```

Highlighted suites:

- `tests/integration_api_scheduler_test.go`: API + scheduler integration flow
- `tests/scale_scheduler_test.go`: 50-node / 1000-task scheduler smoke test
- `tests/worker_execution_flow_test.go`: worker polling + status reporting flow
- `tests/postgres_integration_test.go`: Postgres integration flow (`TEST_POSTGRES_DSN`)

## Next

1. Add Kubernetes deployment manifests for scheduler + workers.
2. Add a Grafana dashboard and document key PromQL queries.
3. Extend Postgres integration test to run automatically in CI service containers.
