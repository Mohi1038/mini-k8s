#  mini-k8s

> Distributed mini-kubernetes style scheduler prototype with reliability-aware scheduling, self-healing, and full observability.

**Distributed Task Scheduler** | **Fault Tolerant** | **Observable** | **Production Ready**

---

## Core Goals

| Goal | Status | Details |
|------|--------|---------|
| API Gateway | ✅ Complete | RESTful API for task/cluster interactions |
| Smart Scheduler | ✅ Complete | Filter/score/bind pipeline with reliability awareness |
| Heartbeat System | ✅ Complete | Worker health checks & dynamic reliability tracking |
| Data Persistence | ✅ Complete | Postgres-backed state store with migrations |
| Observability | ✅ Complete | Prometheus metrics & Grafana dashboards |
| Self-Healing | ✅ Complete | Automatic task retry & failure recovery |
| Container Runtime | ✅ Complete | Docker-native task execution on workers |

## System Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        CLI["CLI\n(m8s command)"]
        Dashboard["Dashboard\n(Web UI)"]
    end
    
    subgraph "Control Plane"
        API["API Server\n(Port 8080)"]
        Scheduler["Scheduler\n(Filter/Score)"]
        Leader["Leader Elector\n(HA Control)"]
    end
    
    subgraph "Data Layer"
        Memory["Memory Store\n(Dev/Test)"]
        Postgres["Postgres\n(Production)"]
        Cache["Allocation Cache"]
    end
    
    subgraph "Worker Nodes"
        W1["Worker-1\n(Docker Runtime)"]
        W2["Worker-2\n(Docker Runtime)"]
        WN["Worker-N\n(Docker Runtime)"]
    end
    
    subgraph "Observability"
        Prom["Prometheus\n(Metrics)"]
        Grafana["Grafana\n(Dashboards)"]
    end
    
    CLI -->|REST| API
    Dashboard -->|REST| API
    API -->|Fetch State| Memory
    API -->|Fetch State| Postgres
    Scheduler -->|Read/Write| Memory
    Scheduler -->|Read/Write| Postgres
    Scheduler -->|Update Allocations| Cache
    Leader -->|Lease Mgmt| Postgres
    API -->|Assign Tasks| W1
    API -->|Assign Tasks| W2
    API -->|Assign Tasks| WN
    W1 -->|Heartbeat| API
    W2 -->|Heartbeat| API
    WN -->|Heartbeat| API
    API -->|Scrape| Prom
    Grafana -->|Query| Prom
```

---

## Task Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING: Task Submitted
    
    PENDING --> PENDING: Awaiting Slot
    PENDING --> RUNNING: Scheduler Assigns\n(Filter → Score → Bind)
    PENDING --> FAILED: Max Retries Exceeded
    
    RUNNING --> COMPLETED: Container Exits (0)
    RUNNING --> FAILED: Container Error\nor Node Down
    RUNNING --> PENDING: Stuck Timeout\nOr Node Recovery
    
    FAILED --> PENDING: Self-Heal Retry\n(if attempts < max)
    FAILED --> [*]: Terminal
    
    COMPLETED --> [*]: Terminal
    
    note right of PENDING
        - Fair order by priority
        - Namespace quota checks
        - NextAttemptAt backoff
    end note
    
    note right of RUNNING
        - Per-node resource tracking
        - Shadow map prevents over-schedule
        - Heartbeat-based health detection
    end note
    
    note right of FAILED
        - Exponential backoff
        - Max 3 attempts by default
        - Requeue with reason
    end note
```

---

## Scheduler Pipeline

```mermaid
graph LR
    A["Fetch Pending\nTasks"] -->|Sort by Priority| B["Filter\nNodes"]
    B -->|Remove Unfit| C["Score\nNodes"]
    C -->|Rank by Fitness| D["Bind\nTask"]
    D -->|Update Status| E["Allocated"]
    
    B -->|No Fit| F["Backoff\nNext Attempt"]
    C -->|Low Score| F
    
    style A fill:#e1f5ff,color:#000000
    style B fill:#fff3e0,color:#000000
    style C fill:#fce4ec,color:#000000
    style D fill:#c8e6c9,color:#000000
    style E fill:#a5d6a7,color:#000000
    style F fill:#ffcdd2,color:#000000
```

---

## Feature Matrix

| Feature | Memory | Postgres | Worker | CLI |
|---------|--------|----------|--------|-----|
| **Task Management** | ✅ | ✅ | ✅ | ✅ |
| **Node Registration** | ✅ | ✅ | ✅ | ✅ |
| **Scheduling** | ✅ | ✅ | ✅ | - |
| **Failure Recovery** | ✅ | ✅ | ✅ | - |
| **Leader Election** | - | ✅ | - | - |
| **Idempotency Keys** | ✅ | ✅ | - | ✅ |
| **Secrets Management** | ✅ | ✅ | ✅ | ✅ |
| **Namespaces** | ✅ | ✅ | ✅ | ✅ |
| **Services** | ✅ | ✅ | - | ✅ |
| **Docker Execution** | - | - | ✅ | - |
| **Prometheus Metrics** | ✅ | ✅ | - | - |
| **Health Tracking** | ✅ | ✅ | ✅ | ✅ |

---

## Quick Start

### Start Scheduler (Control Plane)

```bash
# Using in-memory store (default)
go run ./cmd/scheduler

# Using Postgres backend
STORE_BACKEND=postgres POSTGRES_DSN="..." go run ./cmd/scheduler
```

### Start Worker Nodes

```bash
# Terminal 1
WORKER_NODE_ID=worker-1 go run ./cmd/worker

# Terminal 2
WORKER_NODE_ID=worker-2 go run ./cmd/worker
```

### Use CLI

```bash
# List nodes
./bin/m8s --url http://localhost:8080 get nodes

# Submit a task
./bin/m8s run nginx:latest --cpu 1 --memory 128

# View cluster state
./bin/m8s get tasks
./bin/m8s get jobs
```

### Docker Compose Stack (with Prometheus + Grafana)

```bash
docker compose -f deployments/docker-compose.yml up
# Access Grafana at http://localhost:3000
# Access Prometheus at http://localhost:9090
```

---

## API Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `POST` | `/tasks` | Submit a new task |
| `GET` | `/tasks/{taskID}` | Get task details |
| `POST` | `/tasks/{taskID}/status` | Update task status (worker callback) |
| `GET` | `/tasks/{taskID}/logs` | Stream task logs |
| `GET` | `/cluster` | Get all nodes and tasks |
| `GET` | `/metrics` | JSON metrics snapshot |
| `GET` | `/metrics/prometheus` | Prometheus OpenMetrics format |
| `POST` | `/nodes/register` | Register a worker node |
| `POST` | `/nodes/heartbeat` | Send node heartbeat (with resource usage) |
| `GET` | `/nodes/{nodeID}/tasks/running` | Get tasks for this node |
| `POST` | `/jobs` | Create a job (multi-replica task) |
| `GET` | `/jobs` | List all jobs |
| `POST` | `/namespaces` | Create a namespace (quota boundary) |
| `GET` | `/namespaces` | List all namespaces |
| `POST` | `/secrets` | Create/update a secret |
| `GET` | `/secrets` | List secrets in namespace |
| `POST` | `/services` | Create a service endpoint |
| `GET` | `/services` | List all services |
| `GET` | `/services/{name}/endpoints` | Resolve service to healthy backends |
| `GET` | `/insights` | Get cluster health insights |
| `GET` | `/healthz` | Liveness check |

---

## Project Structure

| Path | Purpose |
|------|---------|
| `cmd/scheduler/` | Control plane entry point |
| `cmd/worker/` | Worker node entry point |
| `cmd/cli/` | Interactive CLI tool (`bin/m8s`) |
| `internal/api/` | HTTP server, routes, middleware |
| `internal/scheduler/` | Filtering, scoring, leader election |
| `internal/db/` | Repository interfaces, in-memory, Postgres |
| `internal/metrics/` | Metrics engine & Prometheus collector |
| `internal/models/` | Domain models (Task, Node, Job, etc.) |
| `internal/config/` | Environment variable loader |
| `internal/task/` | Docker container runner |
| `pkg/client/` | Scheduler API client library |
| `pkg/logger/` | Structured JSON logging |
| `deployments/` | Docker Compose, Kubernetes manifests |
| `tests/` | Integration & scale tests |

---

## Configuration

### Scheduler Environment Variables

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `HTTP_ADDR` | `:8080` | string | API server listen address |
| `LOG_LEVEL` | `info` | string | Log level: `debug`, `info`, `warn`, `error` |
| `STORE_BACKEND` | `memory` | string | `memory` for dev, `postgres` for prod |
| `POSTGRES_DSN` | - | string | PostgreSQL connection string |
| `SCHEDULER_TICK_INTERVAL` | `2s` | duration | How often to run reconciliation loops |
| `HEARTBEAT_TIMEOUT` | `15s` | duration | Idle timeout before marking node DOWN |
| `PENDING_TASK_TIMEOUT` | `10m` | duration | Max time for task in PENDING before re-check |
| `RUNNING_TASK_TIMEOUT` | `20m` | duration | Max time for task in RUNNING before restart |
| `API_RATE_LIMIT_RPM` | `0` | int | Requests per minute per client (0 = unlimited) |
| `API_RATE_LIMIT_BURST` | `10` | int | Burst capacity for rate limiter |
| `API_AUTH_TOKEN` | - | string | Optional API key for authentication |

### Worker Environment Variables

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `WORKER_NODE_ID` | `worker-1` | string | Unique worker node identifier |
| `WORKER_TOTAL_CPU` | `4` | int | Total CPU cores available |
| `WORKER_TOTAL_MEMORY` | `8192` | int | Total memory (MB) available |
| `SCHEDULER_API_BASE_URL` | `http://127.0.0.1:8080` | string | Scheduler API endpoint |
| `SCHEDULER_AUTH_TOKEN` | - | string | Auth token for scheduler API |
| `HEARTBEAT_INTERVAL` | `5s` | duration | Frequency of heartbeat sends |
| `TASK_EXECUTION_TIMEOUT` | `5m` | duration | Max execution time per task |
| `HTTP_ADDR` | `:8081` | string | Worker's internal API (logs) |
| `LOG_LEVEL` | `info` | string | Log level |

### CLI Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `M8S_AUTH_TOKEN` | - | Auth token for scheduler API |

---

## Testing

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run specific test suite
go test -v ./tests/

# Integration tests (requires Postgres)
TEST_POSTGRES_DSN="postgres://user:pass@localhost/testdb" go test -v ./tests/
```

### Key Test Suites

| Test | File | Coverage |
|------|------|----------|
| **API Integration** | `tests/integration_api_scheduler_test.go` | API + scheduler workflow |
| **Scale Smoke Test** | `tests/scale_scheduler_test.go` | 50 nodes, 1000 tasks |
| **Worker Flow** | `tests/worker_execution_flow_test.go` | Task execution & polling |
| **Postgres Integration** | `tests/postgres_integration_test.go` | Database persistence |

---

## Metrics & Observability

### Prometheus Metrics

Access at: `GET /metrics/prometheus`

Key metrics:
- `mini_k8ts_tasks_total{status="..."}` - Task count by status
- `mini_k8ts_nodes_total{status="..."}` - Node count by status
- `mini_k8ts_cluster_cpu_total` - Total cluster CPU cores
- `mini_k8ts_cluster_memory_total` - Total cluster memory (MB)
- `mini_k8ts_node_reliability{node_id="..."}` - Node reliability score
- `mini_k8ts_node_uptime_percent{node_id="..."}` - Node uptime %
- `mini_k8ts_task_latency_ms` - Avg task completion time
- `mini_k8ts_scheduling_latency_ms` - Avg time to schedule

### Grafana Dashboards

- Pre-built dashboards in `deployments/grafana-dashboard.json`
- Real-time cluster status, node health, task distribution
- Resource utilization trends
- Reliability scoring visualization

---

## Reliability & Failure Detection

### Node Reliability Scoring

```mermaid
graph TD
    A["Node Heartbeat"] -->|Success| B["↑ Reliability +0.1"]
    A -->|Timeout| C["↓ Reliability -0.25"]
    
    B --> D["Task Success"]
    D -->|+1| E["Adjust Success Ratio"]
    
    C --> F["Mark DOWN"]
    F -->|Requeue Tasks| G["Self-Healing"]
    
    E -->|Affects Future\nScheduling| H["Filter/Score"]
    
    style B fill:#c8e6c9,color:#000000
    style C fill:#ffcdd2,color:#000000
    style G fill:#fff9c4,color:#000000
    style H fill:#e1f5ff,color:#000000
```

**Scoring Factors:**
- **Reliability** (40%): Direct heartbeat success history
- **Uptime** (25%): Percentage of time in READY state
- **Success Ratio** (25%): Task success / total task executions
- **Churn Penalty** (-10%): Down-transition frequency

---

## Worker Reconciliation Loop

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryTextColor':'#000000', 'primaryBorderColor':'#000000', 'fontSize':'14px'}}}%%
sequenceDiagram
    participant W as Worker
    participant API as Control Plane
    participant Docker as Docker
    
    rect rgb(200, 220, 240)
    W->>API: Send Heartbeat (CPU, Mem)
    API->>W: Here are your tasks
    end
    
    rect rgb(240, 200, 200)
    W->>Docker: Check running containers
    Docker-->>W: List of managed tasks
    end
    
    rect rgb(200, 240, 200)
    W->>W: Reconcile expected vs actual
    alt Orphaned container
        W->>Docker: Stop & remove
    else Exited container
        W->>API: Report final status
    else New task
        W->>Docker: Pull image, create, start
    end
    end
    
    rect rgb(240, 240, 200)
    W->>Docker: Await task completion
    Docker-->>W: Exit code & logs
    W->>API: POST /tasks/{id}/status
    end
```

---

## Deployment Topology

### Single-Node Development

```
┌─────────────────────────────────────┐
│  Localhost                          │
├─────────────────────────────────────┤
│ Scheduler + API        (port 8080)  │
│ Worker-1              (port 8081)  │
│ Memory Store (in-process)           │
└─────────────────────────────────────┘
```

### Multi-Node Production

```
┌──────────────────────────────────────────────────┐
│ Scheduler Cluster (HA)                           │
├──────────────────────────────────────────────────┤
│  API  │  Scheduler  │  Leader Elector           │
│  [LB] │  (3 replicas, leader-based)             │
└──────────┬───────────────────────────────────────┘
           │
     Postgres DB
     (Shared State)
           │
    ┌──────┴──────┬──────────┬──────────┐
    │             │          │          │
┌───▼──┐      ┌──▼──┐    ┌──▼──┐   ┌──▼──┐
│ W-1  │      │ W-2 │    │ W-3 │   │ W-N │
├──────┤      ├─────┤    ├─────┤   ├─────┤
│Docker│      │Docker    │Docker    │Docker
│ Task │      │ Task     │ Task     │ Task
│Exec  │      │ Exec     │ Exec     │ Exec
└──────┘      └─────┘    └─────┘   └─────┘

Prometheus ←─── Metrics Scrape
Grafana   ←───── Query Prometheus
```

---

## Development Roadmap

| Phase | Items | Status |
|-------|-------|--------|
| **Core** | Scheduling, heartbeats, API | ✅ Complete |
| **Reliability** | Failure recovery, health tracking | ✅ Complete |
| **Data** | Postgres, migrations, idempotency | ✅ Complete |
| **Observability** | Prometheus, Grafana | ✅ Complete |
| **Runtime** | Docker execution, task logs | ✅ Complete |
| **Tools** | CLI, completion, interactive mode | ✅ Complete |
| **Advanced** | Auto-scaling, multi-region, policy engine | Planned |

---

## Documentation

- **[Architecture Notes](docs/mini_k8ts_HLD.txt)** - High-level design
- **[LLD](docs/mini_k8ts_LLD.txt)** - Low-level design details
- **[Diagrams](docs/Diagrams.md)** - Visual architecture diagrams
- **[K8s Setup](deployments/k8s/README.md)** - Kubernetes manifests guide

---

## Contributing

Contributions welcome! Key areas:
- Performance optimizations
- Additional scheduling strategies
- Enhanced CLI commands
- Kubernetes operator
- Additional observability integrations

---

## License

MIT License - See LICENSE file for details

---

**Made with love as a distributed systems learning project**
