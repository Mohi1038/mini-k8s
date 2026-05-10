Architecture Diagram
flowchart TD

Client[CLI / Dashboard / REST] --> API[API Gateway]

API --> Scheduler
API --> NodeManager

Scheduler --> DB[(Postgres)]
NodeManager --> DB

Scheduler --> Worker1[Worker Node]
Scheduler --> Worker2[Worker Node]
Scheduler --> WorkerN[Worker Node]

Worker1 --> DB
Worker2 --> DB
WorkerN --> DB

Worker1 --> Docker1[Docker Engine]
Worker2 --> Docker2[Docker Engine]

Worker1 --> Metrics
Worker2 --> Metrics

Metrics --> Dashboard


4. Task Scheduling Flow
sequenceDiagram

participant User
participant API
participant Scheduler
participant DB
participant Node
participant Docker

User->>API: Submit Task
API->>DB: Store Task (PENDING)

Scheduler->>DB: Fetch pending tasks
Scheduler->>DB: Fetch nodes

Scheduler->>Scheduler: Filter + Score nodes
Scheduler->>Node: Assign task

Node->>Docker: Run container
Node->>DB: Update status (RUNNING)

Docker->>Node: Complete task
Node->>DB: Update status (COMPLETED)

5. Failure Handling Flow
flowchart TD

Node -->|Heartbeat| Scheduler

Scheduler -->|Missed heartbeat| DetectFailure

DetectFailure --> MarkNodeDown
MarkNodeDown --> FetchTasks

FetchTasks --> RescheduleTasks
RescheduleTasks --> Scheduler

