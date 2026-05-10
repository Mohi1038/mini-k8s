CREATE TABLE IF NOT EXISTS nodes (
  id TEXT PRIMARY KEY,
  total_cpu INT NOT NULL,
  used_cpu INT NOT NULL DEFAULT 0,
  total_memory INT NOT NULL,
  used_memory INT NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL,
  reliability DOUBLE PRECISION NOT NULL,
  heartbeat_count INT NOT NULL DEFAULT 0,
  success_count INT NOT NULL DEFAULT 0,
  failure_count INT NOT NULL DEFAULT 0,
  down_transitions INT NOT NULL DEFAULT 0,
  uptime_percent DOUBLE PRECISION NOT NULL DEFAULT 1,
  success_ratio DOUBLE PRECISION NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL DEFAULT 'default',
  idempotency_key TEXT,
  image TEXT NOT NULL,
  command TEXT NOT NULL DEFAULT '[]',
  secret_refs TEXT NOT NULL DEFAULT '[]',
  volumes TEXT NOT NULL DEFAULT '[]',
  cpu INT NOT NULL,
  memory INT NOT NULL,
  priority INT NOT NULL,
  status TEXT NOT NULL,
  node_id TEXT NOT NULL DEFAULT '',
  job_id TEXT NOT NULL DEFAULT '',
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scheduled_at TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL DEFAULT 'default',
  idempotency_key TEXT,
  image TEXT NOT NULL,
  command TEXT NOT NULL DEFAULT '[]',
  secret_refs TEXT NOT NULL DEFAULT '[]',
  volumes TEXT NOT NULL DEFAULT '[]',
  replicas INT NOT NULL,
  cpu INT NOT NULL,
  memory INT NOT NULL,
  priority INT NOT NULL,
  status TEXT NOT NULL,
  autoscale TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS namespaces (
  name TEXT PRIMARY KEY,
  cpu_quota INT NOT NULL DEFAULT 0,
  memory_quota INT NOT NULL DEFAULT 0,
  task_quota INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS services (
  name TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  selector_job_id TEXT NOT NULL DEFAULT '',
  port INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace, name)
);

CREATE TABLE IF NOT EXISTS secrets (
  name TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'default',
  data TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace, name)
);

CREATE TABLE IF NOT EXISTS scheduler_leases (
  name TEXT PRIMARY KEY,
  holder TEXT NOT NULL DEFAULT '',
  renew_time TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ 'epoch'
);

CREATE TABLE IF NOT EXISTS allocations (
  task_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  PRIMARY KEY (task_id, node_id)
);

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_node_id ON tasks(node_id);
CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_namespace ON tasks(namespace);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency ON tasks(namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);
CREATE INDEX IF NOT EXISTS idx_jobs_namespace ON jobs(namespace);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency ON jobs(namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_secrets_namespace ON secrets(namespace);
