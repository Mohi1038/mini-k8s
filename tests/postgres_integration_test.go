package tests

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"mini-k8ts/internal/api"
	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
	"mini-k8ts/internal/scheduler"
	"mini-k8ts/pkg/logger"

	_ "github.com/lib/pq"
)

func TestPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping postgres integration test")
	}

	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres failed: %v", err)
	}
	defer sqlDB.Close()

	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("postgres ping failed: %v", err)
	}

	if _, err := sqlDB.Exec(`
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
CREATE INDEX IF NOT EXISTS idx_tasks_namespace ON tasks(namespace);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency ON tasks(namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
TRUNCATE TABLE tasks;
TRUNCATE TABLE nodes;
`); err != nil {
		t.Fatalf("migration/setup failed: %v", err)
	}

	store := db.NewPostgresStore(sqlDB, 2*time.Second)
	server := api.NewServerWithRepos(":0", logger.New("debug", bytes.NewBuffer(nil)), store, store, store, store, store, store, func() string {
		return "pg-task-1"
	}, func() string {
		return "pg-job-1"
	}, api.DefaultServerOptions())

	_, err = store.Register(context.Background(), models.Node{ID: "pg-node-1", TotalCPU: 8, TotalMemory: 16000, Reliability: 0.8})
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	_, err = store.Create(context.Background(), models.Task{ID: "pg-task-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusPending})
	if err != nil {
		t.Fatalf("create task failed: %v", err)
	}

	schedulerSvc := scheduler.NewService(store, store, store, store, scheduler.DefaultStrategy{}, logger.New("debug", bytes.NewBuffer(nil)), 0, func() string {
		return "pg-task-1"
	})
	if err := schedulerSvc.ScheduleOnce(context.Background()); err != nil {
		t.Fatalf("schedule once failed: %v", err)
	}

	tasks, err := store.ListTasks(context.Background())
	if err != nil {
		t.Fatalf("list tasks failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Status != models.TaskStatusRunning {
		t.Fatalf("expected RUNNING task, got %s", tasks[0].Status)
	}

	_ = server
}
