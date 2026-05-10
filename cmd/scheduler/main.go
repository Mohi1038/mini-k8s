package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mini-k8ts/internal/api"
	"mini-k8ts/internal/config"
	"mini-k8ts/internal/db"
	"mini-k8ts/internal/scheduler"
	"mini-k8ts/pkg/logger"

	_ "github.com/lib/pq"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	log := logger.New(cfg.LogLevel, nil)
	if strings.EqualFold(cfg.Environment, "prod") && !strings.EqualFold(cfg.StoreBackend, "postgres") {
		log.Error("postgres backend is required in production", "store_backend", cfg.StoreBackend)
		os.Exit(1)
	}

	var taskRepo db.TaskRepository
	var nodeRepo db.NodeRepository
	var taskIDFunc func() string
	var jobRepo db.JobRepository
	var jobIDFunc func() string
	var namespaceRepo db.NamespaceRepository
	var serviceRepo db.ServiceRepository
	var secretRepo db.SecretRepository
	var leaderElector scheduler.LeaderElector

	if strings.EqualFold(cfg.StoreBackend, "postgres") {
		sqlDB, err := sql.Open("postgres", cfg.PostgresDSN)
		if err != nil {
			panic(err)
		}
		if err := sqlDB.Ping(); err != nil {
			panic(err)
		}
		if err := runPostgresMigrations(sqlDB); err != nil {
			panic(err)
		}

		store := db.NewPostgresStore(sqlDB, cfg.HeartbeatTimeout)
		taskRepo = store
		nodeRepo = store
		jobRepo = store
		namespaceRepo = store
		serviceRepo = store
		secretRepo = store
		taskIDFunc = func() string {
			return fmt.Sprintf("task-%d", time.Now().UTC().UnixNano())
		}
		jobIDFunc = func() string {
			return fmt.Sprintf("job-%d", time.Now().UTC().UnixNano())
		}
		leaderElector = scheduler.NewPostgresLeaderElector(sqlDB, "scheduler-control-plane", fmt.Sprintf("scheduler-%d", time.Now().UTC().UnixNano()), 10*time.Second)
		defer sqlDB.Close()
		log.Info("using postgres backend", "dsn", cfg.PostgresDSN)
	} else {
		store := db.NewMemoryStoreWithTimeout(cfg.HeartbeatTimeout)
		taskRepo = store
		nodeRepo = store
		jobRepo = store
		namespaceRepo = store
		serviceRepo = store
		secretRepo = store
		taskIDFunc = store.NextTaskID
		jobIDFunc = store.NextJobID
		log.Info("using memory backend")
	}

	apiServer := api.NewServerWithRepos(cfg.HTTPAddr, log, taskRepo, nodeRepo, jobRepo, namespaceRepo, serviceRepo, secretRepo, taskIDFunc, jobIDFunc, api.ServerOptions{
		AuthToken:      cfg.APIAuthToken,
		RateLimitRPM:   cfg.APIRateLimitRPM,
		RateLimitBurst: cfg.APIRateLimitBurst,
		AllowlistPaths: []string{"/healthz"},
	})
	schedulerService := scheduler.NewService(taskRepo, nodeRepo, jobRepo, namespaceRepo, scheduler.DefaultStrategy{}, log, cfg.SchedulerTickInterval, taskIDFunc)
	schedulerService.SetLeaderElector(leaderElector)
	schedulerService.SetStuckTaskTimeouts(cfg.PendingTaskTimeout, cfg.RunningTaskTimeout)

	schedulerCtx, schedulerCancel := context.WithCancel(context.Background())
	defer schedulerCancel()
	go schedulerService.Run(schedulerCtx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- apiServer.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Info("shutdown signal received", "signal", sig.String())
	case err = <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("server stopped with error", "error", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	schedulerCancel()
	if err := apiServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
}

func runPostgresMigrations(sqlDB *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS nodes (
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

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT 'default';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS job_id TEXT NOT NULL DEFAULT '';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS command TEXT NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS secret_refs TEXT NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS volumes TEXT NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT 'default';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS autoscale TEXT NOT NULL DEFAULT '{}';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS secret_refs TEXT NOT NULL DEFAULT '[]';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS volumes TEXT NOT NULL DEFAULT '[]';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS heartbeat_count INT NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS success_count INT NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS failure_count INT NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS down_transitions INT NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS uptime_percent DOUBLE PRECISION NOT NULL DEFAULT 1;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS success_ratio DOUBLE PRECISION NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_node_id ON tasks(node_id);
CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_namespace ON tasks(namespace);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency ON tasks(namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_namespace ON jobs(namespace);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency ON jobs(namespace, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_secrets_namespace ON secrets(namespace);
CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);`

	_, err := sqlDB.Exec(query)
	return err
}
