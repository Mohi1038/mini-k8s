package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"mini-k8ts/internal/models"
)

type PostgresStore struct {
	db              *sql.DB
	inactiveTimeout time.Duration
}

func NewPostgresStore(db *sql.DB, inactiveTimeout time.Duration) *PostgresStore {
	if inactiveTimeout <= 0 {
		inactiveTimeout = 15 * time.Second
	}
	return &PostgresStore{db: db, inactiveTimeout: inactiveTimeout}
}

func (p *PostgresStore) Create(ctx context.Context, task models.Task) (models.Task, error) {
	now := time.Now().UTC()
	if task.MaxAttempts <= 0 {
		task.MaxAttempts = 3
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	task.UpdatedAt = now
	if task.NextAttemptAt.IsZero() {
		task.NextAttemptAt = now
	}
	commandJSON, err := marshalCommand(task.Command)
	if err != nil {
		return models.Task{}, err
	}
	secretRefsJSON, err := marshalSecretRefs(task.SecretRefs)
	if err != nil {
		return models.Task{}, err
	}
	volumesJSON, err := marshalVolumes(task.Volumes)
	if err != nil {
		return models.Task{}, err
	}

	_, err = p.db.ExecContext(
		ctx,
		`INSERT INTO tasks (id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		 ON CONFLICT (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		task.ID,
		defaultNamespace(task.Namespace),
		task.Image,
		commandJSON,
		secretRefsJSON,
		volumesJSON,
		task.CPU,
		task.Memory,
		task.Priority,
		string(task.Status),
		task.NodeID,
		task.JobID,
		nullString(task.IdempotencyKey),
		task.Attempts,
		task.MaxAttempts,
		task.NextAttemptAt,
		nullTime(task.ScheduledAt),
		task.LastError,
		task.CreatedAt,
		task.UpdatedAt,
	)
	if err != nil {
		return models.Task{}, err
	}
	if task.IdempotencyKey != "" {
		existing, err := p.getTaskByIdempotency(ctx, task.Namespace, task.IdempotencyKey)
		if err == nil {
			return existing, nil
		}
	}
	return task, nil
}

func (p *PostgresStore) GetTask(ctx context.Context, taskID string) (models.Task, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at FROM tasks WHERE id = $1`, taskID)
	task, err := scanTaskRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Task{}, ErrTaskNotFound
		}
		return models.Task{}, err
	}
	return task, nil
}

func (p *PostgresStore) getTaskByIdempotency(ctx context.Context, namespace string, key string) (models.Task, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at FROM tasks WHERE namespace = $1 AND idempotency_key = $2`, defaultNamespace(namespace), strings.TrimSpace(key))
	return scanTaskRow(row)
}

func (p *PostgresStore) ListTasks(ctx context.Context) ([]models.Task, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at FROM tasks ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Task, 0)
	for rows.Next() {
		task, err := scanTaskRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}

	return result, rows.Err()
}

func (p *PostgresStore) ListPending(ctx context.Context) ([]models.Task, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at FROM tasks WHERE status = $1 ORDER BY id`, string(models.TaskStatusPending))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Task, 0)
	for rows.Next() {
		task, err := scanTaskRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}

	return result, rows.Err()
}

func (p *PostgresStore) ListTasksByNodeAndStatus(ctx context.Context, nodeID string, status models.TaskStatus) ([]models.Task, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at FROM tasks WHERE node_id = $1 AND status = $2 ORDER BY id`, nodeID, string(status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Task, 0)
	for rows.Next() {
		task, err := scanTaskRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

func (p *PostgresStore) ListTasksByJob(ctx context.Context, jobID string) ([]models.Task, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, cpu, memory, priority, status, node_id, job_id, idempotency_key, attempts, max_attempts, next_attempt_at, scheduled_at, last_error, created_at, updated_at FROM tasks WHERE job_id = $1 ORDER BY id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Task, 0)
	for rows.Next() {
		task, err := scanTaskRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

func (p *PostgresStore) UpdateStatus(ctx context.Context, taskID string, status models.TaskStatus, nodeID string) error {
	task, err := p.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if !isTransitionAllowed(task.Status, status) {
		return ErrInvalidStatusTransition
	}

	now := time.Now().UTC()
	result, err := p.db.ExecContext(ctx, `UPDATE tasks SET status = $1, node_id = $2, updated_at = $3,
         scheduled_at = CASE WHEN $1 = $5 AND scheduled_at IS NULL THEN $3 ELSE scheduled_at END
         WHERE id = $4`, string(status), nodeID, now, taskID, string(models.TaskStatusRunning))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrTaskNotFound
	}
	return nil
}

func (p *PostgresStore) UpdateTask(ctx context.Context, task models.Task) error {
	commandJSON, err := marshalCommand(task.Command)
	if err != nil {
		return err
	}
	secretRefsJSON, err := marshalSecretRefs(task.SecretRefs)
	if err != nil {
		return err
	}
	volumesJSON, err := marshalVolumes(task.Volumes)
	if err != nil {
		return err
	}
	result, err := p.db.ExecContext(
		ctx,
		`UPDATE tasks SET namespace = $1, image = $2, command = $3, secret_refs = $4, volumes = $5, cpu = $6, memory = $7, priority = $8, status = $9, node_id = $10, job_id = $11,
		 idempotency_key = $12, attempts = $13, max_attempts = $14, next_attempt_at = $15, scheduled_at = $16, last_error = $17, created_at = $18, updated_at = $19
		 WHERE id = $20`,
		defaultNamespace(task.Namespace),
		task.Image,
		commandJSON,
		secretRefsJSON,
		volumesJSON,
		task.CPU,
		task.Memory,
		task.Priority,
		string(task.Status),
		task.NodeID,
		task.JobID,
		nullString(task.IdempotencyKey),
		task.Attempts,
		task.MaxAttempts,
		task.NextAttemptAt,
		nullTime(task.ScheduledAt),
		task.LastError,
		task.CreatedAt,
		time.Now().UTC(),
		task.ID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrTaskNotFound
	}
	return nil
}

func (p *PostgresStore) CreateJob(ctx context.Context, job models.Job) (models.Job, error) {
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.Status == "" {
		job.Status = models.JobStatusPending
	}
	job.UpdatedAt = now
	commandJSON, err := marshalCommand(job.Command)
	if err != nil {
		return models.Job{}, err
	}

	autoscaleJSON, err := marshalAutoscale(job.Autoscale)
	if err != nil {
		return models.Job{}, err
	}
	secretRefsJSON, err := marshalSecretRefs(job.SecretRefs)
	if err != nil {
		return models.Job{}, err
	}
	volumesJSON, err := marshalVolumes(job.Volumes)
	if err != nil {
		return models.Job{}, err
	}

	_, err = p.db.ExecContext(
		ctx,
		`INSERT INTO jobs (id, namespace, image, command, secret_refs, volumes, replicas, cpu, memory, priority, status, autoscale, idempotency_key, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		 ON CONFLICT (namespace, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		job.ID,
		defaultNamespace(job.Namespace),
		job.Image,
		commandJSON,
		secretRefsJSON,
		volumesJSON,
		job.Replicas,
		job.CPU,
		job.Memory,
		job.Priority,
		string(job.Status),
		autoscaleJSON,
		nullString(job.IdempotencyKey),
		job.CreatedAt,
		job.UpdatedAt,
	)
	if err != nil {
		return models.Job{}, err
	}
	if job.IdempotencyKey != "" {
		existing, err := p.getJobByIdempotency(ctx, job.Namespace, job.IdempotencyKey)
		if err == nil {
			return existing, nil
		}
	}
	return job, nil
}

func (p *PostgresStore) GetJob(ctx context.Context, jobID string) (models.Job, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, replicas, cpu, memory, priority, status, autoscale, idempotency_key, created_at, updated_at FROM jobs WHERE id = $1`, jobID)
	job, err := scanJobRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Job{}, ErrJobNotFound
		}
		return models.Job{}, err
	}
	return job, nil
}

func (p *PostgresStore) getJobByIdempotency(ctx context.Context, namespace string, key string) (models.Job, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, replicas, cpu, memory, priority, status, autoscale, idempotency_key, created_at, updated_at FROM jobs WHERE namespace = $1 AND idempotency_key = $2`, defaultNamespace(namespace), strings.TrimSpace(key))
	return scanJobRow(row)
}

func (p *PostgresStore) ListJobs(ctx context.Context) ([]models.Job, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, namespace, image, command, secret_refs, volumes, replicas, cpu, memory, priority, status, autoscale, idempotency_key, created_at, updated_at FROM jobs ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Job, 0)
	for rows.Next() {
		job, err := scanJobRows(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}

	return result, rows.Err()
}

func (p *PostgresStore) UpdateJob(ctx context.Context, job models.Job) error {
	commandJSON, err := marshalCommand(job.Command)
	if err != nil {
		return err
	}
	autoscaleJSON, err := marshalAutoscale(job.Autoscale)
	if err != nil {
		return err
	}
	secretRefsJSON, err := marshalSecretRefs(job.SecretRefs)
	if err != nil {
		return err
	}
	volumesJSON, err := marshalVolumes(job.Volumes)
	if err != nil {
		return err
	}

	result, err := p.db.ExecContext(
		ctx,
		`UPDATE jobs SET namespace = $1, image = $2, command = $3, secret_refs = $4, volumes = $5, replicas = $6, cpu = $7, memory = $8, priority = $9, status = $10, autoscale = $11, idempotency_key = $12, updated_at = $13
		 WHERE id = $14`,
		defaultNamespace(job.Namespace),
		job.Image,
		commandJSON,
		secretRefsJSON,
		volumesJSON,
		job.Replicas,
		job.CPU,
		job.Memory,
		job.Priority,
		string(job.Status),
		autoscaleJSON,
		nullString(job.IdempotencyKey),
		time.Now().UTC(),
		job.ID,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrJobNotFound
	}
	return nil
}

func (p *PostgresStore) CreateNamespace(ctx context.Context, ns models.Namespace) (models.Namespace, error) {
	name := strings.TrimSpace(ns.Name)
	if name == "" {
		return models.Namespace{}, ErrNamespaceNotFound
	}
	now := time.Now().UTC()
	if ns.CreatedAt.IsZero() {
		ns.CreatedAt = now
	}
	ns.UpdatedAt = now
	ns.Name = name

	_, err := p.db.ExecContext(
		ctx,
		`INSERT INTO namespaces (name, cpu_quota, memory_quota, task_quota, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (name) DO UPDATE SET
		  cpu_quota = EXCLUDED.cpu_quota,
		  memory_quota = EXCLUDED.memory_quota,
		  task_quota = EXCLUDED.task_quota,
		  updated_at = EXCLUDED.updated_at`,
		ns.Name,
		ns.CPUQuota,
		ns.MemoryQuota,
		ns.TaskQuota,
		ns.CreatedAt,
		ns.UpdatedAt,
	)
	if err != nil {
		return models.Namespace{}, err
	}
	return ns, nil
}

func (p *PostgresStore) GetNamespace(ctx context.Context, name string) (models.Namespace, error) {
	row := p.db.QueryRowContext(ctx, `SELECT name, cpu_quota, memory_quota, task_quota, created_at, updated_at FROM namespaces WHERE name = $1`, strings.TrimSpace(name))
	var ns models.Namespace
	if err := row.Scan(&ns.Name, &ns.CPUQuota, &ns.MemoryQuota, &ns.TaskQuota, &ns.CreatedAt, &ns.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Namespace{}, ErrNamespaceNotFound
		}
		return models.Namespace{}, err
	}
	return ns, nil
}

func (p *PostgresStore) ListNamespaces(ctx context.Context) ([]models.Namespace, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT name, cpu_quota, memory_quota, task_quota, created_at, updated_at FROM namespaces ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Namespace, 0)
	for rows.Next() {
		var ns models.Namespace
		if err := rows.Scan(&ns.Name, &ns.CPUQuota, &ns.MemoryQuota, &ns.TaskQuota, &ns.CreatedAt, &ns.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, ns)
	}
	return result, rows.Err()
}

func (p *PostgresStore) UpdateNamespace(ctx context.Context, ns models.Namespace) error {
	name := strings.TrimSpace(ns.Name)
	if name == "" {
		return ErrNamespaceNotFound
	}
	result, err := p.db.ExecContext(
		ctx,
		`UPDATE namespaces SET cpu_quota = $1, memory_quota = $2, task_quota = $3, updated_at = $4 WHERE name = $5`,
		ns.CPUQuota,
		ns.MemoryQuota,
		ns.TaskQuota,
		time.Now().UTC(),
		name,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNamespaceNotFound
	}
	return nil
}

func (p *PostgresStore) CreateService(ctx context.Context, svc models.Service) (models.Service, error) {
	name := strings.TrimSpace(svc.Name)
	if name == "" {
		return models.Service{}, ErrServiceNotFound
	}
	if svc.Namespace == "" {
		svc.Namespace = "default"
	}
	now := time.Now().UTC()
	if svc.CreatedAt.IsZero() {
		svc.CreatedAt = now
	}
	svc.UpdatedAt = now

	_, err := p.db.ExecContext(
		ctx,
		`INSERT INTO services (name, namespace, selector_job_id, port, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (namespace, name) DO UPDATE SET
		  selector_job_id = EXCLUDED.selector_job_id,
		  port = EXCLUDED.port,
		  updated_at = EXCLUDED.updated_at`,
		name,
		svc.Namespace,
		svc.SelectorJobID,
		svc.Port,
		svc.CreatedAt,
		svc.UpdatedAt,
	)
	if err != nil {
		return models.Service{}, err
	}
	return svc, nil
}

func (p *PostgresStore) GetService(ctx context.Context, namespace string, name string) (models.Service, error) {
	row := p.db.QueryRowContext(ctx, `SELECT name, namespace, selector_job_id, port, created_at, updated_at FROM services WHERE namespace = $1 AND name = $2`, defaultNamespace(namespace), strings.TrimSpace(name))
	var svc models.Service
	if err := row.Scan(&svc.Name, &svc.Namespace, &svc.SelectorJobID, &svc.Port, &svc.CreatedAt, &svc.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Service{}, ErrServiceNotFound
		}
		return models.Service{}, err
	}
	return svc, nil
}

func (p *PostgresStore) ListServices(ctx context.Context) ([]models.Service, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT name, namespace, selector_job_id, port, created_at, updated_at FROM services ORDER BY namespace, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Service, 0)
	for rows.Next() {
		var svc models.Service
		if err := rows.Scan(&svc.Name, &svc.Namespace, &svc.SelectorJobID, &svc.Port, &svc.CreatedAt, &svc.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, svc)
	}
	return result, rows.Err()
}

func (p *PostgresStore) UpdateService(ctx context.Context, svc models.Service) error {
	name := strings.TrimSpace(svc.Name)
	if name == "" {
		return ErrServiceNotFound
	}
	if svc.Namespace == "" {
		svc.Namespace = "default"
	}
	result, err := p.db.ExecContext(
		ctx,
		`UPDATE services SET selector_job_id = $1, port = $2, updated_at = $3 WHERE namespace = $4 AND name = $5`,
		svc.SelectorJobID,
		svc.Port,
		time.Now().UTC(),
		svc.Namespace,
		name,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrServiceNotFound
	}
	return nil
}

func (p *PostgresStore) CreateSecret(ctx context.Context, secret models.Secret) (models.Secret, error) {
	name := strings.TrimSpace(secret.Name)
	if name == "" {
		return models.Secret{}, ErrSecretNotFound
	}
	secret.Name = name
	secret.Namespace = defaultNamespace(secret.Namespace)
	now := time.Now().UTC()
	if secret.CreatedAt.IsZero() {
		secret.CreatedAt = now
	}
	secret.UpdatedAt = now
	payload, err := json.Marshal(secret.Data)
	if err != nil {
		return models.Secret{}, err
	}
	_, err = p.db.ExecContext(ctx, `INSERT INTO secrets (name, namespace, data, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (namespace, name) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at`,
		secret.Name, secret.Namespace, string(payload), secret.CreatedAt, secret.UpdatedAt)
	if err != nil {
		return models.Secret{}, err
	}
	return secret, nil
}

func (p *PostgresStore) GetSecret(ctx context.Context, namespace string, name string) (models.Secret, error) {
	row := p.db.QueryRowContext(ctx, `SELECT name, namespace, data, created_at, updated_at FROM secrets WHERE namespace = $1 AND name = $2`, defaultNamespace(namespace), strings.TrimSpace(name))
	var secret models.Secret
	var raw string
	if err := row.Scan(&secret.Name, &secret.Namespace, &raw, &secret.CreatedAt, &secret.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Secret{}, ErrSecretNotFound
		}
		return models.Secret{}, err
	}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &secret.Data); err != nil {
			return models.Secret{}, err
		}
	}
	return secret, nil
}

func (p *PostgresStore) ListSecrets(ctx context.Context, namespace string) ([]models.Secret, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT name, namespace, data, created_at, updated_at FROM secrets WHERE namespace = $1 ORDER BY name`, defaultNamespace(namespace))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Secret, 0)
	for rows.Next() {
		var secret models.Secret
		var raw string
		if err := rows.Scan(&secret.Name, &secret.Namespace, &raw, &secret.CreatedAt, &secret.UpdatedAt); err != nil {
			return nil, err
		}
		if raw != "" {
			if err := json.Unmarshal([]byte(raw), &secret.Data); err != nil {
				return nil, err
			}
		}
		result = append(result, secret)
	}
	return result, rows.Err()
}

func (p *PostgresStore) Register(ctx context.Context, node models.Node) (models.Node, error) {
	now := time.Now().UTC()
	if node.Reliability <= 0 {
		node.Reliability = 0.5
	}
	node.LastSeen = now
	node.Status = models.NodeStatusReady
	node.UptimePercent = computeUptimePercent(node.HeartbeatCount, node.DownTransitions)
	node.SuccessRatio = computeSuccessRatio(node.SuccessCount, node.FailureCount)
	node.Reliability = adaptiveReliability(node.Reliability, node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)

	_, err := p.db.ExecContext(
		ctx,
		`INSERT INTO nodes (id, total_cpu, used_cpu, total_memory, used_memory, status, last_seen, reliability, heartbeat_count, success_count, failure_count, down_transitions, uptime_percent, success_ratio)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		 ON CONFLICT (id) DO UPDATE SET
		   total_cpu = EXCLUDED.total_cpu,
		   total_memory = EXCLUDED.total_memory,
		   status = EXCLUDED.status,
		   last_seen = EXCLUDED.last_seen,
		   reliability = EXCLUDED.reliability,
		   heartbeat_count = EXCLUDED.heartbeat_count,
		   success_count = EXCLUDED.success_count,
		   failure_count = EXCLUDED.failure_count,
		   down_transitions = EXCLUDED.down_transitions,
		   uptime_percent = EXCLUDED.uptime_percent,
		   success_ratio = EXCLUDED.success_ratio`,
		node.ID,
		node.TotalCPU,
		node.UsedCPU,
		node.TotalMemory,
		node.UsedMemory,
		string(node.Status),
		node.LastSeen,
		node.Reliability,
		node.HeartbeatCount,
		node.SuccessCount,
		node.FailureCount,
		node.DownTransitions,
		node.UptimePercent,
		node.SuccessRatio,
	)
	if err != nil {
		return models.Node{}, err
	}
	return node, nil
}

func (p *PostgresStore) Heartbeat(ctx context.Context, nodeID string, usedCPU int, usedMemory int) error {
	node, err := p.getNode(ctx, nodeID)
	if err != nil {
		return err
	}

	node.UsedCPU = usedCPU
	node.UsedMemory = usedMemory
	node.LastSeen = time.Now().UTC()
	node.Status = models.NodeStatusReady
	node.HeartbeatCount++
	node.UptimePercent = computeUptimePercent(node.HeartbeatCount, node.DownTransitions)
	node.Reliability = adaptiveReliability(node.Reliability, node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)

	_, err = p.db.ExecContext(
		ctx,
		`UPDATE nodes SET used_cpu = $1, used_memory = $2, last_seen = $3, status = $4,
		 reliability = $5, heartbeat_count = $6, uptime_percent = $7, success_ratio = $8
		 WHERE id = $9`,
		node.UsedCPU,
		node.UsedMemory,
		node.LastSeen,
		string(node.Status),
		node.Reliability,
		node.HeartbeatCount,
		node.UptimePercent,
		node.SuccessRatio,
		node.ID,
	)
	if err != nil {
		return err
	}
	return nil
}

func (p *PostgresStore) RecordTaskResult(ctx context.Context, nodeID string, status models.TaskStatus) error {
	node, err := p.getNode(ctx, nodeID)
	if err != nil {
		return err
	}

	switch status {
	case models.TaskStatusCompleted:
		node.SuccessCount++
	case models.TaskStatusFailed:
		node.FailureCount++
	default:
		return nil
	}

	node.SuccessRatio = computeSuccessRatio(node.SuccessCount, node.FailureCount)
	node.Reliability = adaptiveReliability(node.Reliability, node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)

	_, err = p.db.ExecContext(ctx, `UPDATE nodes SET success_count = $1, failure_count = $2, success_ratio = $3, reliability = $4 WHERE id = $5`,
		node.SuccessCount,
		node.FailureCount,
		node.SuccessRatio,
		node.Reliability,
		node.ID,
	)
	return err
}

func (p *PostgresStore) ListNodes(ctx context.Context) ([]models.Node, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, total_cpu, used_cpu, total_memory, used_memory, status, last_seen, reliability, heartbeat_count, success_count, failure_count, down_transitions, uptime_percent, success_ratio FROM nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Node, 0)
	for rows.Next() {
		var node models.Node
		var status string
		if err := rows.Scan(&node.ID, &node.TotalCPU, &node.UsedCPU, &node.TotalMemory, &node.UsedMemory, &status, &node.LastSeen, &node.Reliability, &node.HeartbeatCount, &node.SuccessCount, &node.FailureCount, &node.DownTransitions, &node.UptimePercent, &node.SuccessRatio); err != nil {
			return nil, err
		}
		node.Status = models.NodeStatus(status)
		result = append(result, node)
	}

	return result, rows.Err()
}

func (p *PostgresStore) MarkDownInactive(ctx context.Context) (int64, error) {
	threshold := time.Now().UTC().Add(-p.inactiveTimeout)
	rows, err := p.db.QueryContext(ctx, `SELECT id, total_cpu, used_cpu, total_memory, used_memory, status, last_seen, reliability, heartbeat_count, success_count, failure_count, down_transitions, uptime_percent, success_ratio FROM nodes WHERE last_seen < $1 AND status <> $2`, threshold, string(models.NodeStatusDown))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var updated int64
	for rows.Next() {
		var node models.Node
		var status string
		if err := rows.Scan(&node.ID, &node.TotalCPU, &node.UsedCPU, &node.TotalMemory, &node.UsedMemory, &status, &node.LastSeen, &node.Reliability, &node.HeartbeatCount, &node.SuccessCount, &node.FailureCount, &node.DownTransitions, &node.UptimePercent, &node.SuccessRatio); err != nil {
			return updated, err
		}
		node.Status = models.NodeStatus(status)
		node.Status = models.NodeStatusDown
		node.DownTransitions++
		node.UptimePercent = computeUptimePercent(node.HeartbeatCount, node.DownTransitions)
		node.Reliability = adaptiveReliability(clampReliability(node.Reliability-0.25), node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)

		_, err := p.db.ExecContext(ctx, `UPDATE nodes SET status = $1, down_transitions = $2, uptime_percent = $3, reliability = $4 WHERE id = $5`,
			string(node.Status),
			node.DownTransitions,
			node.UptimePercent,
			node.Reliability,
			node.ID,
		)
		if err != nil {
			return updated, err
		}
		updated++
	}

	return updated, rows.Err()
}

func (p *PostgresStore) getNode(ctx context.Context, nodeID string) (models.Node, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, total_cpu, used_cpu, total_memory, used_memory, status, last_seen, reliability, heartbeat_count, success_count, failure_count, down_transitions, uptime_percent, success_ratio FROM nodes WHERE id = $1`, nodeID)
	var node models.Node
	var status string
	if err := row.Scan(&node.ID, &node.TotalCPU, &node.UsedCPU, &node.TotalMemory, &node.UsedMemory, &status, &node.LastSeen, &node.Reliability, &node.HeartbeatCount, &node.SuccessCount, &node.FailureCount, &node.DownTransitions, &node.UptimePercent, &node.SuccessRatio); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.Node{}, ErrNodeNotFound
		}
		return models.Node{}, err
	}
	node.Status = models.NodeStatus(status)
	return node, nil
}

func (p *PostgresStore) CreateAllocation(ctx context.Context, allocation models.Allocation) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO allocations (task_id, node_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, allocation.TaskID, allocation.NodeID)
	return err
}

func (p *PostgresStore) ListByNode(ctx context.Context, nodeID string) ([]models.Allocation, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT task_id, node_id FROM allocations WHERE node_id = $1 ORDER BY task_id`, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]models.Allocation, 0)
	for rows.Next() {
		var allocation models.Allocation
		if err := rows.Scan(&allocation.TaskID, &allocation.NodeID); err != nil {
			return nil, err
		}
		result = append(result, allocation)
	}
	return result, rows.Err()
}

func (p *PostgresStore) DeleteAllocation(ctx context.Context, taskID string, nodeID string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM allocations WHERE task_id = $1 AND node_id = $2`, taskID, nodeID)
	return err
}

func (p *PostgresStore) DeleteAllocationsByTask(ctx context.Context, taskID string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM allocations WHERE task_id = $1`, taskID)
	return err
}

func (p *PostgresStore) UpdateStatusAndAllocation(ctx context.Context, taskID string, status models.TaskStatus, nodeID string) error {
	task, err := p.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if !isTransitionAllowed(task.Status, status) {
		return ErrInvalidStatusTransition
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	now := time.Now().UTC()
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET status = $1, node_id = $2, updated_at = $3,
         scheduled_at = CASE WHEN $1 = $5 AND scheduled_at IS NULL THEN $3 ELSE scheduled_at END
         WHERE id = $4`, string(status), nodeID, now, taskID, string(models.TaskStatusRunning))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrTaskNotFound
	}

	if status == models.TaskStatusRunning {
		if _, err := tx.ExecContext(ctx, `INSERT INTO allocations (task_id, node_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, taskID, nodeID); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM allocations WHERE task_id = $1`, taskID); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

type singleRowScanner interface {
	Scan(dest ...any) error
}

func scanTaskRow(scanner singleRowScanner) (models.Task, error) {
	var task models.Task
	var status string
	var commandJSON string
	var secretRefsJSON string
	var volumesJSON string
	var scheduledAt sql.NullTime
	var idempotencyKey sql.NullString
	err := scanner.Scan(&task.ID, &task.Namespace, &task.Image, &commandJSON, &secretRefsJSON, &volumesJSON, &task.CPU, &task.Memory, &task.Priority, &status, &task.NodeID, &task.JobID, &idempotencyKey, &task.Attempts, &task.MaxAttempts, &task.NextAttemptAt, &scheduledAt, &task.LastError, &task.CreatedAt, &task.UpdatedAt)
	if err != nil {
		return models.Task{}, err
	}
	if idempotencyKey.Valid {
		task.IdempotencyKey = idempotencyKey.String
	}
	task.Status = models.TaskStatus(status)
	if err := unmarshalCommand(commandJSON, &task.Command); err != nil {
		return models.Task{}, err
	}
	if err := unmarshalSecretRefs(secretRefsJSON, &task.SecretRefs); err != nil {
		return models.Task{}, err
	}
	if err := unmarshalVolumes(volumesJSON, &task.Volumes); err != nil {
		return models.Task{}, err
	}
	if scheduledAt.Valid {
		task.ScheduledAt = scheduledAt.Time
	}
	return task, nil
}

func nullTime(value time.Time) sql.NullTime {
	if value.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: value, Valid: true}
}

func nullString(value string) sql.NullString {
	if strings.TrimSpace(value) == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: value, Valid: true}
}

func scanTaskRows(rows *sql.Rows) (models.Task, error) {
	return scanTaskRow(rows)
}

func scanJobRow(scanner singleRowScanner) (models.Job, error) {
	var job models.Job
	var status string
	var commandJSON string
	var secretRefsJSON string
	var volumesJSON string
	var autoscaleJSON string
	var idempotencyKey sql.NullString
	if err := scanner.Scan(&job.ID, &job.Namespace, &job.Image, &commandJSON, &secretRefsJSON, &volumesJSON, &job.Replicas, &job.CPU, &job.Memory, &job.Priority, &status, &autoscaleJSON, &idempotencyKey, &job.CreatedAt, &job.UpdatedAt); err != nil {
		return models.Job{}, err
	}
	if idempotencyKey.Valid {
		job.IdempotencyKey = idempotencyKey.String
	}
	job.Status = models.JobStatus(status)
	if err := unmarshalCommand(commandJSON, &job.Command); err != nil {
		return models.Job{}, err
	}
	if err := unmarshalSecretRefs(secretRefsJSON, &job.SecretRefs); err != nil {
		return models.Job{}, err
	}
	if err := unmarshalVolumes(volumesJSON, &job.Volumes); err != nil {
		return models.Job{}, err
	}
	if err := unmarshalAutoscale(autoscaleJSON, &job.Autoscale); err != nil {
		return models.Job{}, err
	}
	return job, nil
}

func scanJobRows(rows *sql.Rows) (models.Job, error) {
	return scanJobRow(rows)
}

func marshalCommand(command []string) (string, error) {
	data, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func unmarshalCommand(raw string, dest *[]string) error {
	if raw == "" {
		*dest = nil
		return nil
	}
	return json.Unmarshal([]byte(raw), dest)
}

func marshalAutoscale(autoscale models.Autoscale) (string, error) {
	data, err := json.Marshal(autoscale)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func marshalSecretRefs(refs []models.SecretRef) (string, error) {
	data, err := json.Marshal(refs)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func unmarshalSecretRefs(raw string, dest *[]models.SecretRef) error {
	if raw == "" {
		*dest = nil
		return nil
	}
	return json.Unmarshal([]byte(raw), dest)
}

func marshalVolumes(volumes []models.VolumeMount) (string, error) {
	data, err := json.Marshal(volumes)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func unmarshalVolumes(raw string, dest *[]models.VolumeMount) error {
	if raw == "" {
		*dest = nil
		return nil
	}
	return json.Unmarshal([]byte(raw), dest)
}

func unmarshalAutoscale(raw string, dest *models.Autoscale) error {
	if raw == "" {
		*dest = models.Autoscale{}
		return nil
	}
	return json.Unmarshal([]byte(raw), dest)
}

func defaultNamespace(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}
	return strings.TrimSpace(value)
}
