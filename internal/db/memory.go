package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"mini-k8ts/internal/models"
)

var ErrNodeNotFound = errors.New("node not found")
var ErrTaskNotFound = errors.New("task not found")
var ErrJobNotFound = errors.New("job not found")
var ErrNamespaceNotFound = errors.New("namespace not found")
var ErrServiceNotFound = errors.New("service not found")
var ErrSecretNotFound = errors.New("secret not found")
var ErrInvalidStatusTransition = errors.New("invalid task status transition")

type MemoryStore struct {
	mu              sync.RWMutex
	tasks           map[string]models.Task
	nodes           map[string]models.Node
	jobs            map[string]models.Job
	namespaces      map[string]models.Namespace
	services        map[string]models.Service
	secrets         map[string]models.Secret
	allocations     []models.Allocation
	taskSeq         int64
	jobSeq          int64
	inactiveTimeout time.Duration
}

func NewMemoryStore() *MemoryStore {
	return NewMemoryStoreWithTimeout(15 * time.Second)
}

func NewMemoryStoreWithTimeout(inactiveTimeout time.Duration) *MemoryStore {
	if inactiveTimeout <= 0 {
		inactiveTimeout = 15 * time.Second
	}

	return &MemoryStore{
		tasks:           map[string]models.Task{},
		nodes:           map[string]models.Node{},
		jobs:            map[string]models.Job{},
		namespaces:      map[string]models.Namespace{},
		services:        map[string]models.Service{},
		secrets:         map[string]models.Secret{},
		allocations:     make([]models.Allocation, 0),
		inactiveTimeout: inactiveTimeout,
	}
}

func (m *MemoryStore) NextTaskID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskSeq++
	return fmt.Sprintf("task-%d", m.taskSeq)
}

func (m *MemoryStore) NextJobID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobSeq++
	return fmt.Sprintf("job-%d", m.jobSeq)
}

func serviceKey(namespace string, name string) string {
	if namespace == "" {
		namespace = "default"
	}
	return namespace + "/" + name
}

func secretKey(namespace string, name string) string {
	return serviceKey(namespace, name)
}

func (m *MemoryStore) Create(_ context.Context, task models.Task) (models.Task, error) {
	// Write lock protects sequence-consistent task creation under concurrent API calls.
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	if task.MaxAttempts <= 0 {
		task.MaxAttempts = 3
	}
	if strings.TrimSpace(task.Namespace) == "" {
		task.Namespace = "default"
	}
	if strings.TrimSpace(task.IdempotencyKey) != "" {
		for _, existing := range m.tasks {
			if existing.Namespace == task.Namespace && existing.IdempotencyKey == task.IdempotencyKey {
				return existing, nil
			}
		}
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = now
	}
	task.UpdatedAt = now
	if task.NextAttemptAt.IsZero() {
		task.NextAttemptAt = now
	}
	m.tasks[task.ID] = task
	return task, nil
}

func (m *MemoryStore) GetTask(_ context.Context, taskID string) (models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.tasks[taskID]
	if !ok {
		return models.Task{}, ErrTaskNotFound
	}
	return task, nil
}

func (m *MemoryStore) ListTasks(_ context.Context) ([]models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		result = append(result, task)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (m *MemoryStore) ListPending(_ context.Context) ([]models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Task, 0)
	for _, task := range m.tasks {
		if task.Status == models.TaskStatusPending {
			result = append(result, task)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (m *MemoryStore) UpdateStatus(_ context.Context, taskID string, status models.TaskStatus, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}

	if !isTransitionAllowed(task.Status, status) {
		return ErrInvalidStatusTransition
	}

	task.Status = status
	task.NodeID = nodeID
	task.UpdatedAt = time.Now().UTC()
	if status == models.TaskStatusRunning {
		task.LastError = ""
		if task.ScheduledAt.IsZero() {
			task.ScheduledAt = task.UpdatedAt
		}
	}
	m.tasks[taskID] = task
	return nil
}

func (m *MemoryStore) UpdateStatusAndAllocation(_ context.Context, taskID string, status models.TaskStatus, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}

	if !isTransitionAllowed(task.Status, status) {
		return ErrInvalidStatusTransition
	}

	task.Status = status
	task.NodeID = nodeID
	task.UpdatedAt = time.Now().UTC()
	if status == models.TaskStatusRunning {
		task.LastError = ""
		if task.ScheduledAt.IsZero() {
			task.ScheduledAt = task.UpdatedAt
		}
		found := false
		for _, allocation := range m.allocations {
			if allocation.TaskID == task.ID && allocation.NodeID == nodeID {
				found = true
				break
			}
		}
		if !found {
			m.allocations = append(m.allocations, models.Allocation{TaskID: task.ID, NodeID: nodeID})
		}
	} else {
		filtered := m.allocations[:0]
		for _, allocation := range m.allocations {
			if allocation.TaskID != task.ID {
				filtered = append(filtered, allocation)
			}
		}
		m.allocations = filtered
	}

	m.tasks[taskID] = task
	return nil
}

func (m *MemoryStore) UpdateTask(_ context.Context, task models.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[task.ID]; !ok {
		return ErrTaskNotFound
	}
	task.UpdatedAt = time.Now().UTC()
	m.tasks[task.ID] = task
	return nil
}

func (m *MemoryStore) ListTasksByNodeAndStatus(_ context.Context, nodeID string, status models.TaskStatus) ([]models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Task, 0)
	for _, task := range m.tasks {
		if task.NodeID == nodeID && task.Status == status {
			result = append(result, task)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (m *MemoryStore) ListTasksByJob(_ context.Context, jobID string) ([]models.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Task, 0)
	for _, task := range m.tasks {
		if task.JobID == jobID {
			result = append(result, task)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (m *MemoryStore) Register(_ context.Context, node models.Node) (models.Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	node.Status = models.NodeStatusReady
	node.LastSeen = time.Now().UTC()
	if node.Reliability <= 0 {
		node.Reliability = 0.5
	}
	node.UptimePercent = computeUptimePercent(node.HeartbeatCount, node.DownTransitions)
	node.SuccessRatio = computeSuccessRatio(node.SuccessCount, node.FailureCount)
	node.Reliability = adaptiveReliability(node.Reliability, node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)
	m.nodes[node.ID] = node
	return node, nil
}

func (m *MemoryStore) Heartbeat(_ context.Context, nodeID string, usedCPU int, usedMemory int) error {
	// Heartbeat is a hot path: update usage + liveness in one critical section.
	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return ErrNodeNotFound
	}

	node.UsedCPU = usedCPU
	node.UsedMemory = usedMemory
	node.LastSeen = time.Now().UTC()
	node.Status = models.NodeStatusReady
	node.HeartbeatCount++
	node.UptimePercent = computeUptimePercent(node.HeartbeatCount, node.DownTransitions)
	node.Reliability = adaptiveReliability(node.Reliability, node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)
	m.nodes[nodeID] = node
	return nil
}

func (m *MemoryStore) RecordTaskResult(_ context.Context, nodeID string, status models.TaskStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.nodes[nodeID]
	if !ok {
		return ErrNodeNotFound
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
	m.nodes[nodeID] = node
	return nil
}

func (m *MemoryStore) ListNodes(_ context.Context) ([]models.Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (m *MemoryStore) MarkDownInactive(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var downCount int64
	// Nodes missing heartbeats beyond timeout are marked DOWN for scheduler safety.
	threshold := time.Now().UTC().Add(-m.inactiveTimeout)
	for id, node := range m.nodes {
		if node.LastSeen.Before(threshold) && node.Status != models.NodeStatusDown {
			node.Status = models.NodeStatusDown
			node.DownTransitions++
			node.UptimePercent = computeUptimePercent(node.HeartbeatCount, node.DownTransitions)
			node.Reliability = adaptiveReliability(clampReliability(node.Reliability-0.25), node.HeartbeatCount, node.DownTransitions, node.SuccessCount, node.FailureCount)
			m.nodes[id] = node
			downCount++
		}
	}

	return downCount, nil
}

func (m *MemoryStore) CreateJob(_ context.Context, job models.Job) (models.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.Status == "" {
		job.Status = models.JobStatusPending
	}
	if strings.TrimSpace(job.Namespace) == "" {
		job.Namespace = "default"
	}
	if strings.TrimSpace(job.IdempotencyKey) != "" {
		for _, existing := range m.jobs {
			if existing.Namespace == job.Namespace && existing.IdempotencyKey == job.IdempotencyKey {
				return existing, nil
			}
		}
	}
	job.UpdatedAt = now
	m.jobs[job.ID] = job
	return job, nil
}

func (m *MemoryStore) GetJob(_ context.Context, jobID string) (models.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[jobID]
	if !ok {
		return models.Job{}, ErrJobNotFound
	}
	return job, nil
}

func (m *MemoryStore) ListJobs(_ context.Context) ([]models.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		result = append(result, job)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (m *MemoryStore) UpdateJob(_ context.Context, job models.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[job.ID]; !ok {
		return ErrJobNotFound
	}
	job.UpdatedAt = time.Now().UTC()
	m.jobs[job.ID] = job
	return nil
}

func (m *MemoryStore) CreateNamespace(_ context.Context, namespace models.Namespace) (models.Namespace, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := strings.TrimSpace(namespace.Name)
	if name == "" {
		return models.Namespace{}, ErrNamespaceNotFound
	}

	now := time.Now().UTC()
	if namespace.CreatedAt.IsZero() {
		namespace.CreatedAt = now
	}
	namespace.UpdatedAt = now
	namespace.Name = name
	m.namespaces[name] = namespace
	return namespace, nil
}

func (m *MemoryStore) GetNamespace(_ context.Context, name string) (models.Namespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	name = strings.TrimSpace(name)
	ns, ok := m.namespaces[name]
	if !ok {
		return models.Namespace{}, ErrNamespaceNotFound
	}
	return ns, nil
}

func (m *MemoryStore) ListNamespaces(_ context.Context) ([]models.Namespace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Namespace, 0, len(m.namespaces))
	for _, ns := range m.namespaces {
		result = append(result, ns)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (m *MemoryStore) UpdateNamespace(_ context.Context, namespace models.Namespace) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := strings.TrimSpace(namespace.Name)
	if name == "" {
		return ErrNamespaceNotFound
	}
	if _, ok := m.namespaces[name]; !ok {
		return ErrNamespaceNotFound
	}
	namespace.UpdatedAt = time.Now().UTC()
	namespace.Name = name
	m.namespaces[name] = namespace
	return nil
}

func (m *MemoryStore) CreateService(_ context.Context, svc models.Service) (models.Service, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	m.services[serviceKey(svc.Namespace, name)] = svc
	return svc, nil
}

func (m *MemoryStore) GetService(_ context.Context, namespace string, name string) (models.Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := serviceKey(namespace, strings.TrimSpace(name))
	svc, ok := m.services[key]
	if !ok {
		return models.Service{}, ErrServiceNotFound
	}
	return svc, nil
}

func (m *MemoryStore) ListServices(_ context.Context) ([]models.Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Service, 0, len(m.services))
	for _, svc := range m.services {
		result = append(result, svc)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace == result[j].Namespace {
			return result[i].Name < result[j].Name
		}
		return result[i].Namespace < result[j].Namespace
	})
	return result, nil
}

func (m *MemoryStore) UpdateService(_ context.Context, svc models.Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := strings.TrimSpace(svc.Name)
	if name == "" {
		return ErrServiceNotFound
	}
	if svc.Namespace == "" {
		svc.Namespace = "default"
	}
	key := serviceKey(svc.Namespace, name)
	if _, ok := m.services[key]; !ok {
		return ErrServiceNotFound
	}
	svc.UpdatedAt = time.Now().UTC()
	m.services[key] = svc
	return nil
}

func (m *MemoryStore) CreateSecret(_ context.Context, secret models.Secret) (models.Secret, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	if secret.Data == nil {
		secret.Data = map[string]string{}
	}
	m.secrets[secretKey(secret.Namespace, secret.Name)] = secret
	return secret, nil
}

func (m *MemoryStore) GetSecret(_ context.Context, namespace string, name string) (models.Secret, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	secret, ok := m.secrets[secretKey(defaultNamespace(namespace), strings.TrimSpace(name))]
	if !ok {
		return models.Secret{}, ErrSecretNotFound
	}
	return secret, nil
}

func (m *MemoryStore) ListSecrets(_ context.Context, namespace string) ([]models.Secret, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ns := defaultNamespace(namespace)
	result := make([]models.Secret, 0)
	for _, secret := range m.secrets {
		if secret.Namespace == ns {
			result = append(result, secret)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (m *MemoryStore) CreateAllocation(_ context.Context, allocation models.Allocation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.allocations {
		if existing.TaskID == allocation.TaskID && existing.NodeID == allocation.NodeID {
			return nil
		}
	}
	m.allocations = append(m.allocations, allocation)
	return nil
}

func (m *MemoryStore) ListByNode(_ context.Context, nodeID string) ([]models.Allocation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]models.Allocation, 0)
	for _, allocation := range m.allocations {
		if allocation.NodeID == nodeID {
			result = append(result, allocation)
		}
	}
	return result, nil
}

func (m *MemoryStore) DeleteAllocation(_ context.Context, taskID string, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filtered := m.allocations[:0]
	for _, allocation := range m.allocations {
		if allocation.TaskID == taskID && allocation.NodeID == nodeID {
			continue
		}
		filtered = append(filtered, allocation)
	}
	m.allocations = filtered
	return nil
}

func (m *MemoryStore) DeleteAllocationsByTask(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filtered := m.allocations[:0]
	for _, allocation := range m.allocations {
		if allocation.TaskID == taskID {
			continue
		}
		filtered = append(filtered, allocation)
	}
	m.allocations = filtered
	return nil
}

func clampReliability(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func computeUptimePercent(heartbeats int, downTransitions int) float64 {
	total := heartbeats + downTransitions
	if total <= 0 {
		return 1
	}
	return float64(heartbeats) / float64(total)
}

func computeSuccessRatio(successes int, failures int) float64 {
	total := successes + failures
	if total <= 0 {
		return 1
	}
	return float64(successes) / float64(total)
}

func computeChurn(heartbeats int, downTransitions int) float64 {
	total := heartbeats + downTransitions
	if total <= 0 {
		return 0
	}
	return float64(downTransitions) / float64(total)
}

func adaptiveReliability(current float64, heartbeats int, downTransitions int, successes int, failures int) float64 {
	if heartbeats == 0 && downTransitions == 0 && successes == 0 && failures == 0 {
		return clampReliability(current)
	}
	uptime := computeUptimePercent(heartbeats, downTransitions)
	successRatio := computeSuccessRatio(successes, failures)
	churn := computeChurn(heartbeats, downTransitions)
	updated := 0.5*current + 0.25*uptime + 0.25*successRatio - 0.15*churn
	return clampReliability(updated)
}

func isTransitionAllowed(current models.TaskStatus, next models.TaskStatus) bool {
	if current == next {
		return true
	}

	switch current {
	case models.TaskStatusPending:
		return next == models.TaskStatusRunning || next == models.TaskStatusFailed
	case models.TaskStatusRunning:
		return next == models.TaskStatusCompleted || next == models.TaskStatusFailed || next == models.TaskStatusPending
	case models.TaskStatusFailed, models.TaskStatusCompleted:
		return false
	default:
		return false
	}
}
