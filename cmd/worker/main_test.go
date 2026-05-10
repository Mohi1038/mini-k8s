package main

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-k8ts/internal/config"
	"mini-k8ts/internal/models"
	"mini-k8ts/internal/task"
	"mini-k8ts/pkg/client"
	"mini-k8ts/pkg/logger"
)

type stubRunner struct {
	mu      sync.Mutex
	stopped []string
	managed map[string]task.ManagedTaskState
	waitFn  func(context.Context, string) (int64, error)
}

func (s *stubRunner) Run(_ context.Context, _ models.Task) error {
	return nil
}

func (s *stubRunner) Stop(_ context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = append(s.stopped, taskID)
	return nil
}

func (s *stubRunner) Logs(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *stubRunner) Wait(_ context.Context, _ string) (int64, error) {
	if s.waitFn != nil {
		return s.waitFn(context.Background(), "")
	}
	return 0, nil
}

func (s *stubRunner) ListManaged(_ context.Context) (map[string]task.ManagedTaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]task.ManagedTaskState, len(s.managed))
	for k, v := range s.managed {
		result[k] = v
	}
	return result, nil
}

var _ task.Runner = (*stubRunner)(nil)

func TestStopActiveTasksStopsContainersAndReportsFailure(t *testing.T) {
	active := map[string]models.Task{
		"task-1": {ID: "task-1", NodeID: "worker-1", Status: models.TaskStatusRunning},
	}
	runner := &stubRunner{}
	client := client.NewSchedulerClient("http://127.0.0.1:1")
	var activeMu sync.Mutex

	stopActiveTasks(logger.New("debug", nil), runner, client, "worker-1", &activeMu, active)

	if len(runner.stopped) != 1 || runner.stopped[0] != "task-1" {
		t.Fatalf("expected task container to be stopped, got %#v", runner.stopped)
	}
	if len(active) != 0 {
		t.Fatalf("expected active task map to be cleared, got %d entries", len(active))
	}
}

func TestReconcileWorkerStateStopsOrphanedManagedContainers(t *testing.T) {
	runner := &stubRunner{
		managed: map[string]task.ManagedTaskState{
			"orphan-task": {TaskID: "orphan-task", Running: true},
		},
	}
	active := map[string]models.Task{
		"orphan-task": {ID: "orphan-task", NodeID: "worker-1", Status: models.TaskStatusRunning},
	}
	var activeMu sync.Mutex

	err := reconcileWorkerState(context.Background(), logger.New("debug", nil), runner, client.NewSchedulerClient("http://127.0.0.1:1"), config.Config{WorkerNodeID: "worker-1", TaskExecutionTimeout: time.Second}, &activeMu, active, nil)
	if err != nil {
		t.Fatalf("reconcileWorkerState returned error: %v", err)
	}

	if len(runner.stopped) != 1 || runner.stopped[0] != "orphan-task" {
		t.Fatalf("expected orphaned task to be stopped, got %#v", runner.stopped)
	}
	if len(active) != 0 {
		t.Fatalf("expected orphaned task removed from active set, got %d entries", len(active))
	}
}

func TestReconcileWorkerStateAdoptsRecoveredRunningTask(t *testing.T) {
	waitCalled := make(chan string, 1)
	runner := &stubRunner{
		managed: map[string]task.ManagedTaskState{
			"task-1": {TaskID: "task-1", Running: true},
		},
		waitFn: func(_ context.Context, _ string) (int64, error) {
			waitCalled <- "task-1"
			return 0, nil
		},
	}
	active := map[string]models.Task{}
	var activeMu sync.Mutex

	err := reconcileWorkerState(context.Background(), logger.New("debug", nil), runner, client.NewSchedulerClient("http://127.0.0.1:1"), config.Config{WorkerNodeID: "worker-1", TaskExecutionTimeout: time.Second}, &activeMu, active, []models.Task{
		{ID: "task-1", NodeID: "worker-1", Status: models.TaskStatusRunning, CPU: 1, Memory: 128},
	})
	if err != nil {
		t.Fatalf("reconcileWorkerState returned error: %v", err)
	}

	select {
	case <-waitCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected recovered running task to be adopted and watched")
	}
}

func TestReconcileWorkerStateReportsExitedRecoveredTask(t *testing.T) {
	runner := &stubRunner{
		managed: map[string]task.ManagedTaskState{
			"task-2": {TaskID: "task-2", Running: false, ExitCode: 7},
		},
	}
	active := map[string]models.Task{}
	var activeMu sync.Mutex

	err := reconcileWorkerState(context.Background(), logger.New("debug", nil), runner, client.NewSchedulerClient("http://127.0.0.1:1"), config.Config{WorkerNodeID: "worker-1", TaskExecutionTimeout: time.Second}, &activeMu, active, []models.Task{
		{ID: "task-2", NodeID: "worker-1", Status: models.TaskStatusRunning},
	})
	if err != nil {
		t.Fatalf("reconcileWorkerState returned error: %v", err)
	}
	if len(runner.stopped) != 1 || runner.stopped[0] != "task-2" {
		t.Fatalf("expected exited recovered task to be cleaned up, got %#v", runner.stopped)
	}
}

func TestEnrichTaskRuntimeEnvResolvesSecrets(t *testing.T) {
	taskModel := models.Task{
		ID:        "task-1",
		Namespace: "default",
		SecretRefs: []models.SecretRef{
			{Name: "db-creds", Key: "username", EnvVar: "DB_USER"},
			{Name: "db-creds", Key: "password", EnvVar: "DB_PASSWORD"},
		},
	}

	if err := resolveTaskRuntimeEnv(&taskModel, func(name string) (models.Secret, error) {
		if name != "db-creds" {
			t.Fatalf("unexpected secret lookup: %s", name)
		}
		return models.Secret{
			Name:      "db-creds",
			Namespace: "default",
			Data: map[string]string{
				"username": "app",
				"password": "secret",
			},
		}, nil
	}); err != nil {
		t.Fatalf("resolveTaskRuntimeEnv returned error: %v", err)
	}
	if len(taskModel.ResolvedEnv) != 2 {
		t.Fatalf("expected 2 env entries, got %#v", taskModel.ResolvedEnv)
	}
	if taskModel.ResolvedEnv[0] != "DB_USER=app" && taskModel.ResolvedEnv[1] != "DB_USER=app" {
		t.Fatalf("expected DB_USER env, got %#v", taskModel.ResolvedEnv)
	}
}
