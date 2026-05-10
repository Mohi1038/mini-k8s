package tests

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-k8ts/internal/api"
	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
	"mini-k8ts/internal/scheduler"
	"mini-k8ts/pkg/client"
	"mini-k8ts/pkg/logger"
)

func TestWorkerExecutionFlow(t *testing.T) {
	store := db.NewMemoryStore()
	log := logger.New("debug", nil)
	server := api.NewServerWithRepos(":0", log, store, store, store, store, store, store, store.NextTaskID, store.NextJobID, api.DefaultServerOptions())
	schedulerService := scheduler.NewService(store, store, store, store, scheduler.DefaultStrategy{}, log, 0, store.NextTaskID)

	registerReq := httptest.NewRequest(http.MethodPost, "/nodes/register", bytes.NewBufferString(`{"node_id":"node-1","cpu":8,"memory":16000}`))
	registerReq.Header.Set("Content-Type", "application/json")
	registerResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(registerResp, registerReq)
	if registerResp.Code != http.StatusCreated {
		t.Fatalf("expected register status 201, got %d", registerResp.Code)
	}

	taskReq := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewBufferString(`{"image":"nginx","cpu":1,"memory":128,"priority":1}`))
	taskReq.Header.Set("Content-Type", "application/json")
	taskReq.Header.Set("X-Idempotency-Key", "worker-flow-1")
	taskResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(taskResp, taskReq)
	if taskResp.Code != http.StatusAccepted {
		t.Fatalf("expected submit status 202, got %d", taskResp.Code)
	}

	if err := schedulerService.ScheduleOnce(context.Background()); err != nil {
		t.Fatalf("schedule once failed: %v", err)
	}

	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	workerClient := client.NewSchedulerClient(httpServer.URL)

	runningTasks, err := workerClient.GetRunningTasks(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("get running tasks failed: %v", err)
	}
	if len(runningTasks) != 1 {
		t.Fatalf("expected one running task, got %d", len(runningTasks))
	}

	if err := workerClient.UpdateTaskStatus(context.Background(), runningTasks[0].ID, models.TaskStatusCompleted, ""); err != nil {
		t.Fatalf("update task status failed: %v", err)
	}

	task, err := store.GetTask(context.Background(), runningTasks[0].ID)
	if err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if task.Status != models.TaskStatusCompleted {
		t.Fatalf("expected COMPLETED task status, got %s", task.Status)
	}
}
