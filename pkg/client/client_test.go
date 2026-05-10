package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-k8ts/internal/models"
)

func TestRegisterAndHeartbeat(t *testing.T) {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /nodes/register", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	handler.HandleFunc("POST /nodes/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler.HandleFunc("GET /nodes/{nodeID}/tasks/running", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []models.Task{{ID: "task-1", Status: models.TaskStatusRunning}}})
	})
	handler.HandleFunc("POST /tasks/{taskID}/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"job_id": "job-1"})
	})
	handler.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []models.Job{{ID: "job-1", Image: "nginx"}}})
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewSchedulerClient(server.URL)
	if err := client.RegisterNode(context.Background(), "node-1", 8, 16000); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if err := client.SendHeartbeat(context.Background(), "node-1", 2, 512); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	runningTasks, err := client.GetRunningTasks(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("get running tasks failed: %v", err)
	}
	if len(runningTasks) != 1 {
		t.Fatalf("expected one running task, got %d", len(runningTasks))
	}
	if err := client.UpdateTaskStatus(context.Background(), "task-1", models.TaskStatusCompleted, ""); err != nil {
		t.Fatalf("task status update failed: %v", err)
	}
	jobID, err := client.CreateJob(context.Background(), models.Job{Image: "nginx", Replicas: 1, CPU: 1, Memory: 128, Priority: 1})
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if jobID != "job-1" {
		t.Fatalf("expected job-1, got %s", jobID)
	}
	jobs, err := client.GetJobs(context.Background())
	if err != nil {
		t.Fatalf("get jobs failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected one job, got %d", len(jobs))
	}
}
