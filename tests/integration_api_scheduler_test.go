package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mini-k8ts/internal/api"
	"mini-k8ts/internal/db"
	"mini-k8ts/internal/scheduler"
	"mini-k8ts/pkg/logger"
)

func TestAPISchedulerIntegration(t *testing.T) {
	store := db.NewMemoryStoreWithTimeout(2 * time.Second)
	log := logger.New("debug", nil)
	server := api.NewServerWithRepos(":0", log, store, store, store, store, store, store, store.NextTaskID, store.NextJobID, api.DefaultServerOptions())
	schedulerService := scheduler.NewService(store, store, store, store, scheduler.DefaultStrategy{}, log, 10*time.Millisecond, store.NextTaskID)

	registerNodeReq := httptest.NewRequest(http.MethodPost, "/nodes/register", bytes.NewBufferString(`{"node_id":"node-1","cpu":8,"memory":16000}`))
	registerNodeReq.Header.Set("Content-Type", "application/json")
	registerNodeResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(registerNodeResp, registerNodeReq)
	if registerNodeResp.Code != http.StatusCreated {
		t.Fatalf("expected register status 201, got %d", registerNodeResp.Code)
	}

	submitTaskReq := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewBufferString(`{"image":"nginx","cpu":1,"memory":128,"priority":1}`))
	submitTaskReq.Header.Set("Content-Type", "application/json")
	submitTaskResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(submitTaskResp, submitTaskReq)
	if submitTaskResp.Code != http.StatusAccepted {
		t.Fatalf("expected task submit status 202, got %d", submitTaskResp.Code)
	}

	if err := schedulerService.ScheduleOnce(context.Background()); err != nil {
		t.Fatalf("schedule once failed: %v", err)
	}

	clusterReq := httptest.NewRequest(http.MethodGet, "/cluster", nil)
	clusterResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(clusterResp, clusterReq)
	if clusterResp.Code != http.StatusOK {
		t.Fatalf("expected cluster status 200, got %d", clusterResp.Code)
	}

	var payload struct {
		Tasks []struct {
			Status string `json:"status"`
			NodeID string `json:"node_id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(clusterResp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode cluster payload failed: %v", err)
	}
	if len(payload.Tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(payload.Tasks))
	}
	if payload.Tasks[0].Status != "RUNNING" {
		t.Fatalf("expected scheduled task status RUNNING, got %s", payload.Tasks[0].Status)
	}
	if payload.Tasks[0].NodeID != "node-1" {
		t.Fatalf("expected node assignment node-1, got %s", payload.Tasks[0].NodeID)
	}
}
