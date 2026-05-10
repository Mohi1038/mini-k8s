package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
	"mini-k8ts/pkg/logger"
)

func TestHealthz(t *testing.T) {
	s := NewServer(":0", logger.New("debug", nil))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	resp := httptest.NewRecorder()

	s.Handler().ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}
}

func TestSubmitTaskNotImplemented(t *testing.T) {
	s := NewServer(":0", logger.New("debug", nil))
	req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewBufferString(`{"image":"nginx","cpu":1,"memory":256,"priority":1}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", "idem-key-1")
	resp := httptest.NewRecorder()

	s.Handler().ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", resp.Code)
	}

	var payload map[string]string
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload["status"] != "PENDING" {
		t.Fatalf("expected PENDING status, got %s", payload["status"])
	}

	duplicateReq := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewBufferString(`{"image":"nginx","cpu":1,"memory":256,"priority":1}`))
	duplicateReq.Header.Set("Content-Type", "application/json")
	duplicateReq.Header.Set("X-Idempotency-Key", "idem-key-1")
	duplicateResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(duplicateResp, duplicateReq)
	if duplicateResp.Code != http.StatusAccepted {
		t.Fatalf("expected duplicate status 202, got %d", duplicateResp.Code)
	}
	var duplicatePayload map[string]string
	if err := json.Unmarshal(duplicateResp.Body.Bytes(), &duplicatePayload); err != nil {
		t.Fatalf("failed to decode duplicate response: %v", err)
	}
	if duplicatePayload["task_id"] != payload["task_id"] {
		t.Fatalf("expected same task id for idempotent request, got %s and %s", payload["task_id"], duplicatePayload["task_id"])
	}
}

func TestRegisterNodeAndHeartbeatAndCluster(t *testing.T) {
	s := NewServer(":0", logger.New("debug", nil))

	registerReq := httptest.NewRequest(http.MethodPost, "/nodes/register", bytes.NewBufferString(`{"node_id":"node-1","cpu":8,"memory":16000}`))
	registerReq.Header.Set("Content-Type", "application/json")
	registerResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(registerResp, registerReq)
	if registerResp.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", registerResp.Code)
	}

	heartbeatReq := httptest.NewRequest(http.MethodPost, "/nodes/heartbeat", bytes.NewBufferString(`{"node_id":"node-1","used_cpu":2,"used_memory":512}`))
	heartbeatReq.Header.Set("Content-Type", "application/json")
	heartbeatResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(heartbeatResp, heartbeatReq)
	if heartbeatResp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", heartbeatResp.Code)
	}

	clusterReq := httptest.NewRequest(http.MethodGet, "/cluster", nil)
	clusterResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(clusterResp, clusterReq)
	if clusterResp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", clusterResp.Code)
	}

	var payload struct {
		Nodes []map[string]any `json:"nodes"`
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(clusterResp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode cluster response: %v", err)
	}
	if len(payload.Nodes) != 1 {
		t.Fatalf("expected one node, got %d", len(payload.Nodes))
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(metricsResp, metricsReq)
	if metricsResp.Code != http.StatusOK {
		t.Fatalf("expected metrics status 200, got %d", metricsResp.Code)
	}

	var metricsPayload map[string]any
	if err := json.Unmarshal(metricsResp.Body.Bytes(), &metricsPayload); err != nil {
		t.Fatalf("failed to decode metrics response: %v", err)
	}
	if _, ok := metricsPayload["total_tasks"]; !ok {
		t.Fatalf("expected total_tasks in metrics response")
	}

	newTaskReq := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewBufferString(`{"image":"busybox","cpu":1,"memory":64,"priority":1}`))
	newTaskReq.Header.Set("Content-Type", "application/json")
	newTaskResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(newTaskResp, newTaskReq)
	if newTaskResp.Code != http.StatusAccepted {
		t.Fatalf("expected task submit status 202, got %d", newTaskResp.Code)
	}

	var newTaskPayload map[string]string
	if err := json.Unmarshal(newTaskResp.Body.Bytes(), &newTaskPayload); err != nil {
		t.Fatalf("failed to decode submitted task: %v", err)
	}

	runningReq := httptest.NewRequest(http.MethodGet, "/nodes/node-1/tasks/running", nil)
	runningResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(runningResp, runningReq)
	if runningResp.Code != http.StatusOK {
		t.Fatalf("expected running tasks status 200, got %d", runningResp.Code)
	}

	statusReq := httptest.NewRequest(http.MethodPost, "/tasks/"+newTaskPayload["task_id"]+"/status", bytes.NewBufferString(`{"status":"FAILED","error":"unit-test"}`))
	statusReq.Header.Set("Content-Type", "application/json")
	statusResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(statusResp, statusReq)
	if statusResp.Code != http.StatusOK {
		t.Fatalf("expected task status update 200, got %d", statusResp.Code)
	}

	clusterResp = httptest.NewRecorder()
	s.Handler().ServeHTTP(clusterResp, clusterReq)
	if clusterResp.Code != http.StatusOK {
		t.Fatalf("expected cluster status 200, got %d", clusterResp.Code)
	}

	if err := json.Unmarshal(clusterResp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode updated cluster response: %v", err)
	}
	if len(payload.Tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(payload.Tasks))
	}
	if payload.Tasks[0]["status_detail"] != "unit-test" {
		t.Fatalf("expected task status_detail to include failure detail, got %#v", payload.Tasks[0]["status_detail"])
	}
}

func TestClusterTaskStatusDetailForPendingCapacityPressure(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())

	if _, err := store.Register(context.Background(), models.Node{
		ID:          "node-1",
		Status:      models.NodeStatusReady,
		TotalCPU:    1,
		UsedCPU:     1,
		TotalMemory: 128,
		UsedMemory:  128,
		Reliability: 0.9,
	}); err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	if _, err := store.Create(context.Background(), models.Task{
		ID:          store.NextTaskID(),
		Image:       "redis:7",
		CPU:         1,
		Memory:      256,
		Priority:    1,
		Status:      models.TaskStatusPending,
		MaxAttempts: 3,
	}); err != nil {
		t.Fatalf("create task failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/cluster", nil)
	resp := httptest.NewRecorder()
	s.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	var payload struct {
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode cluster response: %v", err)
	}
	if len(payload.Tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(payload.Tasks))
	}
	detail, _ := payload.Tasks[0]["status_detail"].(string)
	if detail == "" || detail == "Waiting for scheduler placement." {
		t.Fatalf("expected a capacity-specific pending detail, got %q", detail)
	}
}

func TestSubmitAndListJobs(t *testing.T) {
	s := NewServer(":0", logger.New("debug", nil))

	jobReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"image":"nginx","command":["echo","hi"],"replicas":2,"cpu":1,"memory":128,"priority":1}`))
	jobReq.Header.Set("Content-Type", "application/json")
	jobResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(jobResp, jobReq)
	if jobResp.Code != http.StatusAccepted {
		t.Fatalf("expected job submit status 202, got %d", jobResp.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	listResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("expected jobs list status 200, got %d", listResp.Code)
	}

	var payload struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal(listResp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode jobs response: %v", err)
	}
	if len(payload.Jobs) != 1 {
		t.Fatalf("expected one job, got %d", len(payload.Jobs))
	}
}

func TestAuthMiddlewareEnforcesToken(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, ServerOptions{
		AuthToken:      "secret-token",
		AllowlistPaths: []string{"/healthz"},
	})

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(healthResp, healthReq)
	if healthResp.Code != http.StatusOK {
		t.Fatalf("expected healthz status 200, got %d", healthResp.Code)
	}

	unauthReq := httptest.NewRequest(http.MethodGet, "/cluster", nil)
	unauthResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(unauthResp, unauthReq)
	if unauthResp.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized status 401, got %d", unauthResp.Code)
	}

	authReq := httptest.NewRequest(http.MethodGet, "/cluster", nil)
	authReq.Header.Set("X-Service-Key", "secret-token")
	authResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(authResp, authReq)
	if authResp.Code != http.StatusOK {
		t.Fatalf("expected authorized status 200, got %d", authResp.Code)
	}
}

func TestTaskStatusUpdateCleansAllocation(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())
	if _, err := store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 4, TotalMemory: 2048, Reliability: 0.8}); err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	task := models.Task{
		ID:       store.NextTaskID(),
		Image:    "busybox",
		CPU:      1,
		Memory:   64,
		Priority: 1,
		Status:   models.TaskStatusRunning,
		NodeID:   "node-1",
	}
	if _, err := store.Create(context.Background(), task); err != nil {
		t.Fatalf("create task failed: %v", err)
	}
	if err := store.CreateAllocation(context.Background(), models.Allocation{TaskID: task.ID, NodeID: task.NodeID}); err != nil {
		t.Fatalf("create allocation failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/tasks/"+task.ID+"/status", bytes.NewBufferString(`{"status":"COMPLETED"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	s.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	allocations, err := store.ListByNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("list allocations failed: %v", err)
	}
	if len(allocations) != 0 {
		t.Fatalf("expected allocation cleanup, got %d allocations", len(allocations))
	}

	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}
	if nodes[0].SuccessCount != 1 || nodes[0].SuccessRatio != 1 {
		t.Fatalf("expected node health to record task success, got success_count=%d success_ratio=%f", nodes[0].SuccessCount, nodes[0].SuccessRatio)
	}
}

func TestCreateAndListSecrets(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())

	createReq := httptest.NewRequest(http.MethodPost, "/secrets", bytes.NewBufferString(`{"name":"db-creds","namespace":"default","data":{"username":"app","password":"secret"}}`))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(createResp, createReq)
	if createResp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", createResp.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/secrets?namespace=default", nil)
	listResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(listResp, listReq)
	if listResp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", listResp.Code)
	}

	var payload struct {
		Secrets []models.Secret `json:"secrets"`
	}
	if err := json.Unmarshal(listResp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode secrets response: %v", err)
	}
	if len(payload.Secrets) != 1 || payload.Secrets[0].Name != "db-creds" {
		t.Fatalf("unexpected secrets payload: %#v", payload.Secrets)
	}
}

func TestGetSecret(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())

	createReq := httptest.NewRequest(http.MethodPost, "/secrets", bytes.NewBufferString(`{"name":"db-creds","namespace":"default","data":{"username":"app","password":"secret"}}`))
	createReq.Header.Set("Content-Type", "application/json")
	createResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(createResp, createReq)
	if createResp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", createResp.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/secrets/db-creds?namespace=default", nil)
	getResp := httptest.NewRecorder()
	s.Handler().ServeHTTP(getResp, getReq)
	if getResp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", getResp.Code)
	}

	var secret models.Secret
	if err := json.Unmarshal(getResp.Body.Bytes(), &secret); err != nil {
		t.Fatalf("failed to decode secret response: %v", err)
	}
	if secret.Name != "db-creds" || secret.Namespace != "default" || secret.Data["username"] != "app" {
		t.Fatalf("unexpected secret payload: %#v", secret)
	}
}

func TestServiceEndpointsFiltersUnhealthyNodes(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())
	_, _ = store.Register(context.Background(), models.Node{ID: "node-good", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.9, HeartbeatCount: 3, UptimePercent: 1, SuccessRatio: 1})
	_, _ = store.Register(context.Background(), models.Node{ID: "node-bad", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.1, HeartbeatCount: 3, UptimePercent: 0.1, SuccessRatio: 0.1})
	_, _ = store.Register(context.Background(), models.Node{ID: "node-degraded", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.45, HeartbeatCount: 3, UptimePercent: 0.5, SuccessRatio: 0.4})
	_, _ = store.CreateJob(context.Background(), models.Job{ID: "job-1", Namespace: "default", Image: "nginx", Replicas: 2, CPU: 1, Memory: 128, Priority: 1})
	_, _ = store.CreateService(context.Background(), models.Service{Name: "svc", Namespace: "default", SelectorJobID: "job-1", Port: 80})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-1", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-good"})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-2", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-bad"})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-3", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-degraded"})

	req := httptest.NewRequest(http.MethodGet, "/services/svc/endpoints?namespace=default", nil)
	resp := httptest.NewRecorder()
	s.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	var payload struct {
		Endpoints []map[string]any `json:"endpoints"`
		Selected  map[string]any   `json:"selected"`
		Summary   map[string]any   `json:"summary"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Endpoints) != 2 {
		t.Fatalf("expected healthy and degraded endpoints, got %#v", payload.Endpoints)
	}
	if payload.Endpoints[0]["node_id"] != "node-good" {
		t.Fatalf("expected healthiest endpoint first, got %#v", payload.Endpoints)
	}
	if payload.Selected["node_id"] != "node-good" {
		t.Fatalf("expected healthy selected endpoint, got %#v", payload.Selected)
	}
	if payload.Summary["healthy_endpoints"].(float64) != 1 || payload.Summary["degraded_endpoints"].(float64) != 1 || payload.Summary["unhealthy_endpoints"].(float64) != 1 {
		t.Fatalf("unexpected summary: %#v", payload.Summary)
	}
}

func TestServiceEndpointsPreferHealthierBackends(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())
	_, _ = store.Register(context.Background(), models.Node{ID: "node-best", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.95, HeartbeatCount: 5, UptimePercent: 1, SuccessRatio: 1})
	_, _ = store.Register(context.Background(), models.Node{ID: "node-ok", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.7, HeartbeatCount: 5, UptimePercent: 0.8, SuccessRatio: 0.9})
	_, _ = store.CreateJob(context.Background(), models.Job{ID: "job-1", Namespace: "default", Image: "nginx", Replicas: 2, CPU: 1, Memory: 128, Priority: 1})
	_, _ = store.CreateService(context.Background(), models.Service{Name: "svc", Namespace: "default", SelectorJobID: "job-1", Port: 80})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-best", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-best"})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-ok", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-ok"})

	req := httptest.NewRequest(http.MethodGet, "/services/svc/endpoints?namespace=default", nil)
	resp := httptest.NewRecorder()
	s.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	var payload struct {
		Endpoints []map[string]any `json:"endpoints"`
		Selected  map[string]any   `json:"selected"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(payload.Endpoints) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(payload.Endpoints))
	}
	if payload.Endpoints[0]["node_id"] != "node-best" {
		t.Fatalf("expected healthiest endpoint first, got %#v", payload.Endpoints)
	}
	if payload.Selected["node_id"] != "node-best" {
		t.Fatalf("expected healthiest selected endpoint first, got %#v", payload.Selected)
	}
}

func TestServiceStatusEndpointIncludesSummary(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())
	_, _ = store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.9, HeartbeatCount: 4, UptimePercent: 1, SuccessRatio: 1})
	_, _ = store.CreateJob(context.Background(), models.Job{ID: "job-1", Namespace: "default", Image: "nginx", Replicas: 1, CPU: 1, Memory: 128, Priority: 1})
	_, _ = store.CreateService(context.Background(), models.Service{Name: "svc", Namespace: "default", SelectorJobID: "job-1", Port: 80})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-1", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-1"})

	req := httptest.NewRequest(http.MethodGet, "/services/svc/status?namespace=default", nil)
	resp := httptest.NewRecorder()
	s.Handler().ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	var payload struct {
		Summary   map[string]any   `json:"summary"`
		Endpoints []map[string]any `json:"endpoints"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if payload.Summary["eligible_endpoints"].(float64) != 1 {
		t.Fatalf("unexpected summary payload: %#v", payload.Summary)
	}
	if len(payload.Endpoints) != 1 || payload.Endpoints[0]["health"] != "healthy" {
		t.Fatalf("unexpected endpoints payload: %#v", payload.Endpoints)
	}
}

func TestServiceEndpointsSupportStickySelection(t *testing.T) {
	store := db.NewMemoryStore()
	s := NewServerWithRepos(":0", logger.New("debug", nil), store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())
	_, _ = store.Register(context.Background(), models.Node{ID: "node-a", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.9, HeartbeatCount: 5, UptimePercent: 1, SuccessRatio: 1})
	_, _ = store.Register(context.Background(), models.Node{ID: "node-b", TotalCPU: 4, TotalMemory: 4096, Reliability: 0.85, HeartbeatCount: 5, UptimePercent: 1, SuccessRatio: 1})
	_, _ = store.CreateJob(context.Background(), models.Job{ID: "job-1", Namespace: "default", Image: "nginx", Replicas: 2, CPU: 1, Memory: 128, Priority: 1})
	_, _ = store.CreateService(context.Background(), models.Service{Name: "svc", Namespace: "default", SelectorJobID: "job-1", Port: 80})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-a", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-a"})
	_, _ = store.Create(context.Background(), models.Task{ID: "task-b", Namespace: "default", JobID: "job-1", Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusRunning, NodeID: "node-b"})

	req1 := httptest.NewRequest(http.MethodGet, "/services/svc/endpoints?namespace=default&session=user-123", nil)
	resp1 := httptest.NewRecorder()
	s.Handler().ServeHTTP(resp1, req1)

	req2 := httptest.NewRequest(http.MethodGet, "/services/svc/endpoints?namespace=default&session=user-123", nil)
	resp2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(resp2, req2)

	var payload1 struct {
		Selected map[string]string `json:"selected"`
	}
	var payload2 struct {
		Selected map[string]string `json:"selected"`
	}
	_ = json.Unmarshal(resp1.Body.Bytes(), &payload1)
	_ = json.Unmarshal(resp2.Body.Bytes(), &payload2)
	if payload1.Selected["task_id"] == "" || payload1.Selected["task_id"] != payload2.Selected["task_id"] {
		t.Fatalf("expected sticky selection to be stable, got %#v and %#v", payload1.Selected, payload2.Selected)
	}
}
