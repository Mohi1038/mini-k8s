package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"mini-k8ts/internal/db"
	"mini-k8ts/internal/metrics"
	"mini-k8ts/internal/models"
	"mini-k8ts/internal/task"
	"mini-k8ts/pkg/logger"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	httpServer    *http.Server
	handler       http.Handler
	logger        *logger.Logger
	taskRepo      db.TaskRepository
	nodeRepo      db.NodeRepository
	jobRepo       db.JobRepository
	namespaceRepo db.NamespaceRepository
	serviceRepo   db.ServiceRepository
	secretRepo    db.SecretRepository
	runner        task.Runner
	taskID        func() string
	jobID         func() string
	serviceMu     sync.Mutex
	serviceRR     map[string]int
}

type ServerOptions struct {
	AuthToken       string
	RateLimitRPM    int
	RateLimitBurst  int
	AllowlistPaths  []string
}

func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		AllowlistPaths: []string{"/healthz"},
	}
}

func NewServer(addr string, log *logger.Logger) *Server {
	store := db.NewMemoryStore()
	return NewServerWithRepos(addr, log, store, store, store, store, store, store, store.NextTaskID, store.NextJobID, DefaultServerOptions())
}

func NewServerWithRepos(addr string, log *logger.Logger, taskRepo db.TaskRepository, nodeRepo db.NodeRepository, jobRepo db.JobRepository, namespaceRepo db.NamespaceRepository, serviceRepo db.ServiceRepository, secretRepo db.SecretRepository, taskIDFunc func() string, jobIDFunc func() string, opts ServerOptions) *Server {
	mux := http.NewServeMux()
	server := &Server{
		logger:        log,
		taskRepo:      taskRepo,
		nodeRepo:      nodeRepo,
		jobRepo:       jobRepo,
		namespaceRepo: namespaceRepo,
		serviceRepo:   serviceRepo,
		secretRepo:    secretRepo,
		taskID:        taskIDFunc,
		jobID:         jobIDFunc,
		serviceRR:     map[string]int{},
	}

	// Register Prometheus metrics dynamically against repositories when available.
	if taskRepo != nil && nodeRepo != nil {
		promRegistry := prometheus.NewRegistry()
		promRegistry.MustRegister(metrics.NewClusterCollector(taskRepo, nodeRepo))
		mux.Handle("GET /metrics/prometheus", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
	}

	mux.HandleFunc("GET /healthz", server.handleHealth)
	mux.HandleFunc("GET /metrics", server.handleMetrics)
	mux.HandleFunc("POST /tasks", server.handleSubmitTask)
	mux.HandleFunc("POST /tasks/{taskID}/status", server.handleTaskStatusUpdate)
	mux.HandleFunc("GET /cluster", server.handleClusterState)
	if jobRepo != nil {
		mux.HandleFunc("POST /jobs", server.handleSubmitJob)
		mux.HandleFunc("GET /jobs", server.handleListJobs)
	}
	if namespaceRepo != nil {
		mux.HandleFunc("POST /namespaces", server.handleSubmitNamespace)
		mux.HandleFunc("GET /namespaces", server.handleListNamespaces)
	}
	if secretRepo != nil {
		mux.HandleFunc("POST /secrets", server.handleSubmitSecret)
		mux.HandleFunc("GET /secrets", server.handleListSecrets)
		mux.HandleFunc("GET /secrets/{name}", server.handleGetSecret)
	}
	if serviceRepo != nil {
		mux.HandleFunc("POST /services", server.handleSubmitService)
		mux.HandleFunc("GET /services", server.handleListServices)
		mux.HandleFunc("GET /services/{name}/status", server.handleServiceStatus)
		mux.HandleFunc("GET /services/{name}/endpoints", server.handleServiceEndpoints)
	}
	mux.HandleFunc("GET /insights", server.handleInsights)
	mux.HandleFunc("POST /nodes/register", server.handleRegisterNode)
	mux.HandleFunc("POST /nodes/heartbeat", server.handleNodeHeartbeat)
	mux.HandleFunc("GET /nodes/{nodeID}/tasks/running", server.handleNodeRunningTasks)
	mux.HandleFunc("GET /tasks/{taskID}/logs", server.handleTaskLogs)
	mux.HandleFunc("GET /internal/tasks/{taskID}/logs", server.handleInternalTaskLogs)

	httpServer := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 5 * time.Second,
	}
	allowlist := normalizeAllowlist(opts.AllowlistPaths)
	handler := requestLoggerMiddleware(log, corsMiddleware(rateLimitMiddleware(authMiddleware(mux, authOptions{token: opts.AuthToken, allowPaths: allowlist}), rateLimitOptions{rpm: opts.RateLimitRPM, burst: opts.RateLimitBurst, allowPaths: allowlist})))
	httpServer.Handler = handler
	server.httpServer = httpServer
	server.handler = handler

	return server
}

func normalizeAllowlist(paths []string) map[string]struct{} {
	if len(paths) == 0 {
		return nil
	}
	allow := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			continue
		}
		allow[trimmed] = struct{}{}
	}
	return allow
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) Start() error {
	s.logger.Info("api server starting", "addr", s.httpServer.Addr)

	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("api server shutting down")
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.taskRepo == nil || s.nodeRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "metrics not available on this node"})
		return
	}
	tasks, err := s.taskRepo.ListTasks(r.Context())
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to list tasks for metrics", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list tasks"})
		return
	}

	nodes, err := s.nodeRepo.ListNodes(r.Context())
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to list nodes for metrics", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list nodes"})
		return
	}

	respondJSON(w, http.StatusOK, metrics.ComputeSnapshot(tasks, nodes))
}

func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	var req submitTaskRequest
	// Decode and validate early to keep bad payloads out of scheduling state.
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if strings.TrimSpace(req.Image) == "" || req.CPU <= 0 || req.Memory <= 0 || req.Priority < 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid task payload"})
		return
	}

	namespace := defaultNamespace(req.Namespace)
	if err := s.ensureNamespace(r.Context(), namespace); err != nil {
		s.logger.WithContext(r.Context()).Error("failed to ensure namespace", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to ensure namespace"})
		return
	}
	if ok, err := s.allowNamespaceUsage(r.Context(), namespace, req.CPU, req.Memory, 1); err != nil {
		s.logger.WithContext(r.Context()).Error("failed to check quota", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to check quota"})
		return
	} else if !ok {
		respondJSON(w, http.StatusConflict, map[string]string{"error": "namespace quota exceeded"})
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))

	task := models.Task{
		ID:          s.taskID(),
		Namespace:   namespace,
		Image:       strings.TrimSpace(req.Image),
		Command:     req.Command,
		SecretRefs:  req.SecretRefs,
		Volumes:     req.Volumes,
		CPU:         req.CPU,
		Memory:      req.Memory,
		Priority:    req.Priority,
		Status:      models.TaskStatusPending,
		MaxAttempts: 3,
		IdempotencyKey: idempotencyKey,
	}

	created, err := s.taskRepo.Create(r.Context(), task)
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to create task", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create task"})
		return
	}

	respondJSON(w, http.StatusAccepted, map[string]string{
		"task_id": created.ID,
		"status":  string(created.Status),
	})
}

func (s *Server) handleNodeRunningTasks(w http.ResponseWriter, r *http.Request) {
	nodeID := strings.TrimSpace(r.PathValue("nodeID"))
	if nodeID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing node id"})
		return
	}

	tasks, err := s.taskRepo.ListTasksByNodeAndStatus(r.Context(), nodeID, models.TaskStatusRunning)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list node tasks"})
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) handleTaskStatusUpdate(w http.ResponseWriter, r *http.Request) {
	taskID := strings.TrimSpace(r.PathValue("taskID"))
	if taskID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing task id"})
		return
	}

	var req taskStatusUpdateRequest
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	status := models.TaskStatus(strings.ToUpper(strings.TrimSpace(req.Status)))
	if status != models.TaskStatusCompleted && status != models.TaskStatusFailed {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "status must be COMPLETED or FAILED"})
		return
	}

	taskForNode, err := s.taskRepo.GetTask(r.Context(), taskID)
	if err != nil {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	if err := s.taskRepo.UpdateStatus(r.Context(), taskID, status, taskForNode.NodeID); err != nil {
		if errors.Is(err, db.ErrTaskNotFound) {
			respondJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
			return
		}
		if errors.Is(err, db.ErrInvalidStatusTransition) {
			respondJSON(w, http.StatusConflict, map[string]string{"error": "invalid task status transition"})
			return
		}
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to update task status"})
		return
	}

	if strings.TrimSpace(taskForNode.NodeID) != "" {
		if err := s.nodeRepo.RecordTaskResult(r.Context(), taskForNode.NodeID, status); err != nil {
			s.logger.WithContext(r.Context()).Error("failed to record node task result", "error", err, "task_id", taskID, "node_id", taskForNode.NodeID)
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to record node task result"})
			return
		}
	}

	if allocationRepo := resolveAllocationRepo(s.taskRepo, s.nodeRepo); allocationRepo != nil && (status == models.TaskStatusCompleted || status == models.TaskStatusFailed) {
		if err := allocationRepo.DeleteAllocationsByTask(r.Context(), taskID); err != nil {
			s.logger.WithContext(r.Context()).Error("failed to cleanup task allocation", "error", err, "task_id", taskID)
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to cleanup task allocation"})
			return
		}
	}

	task, err := s.taskRepo.GetTask(r.Context(), taskID)
	if err == nil {
		task.LastError = strings.TrimSpace(req.Error)
		_ = s.taskRepo.UpdateTask(r.Context(), task)
	}

	respondJSON(w, http.StatusOK, map[string]string{"task_id": taskID, "status": string(status)})
}

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request) {
	// Keep cluster reads side-effect free to simplify operator/debug tooling.
	nodes, err := s.nodeRepo.ListNodes(r.Context())
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list nodes"})
		return
	}

	tasks, err := s.taskRepo.ListTasks(r.Context())
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list tasks"})
		return
	}

	tasks = annotateTaskDetails(tasks, nodes)

	respondJSON(w, http.StatusOK, map[string]any{
		"nodes": nodes,
		"tasks": tasks,
	})
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if s.jobRepo == nil || s.jobID == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "jobs are not supported on this node"})
		return
	}

	var req submitJobRequest
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if strings.TrimSpace(req.Image) == "" || req.Replicas <= 0 || req.CPU <= 0 || req.Memory <= 0 || req.Priority < 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid job payload"})
		return
	}

	namespace := defaultNamespace(req.Namespace)
	if err := s.ensureNamespace(r.Context(), namespace); err != nil {
		s.logger.WithContext(r.Context()).Error("failed to ensure namespace", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to ensure namespace"})
		return
	}
	if ok, err := s.allowNamespaceUsage(r.Context(), namespace, req.CPU*req.Replicas, req.Memory*req.Replicas, req.Replicas); err != nil {
		s.logger.WithContext(r.Context()).Error("failed to check quota", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to check quota"})
		return
	} else if !ok {
		respondJSON(w, http.StatusConflict, map[string]string{"error": "namespace quota exceeded"})
		return
	}

	autoscale := req.Autoscale
	if autoscale.Enabled {
		if autoscale.MinReplicas <= 0 {
			autoscale.MinReplicas = 1
		}
		if autoscale.MaxReplicas < autoscale.MinReplicas {
			autoscale.MaxReplicas = autoscale.MinReplicas
		}
		if autoscale.ScaleUpCPU <= 0 {
			autoscale.ScaleUpCPU = 70
		}
		if autoscale.ScaleDownCPU <= 0 {
			autoscale.ScaleDownCPU = 30
		}
		if autoscale.ScaleUpPending <= 0 {
			autoscale.ScaleUpPending = 1
		}
		if autoscale.ScaleUpStep <= 0 {
			autoscale.ScaleUpStep = 1
		}
	}

	job := models.Job{
		ID:         s.jobID(),
		Namespace:  namespace,
		Image:      strings.TrimSpace(req.Image),
		Command:    req.Command,
		SecretRefs: req.SecretRefs,
		Volumes:    req.Volumes,
		Replicas:   req.Replicas,
		CPU:        req.CPU,
		Memory:     req.Memory,
		Priority:   req.Priority,
		Status:     models.JobStatusPending,
		Autoscale:  autoscale,
		IdempotencyKey: strings.TrimSpace(r.Header.Get("X-Idempotency-Key")),
	}

	created, err := s.jobRepo.CreateJob(r.Context(), job)
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to create job", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create job"})
		return
	}

	respondJSON(w, http.StatusAccepted, map[string]any{
		"job_id": created.ID,
		"status": created.Status,
	})
}

func annotateTaskDetails(tasks []models.Task, nodes []models.Node) []models.Task {
	annotated := make([]models.Task, 0, len(tasks))
	for _, task := range tasks {
		task.StatusDetail = deriveTaskStatusDetail(task, nodes)
		annotated = append(annotated, task)
	}
	return annotated
}

func deriveTaskStatusDetail(task models.Task, nodes []models.Node) string {
	lastError := strings.TrimSpace(task.LastError)

	switch task.Status {
	case models.TaskStatusFailed:
		if lastError != "" {
			return lastError
		}
		return "Task execution failed without an explicit error message."
	case models.TaskStatusCompleted:
		return "Task completed successfully."
	case models.TaskStatusRunning:
		if strings.TrimSpace(task.NodeID) != "" {
			return fmt.Sprintf("Task is currently running on %s.", task.NodeID)
		}
		return "Task is currently running."
	case models.TaskStatusPending:
		if !task.NextAttemptAt.IsZero() && task.NextAttemptAt.After(time.Now().UTC()) {
			wait := time.Until(task.NextAttemptAt).Round(time.Second)
			if wait < 0 {
				wait = 0
			}
			if lastError != "" {
				return fmt.Sprintf("Waiting to retry in %s after: %s", wait, lastError)
			}
			return fmt.Sprintf("Waiting to retry in %s.", wait)
		}
		if lastError != "" {
			return fmt.Sprintf("Pending after previous failure: %s", lastError)
		}
		if len(nodes) == 0 {
			return "Waiting for workers to register."
		}
		readyExists := false
		fitsResources := false
		for _, node := range nodes {
			if node.Status != models.NodeStatusReady {
				continue
			}
			readyExists = true
			if node.TotalCPU-node.UsedCPU >= task.CPU && node.TotalMemory-node.UsedMemory >= task.Memory {
				fitsResources = true
				break
			}
		}
		if !readyExists {
			return "Waiting for a READY node."
		}
		if !fitsResources {
			return fmt.Sprintf("Waiting for enough free capacity for %d CPU / %d MB.", task.CPU, task.Memory)
		}
		return "Waiting for scheduler placement."
	default:
		if lastError != "" {
			return lastError
		}
		return ""
	}
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if s.jobRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "jobs are not supported on this node"})
		return
	}

	jobs, err := s.jobRepo.ListJobs(r.Context())
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to list jobs", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list jobs"})
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleSubmitNamespace(w http.ResponseWriter, r *http.Request) {
	if s.namespaceRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "namespaces are not supported on this node"})
		return
	}

	var req submitNamespaceRequest
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing namespace name"})
		return
	}

	ns := models.Namespace{
		Name:        name,
		CPUQuota:    req.CPUQuota,
		MemoryQuota: req.MemoryQuota,
		TaskQuota:   req.TaskQuota,
	}
	created, err := s.namespaceRepo.CreateNamespace(r.Context(), ns)
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to create namespace", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create namespace"})
		return
	}

	respondJSON(w, http.StatusAccepted, created)
}

func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	if s.namespaceRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "namespaces are not supported on this node"})
		return
	}

	list, err := s.namespaceRepo.ListNamespaces(r.Context())
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to list namespaces", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list namespaces"})
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{"namespaces": list})
}

func (s *Server) handleSubmitSecret(w http.ResponseWriter, r *http.Request) {
	if s.secretRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "secrets are not supported on this node"})
		return
	}
	var req submitSecretRequest
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing secret name"})
		return
	}
	namespace := defaultNamespace(req.Namespace)
	if err := s.ensureNamespace(r.Context(), namespace); err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to ensure namespace"})
		return
	}
	secret, err := s.secretRepo.CreateSecret(r.Context(), models.Secret{
		Name:      name,
		Namespace: namespace,
		Data:      req.Data,
	})
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create secret"})
		return
	}
	respondJSON(w, http.StatusAccepted, secret)
}

func (s *Server) handleListSecrets(w http.ResponseWriter, r *http.Request) {
	if s.secretRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "secrets are not supported on this node"})
		return
	}
	namespace := defaultNamespace(r.URL.Query().Get("namespace"))
	list, err := s.secretRepo.ListSecrets(r.Context(), namespace)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list secrets"})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"secrets": list})
}

func (s *Server) handleGetSecret(w http.ResponseWriter, r *http.Request) {
	if s.secretRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "secrets are not supported on this node"})
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	if name == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing secret name"})
		return
	}
	namespace := defaultNamespace(r.URL.Query().Get("namespace"))
	secret, err := s.secretRepo.GetSecret(r.Context(), namespace, name)
	if err != nil {
		if errors.Is(err, db.ErrSecretNotFound) {
			respondJSON(w, http.StatusNotFound, map[string]string{"error": "secret not found"})
			return
		}
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to fetch secret"})
		return
	}
	respondJSON(w, http.StatusOK, secret)
}

func (s *Server) handleSubmitService(w http.ResponseWriter, r *http.Request) {
	if s.serviceRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "services are not supported on this node"})
		return
	}
	var req submitServiceRequest
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing service name"})
		return
	}
	if req.SelectorJobID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing selector job id"})
		return
	}

	svc := models.Service{
		Name:          name,
		Namespace:     defaultNamespace(req.Namespace),
		SelectorJobID: req.SelectorJobID,
		Port:          req.Port,
	}
	created, err := s.serviceRepo.CreateService(r.Context(), svc)
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to create service", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create service"})
		return
	}
	respondJSON(w, http.StatusAccepted, created)
}

func (s *Server) handleListServices(w http.ResponseWriter, r *http.Request) {
	if s.serviceRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "services are not supported on this node"})
		return
	}
	services, err := s.serviceRepo.ListServices(r.Context())
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to list services", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list services"})
		return
	}
	enriched := make([]map[string]any, 0, len(services))
	for _, svc := range services {
		summary, err := s.serviceSummary(r.Context(), svc)
		if err != nil {
			s.logger.WithContext(r.Context()).Error("failed to build service summary", "error", err, "service", svc.Name, "namespace", svc.Namespace)
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build service summary"})
			return
		}
		enriched = append(enriched, map[string]any{
			"name":            svc.Name,
			"namespace":       svc.Namespace,
			"selector_job_id": svc.SelectorJobID,
			"port":            svc.Port,
			"created_at":      svc.CreatedAt,
			"updated_at":      svc.UpdatedAt,
			"summary":         summary,
		})
	}
	respondJSON(w, http.StatusOK, map[string]any{"services": enriched})
}

func (s *Server) handleServiceEndpoints(w http.ResponseWriter, r *http.Request) {
	if s.serviceRepo == nil || s.taskRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "services are not supported on this node"})
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	namespace := defaultNamespace(r.URL.Query().Get("namespace"))
	if name == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing service name"})
		return
	}

	svc, err := s.serviceRepo.GetService(r.Context(), namespace, name)
	if err != nil {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "service not found"})
		return
	}
	endpoints, summary, err := s.serviceSnapshot(r.Context(), svc)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build service endpoints"})
		return
	}

	selected := map[string]any{}
	if len(endpoints) > 0 {
		if stickyKey := firstNonEmpty(strings.TrimSpace(r.URL.Query().Get("session")), strings.TrimSpace(r.Header.Get("X-Service-Key"))); stickyKey != "" {
			index := stickyIndex(stickyKey, len(endpoints))
			selected = endpoints[index]
		} else {
			s.serviceMu.Lock()
			key := namespace + "/" + name
			index := s.serviceRR[key] % len(endpoints)
			s.serviceRR[key] = (index + 1) % len(endpoints)
			selected = endpoints[index]
			s.serviceMu.Unlock()
		}
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"service":   svc,
		"summary":   summary,
		"selected":  selected,
		"endpoints": endpoints,
	})
}

func (s *Server) handleServiceStatus(w http.ResponseWriter, r *http.Request) {
	if s.serviceRepo == nil || s.taskRepo == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "services are not supported on this node"})
		return
	}
	name := strings.TrimSpace(r.PathValue("name"))
	namespace := defaultNamespace(r.URL.Query().Get("namespace"))
	if name == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing service name"})
		return
	}
	svc, err := s.serviceRepo.GetService(r.Context(), namespace, name)
	if err != nil {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "service not found"})
		return
	}
	endpoints, summary, err := s.serviceSnapshot(r.Context(), svc)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build service status"})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{
		"service":   svc,
		"summary":   summary,
		"endpoints": endpoints,
	})
}

func (s *Server) serviceSummary(ctx context.Context, svc models.Service) (map[string]any, error) {
	_, summary, err := s.serviceSnapshot(ctx, svc)
	return summary, err
}

func (s *Server) serviceSnapshot(ctx context.Context, svc models.Service) ([]map[string]any, map[string]any, error) {
	tasks, err := s.taskRepo.ListTasksByJob(ctx, svc.SelectorJobID)
	if err != nil {
		return nil, nil, err
	}

	nodes, err := s.nodeRepo.ListNodes(ctx)
	if err != nil {
		return nil, nil, err
	}
	nodeIndex := make(map[string]models.Node, len(nodes))
	for _, node := range nodes {
		nodeIndex[node.ID] = node
	}

	endpoints := make([]map[string]any, 0)
	totalCandidates := 0
	healthy := 0
	degraded := 0
	unhealthy := 0
	for _, task := range tasks {
		if defaultNamespace(task.Namespace) != svc.Namespace {
			continue
		}
		totalCandidates++
		node, ok := nodeIndex[task.NodeID]
		health := "unhealthy"
		reason := "task is not running"
		score := 0.0
		switch {
		case task.Status != models.TaskStatusRunning:
			health = "unhealthy"
			reason = "task is not running"
		case !ok:
			health = "unhealthy"
			reason = "node is missing"
		case node.Status != models.NodeStatusReady:
			health = "unhealthy"
			reason = "node is not ready"
		default:
			score = node.Reliability + node.UptimePercent + node.SuccessRatio
			switch {
			case node.Reliability >= 0.6 && node.UptimePercent >= 0.6 && node.SuccessRatio >= 0.6:
				health = "healthy"
				reason = "endpoint is healthy"
			case node.Reliability >= 0.2 && node.UptimePercent >= 0.2 && node.SuccessRatio >= 0.2:
				health = "degraded"
				reason = "endpoint is available but node health is weak"
			default:
				health = "unhealthy"
				reason = "node health is below service threshold"
			}
		}
		switch health {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		default:
			unhealthy++
		}
		endpoints = append(endpoints, map[string]any{
			"task_id":      task.ID,
			"node_id":      task.NodeID,
			"task_status":  task.Status,
			"health":       health,
			"health_score": score,
			"reason":       reason,
		})
	}

	sort.SliceStable(endpoints, func(i, j int) bool {
		left, _ := endpoints[i]["health_score"].(float64)
		right, _ := endpoints[j]["health_score"].(float64)
		if left == right {
			return fmt.Sprint(endpoints[i]["task_id"]) < fmt.Sprint(endpoints[j]["task_id"])
		}
		return left > right
	})

	eligible := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint["health"] == "healthy" || endpoint["health"] == "degraded" {
			eligible = append(eligible, endpoint)
		}
	}

	summary := map[string]any{
		"candidate_tasks":     totalCandidates,
		"eligible_endpoints":  len(eligible),
		"healthy_endpoints":   healthy,
		"degraded_endpoints":  degraded,
		"unhealthy_endpoints": unhealthy,
	}
	return eligible, summary, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stickyIndex(key string, size int) int {
	if size <= 0 {
		return 0
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(size))
}

func effectiveNodeSuccessRatio(node models.Node) float64 {
	if node.SuccessCount == 0 && node.FailureCount == 0 && node.SuccessRatio == 0 {
		return 1
	}
	return node.SuccessRatio
}

func (s *Server) handleInsights(w http.ResponseWriter, r *http.Request) {
	insights, err := s.buildInsights(r.Context())
	if err != nil {
		s.logger.WithContext(r.Context()).Error("failed to build insights", "error", err)
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build insights"})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"insights": insights})
}

func (s *Server) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	var req registerNodeRequest
	// Register is idempotent by node_id in memory store: latest payload wins.
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if strings.TrimSpace(req.NodeID) == "" || req.CPU <= 0 || req.Memory <= 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid node payload"})
		return
	}

	node := models.Node{
		ID:          strings.TrimSpace(req.NodeID),
		TotalCPU:    req.CPU,
		TotalMemory: req.Memory,
		Status:      models.NodeStatusReady,
		Reliability: 0.5,
	}

	registered, err := s.nodeRepo.Register(r.Context(), node)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to register node"})
		return
	}

	respondJSON(w, http.StatusCreated, map[string]any{
		"node_id": registered.ID,
		"status":  registered.Status,
	})
}

func (s *Server) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req heartbeatRequest
	// Heartbeat updates liveness timestamp and current resource usage atomically.
	if err := decodeJSON(r, &req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if strings.TrimSpace(req.NodeID) == "" || req.UsedCPU < 0 || req.UsedMemory < 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid heartbeat payload"})
		return
	}

	err := s.nodeRepo.Heartbeat(r.Context(), strings.TrimSpace(req.NodeID), req.UsedCPU, req.UsedMemory)
	if err != nil {
		if errors.Is(err, db.ErrNodeNotFound) {
			respondJSON(w, http.StatusNotFound, map[string]string{"error": "node not found"})
			return
		}
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to process heartbeat"})
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) SetRunner(r task.Runner) {
	s.runner = r
}

func (s *Server) handleTaskLogs(w http.ResponseWriter, r *http.Request) {
	taskID := strings.TrimSpace(r.PathValue("taskID"))
	if taskID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing task id"})
		return
	}

	task, err := s.taskRepo.GetTask(r.Context(), taskID)
	if err != nil {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	if task.NodeID == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "task not assigned to any node"})
		return
	}

	// Proxy to the worker
	// In a real system, we'd lookup the node's actual address.
	// For this demo, we can use the node ID as the hostname (works in Docker Compose)
	url := fmt.Sprintf("http://%s:8080/internal/tasks/%s/logs", task.NodeID, taskID)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build worker logs request"})
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "failed to connect to worker"})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	io.Copy(w, resp.Body)
}

func (s *Server) handleInternalTaskLogs(w http.ResponseWriter, r *http.Request) {
	if s.runner == nil {
		respondJSON(w, http.StatusNotImplemented, map[string]string{"error": "no runner configured on this node"})
		return
	}

	taskID := strings.TrimSpace(r.PathValue("taskID"))
	logs, err := s.runner.Logs(r.Context(), taskID)
	if err != nil {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	defer logs.Close()

	w.Header().Set("Content-Type", "text/plain")
	io.Copy(w, logs)
}

func decodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(r.Body)
	// Relaxed for CLI/Forward compatibility
	// decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid json payload: %w", err)
	}
	return nil
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type submitTaskRequest struct {
	Namespace  string               `json:"namespace"`
	Image      string               `json:"image"`
	Command    []string             `json:"command"`
	SecretRefs []models.SecretRef   `json:"secret_refs"`
	Volumes    []models.VolumeMount `json:"volumes"`
	CPU        int                  `json:"cpu"`
	Memory     int                  `json:"memory"`
	Priority   int                  `json:"priority"`
}

type taskStatusUpdateRequest struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

type registerNodeRequest struct {
	NodeID string `json:"node_id"`
	CPU    int    `json:"cpu"`
	Memory int    `json:"memory"`
}

type heartbeatRequest struct {
	NodeID     string `json:"node_id"`
	UsedCPU    int    `json:"used_cpu"`
	UsedMemory int    `json:"used_memory"`
}

type submitJobRequest struct {
	Namespace  string               `json:"namespace"`
	Image      string               `json:"image"`
	Command    []string             `json:"command"`
	SecretRefs []models.SecretRef   `json:"secret_refs"`
	Volumes    []models.VolumeMount `json:"volumes"`
	Replicas   int                  `json:"replicas"`
	CPU        int                  `json:"cpu"`
	Memory     int                  `json:"memory"`
	Priority   int                  `json:"priority"`
	Autoscale  models.Autoscale     `json:"autoscale"`
}

type submitNamespaceRequest struct {
	Name        string `json:"name"`
	CPUQuota    int    `json:"cpu_quota"`
	MemoryQuota int    `json:"memory_quota"`
	TaskQuota   int    `json:"task_quota"`
}

type submitSecretRequest struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Data      map[string]string `json:"data"`
}

type submitServiceRequest struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	SelectorJobID string `json:"selector_job_id"`
	Port          int    `json:"port"`
}

type insightItem struct {
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion"`
}

func defaultNamespace(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}
	return strings.TrimSpace(value)
}

func (s *Server) ensureNamespace(ctx context.Context, name string) error {
	if s.namespaceRepo == nil {
		return nil
	}
	_, err := s.namespaceRepo.GetNamespace(ctx, name)
	if err == nil {
		return nil
	}
	_, createErr := s.namespaceRepo.CreateNamespace(ctx, models.Namespace{Name: name})
	return createErr
}

func (s *Server) allowNamespaceUsage(ctx context.Context, namespace string, cpu int, mem int, tasks int) (bool, error) {
	if s.namespaceRepo == nil || s.taskRepo == nil {
		return true, nil
	}
	ns, err := s.namespaceRepo.GetNamespace(ctx, namespace)
	if err != nil {
		return true, nil
	}

	allTasks, err := s.taskRepo.ListTasks(ctx)
	if err != nil {
		return false, err
	}

	usedCPU := 0
	usedMem := 0
	usedTasks := 0
	for _, task := range allTasks {
		if defaultNamespace(task.Namespace) != namespace {
			continue
		}
		switch task.Status {
		case models.TaskStatusPending, models.TaskStatusRunning:
			usedCPU += task.CPU
			usedMem += task.Memory
			usedTasks++
		}
	}

	if ns.CPUQuota > 0 && usedCPU+cpu > ns.CPUQuota {
		return false, nil
	}
	if ns.MemoryQuota > 0 && usedMem+mem > ns.MemoryQuota {
		return false, nil
	}
	if ns.TaskQuota > 0 && usedTasks+tasks > ns.TaskQuota {
		return false, nil
	}
	return true, nil
}

func (s *Server) buildInsights(ctx context.Context) ([]insightItem, error) {
	insights := make([]insightItem, 0)

	if s.taskRepo == nil || s.nodeRepo == nil {
		return insights, nil
	}

	nodes, err := s.nodeRepo.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	tasks, err := s.taskRepo.ListTasks(ctx)
	if err != nil {
		return nil, err
	}

	readyNodes := 0
	for _, node := range nodes {
		if node.Status == models.NodeStatusReady {
			readyNodes++
		}
	}
	if readyNodes == 0 {
		insights = append(insights, insightItem{
			Severity:   "error",
			Message:    "No READY nodes are available",
			Suggestion: "Register a worker or restore node heartbeats",
		})
	}

	pending := 0
	failed := 0
	for _, task := range tasks {
		switch task.Status {
		case models.TaskStatusPending:
			pending++
		case models.TaskStatusFailed:
			failed++
			if strings.Contains(strings.ToLower(task.LastError), "timeout") {
				insights = append(insights, insightItem{
					Severity:   "warn",
					Message:    fmt.Sprintf("Task %s failed due to timeout", task.ID),
					Suggestion: "Increase task timeout or reduce workload size",
				})
			}
		}
	}
	if pending > 0 && readyNodes == 0 {
		insights = append(insights, insightItem{
			Severity:   "warn",
			Message:    "Pending tasks exist but no READY nodes",
			Suggestion: "Scale workers or recover node connectivity",
		})
	}
	if failed > 0 {
		insights = append(insights, insightItem{
			Severity:   "info",
			Message:    fmt.Sprintf("%d tasks failed recently", failed),
			Suggestion: "Inspect task errors for missing images or bad commands",
		})
	}

	snapshot := metrics.ComputeSnapshot(tasks, nodes)
	if snapshot.FragmentationIndex > 0.45 {
		insights = append(insights, insightItem{
			Severity:   "warn",
			Message:    fmt.Sprintf("High fragmentation detected (index %.2f)", snapshot.FragmentationIndex),
			Suggestion: "Consider resizing tasks or enabling binpacking-friendly workloads",
		})
	}
	if snapshot.BinpackingWaste > 0.35 {
		insights = append(insights, insightItem{
			Severity:   "info",
			Message:    fmt.Sprintf("Resource waste trending high (%.2f)", snapshot.BinpackingWaste),
			Suggestion: "Co-locate smaller tasks or scale down oversized nodes",
		})
	}

	lowReliability := 0
	lowSuccess := 0
	for _, node := range nodes {
		if node.Status != models.NodeStatusReady {
			continue
		}
		if node.Reliability < 0.4 {
			lowReliability++
		}
		if effectiveNodeSuccessRatio(node) < 0.5 {
			lowSuccess++
		}
	}
	if lowReliability > 0 {
		insights = append(insights, insightItem{
			Severity:   "warn",
			Message:    fmt.Sprintf("%d READY nodes have low reliability", lowReliability),
			Suggestion: "Investigate node churn or re-register unstable workers",
		})
	}
	if lowSuccess > 0 {
		insights = append(insights, insightItem{
			Severity:   "warn",
			Message:    fmt.Sprintf("%d READY nodes report low success ratios", lowSuccess),
			Suggestion: "Check node logs for repeated task failures",
		})
	}

	if s.jobRepo != nil {
		jobs, err := s.jobRepo.ListJobs(ctx)
		if err != nil {
			return nil, err
		}
		for _, job := range jobs {
			tasksForJob, err := s.taskRepo.ListTasksByJob(ctx, job.ID)
			if err != nil {
				return nil, err
			}
			active := 0
			for _, task := range tasksForJob {
				if task.Status == models.TaskStatusPending || task.Status == models.TaskStatusRunning {
					active++
				}
			}
			if active < job.Replicas {
				insights = append(insights, insightItem{
					Severity:   "warn",
					Message:    fmt.Sprintf("Job %s has %d/%d active replicas", job.ID, active, job.Replicas),
					Suggestion: "Check scheduler capacity or quota limits",
				})
			}
		}
	}

	return insights, nil
}

func resolveAllocationRepo(taskRepo db.TaskRepository, nodeRepo db.NodeRepository) db.AllocationRepository {
	if allocationRepo, ok := taskRepo.(db.AllocationRepository); ok {
		return allocationRepo
	}
	if allocationRepo, ok := nodeRepo.(db.AllocationRepository); ok {
		return allocationRepo
	}
	return nil
}
