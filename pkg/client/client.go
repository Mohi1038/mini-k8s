package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"mini-k8ts/internal/metrics"
	"mini-k8ts/internal/models"
)

type SchedulerClient struct {
	baseURL    string
	httpClient *http.Client
	authToken  string
}

func NewSchedulerClient(baseURL string) *SchedulerClient {
	cleanBaseURL := strings.TrimRight(baseURL, "/")
	return &SchedulerClient{
		baseURL: cleanBaseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func NewSchedulerClientWithToken(baseURL string, authToken string) *SchedulerClient {
	client := NewSchedulerClient(baseURL)
	client.SetAuthToken(authToken)
	return client
}

func (c *SchedulerClient) SetAuthToken(token string) {
	c.authToken = strings.TrimSpace(token)
}

func (c *SchedulerClient) RegisterNode(ctx context.Context, nodeID string, cpu int, memory int) error {
	payload := map[string]any{
		"node_id": nodeID,
		"cpu":     cpu,
		"memory":  memory,
	}
	return c.postJSON(ctx, "/nodes/register", payload)
}

func (c *SchedulerClient) SendHeartbeat(ctx context.Context, nodeID string, usedCPU int, usedMemory int) error {
	payload := map[string]any{
		"node_id":     nodeID,
		"used_cpu":    usedCPU,
		"used_memory": usedMemory,
	}
	return c.postJSON(ctx, "/nodes/heartbeat", payload)
}

func (c *SchedulerClient) GetRunningTasks(ctx context.Context, nodeID string) ([]models.Task, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/nodes/%s/tasks/running", c.baseURL, nodeID), nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(request)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode > 299 {
		responseBody, _ := io.ReadAll(response.Body)
		return nil, fmt.Errorf("failed to fetch running tasks with status %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var payload struct {
		Tasks []models.Task `json:"tasks"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return payload.Tasks, nil
}

func (c *SchedulerClient) UpdateTaskStatus(ctx context.Context, taskID string, status models.TaskStatus, taskErr string) error {
	payload := map[string]any{
		"status": string(status),
		"error":  taskErr,
	}
	return c.postJSON(ctx, fmt.Sprintf("/tasks/%s/status", taskID), payload)
}

func (c *SchedulerClient) applyAuth(req *http.Request) {
	if strings.TrimSpace(c.authToken) == "" {
		return
	}
	req.Header.Set("X-Service-Key", c.authToken)
}

func (c *SchedulerClient) postJSON(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	responseBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("scheduler API %s failed with status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(responseBody)))
}

func (c *SchedulerClient) GetClusterState(ctx context.Context) ([]models.Node, []models.Task, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/cluster", nil)
	if err != nil {
		return nil, nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	var payload struct {
		Nodes []models.Node `json:"nodes"`
		Tasks []models.Task `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, nil, err
	}
	return payload.Nodes, payload.Tasks, nil
}

func (c *SchedulerClient) SubmitTask(ctx context.Context, t models.Task) (string, error) {
	// Map to the API request structure exactly
	payload := map[string]any{
		"namespace":   t.Namespace,
		"image":       t.Image,
		"command":     t.Command,
		"secret_refs": t.SecretRefs,
		"volumes":     t.Volumes,
		"cpu":         t.CPU,
		"memory":      t.Memory,
		"priority":    t.Priority,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/tasks", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to submit task: %s", string(b))
	}

	var respPayload struct {
		TaskID string `json:"task_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	return respPayload.TaskID, nil
}

func (c *SchedulerClient) CreateJob(ctx context.Context, job models.Job) (string, error) {
	payload := map[string]any{
		"namespace":   job.Namespace,
		"image":       job.Image,
		"command":     job.Command,
		"secret_refs": job.SecretRefs,
		"volumes":     job.Volumes,
		"replicas":    job.Replicas,
		"cpu":         job.CPU,
		"memory":      job.Memory,
		"priority":    job.Priority,
		"autoscale":   job.Autoscale,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jobs", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to submit job: %s", string(b))
	}

	var respPayload struct {
		JobID string `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	return respPayload.JobID, nil
}

func (c *SchedulerClient) GetJobs(ctx context.Context) ([]models.Job, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/jobs", nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch jobs: %s", string(b))
	}

	var payload struct {
		Jobs []models.Job `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Jobs, nil
}

func (c *SchedulerClient) CreateNamespace(ctx context.Context, ns models.Namespace) (models.Namespace, error) {
	payload := map[string]any{
		"name":         ns.Name,
		"cpu_quota":    ns.CPUQuota,
		"memory_quota": ns.MemoryQuota,
		"task_quota":   ns.TaskQuota,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return models.Namespace{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/namespaces", bytes.NewReader(body))
	if err != nil {
		return models.Namespace{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return models.Namespace{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return models.Namespace{}, fmt.Errorf("failed to create namespace: %s", string(b))
	}
	var created models.Namespace
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return models.Namespace{}, err
	}
	return created, nil
}

func (c *SchedulerClient) GetNamespaces(ctx context.Context) ([]models.Namespace, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/namespaces", nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch namespaces: %s", string(b))
	}
	var payload struct {
		Namespaces []models.Namespace `json:"namespaces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Namespaces, nil
}

func (c *SchedulerClient) CreateService(ctx context.Context, svc models.Service) (models.Service, error) {
	payload := map[string]any{
		"name":            svc.Name,
		"namespace":       svc.Namespace,
		"selector_job_id": svc.SelectorJobID,
		"port":            svc.Port,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return models.Service{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/services", bytes.NewReader(body))
	if err != nil {
		return models.Service{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return models.Service{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return models.Service{}, fmt.Errorf("failed to create service: %s", string(b))
	}
	var created models.Service
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return models.Service{}, err
	}
	return created, nil
}

func (c *SchedulerClient) GetServices(ctx context.Context) ([]models.Service, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/services", nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch services: %s", string(b))
	}
	var payload struct {
		Services []models.Service `json:"services"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Services, nil
}

type ServiceEndpoints struct {
	Service   models.Service   `json:"service"`
	Summary   map[string]any   `json:"summary"`
	Selected  map[string]any   `json:"selected"`
	Endpoints []map[string]any `json:"endpoints"`
}

func (c *SchedulerClient) GetServiceEndpoints(ctx context.Context, namespace string, name string, sessionKey string) (ServiceEndpoints, error) {
	params := url.Values{}
	if strings.TrimSpace(namespace) != "" {
		params.Set("namespace", namespace)
	}
	if strings.TrimSpace(sessionKey) != "" {
		params.Set("session", sessionKey)
	}
	query := ""
	if encoded := params.Encode(); encoded != "" {
		query = "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/services/%s/endpoints%s", c.baseURL, name, query), nil)
	if err != nil {
		return ServiceEndpoints{}, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ServiceEndpoints{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return ServiceEndpoints{}, fmt.Errorf("failed to fetch service endpoints: %s", string(b))
	}
	var payload ServiceEndpoints
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return ServiceEndpoints{}, err
	}
	return payload, nil
}

func (c *SchedulerClient) CreateSecret(ctx context.Context, secret models.Secret) (models.Secret, error) {
	payload := map[string]any{
		"name":      secret.Name,
		"namespace": secret.Namespace,
		"data":      secret.Data,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return models.Secret{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/secrets", bytes.NewReader(body))
	if err != nil {
		return models.Secret{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return models.Secret{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return models.Secret{}, fmt.Errorf("failed to create secret: %s", string(b))
	}
	var created models.Secret
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return models.Secret{}, err
	}
	return created, nil
}

func (c *SchedulerClient) GetSecrets(ctx context.Context, namespace string) ([]models.Secret, error) {
	query := ""
	if strings.TrimSpace(namespace) != "" {
		query = "?namespace=" + url.QueryEscape(namespace)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/secrets"+query, nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch secrets: %s", string(b))
	}
	var payload struct {
		Secrets []models.Secret `json:"secrets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Secrets, nil
}

func (c *SchedulerClient) GetSecret(ctx context.Context, namespace string, name string) (models.Secret, error) {
	query := ""
	if strings.TrimSpace(namespace) != "" {
		query = "?namespace=" + url.QueryEscape(namespace)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/secrets/"+url.PathEscape(name)+query, nil)
	if err != nil {
		return models.Secret{}, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return models.Secret{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return models.Secret{}, fmt.Errorf("failed to fetch secret: %s", string(b))
	}
	var secret models.Secret
	if err := json.NewDecoder(resp.Body).Decode(&secret); err != nil {
		return models.Secret{}, err
	}
	return secret, nil
}

type Insight struct {
	Severity   string `json:"severity"`
	Message    string `json:"message"`
	Suggestion string `json:"suggestion"`
}

func (c *SchedulerClient) GetInsights(ctx context.Context) ([]Insight, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/insights", nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch insights: %s", string(b))
	}
	var payload struct {
		Insights []Insight `json:"insights"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Insights, nil
}

func (c *SchedulerClient) GetLogs(ctx context.Context, taskID string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/tasks/%s/logs", c.baseURL, taskID), nil)
	if err != nil {
		return nil, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("failed to get logs: status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

func (c *SchedulerClient) GetTask(ctx context.Context, taskID string) (models.Task, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/cluster", c.baseURL), nil)
	if err != nil {
		return models.Task{}, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return models.Task{}, err
	}
	defer resp.Body.Close()

	var payload struct {
		Tasks []models.Task `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return models.Task{}, err
	}

	for _, t := range payload.Tasks {
		if t.ID == taskID {
			return t, nil
		}
	}
	return models.Task{}, fmt.Errorf("task %s not found", taskID)
}

func (c *SchedulerClient) GetMetrics(ctx context.Context) (metrics.Snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/metrics", nil)
	if err != nil {
		return metrics.Snapshot{}, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return metrics.Snapshot{}, err
	}
	defer resp.Body.Close()

	var snapshot metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return metrics.Snapshot{}, err
	}
	return snapshot, nil
}

func (c *SchedulerClient) GetNode(ctx context.Context, nodeID string) (models.Node, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/cluster", nil)
	if err != nil {
		return models.Node{}, err
	}
	c.applyAuth(req)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return models.Node{}, err
	}
	defer resp.Body.Close()

	var payload struct {
		Nodes []models.Node `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return models.Node{}, err
	}

	for _, n := range payload.Nodes {
		if n.ID == nodeID {
			return n, nil
		}
	}
	return models.Node{}, fmt.Errorf("node %s not found", nodeID)
}
