package scheduler

import (
	"context"
	"errors"
	"sort"
	"time"

	"fmt"
	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
	"mini-k8ts/pkg/logger"
	"strings"
)

type Service struct {
	taskRepo       db.TaskRepository
	nodeRepo       db.NodeRepository
	jobRepo        db.JobRepository
	namespaceRepo  db.NamespaceRepository
	allocationRepo db.AllocationRepository
	strategy       Strategy
	logger         *logger.Logger
	interval       time.Duration
	taskID         func() string
	elector        LeaderElector
	leaderActive   bool
	pendingTimeout time.Duration
	runningTimeout time.Duration
}

func NewService(taskRepo db.TaskRepository, nodeRepo db.NodeRepository, jobRepo db.JobRepository, namespaceRepo db.NamespaceRepository, strategy Strategy, log *logger.Logger, interval time.Duration, taskIDFunc func() string) *Service {
	if interval <= 0 {
		interval = 2 * time.Second
	}

	return &Service{
		taskRepo:       taskRepo,
		nodeRepo:       nodeRepo,
		jobRepo:        jobRepo,
		namespaceRepo:  namespaceRepo,
		allocationRepo: resolveAllocationRepo(taskRepo, nodeRepo),
		strategy:       strategy,
		logger:         log,
		interval:       interval,
		taskID:         taskIDFunc,
		pendingTimeout: 10 * time.Minute,
		runningTimeout: 20 * time.Minute,
	}
}

func (s *Service) SetLeaderElector(elector LeaderElector) {
	s.elector = elector
}

func (s *Service) SetStuckTaskTimeouts(pending time.Duration, running time.Duration) {
	if pending > 0 {
		s.pendingTimeout = pending
	}
	if running > 0 {
		s.runningTimeout = running
	}
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.logger.Info("scheduler loop started", "interval", s.interval.String())
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler loop stopped")
			return
		case <-ticker.C:
			if s.elector != nil {
				isLeader, err := s.elector.IsLeader(ctx)
				if err != nil {
					s.logger.Error("leader election check failed", "error", err)
					continue
				}
				if isLeader != s.leaderActive {
					s.leaderActive = isLeader
					if isLeader {
						s.logger.Info("scheduler leadership acquired")
					} else {
						s.logger.Info("scheduler leadership lost")
					}
				}
				if !isLeader {
					continue
				}
			}
			if err := s.ReconcileState(ctx); err != nil {
				s.logger.Error("state reconcile cycle failed", "error", err)
			}
			if err := s.ReconcileStuckTasks(ctx); err != nil {
				s.logger.Error("stuck task reconcile cycle failed", "error", err)
			}
			if err := s.RecoverOnce(ctx); err != nil {
				s.logger.Error("failure recovery cycle failed", "error", err)
			}
			if err := s.ReconcileJobs(ctx); err != nil {
				s.logger.Error("job reconcile cycle failed", "error", err)
			}
			if err := s.ScheduleOnce(ctx); err != nil {
				s.logger.Error("scheduler cycle failed", "error", err)
			}
		}
	}
}

func (s *Service) ReconcileState(ctx context.Context) error {
	if s.taskRepo == nil || s.nodeRepo == nil {
		return nil
	}

	tasks, err := s.taskRepo.ListTasks(ctx)
	if err != nil {
		return err
	}
	nodes, err := s.nodeRepo.ListNodes(ctx)
	if err != nil {
		return err
	}

	nodeStatus := make(map[string]models.NodeStatus, len(nodes))
	for _, node := range nodes {
		nodeStatus[node.ID] = node.Status
	}

	for i, task := range tasks {
		switch task.Status {
		case models.TaskStatusPending:
			if strings.TrimSpace(task.NodeID) != "" {
				task.NodeID = ""
				task.ScheduledAt = time.Time{}
				if err := s.taskRepo.UpdateTask(ctx, task); err != nil {
					return err
				}
				tasks[i] = task
				if s.allocationRepo != nil {
					if err := s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID); err != nil {
						return err
					}
				}
			}
		case models.TaskStatusRunning:
			if strings.TrimSpace(task.NodeID) == "" {
				task.Status = models.TaskStatusPending
				task.ScheduledAt = time.Time{}
				if err := s.taskRepo.UpdateTask(ctx, task); err != nil {
					return err
				}
				tasks[i] = task
				if s.allocationRepo != nil {
					if err := s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID); err != nil {
						return err
					}
				}
				continue
			}
			if status, ok := nodeStatus[task.NodeID]; !ok || status != models.NodeStatusReady {
				task.Status = models.TaskStatusPending
				task.NodeID = ""
				task.ScheduledAt = time.Time{}
				if err := s.taskRepo.UpdateTask(ctx, task); err != nil {
					return err
				}
				tasks[i] = task
				if s.allocationRepo != nil {
					if err := s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID); err != nil {
						return err
					}
				}
				continue
			}
			if s.allocationRepo != nil {
				if err := s.allocationRepo.CreateAllocation(ctx, models.Allocation{TaskID: task.ID, NodeID: task.NodeID}); err != nil {
					return err
				}
			}
		case models.TaskStatusCompleted, models.TaskStatusFailed:
			if s.allocationRepo != nil {
				if err := s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID); err != nil {
					return err
				}
			}
		}
	}

	if s.allocationRepo == nil {
		return nil
	}
	runningByNode := make(map[string]map[string]struct{})
	for _, task := range tasks {
		if task.Status != models.TaskStatusRunning || strings.TrimSpace(task.NodeID) == "" {
			continue
		}
		if runningByNode[task.NodeID] == nil {
			runningByNode[task.NodeID] = map[string]struct{}{}
		}
		runningByNode[task.NodeID][task.ID] = struct{}{}
	}

	for _, node := range nodes {
		allocations, err := s.allocationRepo.ListByNode(ctx, node.ID)
		if err != nil {
			return err
		}
		for _, allocation := range allocations {
			if _, ok := runningByNode[node.ID][allocation.TaskID]; ok {
				continue
			}
			if err := s.allocationRepo.DeleteAllocation(ctx, allocation.TaskID, allocation.NodeID); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) ReconcileStuckTasks(ctx context.Context) error {
	if s.taskRepo == nil {
		return nil
	}
	if s.pendingTimeout <= 0 && s.runningTimeout <= 0 {
		return nil
	}

	now := time.Now().UTC()
	tasks, err := s.taskRepo.ListTasks(ctx)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		switch task.Status {
		case models.TaskStatusPending:
			if s.pendingTimeout <= 0 {
				continue
			}
			if !task.NextAttemptAt.IsZero() && task.NextAttemptAt.After(now) {
				continue
			}
			if now.Sub(task.UpdatedAt) < s.pendingTimeout {
				continue
			}
			if strings.TrimSpace(task.LastError) == "" {
				task.LastError = fmt.Sprintf("pending for %s without placement", s.pendingTimeout)
			}
			if strings.TrimSpace(task.NodeID) != "" {
				task.NodeID = ""
				task.ScheduledAt = time.Time{}
			}
			if err := s.taskRepo.UpdateTask(ctx, task); err != nil {
				return err
			}
		case models.TaskStatusRunning:
			if s.runningTimeout <= 0 {
				continue
			}
			start := task.ScheduledAt
			if start.IsZero() {
				start = task.UpdatedAt
			}
			if now.Sub(start) < s.runningTimeout {
				continue
			}
			previousNodeID := task.NodeID
			if s.allocationRepo != nil {
				if err := s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID); err != nil {
					return err
				}
			}
			if strings.TrimSpace(previousNodeID) != "" {
				if err := s.nodeRepo.RecordTaskResult(ctx, previousNodeID, models.TaskStatusFailed); err != nil {
					return err
				}
			}
			task.Attempts++
			task.NodeID = ""
			task.ScheduledAt = time.Time{}
			if task.MaxAttempts <= 0 {
				task.MaxAttempts = 3
			}

			reason := fmt.Sprintf("task exceeded running timeout %s", s.runningTimeout)
			if task.Attempts >= task.MaxAttempts {
				task.Status = models.TaskStatusFailed
				task.LastError = fmt.Sprintf("max retry attempts reached after %s", reason)
			} else {
				task.Status = models.TaskStatusPending
				task.NextAttemptAt = now.Add(backoffDuration(task.Attempts))
				task.LastError = fmt.Sprintf("requeued after %s", reason)
			}

			if err := s.taskRepo.UpdateTask(ctx, task); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) ReconcileJobs(ctx context.Context) error {
	if s.jobRepo == nil {
		return nil
	}
	if s.taskID == nil {
		return errors.New("task id generator not configured")
	}

	jobs, err := s.jobRepo.ListJobs(ctx)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if err := s.applyAutoscale(ctx, &job); err != nil {
			return err
		}

		tasks, err := s.taskRepo.ListTasksByJob(ctx, job.ID)
		if err != nil {
			return err
		}

		completed := 0
		failed := 0
		active := 0
		for _, task := range tasks {
			switch task.Status {
			case models.TaskStatusCompleted:
				completed++
			case models.TaskStatusFailed:
				failed++
			case models.TaskStatusRunning, models.TaskStatusPending:
				active++
			}
		}

		desired := job.Replicas
		missing := desired - (completed + active)
		for i := 0; i < missing; i++ {
			if ok, err := s.canCreateInNamespace(ctx, job.Namespace, job.CPU, job.Memory, 1); err != nil {
				return err
			} else if !ok {
				s.logger.Warn("job task skipped due to namespace quota", "job_id", job.ID, "namespace", job.Namespace)
				break
			}

			newTask := models.Task{
				ID:          s.taskID(),
				Image:       job.Image,
				Command:     job.Command,
				SecretRefs:  job.SecretRefs,
				Volumes:     job.Volumes,
				CPU:         job.CPU,
				Memory:      job.Memory,
				Priority:    job.Priority,
				Status:      models.TaskStatusPending,
				JobID:       job.ID,
				Namespace:   defaultNamespace(job.Namespace),
				MaxAttempts: 3,
			}
			if _, err := s.taskRepo.Create(ctx, newTask); err != nil {
				return err
			}
			active++
		}

		newStatus := models.JobStatusPending
		switch {
		case completed >= desired:
			newStatus = models.JobStatusCompleted
		case active > 0 || completed > 0:
			newStatus = models.JobStatusRunning
		case failed > 0:
			newStatus = models.JobStatusFailed
		}

		if job.Status != newStatus {
			job.Status = newStatus
			if err := s.jobRepo.UpdateJob(ctx, job); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) RecoverOnce(ctx context.Context) error {
	markedDown, err := s.nodeRepo.MarkDownInactive(ctx)
	if err != nil {
		return err
	}

	if markedDown == 0 {
		return nil
	}

	nodes, err := s.nodeRepo.ListNodes(ctx)
	if err != nil {
		return err
	}

	downNodeIDs := map[string]struct{}{}
	for _, node := range nodes {
		if node.Status == models.NodeStatusDown {
			downNodeIDs[node.ID] = struct{}{}
		}
	}

	tasks, err := s.taskRepo.ListTasks(ctx)
	if err != nil {
		return err
	}

	requeued := 0
	for _, task := range tasks {
		if task.Status != models.TaskStatusRunning && task.Status != models.TaskStatusFailed {
			continue
		}

		shouldRequeue := false
		reason := ""

		// Scenario 1: Node went down while task was running
		if task.Status == models.TaskStatusRunning {
			if _, ok := downNodeIDs[task.NodeID]; ok {
				shouldRequeue = true
				reason = "assigned node became unavailable"
			}
		}

		// Scenario 2: Task failed but has retries left (Self-Healing)
		if task.Status == models.TaskStatusFailed && task.Attempts < task.MaxAttempts {
			// Check if it's a real failure (not max attempts hit)
			if !strings.Contains(task.LastError, "max retry attempts reached") {
				shouldRequeue = true
				reason = fmt.Sprintf("task failed (last error: %s)", task.LastError)
			}
		}

		if shouldRequeue {
			previousNodeID := task.NodeID
			if s.allocationRepo != nil {
				if err := s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID); err != nil {
					return err
				}
			}
			if strings.TrimSpace(previousNodeID) != "" {
				if err := s.nodeRepo.RecordTaskResult(ctx, previousNodeID, models.TaskStatusFailed); err != nil {
					return err
				}
			}
			task.Attempts++
			task.NodeID = ""
			task.ScheduledAt = time.Time{}
			if task.MaxAttempts <= 0 {
				task.MaxAttempts = 3
			}

			if task.Attempts >= task.MaxAttempts {
				task.Status = models.TaskStatusFailed
				task.LastError = fmt.Sprintf("max retry attempts reached after %s", reason)
			} else {
				task.Status = models.TaskStatusPending
				task.NextAttemptAt = time.Now().UTC().Add(backoffDuration(task.Attempts))
				task.LastError = fmt.Sprintf("requeued after %s", reason)
			}

			if err := s.taskRepo.UpdateTask(ctx, task); err != nil {
				return err
			}
			requeued++
			s.logger.Warn("task rescheduled", "task_id", task.ID, "reason", reason, "attempt", task.Attempts, "prev_node", previousNodeID)
		}
	}

	s.logger.Warn("failure recovery completed", "nodes_marked_down", markedDown, "tasks_requeued", requeued)
	return nil
}

func (s *Service) ScheduleOnce(ctx context.Context) error {
	pendingTasks, err := s.taskRepo.ListPending(ctx)
	if err != nil {
		return err
	}
	if len(pendingTasks) == 0 {
		s.logger.Debug("no pending tasks found")
		return nil
	}

	pendingTasks = fairOrderPendingTasks(pendingTasks)

	nodes, err := s.nodeRepo.ListNodes(ctx)
	if err != nil {
		return err
	}

	// 2. Local resource tracking (Shadow map)
	// This prevents over-scheduling on a node before the next heartbeat.
	nodeResources := make(map[string]struct {
		freeCPU int
		freeMem int
	})
	for _, n := range nodes {
		nodeResources[n.ID] = struct {
			freeCPU int
			freeMem int
		}{
			freeCPU: n.TotalCPU - n.UsedCPU,
			freeMem: n.TotalMemory - n.UsedMemory,
		}
	}

	for _, task := range pendingTasks {
		if !task.NextAttemptAt.IsZero() && task.NextAttemptAt.After(time.Now().UTC()) {
			continue
		}

		// Update nodes with CURRENT local resources for the strategy to use
		for i := range nodes {
			res := nodeResources[nodes[i].ID]
			nodes[i].UsedCPU = nodes[i].TotalCPU - res.freeCPU
			nodes[i].UsedMemory = nodes[i].TotalMemory - res.freeMem
		}

		selectedNode, scheduleErr := s.strategy.Schedule(task, nodes)
		if scheduleErr != nil {
			if errors.Is(scheduleErr, ErrNoEligibleNode) {
				s.logger.Warn("no eligible node for task", "task_id", task.ID, "priority", task.Priority)
				continue
			}
			return scheduleErr
		}

		// 3. Immediately subtract resources from our local "shadow" map
		res := nodeResources[selectedNode.ID]
		res.freeCPU -= task.CPU
		res.freeMem -= task.Memory
		nodeResources[selectedNode.ID] = res

		if writer, ok := s.taskRepo.(db.TaskAllocationWriter); ok && s.allocationRepo != nil {
			if err := writer.UpdateStatusAndAllocation(ctx, task.ID, models.TaskStatusRunning, selectedNode.ID); err != nil {
				return err
			}
		} else {
			if err := s.taskRepo.UpdateStatus(ctx, task.ID, models.TaskStatusRunning, selectedNode.ID); err != nil {
				return err
			}
			if s.allocationRepo != nil {
				if err := s.allocationRepo.CreateAllocation(ctx, models.Allocation{TaskID: task.ID, NodeID: selectedNode.ID}); err != nil {
					_ = s.taskRepo.UpdateStatus(ctx, task.ID, models.TaskStatusPending, "")
					_ = s.allocationRepo.DeleteAllocationsByTask(ctx, task.ID)
					return err
				}
			}
		}

		s.logger.Info(
			"task scheduled",
			"task_id", task.ID,
			"node_id", selectedNode.ID,
			"task_priority", task.Priority,
			"remaining_node_cpu", res.freeCPU,
		)
	}

	return nil
}

func (s *Service) applyAutoscale(ctx context.Context, job *models.Job) error {
	if job == nil {
		return nil
	}
	if !job.Autoscale.Enabled {
		return nil
	}
	if job.Status == models.JobStatusCompleted || job.Status == models.JobStatusFailed {
		return nil
	}

	minReplicas := job.Autoscale.MinReplicas
	maxReplicas := job.Autoscale.MaxReplicas
	if minReplicas <= 0 {
		minReplicas = 1
	}
	if maxReplicas < minReplicas {
		maxReplicas = minReplicas
	}

	if s.taskRepo == nil {
		return nil
	}

	tasks, err := s.taskRepo.ListTasksByJob(ctx, job.ID)
	if err != nil {
		return err
	}

	pending := 0
	running := 0
	for _, task := range tasks {
		switch task.Status {
		case models.TaskStatusPending:
			pending++
		case models.TaskStatusRunning:
			running++
		}
	}

	newReplicas := job.Replicas
	if pending >= max(1, job.Autoscale.ScaleUpPending) {
		step := job.Autoscale.ScaleUpStep
		if step <= 0 {
			step = 1
		}
		newReplicas = job.Replicas + step
	} else if job.Autoscale.ScaleDownCPU > 0 {
		util, err := s.clusterCPUUtilization(ctx)
		if err != nil {
			return err
		}
		if util >= 0 && util <= job.Autoscale.ScaleDownCPU {
			newReplicas = job.Replicas - 1
		}
	}

	if newReplicas < minReplicas {
		newReplicas = minReplicas
	}
	if newReplicas > maxReplicas {
		newReplicas = maxReplicas
	}
	if newReplicas == job.Replicas {
		return nil
	}

	job.Replicas = newReplicas
	job.UpdatedAt = time.Now().UTC()
	if s.jobRepo == nil {
		return nil
	}
	return s.jobRepo.UpdateJob(ctx, *job)
}

func (s *Service) clusterCPUUtilization(ctx context.Context) (int, error) {
	if s.nodeRepo == nil {
		return -1, nil
	}
	nodes, err := s.nodeRepo.ListNodes(ctx)
	if err != nil {
		return -1, err
	}
	totalCPU := 0
	usedCPU := 0
	for _, node := range nodes {
		if node.Status != models.NodeStatusReady {
			continue
		}
		totalCPU += node.TotalCPU
		usedCPU += node.UsedCPU
	}
	if totalCPU == 0 {
		return -1, nil
	}
	return int(float64(usedCPU) / float64(totalCPU) * 100.0), nil
}

func defaultNamespace(value string) string {
	if strings.TrimSpace(value) == "" {
		return "default"
	}
	return strings.TrimSpace(value)
}

func (s *Service) canCreateInNamespace(ctx context.Context, namespace string, cpu int, mem int, tasks int) (bool, error) {
	if s.namespaceRepo == nil || s.taskRepo == nil {
		return true, nil
	}

	ns, err := s.namespaceRepo.GetNamespace(ctx, defaultNamespace(namespace))
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
		if defaultNamespace(task.Namespace) != ns.Name {
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

func resolveAllocationRepo(taskRepo db.TaskRepository, nodeRepo db.NodeRepository) db.AllocationRepository {
	if allocationRepo, ok := taskRepo.(db.AllocationRepository); ok {
		return allocationRepo
	}
	if allocationRepo, ok := nodeRepo.(db.AllocationRepository); ok {
		return allocationRepo
	}
	return nil
}

func fairOrderPendingTasks(tasks []models.Task) []models.Task {
	if len(tasks) <= 1 {
		return tasks
	}

	priorityBuckets := make(map[int]map[string][]models.Task)
	priorities := make([]int, 0)
	seenPriority := map[int]struct{}{}
	for _, task := range tasks {
		if _, ok := priorityBuckets[task.Priority]; !ok {
			priorityBuckets[task.Priority] = map[string][]models.Task{}
		}
		namespace := defaultNamespace(task.Namespace)
		priorityBuckets[task.Priority][namespace] = append(priorityBuckets[task.Priority][namespace], task)
		if _, ok := seenPriority[task.Priority]; !ok {
			seenPriority[task.Priority] = struct{}{}
			priorities = append(priorities, task.Priority)
		}
	}

	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] > priorities[j]
	})

	ordered := make([]models.Task, 0, len(tasks))
	for _, priority := range priorities {
		namespaces := make([]string, 0, len(priorityBuckets[priority]))
		for namespace := range priorityBuckets[priority] {
			namespaces = append(namespaces, namespace)
		}
		sort.Strings(namespaces)

		for {
			progressed := false
			for _, namespace := range namespaces {
				queue := priorityBuckets[priority][namespace]
				if len(queue) == 0 {
					continue
				}
				ordered = append(ordered, queue[0])
				priorityBuckets[priority][namespace] = queue[1:]
				progressed = true
			}
			if !progressed {
				break
			}
		}
	}

	return ordered
}

func backoffDuration(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}
	seconds := 1 << min(attempt, 5)
	return time.Duration(seconds) * time.Second
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
