package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"mini-k8ts/internal/api"
	"mini-k8ts/internal/config"
	"mini-k8ts/internal/models"
	"mini-k8ts/internal/task"
	"mini-k8ts/pkg/client"
	"mini-k8ts/pkg/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	log := logger.New(cfg.LogLevel, nil)
	log.Info("worker starting", "heartbeat_interval", cfg.HeartbeatInterval.String())

	schedulerClient := client.NewSchedulerClientWithToken(cfg.SchedulerAPIBaseURL, cfg.SchedulerAuthToken)
	if err := schedulerClient.RegisterNode(context.Background(), cfg.WorkerNodeID, cfg.WorkerTotalCPU, cfg.WorkerTotalMemory); err != nil {
		log.Error("worker registration failed", "error", err)
		return
	}
	log.Info("worker registered", "node_id", cfg.WorkerNodeID)

	runner, err := task.NewDockerRunner()
	if err != nil {
		log.Error("failed to initialize docker runner", "error", err)
		return
	}

	// Start internal API server for logs
	// The worker doesn't need to list tasks/nodes from a DB, it only serves logs via the runner
	apiServer := api.NewServerWithRepos(cfg.HTTPAddr, log, nil, nil, nil, nil, nil, nil, nil, nil, api.DefaultServerOptions())
	apiServer.SetRunner(runner)
	go func() {
		if err := apiServer.Start(); err != nil {
			log.Error("internal api server failed", "error", err)
		}
	}()
	activeTasks := make(map[string]models.Task)
	var activeMu sync.Mutex

	ticker := time.NewTicker(cfg.HeartbeatInterval)
	defer ticker.Stop()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			stopActiveTasks(log, runner, schedulerClient, cfg.WorkerNodeID, &activeMu, activeTasks)
			log.Info("worker shutting down")
			return
		case <-ticker.C:
			// Calculate current resource usage from active tasks
			var usedCPU, usedMem int
			activeMu.Lock()
			for _, t := range activeTasks {
				usedCPU += t.CPU
				usedMem += t.Memory
			}
			activeMu.Unlock()

			if err := schedulerClient.SendHeartbeat(ctx, cfg.WorkerNodeID, usedCPU, usedMem); err != nil {
				log.Error("heartbeat failed", "error", err, "node_id", cfg.WorkerNodeID)
				continue
			}
			log.Debug("worker heartbeat sent", "node_id", cfg.WorkerNodeID, "cpu", usedCPU, "mem", usedMem)

			runningTasks, err := schedulerClient.GetRunningTasks(ctx, cfg.WorkerNodeID)
			if err != nil {
				log.Error("failed to fetch running tasks", "error", err, "node_id", cfg.WorkerNodeID)
				continue
			}

			if err := reconcileWorkerState(ctx, log, runner, schedulerClient, cfg, &activeMu, activeTasks, runningTasks); err != nil {
				log.Error("worker state reconciliation failed", "error", err, "node_id", cfg.WorkerNodeID)
			}

			for _, t := range runningTasks {
				activeMu.Lock()
				if _, exists := activeTasks[t.ID]; exists {
					activeMu.Unlock()
					continue
				}
				activeTasks[t.ID] = t
				activeMu.Unlock()

				log.Info("executing container task", "task_id", t.ID, "node_id", cfg.WorkerNodeID, "image", t.Image)

				go executeTask(ctx, log, runner, schedulerClient, cfg, &activeMu, activeTasks, t, true)
			}
		}
	}
}

func executeTask(ctx context.Context, log *logger.Logger, runner task.Runner, schedulerClient *client.SchedulerClient, cfg config.Config, activeMu *sync.Mutex, activeTasks map[string]models.Task, taskModel models.Task, shouldStart bool) {
	taskCtx, cancel := context.WithTimeout(ctx, cfg.TaskExecutionTimeout)
	defer cancel()

	if shouldStart {
		if err := enrichTaskRuntimeEnv(ctx, schedulerClient, &taskModel); err != nil {
			log.Error("failed to resolve task secrets", "error", err, "task_id", taskModel.ID)
			_ = schedulerClient.UpdateTaskStatus(ctx, taskModel.ID, models.TaskStatusFailed, err.Error())
			activeMu.Lock()
			delete(activeTasks, taskModel.ID)
			activeMu.Unlock()
			return
		}
		err := runner.Run(taskCtx, taskModel)
		if err != nil {
			log.Error("failed to start container", "error", err, "task_id", taskModel.ID)
			_ = runner.Stop(context.Background(), taskModel.ID)
			_ = schedulerClient.UpdateTaskStatus(ctx, taskModel.ID, models.TaskStatusFailed, err.Error())
			activeMu.Lock()
			delete(activeTasks, taskModel.ID)
			activeMu.Unlock()
			return
		}
		log.Info("container started successfully", "task_id", taskModel.ID)
	} else {
		log.Info("adopting existing managed task after worker recovery", "task_id", taskModel.ID, "node_id", cfg.WorkerNodeID)
	}

	exitCode, err := runner.Wait(taskCtx, taskModel.ID)
	finalStatus := models.TaskStatusCompleted
	taskErr := ""
	if err != nil {
		finalStatus = models.TaskStatusFailed
		if errors.Is(err, context.DeadlineExceeded) {
			taskErr = fmt.Sprintf("task execution timeout after %s", cfg.TaskExecutionTimeout)
		} else {
			taskErr = err.Error()
		}
	} else if exitCode != 0 {
		finalStatus = models.TaskStatusFailed
		taskErr = fmt.Sprintf("container exited with non-zero code: %d", exitCode)
	}

	_ = runner.Stop(context.Background(), taskModel.ID)

	if err := schedulerClient.UpdateTaskStatus(ctx, taskModel.ID, finalStatus, taskErr); err != nil {
		log.Error("failed to report final status", "error", err, "task_id", taskModel.ID)
	}
	log.Info("task execution finished", "task_id", taskModel.ID, "status", finalStatus)
	activeMu.Lock()
	delete(activeTasks, taskModel.ID)
	activeMu.Unlock()
}

func enrichTaskRuntimeEnv(ctx context.Context, schedulerClient *client.SchedulerClient, taskModel *models.Task) error {
	return resolveTaskRuntimeEnv(taskModel, func(name string) (models.Secret, error) {
		return schedulerClient.GetSecret(ctx, taskModel.Namespace, name)
	})
}

func resolveTaskRuntimeEnv(taskModel *models.Task, fetchSecret func(name string) (models.Secret, error)) error {
	if taskModel == nil || len(taskModel.SecretRefs) == 0 {
		return nil
	}

	env := make([]string, 0, len(taskModel.SecretRefs))
	cache := map[string]models.Secret{}
	for _, ref := range taskModel.SecretRefs {
		name := strings.TrimSpace(ref.Name)
		key := strings.TrimSpace(ref.Key)
		if name == "" || key == "" {
			continue
		}

		secret, ok := cache[name]
		if !ok {
			resolved, err := fetchSecret(name)
			if err != nil {
				if ref.Optional {
					continue
				}
				return fmt.Errorf("failed to resolve secret %s: %w", name, err)
			}
			secret = resolved
			cache[name] = secret
		}

		value, ok := secret.Data[key]
		if !ok {
			if ref.Optional {
				continue
			}
			return fmt.Errorf("secret %s missing key %s", name, key)
		}

		envVar := strings.TrimSpace(ref.EnvVar)
		if envVar == "" {
			envVar = strings.ToUpper(strings.ReplaceAll(key, "-", "_"))
		}
		env = append(env, envVar+"="+value)
	}
	taskModel.ResolvedEnv = env
	return nil
}

func reconcileWorkerState(ctx context.Context, log *logger.Logger, runner task.Runner, schedulerClient *client.SchedulerClient, cfg config.Config, activeMu *sync.Mutex, activeTasks map[string]models.Task, runningTasks []models.Task) error {
	managed, err := runner.ListManaged(ctx)
	if err != nil {
		return err
	}

	expected := make(map[string]models.Task, len(runningTasks))
	for _, runningTask := range runningTasks {
		expected[runningTask.ID] = runningTask
	}

	for taskID, state := range managed {
		expectedTask, ok := expected[taskID]
		if !ok {
			log.Warn("cleaning up orphaned managed container", "task_id", taskID, "node_id", cfg.WorkerNodeID)
			_ = runner.Stop(context.Background(), taskID)
			activeMu.Lock()
			delete(activeTasks, taskID)
			activeMu.Unlock()
			continue
		}

		if state.Running {
			activeMu.Lock()
			_, alreadyActive := activeTasks[taskID]
			if !alreadyActive {
				activeTasks[taskID] = expectedTask
			}
			activeMu.Unlock()
			if !alreadyActive {
				go executeTask(ctx, log, runner, schedulerClient, cfg, activeMu, activeTasks, expectedTask, false)
			}
			continue
		}

		finalStatus := models.TaskStatusCompleted
		taskErr := ""
		if state.ExitCode != 0 {
			finalStatus = models.TaskStatusFailed
			taskErr = fmt.Sprintf("container exited with non-zero code: %d", state.ExitCode)
		}
		log.Warn("reporting recovered exited task state", "task_id", taskID, "status", finalStatus, "node_id", cfg.WorkerNodeID)
		if err := schedulerClient.UpdateTaskStatus(ctx, taskID, finalStatus, taskErr); err != nil {
			log.Error("failed to report recovered task state", "error", err, "task_id", taskID)
		}
		_ = runner.Stop(context.Background(), taskID)
		activeMu.Lock()
		delete(activeTasks, taskID)
		activeMu.Unlock()
	}

	return nil
}

func stopActiveTasks(log *logger.Logger, runner task.Runner, schedulerClient *client.SchedulerClient, nodeID string, activeMu *sync.Mutex, activeTasks map[string]models.Task) {
	activeMu.Lock()
	tasksToStop := make([]models.Task, 0, len(activeTasks))
	for _, task := range activeTasks {
		tasksToStop = append(tasksToStop, task)
	}
	activeMu.Unlock()

	for _, task := range tasksToStop {
		log.Warn("stopping active task during worker shutdown", "task_id", task.ID, "node_id", nodeID)
		if err := runner.Stop(context.Background(), task.ID); err != nil {
			log.Error("failed to stop task during shutdown", "error", err, "task_id", task.ID)
		}
		if err := schedulerClient.UpdateTaskStatus(context.Background(), task.ID, models.TaskStatusFailed, "worker shutdown interrupted task execution"); err != nil {
			log.Error("failed to report interrupted task during shutdown", "error", err, "task_id", task.ID)
		}
		activeMu.Lock()
		delete(activeTasks, task.ID)
		activeMu.Unlock()
	}
}
