package task

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"mini-k8ts/internal/models"
)

// Runner defines the interface for executing tasks on a node.
type Runner interface {
	Run(ctx context.Context, task models.Task) error
	Stop(ctx context.Context, taskID string) error
	Logs(ctx context.Context, taskID string) (io.ReadCloser, error)
	Wait(ctx context.Context, taskID string) (int64, error)
	ListManaged(ctx context.Context) (map[string]ManagedTaskState, error)
}

type ManagedTaskState struct {
	TaskID   string
	Running  bool
	ExitCode int64
}

// DockerRunner implements real container execution using the Docker SDK.
type DockerRunner struct {
	cli *client.Client
}

func NewDockerRunner() (*DockerRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	return &DockerRunner{cli: cli}, nil
}

func (r *DockerRunner) Run(ctx context.Context, t models.Task) error {
	// 1. Pull the image
	fmt.Printf("Pulling image: %s\n", t.Image)
	reader, err := r.cli.ImagePull(ctx, t.Image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer reader.Close()
	// Discard output to avoid clogging logs, but ensure it completes
	_, _ = io.Copy(io.Discard, reader)

	binds := make([]string, 0, len(t.Volumes))
	for _, volume := range t.Volumes {
		if strings.TrimSpace(volume.Source) == "" || strings.TrimSpace(volume.Target) == "" {
			continue
		}
		spec := fmt.Sprintf("%s:%s", filepath.Clean(volume.Source), volume.Target)
		if volume.ReadOnly {
			spec += ":ro"
		}
		binds = append(binds, spec)
	}

	// 2. Create the container with limits
	resp, err := r.cli.ContainerCreate(ctx, &container.Config{
		Image: t.Image,
		Cmd:   t.Command,
		Env:   t.ResolvedEnv,
		Labels: map[string]string{
			"mini-k8ts-task-id": t.ID,
		},
	}, &container.HostConfig{
		Resources: container.Resources{
			NanoCPUs: int64(t.CPU) * 1e9,            // Convert cores to NanoCPUs
			Memory:   int64(t.Memory) * 1024 * 1024, // MB to bytes
		},
		Binds:      binds,
	}, nil, nil, t.ID)
	if err != nil {
		if strings.Contains(err.Error(), "Conflict") {
			startErr := r.cli.ContainerStart(ctx, t.ID, container.StartOptions{})
			if startErr == nil {
				return nil
			}
			return fmt.Errorf("failed to start existing container: %w", startErr)
		}
		return fmt.Errorf("failed to create container: %w", err)
	}

	// 3. Start the container
	if err := r.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	return nil
}

func (r *DockerRunner) Stop(ctx context.Context, taskID string) error {
	return r.cli.ContainerRemove(ctx, taskID, container.RemoveOptions{
		Force: true,
	})
}

func (r *DockerRunner) Logs(ctx context.Context, taskID string) (io.ReadCloser, error) {
	return r.cli.ContainerLogs(ctx, taskID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     false,
		Timestamps: true,
	})
}

func (r *DockerRunner) Wait(ctx context.Context, taskID string) (int64, error) {
	statusCh, errCh := r.cli.ContainerWait(ctx, taskID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		return -1, err
	case status := <-statusCh:
		return status.StatusCode, nil
	}
}

func (r *DockerRunner) ListManaged(ctx context.Context) (map[string]ManagedTaskState, error) {
	containers, err := r.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("label", "mini-k8ts-task-id")),
	})
	if err != nil {
		return nil, err
	}

	states := make(map[string]ManagedTaskState, len(containers))
	for _, c := range containers {
		taskID := c.Labels["mini-k8ts-task-id"]
		if strings.TrimSpace(taskID) == "" {
			continue
		}
		state := ManagedTaskState{
			TaskID:  taskID,
			Running: strings.EqualFold(c.State, "running"),
		}
		if !state.Running && c.State == "exited" {
			inspect, err := r.cli.ContainerInspect(ctx, c.ID)
			if err == nil && inspect.State != nil {
				state.ExitCode = int64(inspect.State.ExitCode)
			}
		}
		states[taskID] = state
	}
	return states, nil
}
