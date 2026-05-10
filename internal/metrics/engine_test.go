package metrics

import (
	"testing"
	"time"

	"mini-k8ts/internal/models"
)

func TestComputeSnapshot(t *testing.T) {
	tasks := []models.Task{
		{ID: "t1", Status: models.TaskStatusPending},
		{ID: "t2", Status: models.TaskStatusFailed},
	}
	nodes := []models.Node{
		{ID: "n1", TotalCPU: 8, UsedCPU: 2, TotalMemory: 16000, UsedMemory: 1000, Reliability: 0.8},
		{ID: "n2", TotalCPU: 4, UsedCPU: 2, TotalMemory: 8000, UsedMemory: 2000, Reliability: 0.6},
	}

	snapshot := ComputeSnapshot(tasks, nodes)
	if snapshot.TotalTasks != 2 {
		t.Fatalf("expected 2 total tasks, got %d", snapshot.TotalTasks)
	}
	if snapshot.FailedTasks != 1 {
		t.Fatalf("expected 1 failed task, got %d", snapshot.FailedTasks)
	}
	if snapshot.CPUUtilization <= 0 {
		t.Fatalf("expected positive cpu utilization, got %f", snapshot.CPUUtilization)
	}
	if snapshot.NodeReliability <= 0 {
		t.Fatalf("expected positive reliability, got %f", snapshot.NodeReliability)
	}
}

func TestComputeSnapshotSchedulingAndFragmentation(t *testing.T) {
	createdAt := time.Now().UTC().Add(-2 * time.Minute)
	scheduledAt := createdAt.Add(10 * time.Second)

	tasks := []models.Task{
		{ID: "t1", Status: models.TaskStatusRunning, CreatedAt: createdAt, ScheduledAt: scheduledAt, Attempts: 0},
		{ID: "t2", Status: models.TaskStatusCompleted, CreatedAt: createdAt, ScheduledAt: scheduledAt, Attempts: 1},
	}
	nodes := []models.Node{
		{ID: "n1", Status: models.NodeStatusReady, TotalCPU: 10, UsedCPU: 6, TotalMemory: 100, UsedMemory: 20, Reliability: 0.9},
		{ID: "n2", Status: models.NodeStatusReady, TotalCPU: 10, UsedCPU: 2, TotalMemory: 100, UsedMemory: 80, Reliability: 0.9},
	}

	snapshot := ComputeSnapshot(tasks, nodes)
	if snapshot.SchedulingLatency <= 0 {
		t.Fatalf("expected scheduling latency to be positive, got %f", snapshot.SchedulingLatency)
	}
	if snapshot.SchedulingAttempts < 1 {
		t.Fatalf("expected scheduling attempts to be at least 1, got %f", snapshot.SchedulingAttempts)
	}
	if snapshot.FragmentationIndex <= 0 {
		t.Fatalf("expected fragmentation index to be positive, got %f", snapshot.FragmentationIndex)
	}
	if snapshot.BinpackingWaste <= 0 {
		t.Fatalf("expected binpacking waste to be positive, got %f", snapshot.BinpackingWaste)
	}
}
