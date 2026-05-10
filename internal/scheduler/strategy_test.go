package scheduler

import (
	"testing"

	"mini-k8ts/internal/models"
)

func TestFilterNodes(t *testing.T) {
	task := models.Task{CPU: 2, Memory: 256}
	nodes := []models.Node{
		{ID: "n1", TotalCPU: 4, UsedCPU: 1, TotalMemory: 1024, UsedMemory: 256, Status: models.NodeStatusReady, Reliability: 0.9},
		{ID: "n2", TotalCPU: 1, UsedCPU: 0, TotalMemory: 1024, UsedMemory: 0, Status: models.NodeStatusReady, Reliability: 0.9},
		{ID: "n3", TotalCPU: 4, UsedCPU: 1, TotalMemory: 1024, UsedMemory: 256, Status: models.NodeStatusDown, Reliability: 0.9},
		{ID: "n4", TotalCPU: 4, UsedCPU: 1, TotalMemory: 1024, UsedMemory: 256, Status: models.NodeStatusReady, Reliability: 0.1},
	}

	filtered := FilterNodes(task, nodes)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 eligible node, got %d", len(filtered))
	}
	if filtered[0].ID != "n1" {
		t.Fatalf("expected node n1, got %s", filtered[0].ID)
	}
}

func TestSchedule(t *testing.T) {
	strategy := DefaultStrategy{}
	task := models.Task{CPU: 2, Memory: 512}
	nodes := []models.Node{
		{ID: "n1", TotalCPU: 8, UsedCPU: 4, TotalMemory: 16000, UsedMemory: 6000, Status: models.NodeStatusReady, Reliability: 0.4},
		{ID: "n2", TotalCPU: 8, UsedCPU: 1, TotalMemory: 16000, UsedMemory: 1000, Status: models.NodeStatusReady, Reliability: 0.95},
	}

	node, err := strategy.Schedule(task, nodes)
	if err != nil {
		t.Fatalf("unexpected schedule error: %v", err)
	}
	if node.ID != "n2" {
		t.Fatalf("expected n2 to be selected, got %s", node.ID)
	}
}

func TestHighPriorityPrefersStableNodes(t *testing.T) {
	task := models.Task{CPU: 1, Memory: 128, Priority: 8}
	nodes := []models.Node{
		{ID: "unstable", TotalCPU: 4, UsedCPU: 1, TotalMemory: 1024, UsedMemory: 128, Status: models.NodeStatusReady, Reliability: 0.25, HeartbeatCount: 2, DownTransitions: 2, UptimePercent: 0.5, SuccessRatio: 0.5},
		{ID: "stable", TotalCPU: 4, UsedCPU: 1, TotalMemory: 1024, UsedMemory: 128, Status: models.NodeStatusReady, Reliability: 0.8, HeartbeatCount: 5, DownTransitions: 0, UptimePercent: 1, SuccessRatio: 0.9},
	}

	filtered := FilterNodes(task, nodes)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 eligible node, got %d", len(filtered))
	}
	if filtered[0].ID != "stable" {
		t.Fatalf("expected stable node, got %s", filtered[0].ID)
	}
}
