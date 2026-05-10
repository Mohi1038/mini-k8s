package db

import (
	"context"
	"testing"
	"time"

	"mini-k8ts/internal/models"
)

func TestMemoryStoreTaskLifecycle(t *testing.T) {
	store := NewMemoryStore()
	task := models.Task{ID: store.NextTaskID(), Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusPending}

	_, err := store.Create(context.Background(), task)
	if err != nil {
		t.Fatalf("create task failed: %v", err)
	}

	pending, err := store.ListPending(context.Background())
	if err != nil {
		t.Fatalf("list pending failed: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending task, got %d", len(pending))
	}

	err = store.UpdateStatus(context.Background(), task.ID, models.TaskStatusRunning, "node-1")
	if err != nil {
		t.Fatalf("update status failed: %v", err)
	}

	tasks, err := store.ListTasks(context.Background())
	if err != nil {
		t.Fatalf("list tasks failed: %v", err)
	}
	if tasks[0].Status != models.TaskStatusRunning {
		t.Fatalf("expected task status RUNNING, got %s", tasks[0].Status)
	}
	if tasks[0].ScheduledAt.IsZero() {
		t.Fatalf("expected scheduled_at to be set when task is RUNNING")
	}
}

func TestMemoryStoreNodeLifecycle(t *testing.T) {
	store := NewMemoryStore()
	node := models.Node{ID: "node-1", TotalCPU: 8, TotalMemory: 16000}

	_, err := store.Register(context.Background(), node)
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	err = store.Heartbeat(context.Background(), "node-1", 2, 512)
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].UsedCPU != 2 || nodes[0].UsedMemory != 512 {
		t.Fatalf("unexpected resource values cpu=%d memory=%d", nodes[0].UsedCPU, nodes[0].UsedMemory)
	}
	if nodes[0].HeartbeatCount != 1 {
		t.Fatalf("expected heartbeat count 1, got %d", nodes[0].HeartbeatCount)
	}
	if nodes[0].UptimePercent <= 0 {
		t.Fatalf("expected positive uptime percent, got %f", nodes[0].UptimePercent)
	}
}

func TestReliabilityUpdates(t *testing.T) {
	store := NewMemoryStoreWithTimeout(1 * time.Millisecond)
	_, err := store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 4, TotalMemory: 8000, Reliability: 0.5})
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	err = store.Heartbeat(context.Background(), "node-1", 1, 256)
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	nodes, _ := store.ListNodes(context.Background())
	if nodes[0].Reliability <= 0.5 {
		t.Fatalf("expected reliability to increase on heartbeat, got %f", nodes[0].Reliability)
	}

	time.Sleep(5 * time.Millisecond)
	_, err = store.MarkDownInactive(context.Background())
	if err != nil {
		t.Fatalf("markdown failed: %v", err)
	}
	nodes, _ = store.ListNodes(context.Background())
	if nodes[0].Status != models.NodeStatusDown {
		t.Fatalf("expected node to be DOWN, got %s", nodes[0].Status)
	}
	if nodes[0].Reliability >= 0.53 {
		t.Fatalf("expected reliability to decrease after markdown, got %f", nodes[0].Reliability)
	}
	if nodes[0].DownTransitions != 1 {
		t.Fatalf("expected one down transition, got %d", nodes[0].DownTransitions)
	}
}

func TestRecordTaskResultUpdatesNodeHealth(t *testing.T) {
	store := NewMemoryStore()
	_, err := store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 4, TotalMemory: 8000, Reliability: 0.5})
	if err != nil {
		t.Fatalf("register node failed: %v", err)
	}

	if err := store.RecordTaskResult(context.Background(), "node-1", models.TaskStatusCompleted); err != nil {
		t.Fatalf("record completed task failed: %v", err)
	}
	if err := store.RecordTaskResult(context.Background(), "node-1", models.TaskStatusFailed); err != nil {
		t.Fatalf("record failed task failed: %v", err)
	}

	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}
	if nodes[0].SuccessCount != 1 || nodes[0].FailureCount != 1 {
		t.Fatalf("unexpected success/failure counts: %d/%d", nodes[0].SuccessCount, nodes[0].FailureCount)
	}
	if nodes[0].SuccessRatio != 0.5 {
		t.Fatalf("expected success ratio 0.5, got %f", nodes[0].SuccessRatio)
	}
}
