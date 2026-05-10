package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
	"mini-k8ts/internal/scheduler"
	"mini-k8ts/pkg/logger"
)

func TestSchedulerScaleSmoke(t *testing.T) {
	store := db.NewMemoryStoreWithTimeout(5 * time.Second)
	ctx := context.Background()

	for index := 1; index <= 50; index++ {
		nodeID := fmt.Sprintf("node-%d", index)
		_, err := store.Register(ctx, models.Node{ID: nodeID, TotalCPU: 8, TotalMemory: 16000, Reliability: 0.7})
		if err != nil {
			t.Fatalf("register node failed for %s: %v", nodeID, err)
		}
	}

	for index := 1; index <= 1000; index++ {
		taskID := store.NextTaskID()
		_, err := store.Create(ctx, models.Task{ID: taskID, Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusPending})
		if err != nil {
			t.Fatalf("create task failed for %s: %v", taskID, err)
		}
	}

	service := scheduler.NewService(store, store, store, store, scheduler.DefaultStrategy{}, logger.New("error", nil), 1*time.Millisecond, store.NextTaskID)
	start := time.Now()

	for cycle := 0; cycle < 20; cycle++ {
		if err := service.ScheduleOnce(ctx); err != nil {
			t.Fatalf("schedule cycle %d failed: %v", cycle, err)
		}
		pending, err := store.ListPending(ctx)
		if err != nil {
			t.Fatalf("list pending failed: %v", err)
		}
		if len(pending) == 0 {
			break
		}
	}

	pending, err := store.ListPending(ctx)
	if err != nil {
		t.Fatalf("list pending failed: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected all tasks to be scheduled, pending=%d", len(pending))
	}

	if time.Since(start) > 5*time.Second {
		t.Fatalf("scale smoke took too long: %s", time.Since(start))
	}
}
