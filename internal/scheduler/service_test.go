package scheduler

import (
	"context"
	"testing"
	"time"

	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
	"mini-k8ts/pkg/logger"
)

func TestScheduleOnce(t *testing.T) {
	store := db.NewMemoryStore()
	_, _ = store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 8, TotalMemory: 16000, Reliability: 0.9})
	_, _ = store.Create(context.Background(), models.Task{ID: store.NextTaskID(), Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusPending})

	svc := NewService(store, store, store, store, DefaultStrategy{}, logger.New("debug", nil), 0, store.NextTaskID)
	if err := svc.ScheduleOnce(context.Background()); err != nil {
		t.Fatalf("ScheduleOnce failed: %v", err)
	}

	tasks, err := store.ListTasks(context.Background())
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Status != models.TaskStatusRunning {
		t.Fatalf("expected task status RUNNING, got %s", tasks[0].Status)
	}
	if tasks[0].NodeID != "node-1" {
		t.Fatalf("expected assigned node node-1, got %s", tasks[0].NodeID)
	}
}

func TestRecoverOnceRequeuesTaskFromDownNode(t *testing.T) {
	store := db.NewMemoryStoreWithTimeout(1 * time.Millisecond)
	_, _ = store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 8, TotalMemory: 16000, Reliability: 0.9})
	_, _ = store.Create(context.Background(), models.Task{ID: store.NextTaskID(), Image: "nginx", CPU: 1, Memory: 128, Priority: 1, Status: models.TaskStatusPending})

	svc := NewService(store, store, store, store, DefaultStrategy{}, logger.New("debug", nil), 0, store.NextTaskID)
	if err := svc.ScheduleOnce(context.Background()); err != nil {
		t.Fatalf("ScheduleOnce failed: %v", err)
	}

	time.Sleep(5 * time.Millisecond)
	if err := svc.RecoverOnce(context.Background()); err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}

	tasks, err := store.ListTasks(context.Background())
	if err != nil {
		t.Fatalf("ListTasks failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Status != models.TaskStatusPending {
		t.Fatalf("expected task status PENDING after recovery, got %s", tasks[0].Status)
	}
	if tasks[0].NodeID != "" {
		t.Fatalf("expected cleared node assignment after recovery, got %s", tasks[0].NodeID)
	}
	if tasks[0].Attempts != 1 {
		t.Fatalf("expected one retry attempt, got %d", tasks[0].Attempts)
	}
	if tasks[0].NextAttemptAt.IsZero() {
		t.Fatalf("expected next attempt timestamp to be set")
	}
	allocations, err := store.ListByNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("list allocations failed: %v", err)
	}
	if len(allocations) != 0 {
		t.Fatalf("expected allocation cleanup after recovery, got %d", len(allocations))
	}

	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("ListNodes failed: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected one node, got %d", len(nodes))
	}
	if nodes[0].Status != models.NodeStatusDown {
		t.Fatalf("expected node status DOWN, got %s", nodes[0].Status)
	}
	if nodes[0].FailureCount != 1 {
		t.Fatalf("expected node failure count 1 after recovery requeue, got %d", nodes[0].FailureCount)
	}
}

func TestReconcileJobsCreatesTasks(t *testing.T) {
	store := db.NewMemoryStore()
	job := models.Job{
		ID:         store.NextJobID(),
		Image:      "nginx",
		Command:    []string{"echo", "hello"},
		SecretRefs: []models.SecretRef{{Name: "db-creds", Key: "password", EnvVar: "DB_PASSWORD"}},
		Volumes:    []models.VolumeMount{{Source: "/tmp/data", Target: "/data", ReadOnly: true}},
		Replicas:   2,
		CPU:        1,
		Memory:     128,
		Priority:   1,
		Status:     models.JobStatusPending,
	}
	if _, err := store.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	svc := NewService(store, store, store, store, DefaultStrategy{}, logger.New("debug", nil), 0, store.NextTaskID)
	if err := svc.ReconcileJobs(context.Background()); err != nil {
		t.Fatalf("reconcile jobs failed: %v", err)
	}

	tasks, err := store.ListTasksByJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("list tasks by job failed: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	if len(tasks[0].SecretRefs) != 1 || tasks[0].SecretRefs[0].EnvVar != "DB_PASSWORD" {
		t.Fatalf("expected task to inherit secret refs, got %#v", tasks[0].SecretRefs)
	}
	if len(tasks[0].Volumes) != 1 || tasks[0].Volumes[0].Target != "/data" {
		t.Fatalf("expected task to inherit volumes, got %#v", tasks[0].Volumes)
	}

	updatedJob, err := store.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("get job failed: %v", err)
	}
	if updatedJob.Status != models.JobStatusRunning {
		t.Fatalf("expected job status RUNNING, got %s", updatedJob.Status)
	}
}

func TestReconcileStateRepairsInconsistentTaskState(t *testing.T) {
	store := db.NewMemoryStore()
	_, _ = store.Register(context.Background(), models.Node{ID: "node-1", TotalCPU: 8, TotalMemory: 16000, Reliability: 0.9})
	_, _ = store.Create(context.Background(), models.Task{
		ID:       store.NextTaskID(),
		Image:    "nginx",
		CPU:      1,
		Memory:   128,
		Priority: 1,
		Status:   models.TaskStatusRunning,
	})
	_, _ = store.Create(context.Background(), models.Task{
		ID:       store.NextTaskID(),
		Image:    "nginx",
		CPU:      1,
		Memory:   128,
		Priority: 1,
		Status:   models.TaskStatusPending,
		NodeID:   "node-1",
	})

	svc := NewService(store, store, store, store, DefaultStrategy{}, logger.New("debug", nil), 0, store.NextTaskID)
	if err := svc.ReconcileState(context.Background()); err != nil {
		t.Fatalf("reconcile state failed: %v", err)
	}

	tasks, err := store.ListTasks(context.Background())
	if err != nil {
		t.Fatalf("list tasks failed: %v", err)
	}
	if tasks[0].Status != models.TaskStatusPending || tasks[0].NodeID != "" {
		t.Fatalf("expected running task without node to be reset pending, got status=%s node=%s", tasks[0].Status, tasks[0].NodeID)
	}
	if tasks[1].NodeID != "" {
		t.Fatalf("expected pending task assignment to be cleared, got node=%s", tasks[1].NodeID)
	}
}

func TestFairOrderPendingTasksRoundsRobinNamespaces(t *testing.T) {
	ordered := fairOrderPendingTasks([]models.Task{
		{ID: "t1", Namespace: "a", Priority: 5},
		{ID: "t2", Namespace: "a", Priority: 5},
		{ID: "t3", Namespace: "b", Priority: 5},
		{ID: "t4", Namespace: "b", Priority: 5},
	})

	got := []string{ordered[0].ID, ordered[1].ID, ordered[2].ID, ordered[3].ID}
	want := []string{"t1", "t3", "t2", "t4"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected fair ordering: got=%v want=%v", got, want)
		}
	}
}
