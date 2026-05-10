package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("SCHEDULER_TICK_INTERVAL", "")
	t.Setenv("HEARTBEAT_TIMEOUT", "")
	t.Setenv("HEARTBEAT_INTERVAL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("expected default HTTP_ADDR :8080, got %s", cfg.HTTPAddr)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("expected default log level info, got %s", cfg.LogLevel)
	}
	if cfg.Environment != "dev" {
		t.Fatalf("expected default APP_ENV dev, got %s", cfg.Environment)
	}
	if cfg.APIAuthToken != "" {
		t.Fatalf("expected default API auth token to be empty, got %s", cfg.APIAuthToken)
	}
	if cfg.APIRateLimitRPM != 0 {
		t.Fatalf("expected default API rate limit rpm 0, got %d", cfg.APIRateLimitRPM)
	}
	if cfg.APIRateLimitBurst != 0 {
		t.Fatalf("expected default API rate limit burst 0, got %d", cfg.APIRateLimitBurst)
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Fatalf("expected default heartbeat interval 5s, got %s", cfg.HeartbeatInterval)
	}
	if cfg.TaskExecutionTimeout != 5*time.Minute {
		t.Fatalf("expected default task execution timeout 5m, got %s", cfg.TaskExecutionTimeout)
	}
	if cfg.PendingTaskTimeout != 10*time.Minute {
		t.Fatalf("expected default pending task timeout 10m, got %s", cfg.PendingTaskTimeout)
	}
	if cfg.RunningTaskTimeout != 20*time.Minute {
		t.Fatalf("expected default running task timeout 20m, got %s", cfg.RunningTaskTimeout)
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	t.Setenv("SCHEDULER_TICK_INTERVAL", "bad-duration")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid SCHEDULER_TICK_INTERVAL")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
