package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr              string
	SchedulerAPIBaseURL   string
	SchedulerAuthToken    string
	StoreBackend          string
	PostgresDSN           string
	LogLevel              string
	Environment           string
	APIAuthToken          string
	APIRateLimitRPM       int
	APIRateLimitBurst     int
	SchedulerTickInterval time.Duration
	HeartbeatTimeout      time.Duration
	HeartbeatInterval     time.Duration
	TaskExecutionTimeout  time.Duration
	PendingTaskTimeout    time.Duration
	RunningTaskTimeout    time.Duration
	WorkerNodeID          string
	WorkerTotalCPU        int
	WorkerTotalMemory     int
}

func Load() (Config, error) {
	schedulerTickInterval, err := readDurationEnv("SCHEDULER_TICK_INTERVAL", 2*time.Second)
	if err != nil {
		return Config{}, err
	}

	heartbeatTimeout, err := readDurationEnv("HEARTBEAT_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	heartbeatInterval, err := readDurationEnv("HEARTBEAT_INTERVAL", 5*time.Second)
	if err != nil {
		return Config{}, err
	}

	taskExecutionTimeout, err := readDurationEnv("TASK_EXECUTION_TIMEOUT", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}

	pendingTaskTimeout, err := readDurationEnv("PENDING_TASK_TIMEOUT", 10*time.Minute)
	if err != nil {
		return Config{}, err
	}

	runningTaskTimeout, err := readDurationEnv("RUNNING_TASK_TIMEOUT", 20*time.Minute)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		HTTPAddr:              readStringEnv("HTTP_ADDR", ":8080"),
		SchedulerAPIBaseURL:   readStringEnv("SCHEDULER_API_BASE_URL", "http://127.0.0.1:8080"),
		SchedulerAuthToken:    readStringEnv("SCHEDULER_AUTH_TOKEN", ""),
		StoreBackend:          readStringEnv("STORE_BACKEND", "memory"),
		PostgresDSN:           readStringEnv("POSTGRES_DSN", "postgres://mini_k8ts:mini_k8ts@127.0.0.1:5432/mini_k8ts?sslmode=disable"),
		LogLevel:              readStringEnv("LOG_LEVEL", "info"),
		Environment:           readStringEnv("APP_ENV", "dev"),
		APIAuthToken:          readStringEnv("API_AUTH_TOKEN", ""),
		APIRateLimitRPM:       readIntEnv("API_RATE_LIMIT_RPM", 0),
		APIRateLimitBurst:     readIntEnv("API_RATE_LIMIT_BURST", 0),
		SchedulerTickInterval: schedulerTickInterval,
		HeartbeatTimeout:      heartbeatTimeout,
		HeartbeatInterval:     heartbeatInterval,
		TaskExecutionTimeout:  taskExecutionTimeout,
		PendingTaskTimeout:    pendingTaskTimeout,
		RunningTaskTimeout:    runningTaskTimeout,
		WorkerNodeID:          readStringEnv("WORKER_NODE_ID", "worker-1"),
		WorkerTotalCPU:        readIntEnv("WORKER_TOTAL_CPU", 4),
		WorkerTotalMemory:     readIntEnv("WORKER_TOTAL_MEMORY", 8192),
	}

	return cfg, nil
}

func readIntEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func readStringEnv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func readDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err == nil {
		return parsed, nil
	}

	seconds, err := strconv.Atoi(value)
	if err == nil {
		return time.Duration(seconds) * time.Second, nil
	}

	return 0, fmt.Errorf("invalid value for %s: %q", key, value)
}
