package models

import "time"

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusFailed    TaskStatus = "FAILED"
)

type Task struct {
	ID            string        `json:"id"`
	Image         string        `json:"image"`
	Command       []string      `json:"command"`
	SecretRefs    []SecretRef   `json:"secret_refs"`
	Volumes       []VolumeMount `json:"volumes"`
	ResolvedEnv   []string      `json:"-"`
	CPU           int           `json:"cpu"`
	Memory        int           `json:"memory"`
	Priority      int           `json:"priority"`
	Status        TaskStatus    `json:"status"`
	NodeID        string        `json:"node_id"`
	JobID         string        `json:"job_id"`
	Namespace     string        `json:"namespace"`
	IdempotencyKey string       `json:"-"`
	Attempts      int           `json:"attempts"`
	MaxAttempts   int           `json:"max_attempts"`
	NextAttemptAt time.Time     `json:"next_attempt_at"`
	ScheduledAt   time.Time     `json:"scheduled_at"`
	LastError     string        `json:"last_error"`
	StatusDetail  string        `json:"status_detail,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

type SecretRef struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	EnvVar   string `json:"env_var"`
	Optional bool   `json:"optional"`
}

type VolumeMount struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
}
