package models

import "time"

type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
)

type Job struct {
	ID         string        `json:"id"`
	Namespace  string        `json:"namespace"`
	Image      string        `json:"image"`
	Command    []string      `json:"command"`
	SecretRefs []SecretRef   `json:"secret_refs"`
	Volumes    []VolumeMount `json:"volumes"`
	Replicas   int           `json:"replicas"`
	CPU        int           `json:"cpu"`
	Memory     int           `json:"memory"`
	Priority   int           `json:"priority"`
	Status     JobStatus     `json:"status"`
	Autoscale  Autoscale     `json:"autoscale"`
	IdempotencyKey string    `json:"-"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
}

type Autoscale struct {
	Enabled        bool `json:"enabled"`
	MinReplicas    int  `json:"min_replicas"`
	MaxReplicas    int  `json:"max_replicas"`
	ScaleUpCPU     int  `json:"scale_up_cpu"`
	ScaleDownCPU   int  `json:"scale_down_cpu"`
	ScaleUpPending int  `json:"scale_up_pending"`
	ScaleUpStep    int  `json:"scale_up_step"`
}
