package models

import "time"

type Namespace struct {
	Name        string    `json:"name"`
	CPUQuota    int       `json:"cpu_quota"`
	MemoryQuota int       `json:"memory_quota"`
	TaskQuota   int       `json:"task_quota"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
