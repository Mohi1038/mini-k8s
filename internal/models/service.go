package models

import "time"

type Service struct {
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace"`
	SelectorJobID string `json:"selector_job_id"`
	Port       int       `json:"port"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
