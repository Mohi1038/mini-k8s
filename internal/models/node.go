package models

import "time"

type NodeStatus string

const (
	NodeStatusReady NodeStatus = "READY"
	NodeStatusDown  NodeStatus = "DOWN"
)

type Node struct {
	ID              string     `json:"id"`
	TotalCPU        int        `json:"total_cpu"`
	UsedCPU         int        `json:"used_cpu"`
	TotalMemory     int        `json:"total_memory"`
	UsedMemory      int        `json:"used_memory"`
	Status          NodeStatus `json:"status"`
	LastSeen        time.Time  `json:"last_seen"`
	Reliability     float64    `json:"reliability"`
	HeartbeatCount  int        `json:"heartbeat_count"`
	SuccessCount    int        `json:"success_count"`
	FailureCount    int        `json:"failure_count"`
	DownTransitions int        `json:"down_transitions"`
	UptimePercent   float64    `json:"uptime_percent"`
	SuccessRatio    float64    `json:"success_ratio"`
}
