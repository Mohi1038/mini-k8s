package metrics

type Snapshot struct {
	TotalTasks         int     `json:"total_tasks"`
	FailedTasks        int     `json:"failed_tasks"`
	AverageLatency     float64 `json:"average_latency"`
	SuccessRate        float64 `json:"success_rate"`
	FailureRate        float64 `json:"failure_rate"`
	CPUUtilization     float64 `json:"cpu_utilization"`
	MemoryUtilization  float64 `json:"memory_utilization"`
	NodeReliability    float64 `json:"node_reliability"`
	NodeUptimePercent  float64 `json:"node_uptime_percent"`
	NodeSuccessRatio   float64 `json:"node_success_ratio"`
	FragmentationIndex float64 `json:"fragmentation_index"`
	BinpackingWaste    float64 `json:"binpacking_waste"`
	SchedulingLatency  float64 `json:"scheduling_latency_ms"`
	SchedulingAttempts float64 `json:"scheduling_attempts_avg"`
}
