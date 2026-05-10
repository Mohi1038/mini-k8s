package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"mini-k8ts/internal/db"
	"mini-k8ts/internal/models"
)

// ClusterCollector implements the prometheus.Collector interface to
// dynamically fetch tasks and nodes from exactly the same repositories
// the scheduler uses, ensuring perfectly accurate OpenMetrics output.
type ClusterCollector struct {
	taskRepo db.TaskRepository
	nodeRepo db.NodeRepository

	tasksTotal         *prometheus.Desc
	nodesTotal         *prometheus.Desc
	clusterCPU         *prometheus.Desc
	clusterMem         *prometheus.Desc
	clusterCPUUsed     *prometheus.Desc
	clusterMemUsed     *prometheus.Desc
	nodeReliability    *prometheus.Desc
	nodeUptime         *prometheus.Desc
	nodeSuccessRatio   *prometheus.Desc
	nodeCPUUsage       *prometheus.Desc
	nodeMemoryUsage    *prometheus.Desc
	taskLatency        *prometheus.Desc
	fragmentationIndex *prometheus.Desc
	binpackingWaste    *prometheus.Desc
	schedulingLatency  *prometheus.Desc
	schedulingAttempts *prometheus.Desc
}

func NewClusterCollector(taskRepo db.TaskRepository, nodeRepo db.NodeRepository) *ClusterCollector {
	return &ClusterCollector{
		taskRepo: taskRepo,
		nodeRepo: nodeRepo,
		tasksTotal: prometheus.NewDesc(
			"mini_k8ts_tasks_total",
			"Total number of tasks in the cluster, partitioned by status.",
			[]string{"status"}, nil,
		),
		nodesTotal: prometheus.NewDesc(
			"mini_k8ts_nodes_total",
			"Total number of registered worker nodes, partitioned by status.",
			[]string{"status"}, nil,
		),
		clusterCPU: prometheus.NewDesc(
			"mini_k8ts_cluster_cpu_total",
			"Total usable CPU cores across all READY nodes.",
			nil, nil,
		),
		clusterMem: prometheus.NewDesc(
			"mini_k8ts_cluster_memory_total",
			"Total usable Memory (MB) across all READY nodes.",
			nil, nil,
		),
		clusterCPUUsed: prometheus.NewDesc(
			"mini_k8ts_cluster_cpu_used",
			"Total CPU cores currently allocated to running tasks.",
			nil, nil,
		),
		clusterMemUsed: prometheus.NewDesc(
			"mini_k8ts_cluster_memory_used",
			"Total Memory (MB) currently allocated to running tasks.",
			nil, nil,
		),
		nodeReliability: prometheus.NewDesc(
			"mini_k8ts_node_reliability",
			"The reliability score of a worker node.",
			[]string{"node_id"}, nil,
		),
		nodeUptime: prometheus.NewDesc(
			"mini_k8ts_node_uptime_percent",
			"The observed uptime ratio of a worker node.",
			[]string{"node_id"}, nil,
		),
		nodeSuccessRatio: prometheus.NewDesc(
			"mini_k8ts_node_success_ratio",
			"The observed task success ratio of a worker node.",
			[]string{"node_id"}, nil,
		),
		nodeCPUUsage: prometheus.NewDesc(
			"mini_k8ts_node_cpu_usage",
			"The current CPU usage of a worker node.",
			[]string{"node_id"}, nil,
		),
		nodeMemoryUsage: prometheus.NewDesc(
			"mini_k8ts_node_memory_usage",
			"The current Memory usage of a worker node.",
			[]string{"node_id"}, nil,
		),
		taskLatency: prometheus.NewDesc(
			"mini_k8ts_task_latency_ms",
			"Average latency (ms) across all cluster tasks.",
			nil, nil,
		),
		fragmentationIndex: prometheus.NewDesc(
			"mini_k8ts_fragmentation_index",
			"Average resource fragmentation across READY nodes.",
			nil, nil,
		),
		binpackingWaste: prometheus.NewDesc(
			"mini_k8ts_binpacking_waste",
			"Average binpacking waste from imbalanced free resources.",
			nil, nil,
		),
		schedulingLatency: prometheus.NewDesc(
			"mini_k8ts_scheduling_latency_ms",
			"Average time (ms) from task creation to scheduling.",
			nil, nil,
		),
		schedulingAttempts: prometheus.NewDesc(
			"mini_k8ts_scheduling_attempts_avg",
			"Average scheduling attempts per scheduled task.",
			nil, nil,
		),
	}
}

// Describe sends all possible metric descriptions to the prometheus channel.
func (c *ClusterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasksTotal
	ch <- c.nodesTotal
	ch <- c.clusterCPU
	ch <- c.clusterMem
	ch <- c.clusterCPUUsed
	ch <- c.clusterMemUsed
	ch <- c.nodeReliability
	ch <- c.nodeUptime
	ch <- c.nodeSuccessRatio
	ch <- c.nodeCPUUsage
	ch <- c.nodeMemoryUsage
	ch <- c.taskLatency
	ch <- c.fragmentationIndex
	ch <- c.binpackingWaste
	ch <- c.schedulingLatency
	ch <- c.schedulingAttempts
}

// Collect is invoked dynamically every time the /metrics/prometheus endpoint is scraped.
func (c *ClusterCollector) Collect(ch chan<- prometheus.Metric) {
	// Use background context for metrics reads
	ctx := context.Background()

	// 1. Gather Node Metrics
	nodes, err := c.nodeRepo.ListNodes(ctx)
	if err == nil {
		nodeStatusCounts := make(map[models.NodeStatus]float64)
		var totalCPU float64
		var totalMem float64
		var usedCPU float64
		var usedMem float64
		var fragmentationSum float64
		var binpackingWasteSum float64
		readyNodes := 0

		for _, node := range nodes {
			nodeStatusCounts[node.Status]++
			if node.Status == models.NodeStatusReady {
				totalCPU += float64(node.TotalCPU)
				totalMem += float64(node.TotalMemory)
				usedCPU += float64(node.UsedCPU)
				usedMem += float64(node.UsedMemory)

				if node.TotalCPU > 0 && node.TotalMemory > 0 {
					freeCPU := float64(node.TotalCPU-node.UsedCPU) / float64(node.TotalCPU)
					freeMem := float64(node.TotalMemory-node.UsedMemory) / float64(node.TotalMemory)
					fragmentationSum += absFloat(freeCPU - freeMem)
					totalFree := (freeCPU + freeMem) / 2
					usableFree := minFloat(freeCPU, freeMem)
					binpackingWasteSum += totalFree - usableFree
					readyNodes++
				}

				// Per-node metrics with labels
				ch <- prometheus.MustNewConstMetric(c.nodeReliability, prometheus.GaugeValue, node.Reliability, node.ID)
				ch <- prometheus.MustNewConstMetric(c.nodeUptime, prometheus.GaugeValue, effectiveNodeUptime(node), node.ID)
				ch <- prometheus.MustNewConstMetric(c.nodeSuccessRatio, prometheus.GaugeValue, effectiveNodeSuccessRatio(node), node.ID)
				ch <- prometheus.MustNewConstMetric(c.nodeCPUUsage, prometheus.GaugeValue, float64(node.UsedCPU), node.ID)
				ch <- prometheus.MustNewConstMetric(c.nodeMemoryUsage, prometheus.GaugeValue, float64(node.UsedMemory), node.ID)
			}
		}

		for status, count := range nodeStatusCounts {
			ch <- prometheus.MustNewConstMetric(c.nodesTotal, prometheus.GaugeValue, count, string(status))
		}
		ch <- prometheus.MustNewConstMetric(c.clusterCPU, prometheus.GaugeValue, totalCPU)
		ch <- prometheus.MustNewConstMetric(c.clusterMem, prometheus.GaugeValue, totalMem)
		ch <- prometheus.MustNewConstMetric(c.clusterCPUUsed, prometheus.GaugeValue, usedCPU)
		ch <- prometheus.MustNewConstMetric(c.clusterMemUsed, prometheus.GaugeValue, usedMem)
		if readyNodes > 0 {
			ch <- prometheus.MustNewConstMetric(c.fragmentationIndex, prometheus.GaugeValue, fragmentationSum/float64(readyNodes))
			ch <- prometheus.MustNewConstMetric(c.binpackingWaste, prometheus.GaugeValue, binpackingWasteSum/float64(readyNodes))
		}
	}

	// 2. Gather Task Metrics
	tasks, err := c.taskRepo.ListTasks(ctx)
	if err == nil {
		taskStatusCounts := make(map[models.TaskStatus]float64)
		var latencySum float64
		countWithLatency := 0
		var schedulingLatencySum float64
		var schedulingAttemptsSum float64
		schedulingCount := 0

		for _, task := range tasks {
			taskStatusCounts[task.Status]++
			if !task.CreatedAt.IsZero() && !task.UpdatedAt.IsZero() {
				latencySum += task.UpdatedAt.Sub(task.CreatedAt).Seconds() * 1000
				countWithLatency++
			}
			if !task.CreatedAt.IsZero() && !task.ScheduledAt.IsZero() {
				schedulingLatencySum += task.ScheduledAt.Sub(task.CreatedAt).Seconds() * 1000
				schedulingAttemptsSum += float64(task.Attempts + 1)
				schedulingCount++
			}
		}
		for status, count := range taskStatusCounts {
			ch <- prometheus.MustNewConstMetric(c.tasksTotal, prometheus.GaugeValue, count, string(status))
		}
		if countWithLatency > 0 {
			ch <- prometheus.MustNewConstMetric(c.taskLatency, prometheus.GaugeValue, latencySum/float64(len(tasks)))
		}
		if schedulingCount > 0 {
			ch <- prometheus.MustNewConstMetric(c.schedulingLatency, prometheus.GaugeValue, schedulingLatencySum/float64(schedulingCount))
			ch <- prometheus.MustNewConstMetric(c.schedulingAttempts, prometheus.GaugeValue, schedulingAttemptsSum/float64(schedulingCount))
		}
	}
}
