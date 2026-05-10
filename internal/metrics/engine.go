package metrics

import "mini-k8ts/internal/models"

func ComputeSnapshot(tasks []models.Task, nodes []models.Node) Snapshot {
	snapshot := Snapshot{TotalTasks: len(tasks)}
	completedTasks := 0
	latencyTotalMs := 0.0
	schedulingLatencyTotalMs := 0.0
	schedulingLatencyCount := 0
	schedulingAttemptsTotal := 0.0

	for _, task := range tasks {
		if task.Status == models.TaskStatusFailed {
			snapshot.FailedTasks++
		}
		if task.Status == models.TaskStatusCompleted {
			completedTasks++
		}
		if !task.CreatedAt.IsZero() && !task.UpdatedAt.IsZero() {
			latencyTotalMs += task.UpdatedAt.Sub(task.CreatedAt).Seconds() * 1000
		}
		if !task.CreatedAt.IsZero() && !task.ScheduledAt.IsZero() {
			schedulingLatencyTotalMs += task.ScheduledAt.Sub(task.CreatedAt).Seconds() * 1000
			schedulingLatencyCount++
			schedulingAttemptsTotal += float64(task.Attempts + 1)
		}
	}

	if len(tasks) > 0 {
		snapshot.FailureRate = float64(snapshot.FailedTasks) / float64(len(tasks))
		snapshot.SuccessRate = float64(completedTasks) / float64(len(tasks))
		snapshot.AverageLatency = latencyTotalMs / float64(len(tasks))
	}
	if schedulingLatencyCount > 0 {
		snapshot.SchedulingLatency = schedulingLatencyTotalMs / float64(schedulingLatencyCount)
		snapshot.SchedulingAttempts = schedulingAttemptsTotal / float64(schedulingLatencyCount)
	}

	var totalCPU int
	var usedCPU int
	var totalMemory int
	var usedMemory int
	var reliabilitySum float64
	var uptimeSum float64
	var successRatioSum float64
	var fragmentationSum float64
	var binpackingWasteSum float64
	readyNodes := 0

	for _, node := range nodes {
		totalCPU += node.TotalCPU
		usedCPU += node.UsedCPU
		totalMemory += node.TotalMemory
		usedMemory += node.UsedMemory
		reliabilitySum += node.Reliability
		uptimeSum += effectiveNodeUptime(node)
		successRatioSum += effectiveNodeSuccessRatio(node)
		if node.Status == models.NodeStatusReady {
			if node.TotalCPU <= 0 || node.TotalMemory <= 0 {
				continue
			}
			freeCPU := float64(node.TotalCPU-node.UsedCPU) / float64(node.TotalCPU)
			freeMem := float64(node.TotalMemory-node.UsedMemory) / float64(node.TotalMemory)
			fragmentationSum += absFloat(freeCPU - freeMem)
			totalFree := (freeCPU + freeMem) / 2
			usableFree := minFloat(freeCPU, freeMem)
			binpackingWasteSum += totalFree - usableFree
			readyNodes++
		}
	}

	if totalCPU > 0 {
		snapshot.CPUUtilization = float64(usedCPU) / float64(totalCPU)
	}
	if totalMemory > 0 {
		snapshot.MemoryUtilization = float64(usedMemory) / float64(totalMemory)
	}
	if len(nodes) > 0 {
		snapshot.NodeReliability = reliabilitySum / float64(len(nodes))
		snapshot.NodeUptimePercent = uptimeSum / float64(len(nodes))
		snapshot.NodeSuccessRatio = successRatioSum / float64(len(nodes))
	}
	if readyNodes > 0 {
		snapshot.FragmentationIndex = fragmentationSum / float64(readyNodes)
		snapshot.BinpackingWaste = binpackingWasteSum / float64(readyNodes)
	}

	return snapshot
}

func minFloat(a float64, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func effectiveNodeUptime(node models.Node) float64 {
	if node.HeartbeatCount == 0 && node.DownTransitions == 0 && node.UptimePercent == 0 {
		return 1
	}
	return node.UptimePercent
}

func effectiveNodeSuccessRatio(node models.Node) float64 {
	if node.SuccessCount == 0 && node.FailureCount == 0 && node.SuccessRatio == 0 {
		return 1
	}
	return node.SuccessRatio
}
