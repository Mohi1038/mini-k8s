package scheduler

import (
	"errors"
	"sort"

	"mini-k8ts/internal/models"
)

var ErrNoEligibleNode = errors.New("no eligible node found")

type Strategy interface {
	Schedule(task models.Task, nodes []models.Node) (models.Node, error)
}

type DefaultStrategy struct{}

func (s DefaultStrategy) Schedule(task models.Task, nodes []models.Node) (models.Node, error) {
	eligible := FilterNodes(task, nodes)
	if len(eligible) == 0 {
		return models.Node{}, ErrNoEligibleNode
	}

	scores := ScoreNodes(task, eligible)
	sort.SliceStable(eligible, func(i, j int) bool {
		return scores[eligible[i].ID] > scores[eligible[j].ID]
	})

	return eligible[0], nil
}

func FilterNodes(task models.Task, nodes []models.Node) []models.Node {
	filtered := make([]models.Node, 0, len(nodes))
	priorityWeight := priorityWeight(task.Priority)
	minReliability := 0.2 + 0.4*priorityWeight
	minUptime := 0.2 + 0.4*priorityWeight
	minSuccess := 0.2 + 0.4*priorityWeight
	minProfile := 0.2 + 0.4*priorityWeight
	for _, node := range nodes {
		uptime := effectiveUptime(node)
		successRatio := effectiveSuccessRatio(node)
		profile := profileScore(node)

		if node.Status != models.NodeStatusReady {
			continue
		}
		if node.TotalCPU-node.UsedCPU < task.CPU {
			continue
		}
		if node.TotalMemory-node.UsedMemory < task.Memory {
			continue
		}
		if node.Reliability < minReliability {
			continue
		}
		if uptime < minUptime {
			continue
		}
		if successRatio < minSuccess {
			continue
		}
		if profile < minProfile {
			continue
		}

		filtered = append(filtered, node)
	}

	return filtered
}

func ScoreNodes(task models.Task, nodes []models.Node) map[string]float64 {
	scores := make(map[string]float64, len(nodes))
	riskWeight := priorityRisk(task.Priority)
	for _, node := range nodes {
		freeCPU := float64(node.TotalCPU-node.UsedCPU-task.CPU) / float64(node.TotalCPU)
		freeMem := float64(node.TotalMemory-node.UsedMemory-task.Memory) / float64(node.TotalMemory)

		resourceFit := 1 - (absFloat(freeCPU)+absFloat(freeMem))/2
		fragmentationIndex := absFloat(freeCPU - freeMem)
		binpackingWaste := ((freeCPU + freeMem) / 2) - minFloat(freeCPU, freeMem)

		profile := profileScore(node)
		riskPenalty := riskWeight * (1 - profile)

		score := 0.35*resourceFit + 0.55*profile - 0.05*fragmentationIndex - 0.05*binpackingWaste - riskPenalty
		scores[node.ID] = score
	}

	return scores
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}

	return value
}

func minFloat(a float64, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func priorityWeight(priority int) float64 {
	if priority <= 0 {
		return 0
	}
	weight := float64(priority) / 10.0
	if weight > 1 {
		return 1
	}
	return weight
}

func priorityRisk(priority int) float64 {
	return 0.25 * priorityWeight(priority)
}

func effectiveUptime(node models.Node) float64 {
	if node.HeartbeatCount == 0 && node.DownTransitions == 0 && node.UptimePercent == 0 {
		return 1
	}
	return node.UptimePercent
}

func effectiveSuccessRatio(node models.Node) float64 {
	if node.SuccessCount == 0 && node.FailureCount == 0 && node.SuccessRatio == 0 {
		return 1
	}
	return node.SuccessRatio
}

func profileScore(node models.Node) float64 {
	uptime := effectiveUptime(node)
	successRatio := effectiveSuccessRatio(node)
	churn := 0.0
	if node.HeartbeatCount > 0 || node.DownTransitions > 0 {
		churn = float64(node.DownTransitions) / float64(node.DownTransitions+node.HeartbeatCount)
	}
	score := 0.4*node.Reliability + 0.25*uptime + 0.25*successRatio - 0.10*churn
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}
