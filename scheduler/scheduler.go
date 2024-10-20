package scheduler

import (
	"github.com/jhonnyV-V/orch-in-go/node"
	"github.com/jhonnyV-V/orch-in-go/task"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

type RoundRobin struct {
	Name       string
	LastWorker int
}

func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	scores := make(map[string]float64)
	var newWorker int
	if r.LastWorker+1 < len(nodes) {
		newWorker = r.LastWorker + 1
		r.LastWorker++
	} else {
		newWorker = 0
		r.LastWorker = 0
	}
	for i, n := range nodes {
		if i == newWorker {
			scores[n.Name] = 0.1
		} else {
			scores[n.Name] = 1.0
		}
	}
	return scores
}
func (r *RoundRobin) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var bestNode *node.Node
	var lowestScore float64 = 10000000.0
	for _, node := range candidates {
		if scores[node.Name] < lowestScore {
			bestNode = node
			lowestScore = scores[node.Name]
		}
	}
	return bestNode
}
