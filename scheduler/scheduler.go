package scheduler

import (
	"github.com/jhonnyV-V/orch-in-go/node"
	"github.com/jhonnyV-V/orch-in-go/task"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) *node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

type RoundRobin struct {
	Name       string
	LastWorker int
}
