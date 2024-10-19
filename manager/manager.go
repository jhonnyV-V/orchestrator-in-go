package manager

import (
	"fmt"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/task"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[string][]*task.Task
	EventDb       map[string][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID][]string
}

func (m *Manager) SelectWorker() {
	fmt.Println("SelectWorker")
}
func (m *Manager) UpdateTasks() {
	fmt.Println("UpdateTasks")
}
func (m *Manager) SendWork() {
	fmt.Println("SendWork")
}