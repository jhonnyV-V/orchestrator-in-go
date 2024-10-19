package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) CollectStats() {
	fmt.Println("CollectStats")
}

func (w *Worker) RunTask() {
	fmt.Println("RunTask")
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	fmt.Println("StartTask")
	t.StartTime = time.Now().UTC()
	config := task.NewConfig(&t)
	dockerTask := task.NewDocker(config)
	result := dockerTask.Run()
	if result.Error != nil {
		log.Printf("Error running task %v: %v\n", t.ID, result.Error)
		t.State = task.FAILED
		w.Db[t.ID] = &t
		return result
	}

	t.State = task.RUNNING
	t.ContainerID = result.ContainerId
	w.Db[t.ID] = &t

	return result
}
func (w *Worker) StopTask(t task.Task) task.DockerResult {
	fmt.Println("StopTask")
	config := task.NewConfig(&t)
	dockerTask := task.NewDocker(config)
	result := dockerTask.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v\n", t.ContainerID, result.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.COMPLETED
	w.Db[t.ID] = &t

	log.Printf("Stoped and removed container %v for task %v\n", t.ContainerID, t.ID)
	return result
}
