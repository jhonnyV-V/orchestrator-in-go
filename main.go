package main

import (
	"fmt"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/task"
	"github.com/jhonnyV-V/orch-in-go/worker"
)

func main() {
	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}

	t := task.Task{
		ID:    uuid.New(),
		Name:  "test-container-1",
		State: task.SCHEDULED,
		Image: "strm/helloworld-http",
	}

	fmt.Println("starting task")
	w.AddTask(t)
	result := w.RunTask()
	if result.Error != nil {
		panic(result.Error)
	}

	t.ContainerID = result.ContainerId
	fmt.Printf("task %v is running in container %v\n", t.ID, t.ContainerID)
	fmt.Println("Sleeping")
	time.Sleep(time.Second * 10)

	fmt.Printf("stopping task %v\n", t.ID)
	t.State = task.COMPLETED
	w.AddTask(t)
	result = w.RunTask()

	if result.Error != nil {
		panic(result.Error)
	}
}
