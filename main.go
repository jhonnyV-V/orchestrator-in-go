package main

import (
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/client"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/manager"
	"github.com/jhonnyV-V/orch-in-go/node"
	"github.com/jhonnyV-V/orch-in-go/task"
	"github.com/jhonnyV-V/orch-in-go/worker"
)

func createContainer() (*task.Docker, *task.DockerResult) {
	conf := task.Config{
		Name:  "test-container-1",
		Image: "postgres:13",
		Env: []string{
			"POSTGRES_USER=cube",
			"POSTGRES_PASSWORD=secret",
		},
	}

	dockerClient, _ := client.NewClientWithOpts(client.FromEnv)
	dockerTask := task.Docker{
		Client: dockerClient,
		Config: conf,
	}

	result := dockerTask.Run()
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return nil, &result
	}
	fmt.Printf("Container %s is running with config %v\n", result.ContainerId, conf)
	return &dockerTask, &result
}

func stopContainer(dockerTask *task.Docker, id string) *task.DockerResult {
	result := dockerTask.Stop(id)
	if result.Error != nil {
		fmt.Printf("%v\n", result.Error)
		return &result
	}

	fmt.Printf("Container %s has been stoped and removed %v\n", result.ContainerId)
	return &result
}

func main() {
	t := task.Task{
		ID:     uuid.New(),
		Name:   "task-1",
		State:  task.PENDING,
		Image:  "image-1",
		Memory: 1024,
		Disk:   1,
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.PENDING,
		Timestamp: time.Now(),
		Task:      t,
	}

	fmt.Printf("task %v\n", t)
	fmt.Printf("task event %v\n", te)

	w := worker.Worker{
		Name:  "worker-1",
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}

	fmt.Printf("worker %v\n", w)

	w.CollectStats()
	w.RunTask()
	w.StartTask()
	w.StopTask()

	m := manager.Manager{
		Pending: *queue.New(),
		TaskDb:  make(map[string][]*task.Task),
		EventDb: make(map[string][]*task.TaskEvent),
		Workers: []string{w.Name},
	}
	fmt.Printf("manager %v\n", m)
	m.SelectWorker()
	m.UpdateTasks()
	m.SendWork()

	n := node.Node{
		Name:   "node-1",
		Ip:     "192.168.1.1",
		Cores:  4,
		Memory: 1024,
		Disk:   25,
		Role:   "worker",
	}
	fmt.Printf("node %v\n", n)

	fmt.Printf("create a test container\n")
	dockerTask, result := createContainer()
	if result.Error != nil {
		fmt.Printf("create result err: %v\n", result.Error)
		os.Exit(1)
	}

	time.Sleep(time.Second * 5)

	fmt.Printf("stopping test container %s\n", result.ContainerId)
	_ = stopContainer(dockerTask, result.ContainerId)
}
