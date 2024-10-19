package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/manager"
	"github.com/jhonnyV-V/orch-in-go/task"
	"github.com/jhonnyV-V/orch-in-go/worker"
)

func main() {

	whost := os.Getenv("CUBE_WORKER_HOST")
	wport, _ := strconv.Atoi(os.Getenv("CUBE_WORKER_PORT"))

	if whost == "" {
		whost = "localhost"
	}
	if wport == 0 {
		wport = 8089
	}

	mhost := os.Getenv("CUBE_MANAGER_HOST")
	mport, _ := strconv.Atoi(os.Getenv("CUBE_MANAGER_PORT"))

	if mhost == "" {
		mhost = "localhost"
	}
	if mport == 0 {
		mport = 8099
	}

	fmt.Println("Starting Cube worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}
	wapi := worker.Api{Address: whost, Port: wport, Worker: &w}

	go w.RunTasks()
	go w.CollectStats()
	go wapi.Start()

	fmt.Println("Starting Cube manager")

	workers := []string{fmt.Sprintf("%s:%d", whost, wport)}
	m := manager.New(workers)
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	mapi.Start()
}
