package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/jhonnyV-V/orch-in-go/manager"
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

	//w1 := worker.New("woker-1", "memory")
	w1 := worker.New("woker-1", "persistent")
	wapi1 := worker.Api{Address: whost, Port: wport, Worker: w1}
	//w2 := worker.New("woker-2", "memory")
	w2 := worker.New("woker-2", "persistent")
	wapi2 := worker.Api{Address: whost, Port: wport + 1, Worker: w2}
	//w3 := worker.New("woker-3", "memory")
	w3 := worker.New("woker-3", "persistent")
	wapi3 := worker.Api{Address: whost, Port: wport + 2, Worker: w3}

	go w1.RunTasks()
	go w1.UpdateTasks()
	go w1.CollectStats()
	go wapi1.Start()

	go w2.RunTasks()
	go w2.UpdateTasks()
	go w2.CollectStats()
	go wapi2.Start()

	go w3.RunTasks()
	go w3.UpdateTasks()
	go w3.CollectStats()
	go wapi3.Start()

	fmt.Println("Starting Cube manager")

	workers := []string{
		fmt.Sprintf("%s:%d", whost, wport),
		fmt.Sprintf("%s:%d", whost, wport+1),
		fmt.Sprintf("%s:%d", whost, wport+2),
	}
	//m := manager.New(workers, "roundrobin", "memory")
	//m := manager.New(workers, "epvm", "memory")
	m := manager.New(workers, "epvm", "persistent")
	mapi := manager.Api{Address: mhost, Port: mport, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	mapi.Start()
}
