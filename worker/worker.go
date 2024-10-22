package worker

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/stats"
	"github.com/jhonnyV-V/orch-in-go/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	Stats     *stats.Stats
	TaskCount int
}

func (w *Worker) GetTasks() []*task.Task {
	tasks := []*task.Task{}

	for _, t := range w.Db {
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID.String() < tasks[j].ID.String()
	})
	return tasks
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats")
		w.Stats = stats.GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			result := w.runTask()
			if result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently.\n")
		}
		log.Println("Sleeping for 10 seconds.")
		time.Sleep(10 * time.Second)
	}

}

func (w *Worker) runTask() task.DockerResult {
	fmt.Println("RunTask")
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("No task in queue")
		return task.DockerResult{
			Error: nil,
		}
	}

	taskQueued := t.(task.Task)
	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = taskPersisted
	}

	var result task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.SCHEDULED:
			result = w.StartTask(taskQueued)
		case task.COMPLETED:
			result = w.StopTask(taskQueued)
		default:
			result.Error = fmt.Errorf("We should not get here")
		}
	} else {
		result.Error = fmt.Errorf("Invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
	}

	return result
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

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("Checking status of tasks")
		w.updateTasks()
		log.Println("Task updates completed")
		log.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	// for each task in the worker's datastore:
	// 1. call InspectTask method
	// 2. verify task is in running state
	// 3. if task is not in running state, or not running at all, mark task as `failed`
	for id, t := range w.Db {
		if t.State == task.RUNNING {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("No container for running task %s\n", id)
				w.Db[id].State = task.FAILED
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("Container for task %s in non-running state %s\n", id, resp.Container.State.Status)
				w.Db[id].State = task.FAILED
			}

			// task is running, update exposed ports
			w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
		}
	}
}
