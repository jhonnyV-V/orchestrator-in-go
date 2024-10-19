package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/task"
	"github.com/jhonnyV-V/orch-in-go/worker"
)

type Manager struct {
	Pending       queue.Queue
	TaskDb        map[uuid.UUID]*task.Task
	EventDb       map[uuid.UUID]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
}

func New(workers []string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)
	for worker := range workers {
		workerTaskMap[workers[worker]] = []uuid.UUID{}
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        taskDb,
		EventDb:       eventDb,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
	}
}

func (m *Manager) GetTasks() []*task.Task {
	tasks := []*task.Task{}

	for _, t := range m.TaskDb {
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID.String() < tasks[j].ID.String()
	})
	return tasks
}

func (m *Manager) SelectWorker() string {
	fmt.Println("SelectWorker")
	var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
		m.LastWorker++
	} else {
		newWorker = 0
		m.LastWorker = 0
	}
	return m.Workers[newWorker]
}
func (m *Manager) SendWork() {
	fmt.Println("SendWork")
	if m.Pending.Len() <= 0 {
		log.Println("No work in Queue")
		return
	}
	workerAddr := m.SelectWorker()
	taskEvent := m.Pending.Dequeue().(task.TaskEvent)
	log.Printf("Pulled %v off pending queue\n", taskEvent.Task)

	m.EventDb[taskEvent.ID] = &taskEvent
	m.WorkerTaskMap[workerAddr] = append(m.WorkerTaskMap[workerAddr], taskEvent.Task.ID)
	m.TaskWorkerMap[taskEvent.Task.ID] = workerAddr

	taskEvent.Task.State = task.SCHEDULED
	m.TaskDb[taskEvent.Task.ID] = &taskEvent.Task

	data, err := json.Marshal(taskEvent)
	if err != nil {
		log.Printf("failed to unmarshall task event object %v\n", taskEvent)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", workerAddr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("failed to connect to %v: %v\n", workerAddr, err)
		m.Pending.Enqueue(taskEvent)
		return
	}
	decoder := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err = decoder.Decode(&e)
		if err != nil {
			fmt.Printf("failed to decode response %v\n", err)
		}
		log.Printf("response error (%d): %s\n", e.HTTPStatusCode, e.Message)
		return
	}
	newTask := task.Task{}
	err = decoder.Decode(&newTask)
	if err != nil {
		fmt.Printf("failed to decode response %v\n", err)
		return
	}
	log.Printf("%v\n", newTask)
}
func (m *Manager) UpdateTasks() {
	for {
		fmt.Printf("[Manager] Updating tasks from %d workers\n", len(m.Workers))
		m.updateTasks()
		time.Sleep(15 * time.Second)
	}
}
func (m *Manager) updateTasks() {
	fmt.Println("UpdateTasks")
	for _, workerData := range m.Workers {
		log.Printf("Checking worker %v for updates\n", workerData)
		url := fmt.Sprintf("http://%s/tasks", workerData)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("failed to connect to %v: %v\n", workerData, err)
			continue
		}
		decoder := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusOK {
			e := worker.ErrResponse{}
			err = decoder.Decode(&e)
			if err != nil {
				fmt.Printf("failed to decode response %v\n", err)
			}
			log.Printf("response error (%d): %s\n", e.HTTPStatusCode, e.Message)
			continue
		}

		var task []*task.Task
		err = decoder.Decode(&task)
		if err != nil {
			fmt.Printf("failed to decode response %v\n", err)
			return
		}

		for _, t := range task {
			_, ok := m.TaskDb[t.ID]
			if !ok {
				log.Printf("task with id %v was not found\n", t.ID)
				continue
			}

			if m.TaskDb[t.ID].State != t.State {
				m.TaskDb[t.ID].State = t.State
			}

			m.TaskDb[t.ID].StartTime = t.StartTime
			m.TaskDb[t.ID].FinishTime = t.FinishTime
			m.TaskDb[t.ID].ContainerID = t.ContainerID
		}
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("Processing any tasks in the queue")
		m.SendWork()
		log.Println("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}

}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}
