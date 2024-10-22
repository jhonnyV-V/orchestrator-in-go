package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/node"
	"github.com/jhonnyV-V/orch-in-go/scheduler"
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
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
}

func New(workers []string, schedulerType string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)
	nodes := []*node.Node{}
	for worker := range workers {
		nApi := fmt.Sprintf("http://%v", workers[worker])
		n := node.NewNode(workers[worker], nApi, "workers")
		workerTaskMap[workers[worker]] = []uuid.UUID{}
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "", "roundrobin":
		s = &scheduler.RoundRobin{Name: "roundrobin"}

	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}

	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	return &Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		TaskDb:        taskDb,
		EventDb:       eventDb,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		Scheduler:     s,
		WorkerNodes:   nodes,
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

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	fmt.Println("SelectWorker")
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		err := fmt.Errorf("No available candidates to match resource request for task %v", t.ID)
		return nil, err
	}
	scores := m.Scheduler.Score(t, candidates)
	chosenOne := m.Scheduler.Pick(scores, candidates)
	return chosenOne, nil
}
func (m *Manager) SendWork() {
	fmt.Println("SendWork")
	if m.Pending.Len() <= 0 {
		log.Println("No work in Queue")
		return
	}
	taskEvent := m.Pending.Dequeue().(task.TaskEvent)
	log.Printf("Pulled %v off pending queue\n", taskEvent.Task)

	m.EventDb[taskEvent.ID] = &taskEvent

	taskWorker, ok := m.TaskWorkerMap[taskEvent.Task.ID]
	if ok {
		persistedTask := m.TaskDb[taskEvent.Task.ID]
		if taskEvent.State == task.COMPLETED && task.ValidStateTransition(persistedTask.State, taskEvent.State) {
			m.stopTask(taskWorker, taskEvent.Task.ID.String())
			return
		}
		log.Printf("invalid request: existing task %s is in state %v and cannot transition to the completed state", persistedTask.ID.String(), persistedTask.State)
		return
	}

	w, err := m.SelectWorker(taskEvent.Task)
	if err != nil {
		log.Printf("error selecting worker for task %s: %v\n", taskEvent.Task.ID, err)
		return
	}

	m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], taskEvent.Task.ID)
	m.TaskWorkerMap[taskEvent.Task.ID] = w.Name

	taskEvent.Task.State = task.SCHEDULED
	m.TaskDb[taskEvent.Task.ID] = &taskEvent.Task

	data, err := json.Marshal(taskEvent)
	if err != nil {
		log.Printf("failed to unmarshall task event object %v\n", taskEvent)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w.Name)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("failed to connect to %v: %v\n", w, err)
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
			m.TaskDb[t.ID].HostPorts = t.HostPorts
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

func getHostPort(ports nat.PortMap) *string {
	for k := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}

func (m *Manager) checkHealthTask(t task.Task) error {
	log.Printf("calling health check for task %v: %s\n", t.ID, t.HealthCheck)
	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	worker := strings.Split(w, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	log.Printf("calling health check for task %v: %s\n", t.ID, url)

	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Errorf("Error connecting to Health check %s %v\n", url, err)
		log.Printf(msg.Error())
		return msg
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Errorf("Error Health check for task %s, did not return 200\n", t.ID)
		log.Printf(msg.Error())
		return msg
	}
	log.Printf("Task %s Health check response: %v\n", t.ID, resp.StatusCode)

	return nil
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State == task.RUNNING && t.RestartCount < 3 {
			err := m.checkHealthTask(*t)
			if err != nil {
				m.restartTask(t)
			}
		} else if t.State == task.FAILED && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.SCHEDULED
	t.RestartCount++
	m.TaskDb[t.ID] = t

	taskEvent := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.RUNNING,
		Timestamp: time.Now(),
		Task:      *t,
	}

	data, err := json.Marshal(taskEvent)
	if err != nil {
		log.Printf("Unable to Marshal object %v\n", taskEvent)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Unable to connect to %v %v\n", w, err)
		m.Pending.Enqueue(t)
		return
	}
	decoder := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err = decoder.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response %v\n", err)
			return
		}
		log.Printf("response error (%d) %s\n", e.HTTPStatusCode, e.Message)
		return
	}

	newTask := task.Task{}
	err = decoder.Decode(&newTask)
	if err != nil {
		fmt.Printf("Error decoding response %v\n", err)
		return
	}

	log.Printf("%v\n", newTask)
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("Performing task health check")
		m.doHealthChecks()
		log.Println("Task health checks completed")
		log.Println("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) stopTask(worker string, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("error creating request to delete task %s: %v", taskID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("error connecting to worker at %s: %v", url, err)
		return
	}

	if resp.StatusCode != 204 {
		log.Printf("Error sending request: %v", err)
		return
	}

	log.Printf("task %s has been scheduled to be stopped", taskID)
}
