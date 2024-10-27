package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/task"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	taskEvent := task.TaskEvent{}
	err := decoder.Decode(&taskEvent)
	if err != nil {
		msg := fmt.Sprintf("Eror unmarshaling body %v\n", err)
		log.Printf(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Manager.AddTask(taskEvent)
	log.Printf("Added task %v\n", taskEvent.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(taskEvent.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Println("no task id")
		w.WriteHeader(400)
		return
	}

	//TODO: handle bad uuid
	tId, _ := uuid.Parse(taskID)

	taskToStop, err := a.Manager.TaskDb.Get(tId)
	if err != nil {
		log.Printf("No task with id %v found\n", tId)
		w.WriteHeader(404)
		return
	}

	taskEvent := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.COMPLETED,
		Timestamp: time.Now(),
	}

	taskCopy := *(taskToStop.(*task.Task))
	taskCopy.State = task.COMPLETED
	taskEvent.Task = taskCopy

	a.Manager.AddTask(taskEvent)
	log.Printf("added task %v to stop container %v\n", taskCopy.ID, taskCopy.ContainerID)
	w.WriteHeader(204)
}

func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.WorkerNodes)
}
