package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Printf(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Worker.AddTask(taskEvent.Task)
	log.Printf("added task %v\n", taskEvent.Task.ID)
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(taskEvent.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Worker.GetTasks())
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

	taskToStop, ok := a.Worker.Db[tId]
	if !ok {
		log.Printf("No task with id %v found\n", tId)
		w.WriteHeader(404)
		return
	}

	taskCopy := *taskToStop
	taskCopy.State = task.COMPLETED
	a.Worker.AddTask(taskCopy)
	log.Printf("added task %v to stop container %v\n", taskCopy.ID, taskCopy.ContainerID)
	w.WriteHeader(204)
}
