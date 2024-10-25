package storage

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/jhonnyV-V/orch-in-go/task"
)

type StorageType int

type Storage interface {
	Put(key uuid.UUID, value interface{}) error
	Get(key uuid.UUID) (interface{}, error)
	List() (interface{}, error)
	Count() (int, error)
}

type InMemoryTaskStore struct {
	Db map[uuid.UUID]*task.Task
}

func NewInMemoryTaskStorage() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[uuid.UUID]*task.Task),
	}
}

func (i *InMemoryTaskStore) Put(key uuid.UUID, value interface{}) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("value is %v is not a *task.Task type", value)
	}
	i.Db[key] = t
	return nil
}

func (i *InMemoryTaskStore) Get(key uuid.UUID) (interface{}, error) {
	t, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %v does not exist", key)
	}

	return t, nil
}

func (i *InMemoryTaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	for _, t := range i.Db {
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID.String() < tasks[j].ID.String()
	})
	return tasks, nil
}

func (i *InMemoryTaskStore) Count() (int, error) {
	return len(i.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[uuid.UUID]*task.TaskEvent
}

func NewInMemoryTaskEventStorage() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[uuid.UUID]*task.TaskEvent),
	}
}

func (e *InMemoryTaskEventStore) Put(key uuid.UUID, value interface{}) error {
	t, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("value is %v is not a *task.TaskEvent type", value)
	}
	e.Db[key] = t
	return nil
}

func (e *InMemoryTaskEventStore) Get(key uuid.UUID) (interface{}, error) {
	t, ok := e.Db[key]
	if !ok {
		return nil, fmt.Errorf("taskEvent with key %v does not exist", key)
	}

	return t, nil
}

func (e *InMemoryTaskEventStore) List() (interface{}, error) {
	var tasks []*task.TaskEvent
	for _, t := range e.Db {
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID.String() < tasks[j].ID.String()
	})
	return tasks, nil
}

func (e *InMemoryTaskEventStore) Count() (int, error) {
	return len(e.Db), nil
}
