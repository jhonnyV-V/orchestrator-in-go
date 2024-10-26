package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/boltdb/bolt"
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

type TaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewTaskStore(file string, mode os.FileMode, bucket string) (*TaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open file %s: %v\n", file, err)
	}
	t := &TaskStore{
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
		Db:       db,
	}

	err = t.CreateBucket()
	if err != nil {
		log.Printf("bucket %s already exist", bucket)
	}

	return t, nil
}
func (t *TaskStore) CreateBucket() error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(t.Bucket))
		if err != nil {
			return fmt.Errorf("failed to create bucker %s: %v", t.Bucket, err)
		}
		return nil
	})
}

func (t *TaskStore) Close() {
	t.Db.Close()
}

func (t *TaskStore) Count() (int, error) {
	taskCount := 0
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, nil
}

func (t *TaskStore) Put(key uuid.UUID, value interface{}) error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.Bucket))

		buf, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(key.String()), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (t *TaskStore) Get(key uuid.UUID) (interface{}, error) {
	var sTask task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.Bucket))
		result := bucket.Get([]byte(key.String()))
		if result == nil {
			return fmt.Errorf("task %v not found", key)
		}

		err := json.Unmarshal(result, &sTask)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &sTask, nil
}

func (t *TaskStore) List() (interface{}, error) {
	var tasks []*task.Task

	err := t.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(t.Bucket))
		err := bucket.ForEach(func(k, v []byte) error {
			var sTask task.Task
			err := json.Unmarshal(v, &sTask)
			if err != nil {
				return err
			}
			tasks = append(tasks, &sTask)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return tasks, nil
}

type EventStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewEventStore(file string, mode os.FileMode, bucket string) (*EventStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open file %s: %v\n", file, err)
	}
	t := &EventStore{
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
		Db:       db,
	}

	err = t.CreateBucket()
	if err != nil {
		log.Printf("bucket %s already exist", bucket)
	}

	return t, nil
}
func (e *EventStore) CreateBucket() error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(e.Bucket))
		if err != nil {
			return fmt.Errorf("failed to create bucker %s: %v", e.Bucket, err)
		}
		return nil
	})
}

func (e *EventStore) Close() {
	e.Db.Close()
}

func (e *EventStore) Count() (int, error) {
	eventCount := 0
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		b.ForEach(func(k, v []byte) error {
			eventCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return eventCount, nil
}

func (e *EventStore) Put(key uuid.UUID, value interface{}) error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(e.Bucket))

		buf, err := json.Marshal(value.(*task.TaskEvent))
		if err != nil {
			return err
		}
		err = bucket.Put([]byte(key.String()), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (e *EventStore) Get(key uuid.UUID) (interface{}, error) {
	var sTask task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(e.Bucket))
		result := bucket.Get([]byte(key.String()))
		if result == nil {
			return fmt.Errorf("task %v not found", key)
		}

		err := json.Unmarshal(result, &sTask)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &sTask, nil
}

func (e *EventStore) List() (interface{}, error) {
	var tasks []*task.TaskEvent

	err := e.Db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(e.Bucket))
		err := bucket.ForEach(func(k, v []byte) error {
			var sTask task.TaskEvent
			err := json.Unmarshal(v, &sTask)
			if err != nil {
				return err
			}
			tasks = append(tasks, &sTask)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return tasks, nil
}
