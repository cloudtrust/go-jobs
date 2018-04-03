package controller

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cloudtrust/go-jobs/job"
	"github.com/stretchr/testify/assert"
)

func TestNominalUsage(t *testing.T) {
	//new, register, schedule, now
	var jobController, err = NewController("componentName", &DummyIDGenerator{}, &DummyLockManager{make(map[string]bool)})

	assert.Nil(t, err)
	var count = 0

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		count = count + 1
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob("job", job.Steps(dummyStep))

	jobController.Register(job)
	jobController.Register(job)

	jobController.Schedule("* * * * * *", job.Name())
	time.Sleep(3 * time.Second)

	assert.True(t, count > 2)

	jobController.Stop()
	var countSnapshot = count
	time.Sleep(2 * time.Second)
	assert.Equal(t, countSnapshot, count)

	jobController.Execute("job")
	time.Sleep(1 * time.Second)
	assert.Equal(t, countSnapshot+1, count)

	jobController.Start()
	time.Sleep(2 * time.Second)
	assert.True(t, count > countSnapshot+1)
}

func TestDisabledFunctions(t *testing.T) {
	var jobController, err = NewController("componentName", &DummyIDGenerator{}, &DummyLockManager{make(map[string]bool)})

	assert.Nil(t, err)

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob("job", job.Steps(dummyStep))

	jobController.Register(job)

	assert.NotNil(t, jobController.Schedule("* * * * *", "job2"))
	assert.NotNil(t, jobController.Execute("job2"))
	assert.NotNil(t, jobController.Enable("job2"))
	assert.NotNil(t, jobController.Disable("job2"))

	assert.Nil(t, jobController.Enable("job"))
	assert.Nil(t, jobController.Disable("job"))
	assert.Nil(t, jobController.EnableAll())
	assert.Nil(t, jobController.DisableAll())

	assert.Equal(t, "componentName", jobController.ComponentName())
	assert.NotNil(t, jobController.ComponentID())

}

func TestStatusManagerOption(t *testing.T) {
	var statusStorage = &DummyStatusManager{}
	var jobController, err = NewController("componentName", &DummyIDGenerator{}, &DummyLockManager{make(map[string]bool)}, EnableStatusStorage(statusStorage))

	assert.Nil(t, err)

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob("job", job.Steps(dummyStep))

	jobController.Register(job)
	assert.Nil(t, jobController.Execute("job"))

	time.Sleep(1 * time.Second)

	assert.True(t, statusStorage.StartCalled)
	assert.True(t, statusStorage.UpdateCalled)
	assert.True(t, statusStorage.CompleteCalled)
	assert.False(t, statusStorage.FailCalled)
}

func TestLockError(t *testing.T) {
	//This test is mainly for coverage

	var jobController, err = NewController("componentName", &DummyIDGenerator{}, &FailLockManager{}, LogWith(&MockLogger{}))

	assert.Nil(t, err)

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob("job", job.Steps(dummyStep))

	jobController.Register(job)

	assert.NotNil(t, jobController.Enable("job"))
	assert.NotNil(t, jobController.Disable("job"))
	assert.NotNil(t, jobController.EnableAll())
	assert.NotNil(t, jobController.DisableAll())

}

/* Utils */

//IDGenerator
type DummyIDGenerator struct {
	i int
}

func (g *DummyIDGenerator) NextID() string {
	g.i = g.i + 1
	return strconv.Itoa(g.i)
}

//LockManager
type DummyLockManager struct {
	register map[string]bool
}

func (l *DummyLockManager) Lock(componentName, componentID, jobName, jobID string, p time.Duration) error {
	l.register[componentName+componentID+jobName+jobID] = true
	return nil
}

func (l *DummyLockManager) Unlock(componentName, componentID, jobName, jobID string) error {
	l.register[componentName+componentID+jobName+jobID] = false
	return nil
}

func (l *DummyLockManager) Disable(componentName, jobName string) error {
	return nil
}

func (l *DummyLockManager) Enable(componentName, jobName string) error {
	return nil
}

// FailLockManager
type FailLockManager struct{}

func (l *FailLockManager) Lock(componentName, componentID, jobName, jobID string, p time.Duration) error {
	return fmt.Errorf("Error")
}

func (l *FailLockManager) Unlock(componentName, componentID, jobName, jobID string) error {
	return fmt.Errorf("Error")
}

func (l *FailLockManager) Disable(componentName, jobName string) error {
	return fmt.Errorf("Error")
}

func (l *FailLockManager) Enable(componentName, jobName string) error {
	return fmt.Errorf("Error")
}

//StatusManager
type DummyStatusManager struct {
	StartCalled    bool
	UpdateCalled   bool
	CompleteCalled bool
	FailCalled     bool
}

func (s *DummyStatusManager) Start(componentName, jobName string) error {
	s.StartCalled = true
	return nil
}

func (s *DummyStatusManager) Update(componentName, jobName string, stepInfos map[string]string) error {
	s.UpdateCalled = true
	return nil
}

func (s *DummyStatusManager) Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error {
	s.CompleteCalled = true
	return nil
}

func (s *DummyStatusManager) Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error {
	s.FailCalled = true
	return nil
}

// Logger

type MockLogger struct{}

func (l *MockLogger) Log(...interface{}) error {
	return nil
}
