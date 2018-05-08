package controller

//go:generate mockgen -destination=./mock/lock_manager.go -package=mock -mock_names=LockManager=LockManager github.com/cloudtrust/go-jobs LockManager
//go:generate mockgen -destination=./mock/status_manager.go -package=mock -mock_names=StatusManager=StatusManager github.com/cloudtrust/go-jobs StatusManager
//go:generate mockgen -destination=./mock/id_generator.go -package=mock -mock_names=IDGenerator=IDGenerator github.com/cloudtrust/go-jobs IDGenerator
//go:generate mockgen -destination=./mock/logger.go -package=mock -mock_names=Logger=Logger github.com/cloudtrust/go-jobs Logger

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cloudtrust/go-jobs/job"
	"github.com/cloudtrust/go-jobs/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestNominalUsage(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		jobName       = "jobName"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		id            = rand.Uint64()
	)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().DoAndReturn(func() string {
		id = id + 1
		return strconv.FormatUint(id, 10)
	}).Times(6)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Return(nil).Times(6)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Return(nil).Times(6)

	//new, register, schedule, now
	var jobController = NewController(componentName, componentID, mockIDGen, mockLockManager)

	var count = 0

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		count = count + 1
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob(jobName, job.Steps(dummyStep))

	jobController.Register(job)
	jobController.Register(job)

	jobController.Schedule("* * * * * *", job.Name())
	time.Sleep(3 * time.Second)

	assert.True(t, count > 2)

	jobController.Stop()
	var countSnapshot = count
	time.Sleep(2 * time.Second)
	assert.Equal(t, countSnapshot, count)

	jobController.Execute(jobName)
	time.Sleep(1 * time.Second)
	assert.Equal(t, countSnapshot+1, count)

	jobController.Start()
	time.Sleep(2 * time.Second)
	assert.True(t, count > countSnapshot+1)

	jobController.Stop()
}

func TestDisabledFunctions(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Enable(componentName, "job").Return(nil).Times(2)
	mockLockManager.EXPECT().Disable(componentName, "job").Return(nil).Times(2)

	var jobController = NewController(componentName, componentID, mockIDGen, mockLockManager)

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

	assert.Equal(t, componentName, jobController.ComponentName())
	assert.NotNil(t, jobController.ComponentID())
}

func TestStatusManagerOption(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		jobName       = "jobName"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		id            = strconv.FormatUint(rand.Uint64(), 10)
		stepInfos     = map[string]string{"github.com/cloudtrust/go-jobs.TestStatusManagerOption.func1": "Completed"}
		message       = map[string]string{"message": "done"}
	)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Register(componentName, componentID, jobName, id).Times(1)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(componentName, componentID, jobName, stepInfos).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(componentName, componentID, jobName, stepInfos).Return(nil).Times(1)
	mockStatusManager.EXPECT().Complete(componentName, componentID, jobName, id, stepInfos, message).Return(nil).Times(1)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, id, gomock.Any()).Return(nil).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, id).Return(nil).Times(1)

	var jobController = NewController(componentName, componentID, mockIDGen, mockLockManager, EnableStatusStorage(mockStatusManager))

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob(jobName, job.Steps(dummyStep))

	jobController.Register(job)
	assert.Nil(t, jobController.Execute(jobName))

	time.Sleep(1 * time.Second)
}

func TestEnableDisable(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		jobName       = "jobName"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Enable(componentName, jobName).Return(fmt.Errorf("fail")).Times(2)
	mockLockManager.EXPECT().Disable(componentName, jobName).Return(fmt.Errorf("fail")).Times(2)

	var mockLogger = mock.NewLogger(mockCtrl)

	var jobController = NewController(componentName, componentID, mockIDGen, mockLockManager, LogWith(mockLogger))

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob(jobName, job.Steps(dummyStep))

	jobController.Register(job)

	assert.NotNil(t, jobController.Enable(jobName))
	assert.NotNil(t, jobController.Disable(jobName))
	assert.NotNil(t, jobController.EnableAll())
	assert.NotNil(t, jobController.DisableAll())

}
