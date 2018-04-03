package actor

import (
	"context"
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// test nominal

func TestNominalCase(t *testing.T) {
	var componentName = "componentName"
	var componentID = "componentID"
	var jobName = "jobName"
	var jobID = "id1"

	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, jobID, 0).Do(func() { wg.Done() }).Return(nil).Times(1)

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, jobName).Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	props := BuildMasterActorProps(componentName, componentID, workerPropsBuilder(mockBuilderWorkerActorProps))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{JobID: "job", Job: job, IdGenerator: nil, LockManager: mockLockManager, StatusManager: mockStatusManager})
	master.Tell(&StartJob{JobID: "job"})

	wg.Wait()
	master.GracefulStop()

}

// test handling of worker panic -> restart

func TestWorkerRestartWhenPanicOccurs(t *testing.T) {
	var componentName = "componentName"
	var componentID = "componentID"

	var wg sync.WaitGroup

	wg.Add(1)

	var workerNumberOfCall = 0

	var mockStep = func(context.Context, interface{}) (interface{}, error) {
		workerNumberOfCall = workerNumberOfCall + 1

		if workerNumberOfCall == 2 {
			wg.Done()
		}

		return nil, nil
	}

	var job, _ = job.NewJob("job", job.Steps(mockStep))

	props := BuildMasterActorProps(componentName, componentID, workerPropsBuilder(mockBuilderFailingWorkerActorProps))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{JobID: "job", Job: job, LockManager: nil, StatusManager: nil})
	master.Tell(&StartJob{JobID: "job"})

	wg.Wait()
	master.GracefulStop()

	assert.Equal(t, 2, workerNumberOfCall)
}

func TestAlwaysPanicDecider(t *testing.T) {
	assert.Panics(t, func() {
		alwaysPanicDecider(nil)
	})
}

//Note: Supervision and Guardian strategy for Master Actor is tested in worker_test.

/* Utils */

/** Working Worker Actor **/
type mockWorkerActor struct {
	componentName string
	componentID   string
	idGenerator   IdGenerator
	job           *job.Job
	lockManager   LockManager
	statusManager StatusManager
}

func mockBuilderWorkerActorProps(componentName, componentID string, j *job.Job, idGenerator IdGenerator, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockWorkerActor{job: j, lockManager: l, statusManager: s}
	})
}

func (state *mockWorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		state.statusManager.Start(state.componentName, state.job.Name())
	case *Execute:
		var jobID = state.idGenerator.NextId()
		state.lockManager.Lock(state.componentName, state.componentID, state.job.Name(), jobID, 0)
	}

}

/** Failing Worker Actor **/
type mockFailingWorkerActor struct {
	job           *job.Job
	lockManager   LockManager
	statusManager StatusManager
}

func mockBuilderFailingWorkerActorProps(componentName, componentID string, j *job.Job, idGenerator IdGenerator, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockFailingWorkerActor{job: j, lockManager: l, statusManager: s}
	})
}

func (state *mockFailingWorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		state.job.Steps()[0](nil, nil)
	case *Execute:
		panic("Test")
	}

}
