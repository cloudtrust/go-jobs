package actor

import (
	"context"
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/cloudtrust/go-jobs/lock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// test nominal

func TestNominalCase(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start().Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	var idGenerator = &DummyIdGenerator{}
	var lockMode = lock.Local
	var statusStorageEnabled = false
	var dbStatus, dbLock DB

	props := BuildMasterActorProps("componentName", "componentID", idGenerator, lockMode, statusStorageEnabled, dbStatus, dbLock, workerPropsBuilder(mockBuilderWorkerActorProps))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{JobID: "job", Job: job})
	master.Tell(&StartJob{JobID: "job"})

	wg.Wait()
	master.GracefulStop()
}

// test handling of worker panic -> restart

func TestWorkerRestartWhenPanicOccurs(t *testing.T) {

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

	var idGenerator = &DummyIdGenerator{}
	var lockMode = lock.Local
	var statusStorageEnabled = false
	var dbStatus, dbLock DB

	props := BuildMasterActorProps("componentName", "componentID", idGenerator, lockMode, statusStorageEnabled, dbStatus, dbLock, workerPropsBuilder(mockBuilderFailingWorkerActorProps))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{JobID: "job", Job: job})
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

type DummyIdGenerator struct {
	i int
}

func (g *DummyIdGenerator) NextId() string {
	g.i = g.i + 1
	return string(g.i)
}

/** Working Worker Actor **/
type mockWorkerActor struct {
	job           *job.Job
	lockManager   LockManager
	statusManager StatusManager
}

func mockBuilderWorkerActorProps(j *job.Job, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockWorkerActor{job: j, lockManager: l, statusManager: s}
	})
}

func (state *mockWorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		state.statusManager.Start()
	case *Execute:
		state.lockManager.Lock()
	}

}

/** Failing Worker Actor **/
type mockFailingWorkerActor struct {
	job           *job.Job
	lockManager   LockManager
	statusManager StatusManager
}

func mockBuilderFailingWorkerActorProps(j *job.Job, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props {
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
