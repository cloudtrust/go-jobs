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
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)
	mockLock.EXPECT().Lock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	props := BuildMasterActorProps(workerPropsBuilder(mockBuilderWorkerActorProps))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{label: "job", job: job, lock: mockLock, statistics: mockStatistics})
	master.Tell(&StartJob{label: "job"})

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

	props := BuildMasterActorProps(workerPropsBuilder(mockBuilderFailingWorkerActorProps))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{label: "job", job: job, lock: nil, statistics: nil})
	master.Tell(&StartJob{label: "job"})

	wg.Wait()
	master.GracefulStop()

	assert.Equal(t, 2, workerNumberOfCall)
}

func TestAlwaysPanicDecider(t *testing.T){
	assert.Panics(t, func(){
		alwaysPanicDecider(nil)
	})
}

//Note: Supervision and Guardian strategy for Master Actor is tested in worker_test.


/* Utils */

/** Working Worker Actor **/
type mockWorkerActor struct {
	job        *job.Job
	lock       Lock
	statistics Statistics
}

func mockBuilderWorkerActorProps(j *job.Job, l Lock, s Statistics, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockWorkerActor{job: j, lock: l, statistics: s}
	})
}

func (state *mockWorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		state.statistics.Start()
	case *Execute:
		state.lock.Lock()
	}

}

/** Failing Worker Actor **/
type mockFailingWorkerActor struct {
	job        *job.Job
	lock       Lock
	statistics Statistics
}

func mockBuilderFailingWorkerActorProps(j *job.Job, l Lock, s Statistics, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockFailingWorkerActor{job: j, lock: l, statistics: s}
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
