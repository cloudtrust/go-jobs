package actor

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/golang/mock/gomock"
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

	props := actor.FromProducer(NewMasterActor(workerProducerBuilder(mockNewWorkerActor)))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{label: "job", job: job, lock: mockLock, statistics: mockStatistics})
	master.Tell(&StartJob{label: "job"})

	wg.Wait()
	master.GracefulStop()

}

// test panic restart
/*
func TestRestartCase(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)
	mockLock.EXPECT().Lock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	props := actor.FromProducer(NewMasterActor(workerProducerBuilder(mockNewFailingWorkerActor))).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(masterActorGuardianStrategy())
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{label: "job", job: job, lock: mockLock, statistics: mockStatistics})
	master.Tell(&StartJob{label: "job"})

	wg.Wait()
	master.GracefulStop()

}*/

//test suicide

/* Utils */

/** Working Worker Actor **/
type mockWorkerActor struct {
	job        *job.Job
	lock       Lock
	statistics Statistics
}

func mockNewWorkerActor(j *job.Job, l Lock, s Statistics, options ...WorkerOption) func() actor.Actor {
	return func() actor.Actor {
		return &mockWorkerActor{job: j, lock: l, statistics: s}
	}
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

func mockNewFailingWorkerActor(j *job.Job, l Lock, s Statistics, options ...WorkerOption) func() actor.Actor {
	return func() actor.Actor {
		return &mockFailingWorkerActor{job: j, lock: l, statistics: s}
	}
}

func (state *mockFailingWorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		fmt.Println("Start")
		state.statistics.Start()
	case *actor.Stopped:
		fmt.Println("stop")
	case *Execute:
		fmt.Println("testtst")
		panic("Test")
	}

}
