package actor

//go:generate mockgen -destination=./mock/lock.go -package=mock -mock_names=Lock=Lock github.com/cloudtrust/go-jobs/actor Lock
//go:generate mockgen -destination=./mock/statistics.go -package=mock -mock_names=Statistics=Statistics github.com/cloudtrust/go-jobs/actor Statistics

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/golang/mock/gomock"
)

// Test nominal use case
// Check message sent & received
func TestNominalCase(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)
	mockLock.EXPECT().Lock().Return(nil).Times(1)
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Completed"}
	var expectedMessage = map[string]string{"Output": "123"}

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)
	mockStatistics.EXPECT().Update(expectedStepInfos1).Return(nil).Times(1)
	mockStatistics.EXPECT().Finish(expectedStepInfos2, expectedMessage).Times(1)
	mockStatistics.EXPECT().Cancel(gomock.Any(), gomock.Any()).MaxTimes(0)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, runnerProducer(mockNewWorkingRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}))

	wg.Wait()
	master.GracefulStop()

}

func TestAlreadyLocked(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)

	mockLock.EXPECT().Lock().Do(func() { wg.Done() }).Return(errors.New("Error")).Times(1)
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).MaxTimes(0)

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).MaxTimes(0)
	mockStatistics.EXPECT().Update(gomock.Any()).Return(nil).MaxTimes(0)
	mockStatistics.EXPECT().Finish(gomock.Any(), gomock.Any()).MaxTimes(0)
	mockStatistics.EXPECT().Cancel(gomock.Any(), gomock.Any()).MaxTimes(0)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, runnerProducer(mockNewWorkingRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}))

	wg.Wait()
	master.GracefulStop()
}

func TestFailure(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)

	mockLock.EXPECT().Lock().Return(nil).Times(1)
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Failed"}
	var expectedMessage = map[string]string{"Reason": "Invalid input"}

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)
	mockStatistics.EXPECT().Update(expectedStepInfos1).Return(nil).Times(1)
	mockStatistics.EXPECT().Cancel(expectedStepInfos2, expectedMessage).Times(1)
	mockStatistics.EXPECT().Finish(gomock.Any(), gomock.Any()).MaxTimes(0)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, runnerProducer(mockNewFailingRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}))

	wg.Wait()
	master.GracefulStop()
}

func TestExecutionTimeout(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)

	mockLock.EXPECT().Lock().Return(nil).Times(1)
	mockLock.EXPECT().Unlock().Do(func() {}).Return(nil).Times(1)

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep), job.ExecutionTimeout(1*time.Second))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, SuicideTimeout(5*time.Second), runnerProducer(mockNewInfiniteLoopRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		case *actor.Stop:
			wg.Done()
			panic("YEP")
		}

	}).WithGuardian(actor.NewOneForOneStrategy(10, 10, func(reason interface{}) actor.Directive {
		return actor.StopDirective
	})))

	wg.Wait()
	master.GracefulStop()

}

func TestSuicideTimeout(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)

	mockLock.EXPECT().Lock().Return(nil).Times(1)
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Failed"}
	var expectedMessage = map[string]string{"Reason": "Invalid input"}

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)
	mockStatistics.EXPECT().Update(expectedStepInfos1).Return(nil).Times(1)
	mockStatistics.EXPECT().Cancel(expectedStepInfos2, expectedMessage).Times(1)
	mockStatistics.EXPECT().Finish(gomock.Any(), gomock.Any()).MaxTimes(0)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, SuicideTimeout(1*time.Second), runnerProducer(mockNewInfiniteLoopRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}))

	wg.Wait()
	master.GracefulStop()
}

//Test timeout mechanism, normal timeout, execution timeout, suicide timeout

/* Utils */

/** Mock Working Runner Actor **/

type mockWorkingRunnerActor struct{}

func mockNewWorkingRunnerActor() actor.Actor {
	return &mockWorkingRunnerActor{}
}

func (state *mockWorkingRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		var stepInfos1 = map[string]string{"step1": "Running"}
		var stepInfos2 = map[string]string{"step1": "Completed"}
		var message = map[string]string{"Output": "123"}
		context.Parent().Tell(&HeartBeat{StepInfos: stepInfos1})
		context.Parent().Tell(&Status{status: Completed, message: message, infos: stepInfos2})
		context.Parent().Tell(&RunnerStopped{})
	}
}

// /** Mock Failing Runner Actor **/

type mockFailingRunnerActor struct{}

func mockNewFailingRunnerActor() actor.Actor {
	return &mockFailingRunnerActor{}
}

func (state *mockFailingRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		var stepInfos1 = map[string]string{"step1": "Running"}
		var stepInfos2 = map[string]string{"step1": "Failed"}
		var message = map[string]string{"Reason": "Invalid input"}
		context.Parent().Tell(&HeartBeat{StepInfos: stepInfos1})
		context.Parent().Tell(&Status{status: Failed, message: message, infos: stepInfos2})
		context.Parent().Tell(&RunnerStopped{})
	}
}

// /** Mock Infinite Loop Runner Actor **/

type mockInfiniteLoopRunnerActor struct{}

func mockNewInfiniteLoopRunnerActor() actor.Actor {
	return &mockInfiniteLoopRunnerActor{}
}

func (state *mockInfiniteLoopRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		time.Sleep(10 * time.Second)
	}
}

func mockActorSupervisorStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(10, 10, mockActorDecider)
}

func mockActorDecider(reason interface{}) actor.Directive {
	switch reason {
	case "KillTimeoutExceeded":
		return actor.EscalateDirective
	default:
		return actor.RestartDirective
	}
}
