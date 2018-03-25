package actor

//go:generate mockgen -destination=./mock/lock.go -package=mock -mock_names=Lock=Lock github.com/cloudtrust/go-jobs/actor Lock
//go:generate mockgen -destination=./mock/statistics.go -package=mock -mock_names=Statistics=Statistics github.com/cloudtrust/go-jobs/actor Statistics

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Test nominal use case
// Check message sent & received
func TestWorkerNominalCase(t *testing.T) {
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
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, runnerPropsBuilder(mockNewWorkingRunnerActorBuilder)))
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
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, runnerPropsBuilder(mockNewWorkingRunnerActorBuilder)))
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
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, runnerPropsBuilder(mockNewFailingRunnerActorBuilder)))
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
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)
	mockStatistics.EXPECT().Update(gomock.Any()).Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep), job.ExecutionTimeout(2*time.Second))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, SuicideTimeout(10*time.Second), runnerPropsBuilder(mockNewSlowRunnerActorBuilder)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}
	}))

	wg.Wait()
	master.GracefulStop()

}

func TestSuicideTimeout(t *testing.T) {

	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)

	mockLock.EXPECT().Lock().Return(nil).Times(1)
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).MaxTimes(0)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Failed"}
	var expectedMessage = map[string]string{"Reason": "Invalid input"}

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)
	mockStatistics.EXPECT().Update(expectedStepInfos1).Return(nil).Times(1)
	mockStatistics.EXPECT().Cancel(expectedStepInfos2, expectedMessage).MaxTimes(0)
	mockStatistics.EXPECT().Finish(gomock.Any(), gomock.Any()).MaxTimes(0)

	var suicide = false

	var mockActorGuardianStrategy = func() actor.SupervisorStrategy {
		return actor.NewOneForOneStrategy(10, 1000, func(reason interface{}) actor.Directive {
			suicide = true
			wg.Done()
			return actor.StopDirective
		})
	}

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep), job.ExecutionTimeout(1*time.Second))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, mockStatistics, SuicideTimeout(1*time.Second), runnerPropsBuilder(mockNewInfiniteLoopRunnerActorBuilder)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(mockActorGuardianStrategy()))

	wg.Wait()
	master.GracefulStop()

	assert.True(t, suicide)

}

//Test timeout mechanism, normal timeout, execution timeout, suicide timeout

/* Utils */

/** Mock Working Runner Actor **/

type mockWorkingRunnerActor struct{}

func mockNewWorkingRunnerActorBuilder() *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockWorkingRunnerActor{}
	})
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

func mockNewFailingRunnerActorBuilder() *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockFailingRunnerActor{}
	})
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

/** Mock Slow Runner Actor **/

type Run2 struct{}
type Run3 struct{}
type Run4 struct{}

type mockSlowRunnerActor struct{}

func mockNewSlowRunnerActorBuilder() *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockSlowRunnerActor{}
	})
}

func (state *mockSlowRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Stopped:
		context.Parent().Tell(&RunnerStopped{})
	case *Run:
		var stepInfos1 = map[string]string{"step1": "Running"}
		context.Parent().Tell(&HeartBeat{StepInfos: stepInfos1})
		time.Sleep(5 * time.Second)
		context.Self().Tell(&Run4{})
	case *Run2:
		time.Sleep(5 * time.Second)
		fmt.Println("4")
		context.Self().Tell(&Run3{})
	case *Run3:
		time.Sleep(5 * time.Second)
		fmt.Println("4")
		context.Self().Tell(&Run2{})
	case *Run4:
		for {
		}
	}
}

/** Mock Infinite Loop Runner Actor **/

type mockInfiniteLoopRunnerActor struct{}

func mockNewInfiniteLoopRunnerActorBuilder() *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockInfiniteLoopRunnerActor{}
	})
}

func (state *mockInfiniteLoopRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Stopped:
		context.Parent().Tell(&RunnerStopped{})
	case *Run:
		var stepInfos1 = map[string]string{"step1": "Running"}
		context.Parent().Tell(&HeartBeat{StepInfos: stepInfos1})
		for {
			time.Sleep(2 * time.Second)
		}
	case *Run2:
		time.Sleep(2 * time.Second)
		context.Self().Tell(&Run3{})
	case *Run3:
		time.Sleep(2 * time.Second)
		context.Self().Tell(&Run4{})
	case *Run4:
		for {
			fmt.Printf("ttt")
		}
	}
}
