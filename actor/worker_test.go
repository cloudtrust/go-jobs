package actor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Test nominal use case
// Check message sent & received
func TestWorkerNominalCase(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = "componentID"
		jobName       = "jobName"
		wg            sync.WaitGroup
		id            = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Do(func(string, string, string, string) { wg.Done() }).Return(nil).Times(1)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Completed"}
	var expectedMessage = map[string]string{"Output": "123"}

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(componentName, componentID, jobName, expectedStepInfos1).Return(nil).Times(1)
	mockStatusManager.EXPECT().Complete(componentName, componentID, jobName, gomock.Any(), expectedStepInfos2, expectedMessage).Times(1)
	mockStatusManager.EXPECT().Register(componentName, componentID, jobName, id)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob(jobName, job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := BuildWorkerActorProps(componentName, componentID, log.NewNopLogger(), job, mockIDGen, mockLockManager, mockStatusManager,
				runnerPropsBuilder(mockNewWorkingRunnerActorBuilder))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}
	}))

	wg.Wait()
	master.GracefulStop()
}

func TestAlreadyLocked(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = "componentID"
		jobName       = "jobName"
		wg            sync.WaitGroup
		id            = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Do(func(string, string, string, string, time.Duration) { wg.Done() }).Return(errors.New("Already locked")).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Do(func(string, string, string, string) { wg.Done() }).Return(nil).MaxTimes(0)

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).MaxTimes(0)
	mockStatusManager.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).MaxTimes(0)
	mockStatusManager.EXPECT().Complete(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(0)
	mockStatusManager.EXPECT().Fail(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(0)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob(jobName, job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := BuildWorkerActorProps(componentName, componentID, log.NewNopLogger(), job, mockIDGen, mockLockManager, mockStatusManager,
				runnerPropsBuilder(mockNewWorkingRunnerActorBuilder))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}))

	wg.Wait()
	master.GracefulStop()
}

func TestFailure(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = "componentID"
		jobName       = "jobName"
		wg            sync.WaitGroup
		id            = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Do(func(string, string, string, string) { wg.Done() }).Return(nil).Times(1)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Failed"}
	var expectedMessage = map[string]string{"Reason": "Invalid input"}

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(componentName, componentID, jobName, expectedStepInfos1).Return(nil).Times(1)
	mockStatusManager.EXPECT().Fail(componentName, componentID, jobName, gomock.Any(), expectedStepInfos2, expectedMessage).Times(1)
	mockStatusManager.EXPECT().Complete(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(0)
	mockStatusManager.EXPECT().Register(componentName, componentID, jobName, id)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob(jobName, job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := BuildWorkerActorProps(componentName, componentID, log.NewNopLogger(), job, mockIDGen, mockLockManager, mockStatusManager,
				runnerPropsBuilder(mockNewFailingRunnerActorBuilder))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}
	}))

	wg.Wait()
	master.GracefulStop()
}

func TestExecutionTimeout(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = "componentID"
		jobName       = "jobName"
		wg            sync.WaitGroup
		id            = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Do(func(string, string, string, string) { wg.Done() }).Return(nil).Times(1)

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mockStatusManager.EXPECT().Register(componentName, componentID, jobName, id)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob(jobName, job.Steps(successfulStep), job.NormalDuration(10*time.Millisecond), job.ExecutionTimeout(2*time.Second), job.SuicideTimeout(10*time.Second))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := BuildWorkerActorProps(componentName, componentID, log.NewNopLogger(), job, mockIDGen, mockLockManager, mockStatusManager,
				runnerPropsBuilder(mockNewSlowRunnerActorBuilder))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}
	}))

	wg.Wait()
	master.GracefulStop()

	// No more assertion needed, if the job finally terminates, it means unlock has been called.
	// Unlock is only called if the runner actor has stopped.

}

func TestSuicideTimeout(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = "componentID"
		jobName       = "jobName"
		wg            sync.WaitGroup
		id            = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Do(func() { wg.Done() }).Return(nil).MaxTimes(0)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Failed"}
	var expectedMessage = map[string]string{"Reason": "Invalid input"}

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(componentName, componentID, jobName, expectedStepInfos1).Return(nil).Times(1)
	mockStatusManager.EXPECT().Fail(componentName, componentID, jobName, gomock.Any(), expectedStepInfos2, expectedMessage).MaxTimes(0)
	mockStatusManager.EXPECT().Complete(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(0)
	mockStatusManager.EXPECT().Register(componentName, componentID, jobName, id)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	var suicide = false

	var mockActorGuardianStrategy = func() actor.SupervisorStrategy {
		return actor.NewOneForOneStrategy(10, 1000, func(reason interface{}) actor.Directive {
			suicide = true
			wg.Done()
			return actor.StopDirective
		})
	}

	wg.Add(1)

	var job, _ = job.NewJob(jobName, job.Steps(successfulStep), job.ExecutionTimeout(1*time.Second), job.SuicideTimeout(1*time.Second))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := BuildWorkerActorProps(componentName, componentID, log.NewNopLogger(), job, mockIDGen, mockLockManager, mockStatusManager,
				runnerPropsBuilder(mockNewInfiniteLoopRunnerActorBuilder))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}

	}).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(mockActorGuardianStrategy()))

	wg.Wait()
	master.GracefulStop()

	assert.True(t, suicide)

}

func TestRunnerRestartWhenPanicOccurs(t *testing.T) {
	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		componentName = "componentName"
		componentID   = "componentID"
		jobName       = "jobName"
		wg            sync.WaitGroup
		id            = strconv.FormatUint(rand.Uint64(), 10)
	)

	var mockLockManager = mock.NewLockManager(mockCtrl)
	mockLockManager.EXPECT().Lock(componentName, componentID, jobName, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	mockLockManager.EXPECT().Unlock(componentName, componentID, jobName, gomock.Any()).Do(func() { wg.Done() }).Return(nil).MinTimes(0)

	var mockStatusManager = mock.NewStatusManager(mockCtrl)
	mockStatusManager.EXPECT().Start(componentName, componentID, jobName).Return(nil).Times(1)
	mockStatusManager.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).MinTimes(4)
	mockStatusManager.EXPECT().Fail(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(0).MaxTimes(0)
	mockStatusManager.EXPECT().Complete(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(0).MaxTimes(0)
	mockStatusManager.EXPECT().Register(componentName, componentID, jobName, id)

	var mockIDGen = mock.NewIDGenerator(mockCtrl)
	mockIDGen.EXPECT().NextID().Return(id).Times(1)

	wg.Add(1)

	var firstStepNumberOfLaunch = 0

	var firstStepLaunched = func(context.Context, interface{}) (interface{}, error) {
		firstStepNumberOfLaunch = firstStepNumberOfLaunch + 1

		if firstStepNumberOfLaunch == 2 {
			wg.Done()
		}

		return nil, nil
	}

	var job, _ = job.NewJob(jobName, job.Steps(firstStepLaunched, panicStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *actor.Started:
			props := BuildWorkerActorProps(componentName, componentID, log.NewNopLogger(), job, mockIDGen, mockLockManager, mockStatusManager)
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		}
	}))

	wg.Wait()
	master.GracefulStop()

	assert.True(t, firstStepNumberOfLaunch >= 2)
}

//TODO Test normal timeout

/* Utils */

/** Mock Working Runner Actor **/

type mockWorkingRunnerActor struct {
	jobID string
	job   *job.Job
}

func mockNewWorkingRunnerActorBuilder(logger Logger, jobID string, j *job.Job) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockWorkingRunnerActor{jobID: jobID, job: j}
	})
}

func (state *mockWorkingRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		var stepInfos1 = map[string]string{"step1": "Running"}
		var stepInfos2 = map[string]string{"step1": "Completed"}
		var message = map[string]string{"Output": "123"}
		context.Parent().Tell(&StepStatus{JobID: state.jobID, StepInfos: stepInfos1})
		context.Parent().Tell(&Status{JobID: state.jobID, status: Completed, message: message, infos: stepInfos2})
		context.Parent().Tell(&RunnerStopped{JobID: state.jobID})
	}
}

// /** Mock Failing Runner Actor **/

type mockFailingRunnerActor struct {
	jobID string
	job   *job.Job
}

func mockNewFailingRunnerActorBuilder(logger Logger, jobID string, j *job.Job) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockFailingRunnerActor{jobID: jobID, job: j}
	})
}

func (state *mockFailingRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Started:
		var stepInfos1 = map[string]string{"step1": "Running"}
		var stepInfos2 = map[string]string{"step1": "Failed"}
		var message = map[string]string{"Reason": "Invalid input"}
		context.Parent().Tell(&StepStatus{JobID: state.jobID, StepInfos: stepInfos1})
		context.Parent().Tell(&Status{JobID: state.jobID, status: Failed, message: message, infos: stepInfos2})
		context.Parent().Tell(&RunnerStopped{JobID: state.jobID})
	}
}

/** Mock Slow Runner Actor **/

type Run2 struct{}
type Run3 struct{}
type Run4 struct{}

type mockSlowRunnerActor struct {
	jobID string
	job   *job.Job
}

func mockNewSlowRunnerActorBuilder(logger Logger, jobID string, j *job.Job) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockSlowRunnerActor{jobID: jobID, job: j}
	})
}

func (state *mockSlowRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Stopped:
		context.Parent().Tell(&RunnerStopped{JobID: state.jobID})
	case *actor.Started:
		var stepInfos1 = map[string]string{"step1": "Running"}
		context.Parent().Tell(&StepStatus{JobID: state.jobID, StepInfos: stepInfos1})
		time.Sleep(5 * time.Second)
		context.Self().Tell(&Run2{})
	case *Run2:
		time.Sleep(5 * time.Second)
		context.Self().Tell(&Run3{})
	case *Run3:
		time.Sleep(5 * time.Second)
		context.Self().Tell(&Run2{})
	}
}

/** Mock Infinite Loop Runner Actor **/

type mockInfiniteLoopRunnerActor struct {
	jobID string
	job   *job.Job
}

func mockNewInfiniteLoopRunnerActorBuilder(logger Logger, jobID string, j *job.Job) *actor.Props {
	return actor.FromProducer(func() actor.Actor {
		return &mockInfiniteLoopRunnerActor{jobID: jobID, job: j}
	})
}

func (state *mockInfiniteLoopRunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *actor.Stopped:
		context.Parent().Tell(&RunnerStopped{JobID: state.jobID})
	case *actor.Started:
		var stepInfos1 = map[string]string{"step1": "Running"}
		context.Parent().Tell(&StepStatus{JobID: state.jobID, StepInfos: stepInfos1})
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
			fmt.Printf("")
		}
	}
}
