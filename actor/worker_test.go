package actor

//go:generate mockgen -destination=./mock/lock.go -package=mock -mock_names=Lock=Lock github.com/cloudtrust/go-jobs/actor Lock

import (
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/golang/mock/gomock"
	_ "github.com/stretchr/testify/assert"
)

// Test nominal use case
// Check message sent & received
func TestNominalCase(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)

	mockLock.EXPECT().Lock().Do(func() { wg.Done() }).Return(nil).Times(1)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(job, mockLock, nil, runnerProducer(mockNewWorkingRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Execute{})
		case *Status:
			result = msg.status
			output = msg.message

		}

	}))

	wg.Wait()
	master.GracefulStop()

	//	assert.Equal(t, Completed, result)
	// var expectedOutput = map[string]string{"message": "done"}
	// assert.Equal(t, expectedOutput, output)
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
		// var stepInfos1 = map[string]string{"step1": "Running"}
		// var stepInfos2 = map[string]string{"step1": "Completed"}
		var message = map[string]string{"Output": "123"}
		// context.Parent().Tell(&HeartBeat{StepInfos: stepInfos1})
		// context.Parent().Tell(&HeartBeat{StepInfos: stepInfos2})
		context.Parent().Tell(&Status{status: Completed, message: message})
	}
}

// /** Mock Failing Runner Actor **/

// type mockFailingRunnerActor struct{}

// func mockNewFailingRunnerActor() actor.Actor {
// 	return &mockFailingRunnerActor{}
// }

// func (state *mockFailingRunnerActor) Receive(context actor.Context) {
// 	switch context.Message().(type) {
// 	case *Run:
// 		var stepInfos1 = map[string]string{"step1": "Running"}
// 		var stepInfos2 = map[string]string{"step1": "Completed"}
// 		var message = map[string]string{"Output": "123"}
// 		context.Parent().Tell(&HeartBeat{StepInfos: stepInfos1})
// 		context.Parent().Tell(&HeartBeat{StepInfos: stepInfos2})
// 		context.Parent().Tell(&Status{status: Completed, message: message})
// 	}
// }

// /** Mock Infinite Loop Runner Actor **/

// type mockInfiniteLoopRunnerActor struct{}

// func mockNewInfiniteLoopRunnerActor() actor.Actor {
// 	return &mockInfiniteLoopRunnerActor{}
// }

// func (state *mockInfiniteLoopRunnerActor) Receive(context actor.Context) {
// 	switch context.Message().(type) {
// 	case *Run:
// 		// Infinite loop
// 		for {
// 		}
// 	}
// }
