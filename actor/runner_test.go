package actor

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

//TODO add check output of stepInfos + check number of call to HeartBeat

func TestOneSuccessfulStepCase(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	worker := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := BuildRunnerActorProps(log.NewNopLogger(), "id1", job)
			c.Spawn(props)
		case *Status:
			result = msg.status
			output = msg.message
			wg.Done()
		}

	}))

	wg.Wait()
	worker.GracefulStop()

	assert.Equal(t, Completed, result)
	var expectedOutput = map[string]string{"message": "done"}
	assert.Equal(t, expectedOutput, output)
}

func TestMultipleStepWiring(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(listInt, sum))

	worker := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := BuildRunnerActorProps(log.NewNopLogger(), "id1", job)
			c.Spawn(props)
		case *Status:
			result = msg.status
			output = msg.message
			wg.Done()
		}

	}))

	wg.Wait()
	worker.GracefulStop()

	assert.Equal(t, Completed, result)

	expectedOutput := map[string]string{"sum": "6"}
	assert.Equal(t, expectedOutput, output)
}

func TestInvalidResultFormat(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(listInt))

	worker := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := BuildRunnerActorProps(log.NewNopLogger(), "id1", job)
			c.Spawn(props)
		case *Status:
			result = msg.status
			output = msg.message
			wg.Done()
		}

	}))

	wg.Wait()
	worker.GracefulStop()

	assert.Equal(t, Failed, result)

	expectedOutput := map[string]string{"Reason": "Invalid type result for last step"}
	assert.Equal(t, expectedOutput, output)
}

func TestCleanupStepExecution(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(listInt), job.Cleanup(cleanUp))

	worker := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := BuildRunnerActorProps(log.NewNopLogger(), "id1", job)
			c.Spawn(props)
		case *Status:
			result = msg.status
			output = msg.message
			wg.Done()
		}

	}))

	wg.Wait()
	worker.GracefulStop()

	assert.Equal(t, Failed, result)

	expectedOutput := map[string]string{"cleanup": "ok", "Reason": "Invalid type result for last step"}
	assert.Equal(t, expectedOutput, output)
}

func TestErrorHandling(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(listInt, errorStep, sum), job.Cleanup(failingCleanUp))

	worker := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := BuildRunnerActorProps(log.NewNopLogger(), "id1", job)
			c.Spawn(props)
		case *Status:
			result = msg.status
			output = msg.message
			wg.Done()
		}

	}))

	wg.Wait()
	worker.GracefulStop()

	assert.Equal(t, Failed, result)

	expectedOutput := map[string]string{"Reason": "Step failed", "CleanupError": "Cleanup error"}
	assert.Equal(t, expectedOutput, output)

}

/* Utils */

func successfulStep(context.Context, interface{}) (interface{}, error) {
	return map[string]string{"message": "done"}, nil
}

func errorStep(context.Context, interface{}) (interface{}, error) {
	return nil, errors.New("Step failed")
}

func panicStep(context.Context, interface{}) (interface{}, error) {
	panic("Unexpected panic")
}

func listInt(context.Context, interface{}) (interface{}, error) {
	return []int{1, 2, 3}, nil
}

func cleanUp(context.Context) (map[string]string, error) {
	return map[string]string{"cleanup": "ok"}, nil
}

func failingCleanUp(context.Context) (map[string]string, error) {
	return nil, errors.New("Cleanup error")
}

func sum(ctx context.Context, l interface{}) (interface{}, error) {
	var list, ok = l.([]int)

	if !ok {
		return nil, errors.New("Invalid argument")
	}

	var sum = 0
	for _, i := range list {
		sum = sum + i
	}

	result := map[string]string{"sum": strconv.Itoa(sum)}
	return result, nil
}
