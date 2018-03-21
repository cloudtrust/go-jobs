package actor

import (
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/stretchr/testify/assert"
)

// Test nominal use case
// Check message sent & received

func TestNominalCase(t *testing.T) {
	var wg sync.WaitGroup
	var result string
	var output map[string]string

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	master := actor.Spawn(actor.FromFunc(func(c actor.Context) {
		switch msg := c.Message().(type) {
		case *actor.Started:
			props := actor.FromProducer(NewWorkerActor(nil,nil,nil, runnerProducer(mockNewRunnerActor)))
			worker := c.Spawn(props)
			worker.Tell(&Run{job})
		case *Status:
			result = msg.status
			output = msg.message
			wg.Done()
		}

	}))

	wg.Wait()
	master.GracefulStop()

	assert.Equal(t, Completed, result)
	var expectedOutput = map[string]string{"message": "done"}
	assert.Equal(t, expectedOutput, output)
}

//Test timeout mechanism, normal timeout, execution timeout, suicide timeout



/* Utils */


/** Mock Runner Actor **/

func mockNewRunnerActor() actor.Actor{

}

