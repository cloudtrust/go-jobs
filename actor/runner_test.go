package actor

import (
	"context"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

func step(context.Context, interface{}) (interface{}, error) {
	return nil, nil
}

type ActorTest struct{}

func (state *ActorTest) Receive(ctx actor.Context){

}

func ActorTestProducer() actor.Actor {
	return &ActorTest{}
}

// Test nominal use case with 1 step
//Check message sent
func TestNominalCase(t *testing.T) {

	props := actor.FromProducer(ActorTestProducer)
	actorPid := actor.Spawn(props)

	var job, err = job.NewJob("jobID", job.Steps(step))

	props := BuildRunnerActorProps()
	actor.Spawn(props)

}

// Test job with multiple steps with passing args and rlinks between steps
//Check message sent

// Test failing step without cleanupStep
//Check message sent

// Test faling step + execution of cleanupStep
//Check message sent

// Test panic restart
