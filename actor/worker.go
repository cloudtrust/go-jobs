package actor

import (
	"fmt"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

type WorkerActor struct {
	job                *job.Job
	currentTimeoutType int
	occupied           bool
}

type Execute struct{}

type Status struct {
	status  string
	message string
}

type HeartBeat struct {
	StepInfos map[string]string
}

func NewWorkerActorBuilder(j *job.Job) func() actor.Actor {
	return func() actor.Actor {
		return &WorkerActor{
			job: j,
		}
	}
}

func (state *WorkerActor) Receive(context actor.Context) {
	switch message := context.Message().(type) {
	case *Execute:
		if state.occupied {
			fmt.Println("OCCUPIED")
			return
		}
		state.occupied = true
		// if DistributedLock
		// 1. CleanupPhase

		// 2. ReservationPhase

		// 3. ExecutionPhase

		// else
		// SwitchBehavior to OCCUPIED or Switch a flag in state of the actor
		//context.SetBehavior(state.ReceiveOccupied)

		// Set NormalExecutionTimeout
		state.currentTimeoutType = normalExecution
		context.SetReceiveTimeout(state.job.ExecutionTimeout())
		// Spawn Runner
		// TODO with specific worker supervisor
		props := actor.FromProducer(NewRunnerActor)
		runner := context.Spawn(props)
		// Tell Run to Runner
		runner.Tell(&Run{state.job})
	case *Status:
		fmt.Println("Status")
	case *HeartBeat:
		//context.SetBehavior(state.ReceiveOccupied)
		fmt.Println("Heartbeat")
		var s = message.StepInfos
		fmt.Println(s)

	case *actor.ReceiveTimeout:
		switch state.currentTimeoutType {
		case normalExecution:
			//TODO LOG the info about normal execution exceeded
			state.currentTimeoutType = EXECUTION_TIMEOUT
			context.SetReceiveTimeout(state.job.ExecutionTimeout())
		case EXECUTION_TIMEOUT:
			state.currentTimeoutType = SUICIDE_TIMEOUT
			context.SetReceiveTimeout(100 * time.Second)
			context.Children()[0].Stop()
		case SUICIDE_TIMEOUT:
			panic("SUICIDE_TIMEOUT")
		default:
			panic("UNKNOWN_TIMEOUT")
		}
	}
}
