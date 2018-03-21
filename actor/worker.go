package actor

import (
	"fmt"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

const (
	normalExecution  = 0
	executionTimeout = 1
	suicideTimeout   = 2
)

type WorkerActor struct {
	job                *job.Job
	lock               *Lock
	statistics         *Statistics
	runnerProducer     actor.Producer
	suicideTimeout     time.Duration
	currentTimeoutType int
}

type WorkerOption func(w *WorkerActor)

type Execute struct{}

type Status struct {
	status  string
	message map[string]string
}

type HeartBeat struct {
	StepInfos map[string]string
}

type Lock interface {
	Lock() error
	Unlock() error
}

type Statistics interface {
}

func NewWorkerActor(j *job.Job, l *Lock, s *Statistics, options ...WorkerOption) func() actor.Actor {
	return func() actor.Actor {
		var worker = &WorkerActor{
			job:            j,
			lock:           l,
			statistics:     s,
			suicideTimeout: 0,
			runnerProducer: NewRunnerActor,
		}

		// Apply options to the job
		for _, opt := range options {
			opt(worker)
		}

		return worker
	}
}

func SuicideTimeout(d time.Duration) WorkerOption {
	return func(w *WorkerActor) {
		w.suicideTimeout = d
	}
}

func runnerProducer(p actor.Producer) WorkerOption {
	return func(w *WorkerActor) {
		w.runnerProducer = p
	}
}

func (state *WorkerActor) Receive(context actor.Context) {
	switch message := context.Message().(type) {
	case *Execute:
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
			state.currentTimeoutType = executionTimeout
			context.SetReceiveTimeout(state.job.ExecutionTimeout())
		case executionTimeout:
			state.currentTimeoutType = suicideTimeout
			context.SetReceiveTimeout(100 * time.Second)
			context.Children()[0].Stop()
		case suicideTimeout:
			panic("SUICIDE_TIMEOUT")
		default:
			panic("UNKNOWN_TIMEOUT")
		}
	}
}
