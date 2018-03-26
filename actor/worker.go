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
	lock               Lock
	statistics         Statistics
	runnerPropsBuilder func(job *job.Job) *actor.Props
	suicideTimeout     time.Duration
	currentTimeoutType int
}

type WorkerOption func(w *WorkerActor)

type Execute struct{}

type Status struct {
	status  string
	message map[string]string
	infos   map[string]string
}

type HeartBeat struct {
	StepInfos map[string]string
}

type RunnerStopped struct{}

type RunnerStarted struct{}

type Lock interface {
	Lock() error
	Unlock() error
}

type Statistics interface {
	Start() error
	Update(stepInfos map[string]string) error
	Finish(stepInfos, message map[string]string)
	Cancel(stepInfos, message map[string]string) error
}

func BuildWorkerActorProps(j *job.Job, l Lock, s Statistics, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(newWorkerActor(j, l, s, options...)).WithSupervisor(workerActorSupervisorStrategy())
}

func newWorkerActor(j *job.Job, l Lock, s Statistics, options ...WorkerOption) func() actor.Actor {
	return func() actor.Actor {
		var worker = &WorkerActor{
			job:                j,
			lock:               l,
			statistics:         s,
			suicideTimeout:     0,
			runnerPropsBuilder: BuildRunnerActorProps,
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

func runnerPropsBuilder(p func(job *job.Job) *actor.Props) WorkerOption {
	return func(w *WorkerActor) {
		w.runnerPropsBuilder = p
	}
}

func (state *WorkerActor) Receive(context actor.Context) {
	switch message := context.Message().(type) {
	case *actor.Started:
		fmt.Println("StartWorker")
		fmt.Println(state.job.Name())
	case *Execute:
		if state.lock.Lock() != nil {
			//TODO log lock not succeeded
			return
		}

		// Set NormalExecutionTimeout
		// TODO normal execution is optional, set it only iff value provided. same for exectimeout and/or suicide timeout
		state.currentTimeoutType = normalExecution
		context.SetReceiveTimeout(state.job.ExecutionTimeout())
		// Spawn Runner
		// TODO with specific worker supervisor ?
		props := state.runnerPropsBuilder(state.job)
		context.Spawn(props)

		state.statistics.Start()
	case *Status:
		if message.status == Completed {
			state.statistics.Finish(message.infos, message.message)
		} else {
			state.statistics.Cancel(message.infos, message.message)
		}

		// unlock will be auto called by stop of runner

	case *HeartBeat:
		state.currentTimeoutType = normalExecution
		context.SetReceiveTimeout(state.job.ExecutionTimeout())
		var s = message.StepInfos
		state.statistics.Update(s)

	case *RunnerStopped:
		state.lock.Unlock()

	case *actor.ReceiveTimeout:
		switch state.currentTimeoutType {
		case normalExecution:
			//TODO LOG the info about normal execution exceeded
			state.currentTimeoutType = executionTimeout
			context.SetReceiveTimeout(state.job.ExecutionTimeout())
		case executionTimeout:
			state.currentTimeoutType = suicideTimeout
			context.SetReceiveTimeout(state.suicideTimeout)
			context.Children()[0].Stop()
			// unlock will be auto called by stop of runner
		case suicideTimeout:
			//cancel
			//unlock
			panic("SUICIDE_TIMEOUT")
		}
	}
}

// Supervision Strategy of WorkActor about its childs (i.e. RunnerActors)
func workerActorSupervisorStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(2, 1*time.Second, restartDecider)
}

func restartDecider(reason interface{}) actor.Directive {
	return actor.RestartDirective
}
