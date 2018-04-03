package actor

import (
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
	componentName      string
	componentID        string
	idGenerator        IdGenerator
	job                *job.Job
	lockManager        LockManager
	statusManager      StatusManager
	runnerPropsBuilder func(jobID string, job *job.Job) *actor.Props
	suicideTimeout     time.Duration
	currentTimeoutType int
}

type WorkerOption func(w *WorkerActor)

type Execute struct{}

type Status struct {
	JobID   string
	status  string
	message map[string]string
	infos   map[string]string
}

type HeartBeat struct {
	JobID     string
	StepInfos map[string]string
}

type RunnerStopped struct {
	JobID string
}

type RunnerStarted struct {
	JobID string
}

type LockManager interface {
	Unlock(componentName string, componentID string, jobName string, jobID string) error
	Lock(componentName string, componentID string, jobName string, jobID string, jobMaxDuration time.Duration) error
}

type StatusManager interface {
	Start(componentName, jobName string) error
	Update(componentName, jobName string, stepInfos map[string]string) error
	Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
	Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
}

func BuildWorkerActorProps(componentName, componentID string, j *job.Job, idGenerator IdGenerator, lm LockManager, sm StatusManager, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(newWorkerActor(componentName, componentID, j, idGenerator, lm, sm, options...)).WithSupervisor(workerActorSupervisorStrategy())
}

func newWorkerActor(componentName, componentID string, j *job.Job, idGenerator IdGenerator, lm LockManager, sm StatusManager, options ...WorkerOption) func() actor.Actor {
	return func() actor.Actor {
		var worker = &WorkerActor{
			componentName:      componentName,
			componentID:        componentID,
			job:                j,
			idGenerator:        idGenerator,
			lockManager:        lm,
			statusManager:      sm,
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

func runnerPropsBuilder(p func(jobID string, job *job.Job) *actor.Props) WorkerOption {
	return func(w *WorkerActor) {
		w.runnerPropsBuilder = p
	}
}

func (state *WorkerActor) Receive(context actor.Context) {
	switch message := context.Message().(type) {
	case *Execute:
		var jobID = state.idGenerator.NextId()
		if state.lockManager.Lock(state.componentName, state.componentID, state.job.Name(), jobID, 0) != nil {
			//TODO log lock not succeeded
			return
		}

		// Set NormalExecutionTimeout
		// TODO normal execution is optional, set it only iff value provided. same for exectimeout and/or suicide timeout
		state.currentTimeoutType = normalExecution
		context.SetReceiveTimeout(state.job.ExecutionTimeout())

		// Spawn Runner
		props := state.runnerPropsBuilder(jobID, state.job)
		context.Spawn(props)

		if state.statusManager != nil {
			state.statusManager.Start(state.componentName, state.job.Name())
		}

	case *Status:
		if state.statusManager != nil {
			if message.status == Completed {
				state.statusManager.Complete(state.componentName, state.componentID, state.job.Name(), message.JobID, message.infos, message.message)
			} else {
				state.statusManager.Fail(state.componentName, state.componentID, state.job.Name(), message.JobID, message.infos, message.message)
			}
		}
		context.Children()[0].Stop()
		// unlock will be auto called by stop of runner

	case *HeartBeat:
		state.currentTimeoutType = normalExecution
		context.SetReceiveTimeout(state.job.ExecutionTimeout())
		var s = message.StepInfos

		if state.statusManager != nil {
			state.statusManager.Update(state.componentName, state.job.Name(), s)
		}

	case *RunnerStopped:
		state.lockManager.Unlock(state.componentName, state.componentID, state.job.Name(), message.JobID)

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
