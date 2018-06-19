package actor

import (
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

const (
	notInitialized int = iota
	normalDuration
	executionTimeout
	suicideTimeout
	noTimeout
)

// WorkerActor is the actor responsibles of the execution of a specific job. It checks the locks, persist the status if enabled. Handles timeout and unexpected failures of RunnerActor.
type WorkerActor struct {
	componentName      string
	componentID        string
	idGenerator        IDGenerator
	job                *job.Job
	lockManager        LockManager
	statusManager      StatusManager
	runnerPropsBuilder func(logger Logger, jobID string, job *job.Job) *actor.Props
	currentTimeoutType int
	logger             Logger
}

// WorkerOption to configure the WorkerActor.
type WorkerOption func(w *WorkerActor)

// Execute message is sent by MasterActor to command to WorkerActor to launch the execution of the job.
type Execute struct{}

// Status message is sent by RunnerActor to inform the Worker about its final execution status.
type Status struct {
	JobID   string
	status  string
	message map[string]string
	infos   map[string]string
}

// StepStatus message is sent by RunnerActor to give sign of live to the Worker and inform about its current status.
type StepStatus struct {
	JobID     string
	StepInfos map[string]string
}

// RunnerStopped message is sent by RunnerActor to Worker to notify its shutdown
type RunnerStopped struct {
	JobID string
}

// RunnerStarted message is sent by RunnerActor to Worker to notify it has started
type RunnerStarted struct {
	JobID string
}

// BuildWorkerActorProps build the Properties for the actor spawning.
func BuildWorkerActorProps(componentName, componentID string, logger Logger, j *job.Job, idGenerator IDGenerator, lm LockManager, sm StatusManager, options ...WorkerOption) *actor.Props {
	return actor.FromProducer(newWorkerActor(componentName, componentID, logger, j, idGenerator, lm, sm, options...)).WithSupervisor(workerActorSupervisorStrategy())
}

func newWorkerActor(componentName, componentID string, logger Logger, j *job.Job, idGenerator IDGenerator, lm LockManager, sm StatusManager, options ...WorkerOption) func() actor.Actor {
	return func() actor.Actor {
		var worker = &WorkerActor{
			componentName:      componentName,
			componentID:        componentID,
			job:                j,
			idGenerator:        idGenerator,
			lockManager:        lm,
			statusManager:      sm,
			runnerPropsBuilder: BuildRunnerActorProps,
			logger:             logger,
		}

		// Apply options to the job
		for _, opt := range options {
			opt(worker)
		}

		return worker
	}
}

func runnerPropsBuilder(p func(logger Logger, jobID string, job *job.Job) *actor.Props) WorkerOption {
	return func(w *WorkerActor) {
		w.runnerPropsBuilder = p
	}
}

// Receive is the implementation of WorkerActor's behavior
func (state *WorkerActor) Receive(context actor.Context) {
	switch message := context.Message().(type) {
	case *Execute:
		var jobID = state.idGenerator.NextID()
		if state.lockManager.Lock(state.componentName, state.componentID, state.job.Name(), jobID, state.job.SuicideTimeout()) != nil {
			//TODO log lock not succeeded
			return
		}
		state.statusManager.Register(state.componentName, state.componentID, state.job.Name(), jobID)

		applyTimeout(context, state)

		// Spawn Runner
		props := state.runnerPropsBuilder(state.logger, jobID, state.job)
		context.Spawn(props)

		state.statusManager.Start(state.componentName, state.componentID, state.job.Name())

	case *Status:
		if message.status == Completed {
			state.statusManager.Complete(state.componentName, state.componentID, state.job.Name(), message.JobID, message.infos, message.message)
		} else {
			state.statusManager.Fail(state.componentName, state.componentID, state.job.Name(), message.JobID, message.infos, message.message)
		}

		context.Children()[0].Stop()
		// unlock will be auto called by stop of runner

	case *StepStatus:
		state.currentTimeoutType = notInitialized
		applyTimeout(context, state)

		var s = message.StepInfos
		state.statusManager.Update(state.componentName, state.componentID, state.job.Name(), s)

	case *RunnerStopped:
		state.lockManager.Unlock(state.componentName, state.componentID, state.job.Name(), message.JobID)

	case *actor.ReceiveTimeout:
		applyTimeout(context, state)
	}
}

// Supervision Strategy of WorkActor about its childs (i.e. RunnerActors)
func workerActorSupervisorStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(2, 1*time.Second, restartDecider)
}

func restartDecider(reason interface{}) actor.Directive {
	return actor.RestartDirective
}

func applyTimeout(context actor.Context, state *WorkerActor) {
	if state.job.NormalDuration() != 0 && state.currentTimeoutType == notInitialized {
		state.currentTimeoutType = normalDuration
		context.SetReceiveTimeout(state.job.NormalDuration())
	} else if state.job.ExecutionTimeout() != 0 && state.currentTimeoutType < executionTimeout {
		// TODO log normal timeout exceeded
		state.currentTimeoutType = executionTimeout
		context.SetReceiveTimeout(state.job.ExecutionTimeout())
	} else if state.job.SuicideTimeout() != 0 && state.currentTimeoutType < suicideTimeout {
		// TODO log execution timeout exceeded
		state.currentTimeoutType = suicideTimeout
		context.SetReceiveTimeout(state.job.SuicideTimeout())
	} else if state.currentTimeoutType == suicideTimeout {
		// TODO log suicide timeout exceeded
		panic("SUICIDE_TIMEOUT")
	} else {
		state.currentTimeoutType = noTimeout
	}
}
