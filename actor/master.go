package actor

import (
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

// MasterActor is the main actor for jobs execution. It handles unexpected Workers crashes.
type MasterActor struct {
	componentName      string
	componentID        string
	workers            map[string]*actor.PID
	workerPropsBuilder func(componentName string, componentID string, j *job.Job, idGenerator IDGenerator, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props
}

// MasterOption is configuration option for MasterActor
type MasterOption func(m *MasterActor)

// RegisterJob is the message received by MasterActor to register a new job.
// IDGenerator and LockManager must be not nil, StatusManager can be nil.
type RegisterJob struct {
	Job           *job.Job
	IDGenerator   IDGenerator
	LockManager   LockManager
	StatusManager StatusManager
}

// StartJob is a message received by MasterActor to launch the execution of a job.
type StartJob struct {
	JobName string
}

// IDGenerator is used to compute a unique identifier for component instance and job execution instance.
type IDGenerator interface {
	NextID() string
}

// LockManager is the lock policy to prevent concurrent job execution.
type LockManager interface {
	Unlock(componentName string, componentID string, jobName string, jobID string) error
	Lock(componentName string, componentID string, jobName string, jobID string, jobMaxDuration time.Duration) error
}

// StatusManager is the component used to persist information about job executions.
type StatusManager interface {
	Start(componentName, jobName string) error
	Update(componentName, jobName string, stepInfos map[string]string) error
	Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
	Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
}

// BuildMasterActorProps build the Properties for the actor spawning.
func BuildMasterActorProps(componentName string, componentID string, options ...MasterOption) *actor.Props {
	return actor.FromProducer(newMasterActor(componentName, componentID, options...)).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(masterActorGuardianStrategy())
}

func newMasterActor(componentName string, componentID string, options ...MasterOption) func() actor.Actor {
	return func() actor.Actor {
		var master = &MasterActor{
			componentName:      componentName,
			componentID:        componentID,
			workers:            make(map[string]*actor.PID),
			workerPropsBuilder: BuildWorkerActorProps,
		}

		// Apply options to the job
		for _, opt := range options {
			opt(master)
		}

		return master
	}
}

// Receive is the implementation of MasterActor's behavior
func (state *MasterActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *RegisterJob:
		var props = state.workerPropsBuilder(state.componentName, state.componentID, msg.Job, msg.IDGenerator, msg.LockManager, msg.StatusManager)
		var worker = context.Spawn(props)
		state.workers[msg.Job.Name()] = worker
	case *StartJob:
		state.workers[msg.JobName].Tell(&Execute{})
	}
}

func workerPropsBuilder(builder func(componentName string, componentID string, j *job.Job, idGenerator IDGenerator, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props) MasterOption {
	return func(m *MasterActor) {
		m.workerPropsBuilder = builder
	}
}

// Supervision Strategy of MasterActor about its childs (i.e. WorkerActors)
// It try to restart the actor multiple times unless the actor has failed due to SuicideTimeout.
func masterActorSupervisorStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(10, 1000, masterActorDecider)
}

func masterActorDecider(reason interface{}) actor.Directive {
	switch reason {
	case "SUICIDE_TIMEOUT":
		return actor.EscalateDirective
	default:
		return actor.RestartDirective
	}
}

// Guardian Strategy for MasterActor which always panics.
func masterActorGuardianStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(10, 1000, alwaysPanicDecider)
}

func alwaysPanicDecider(reason interface{}) actor.Directive {
	panic("MasterActor guardian killed itself")
}
