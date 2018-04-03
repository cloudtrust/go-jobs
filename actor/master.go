package actor

import (
	"database/sql"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

// Root actor
type MasterActor struct {
	componentName      string
	componentID        string
	workers            map[string]*actor.PID
	workerPropsBuilder func(componentName string, componentID string, j *job.Job, idGenerator IdGenerator, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props
}

type MasterOption func(m *MasterActor)

// Message triggered by API to MasterActor to register a new job.
type RegisterJob struct {
	Job           *job.Job
	IdGenerator   IdGenerator
	LockManager   LockManager
	StatusManager StatusManager
}

// Message triggerred by Cron to MasterActor to launch job.
type StartJob struct {
	JobName string
}

// DB is the interface of the DB.
type IdGenerator interface {
	NextId() string
}

// DB is the interface of the DB.
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

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

func (state *MasterActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *RegisterJob:
		var props = state.workerPropsBuilder(state.componentName, state.componentID, msg.Job, msg.IdGenerator, msg.LockManager, msg.StatusManager)
		var worker = context.Spawn(props)
		state.workers[msg.Job.Name()] = worker
	case *StartJob:
		state.workers[msg.JobName].Tell(&Execute{})
	}
}

func workerPropsBuilder(builder func(componentName string, componentID string, j *job.Job, idGenerator IdGenerator, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props) MasterOption {
	return func(m *MasterActor) {
		m.workerPropsBuilder = builder
	}
}

// Supervision Strategy of MasterActor about its childs (i.e. WorkerActors)
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

func masterActorGuardianStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(10, 1000, alwaysPanicDecider)
}

func alwaysPanicDecider(reason interface{}) actor.Directive {
	panic("MasterActor guardian killed itself")
}
