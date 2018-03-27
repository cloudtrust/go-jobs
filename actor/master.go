package actor

import (
	"database/sql"

	"github.com/cloudtrust/go-jobs/status"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/cloudtrust/go-jobs/lock"
)

// Root actor
type MasterActor struct {
	componentName        string
	componentID          string
	idGenerator          IdGenerator
	lockMode             lock.LockMode
	statusStorageEnabled bool
	dbStatus             DB
	dbLock               DB
	workers              map[string]*actor.PID
	workerPropsBuilder   func(j *job.Job, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props
}

type MasterOption func(m *MasterActor)

// Message triggered by API to MasterActor to register a new job.
type RegisterJob struct {
	JobID string
	Job   *job.Job
}

// Message triggerred by Cron to MasterActor to launch job.
type StartJob struct {
	JobID string
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

func BuildMasterActorProps(componentName string, componentID string, idGenerator IdGenerator,
	lockMode lock.LockMode, statusStorageEnabled bool, dbStatus DB, dbLock DB, options ...MasterOption) *actor.Props {
	return actor.FromProducer(newMasterActor(componentName, componentID, idGenerator,
		lockMode, statusStorageEnabled, dbStatus, dbLock, options...)).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(masterActorGuardianStrategy())
}

func newMasterActor(componentName string, componentID string, idGenerator IdGenerator,
	lockMode lock.LockMode, statusStorageEnabled bool, dbStatus DB, dbLock DB, options ...MasterOption) func() actor.Actor {
	return func() actor.Actor {
		var master = &MasterActor{
			componentName:        componentName,
			componentID:          componentID,
			idGenerator:          idGenerator,
			lockMode:             lockMode,
			statusStorageEnabled: statusStorageEnabled,
			dbStatus:             dbStatus,
			dbLock:               dbLock,
			workers:              make(map[string]*actor.PID),
			workerPropsBuilder:   BuildWorkerActorProps,
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
		var l LockManager
		var s StatusManager

		if state.lockMode == lock.Distributed {
			l = lock.New(state.dbLock, state.componentName, state.componentID, msg.Job.Name(), msg.JobID, 0)
		} else {
			l = lock.NewLocalLock()
		}

		if state.statusStorageEnabled {
			s = status.New(state.dbStatus, state.componentName, state.componentID, msg.Job.Name(), msg.JobID)
		}

		var props = state.workerPropsBuilder(msg.Job, l, s)
		var worker = context.Spawn(props)
		state.workers[msg.JobID] = worker
	case *StartJob:
		state.workers[msg.JobID].Tell(&Execute{})
	}
}

func workerPropsBuilder(builder func(j *job.Job, l LockManager, s StatusManager, options ...WorkerOption) *actor.Props) MasterOption {
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
