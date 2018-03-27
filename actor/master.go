package actor

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

// Root actor
type MasterActor struct {
	workers            map[string]*actor.PID
	workerPropsBuilder func(j *job.Job, l Lock, s Statistics, options ...WorkerOption) *actor.Props
}

type MasterOption func(w *MasterActor)

// Message triggered by API to MasterActor to register a new job.
type RegisterJob struct {
	// identifier of the job
	Label      string
	Job        *job.Job
	Statistics Statistics
	Lock       Lock
}

// Message triggerred by Cron to MasterActor to launch job.
type StartJob struct {
	Label string
}

func BuildMasterActorProps(options ...MasterOption) *actor.Props {
	return actor.FromProducer(newMasterActor(options...)).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(masterActorGuardianStrategy())
}

func newMasterActor(options ...MasterOption) func() actor.Actor {
	return func() actor.Actor {
		var master = &MasterActor{
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
		var props = state.workerPropsBuilder(msg.Job, msg.Lock, msg.Statistics)
		var worker = context.Spawn(props)
		state.workers[msg.Label] = worker
	case *StartJob:
		state.workers[msg.Label].Tell(&Execute{})
	}
}

func workerPropsBuilder(builder func(j *job.Job, l Lock, s Statistics, options ...WorkerOption) *actor.Props) MasterOption {
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
