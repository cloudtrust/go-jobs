package actor

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

// Root actor
type MasterActor struct {
	workers map[string]*actor.PID
}

// Message triggered by API to MasterActor to register a new job.
type RegisterJob struct {
	// identifier of the job
	label string
	job   *job.Job
}

// Message triggerred by Cron to MasterActor to launch job.
type StartJob struct {
	label string
}

func NewMasterActor() actor.Actor {
	return &MasterActor{
		workers: make(map[string]*actor.PID),
	}
}

func (state *MasterActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *RegisterJob:
		var props = actor.FromProducer(NewWorkerActor(msg.job, nil, nil)).WithSupervisor(masterActorSupervisorStrategy())
		var worker = context.Spawn(props)
		state.workers[msg.label] = worker
	case *StartJob:
		state.workers[msg.label].Tell(&Execute{})
	}
}

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
