package main

import (
	"fmt"
	"log"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/victorcoder/dkron/cron"
)

// Scheduler is main entry point.
type Scheduler struct {
	cron        *cron.Cron
	masterActor *actor.PID
}

// NewScheduler returns a new scheduler.
// TODO options
// DB connection param
// Local lock mode vs DistributedLock mode
// Kill timeout
func NewScheduler() (*Scheduler, error) {
	var props = actor.FromProducer(newMasterActor).WithSupervisor(masterActorSupervisorStrategy).WithGuardian(masterActorGuardianStrategy)
	var pid = actor.Spawn(props)

	var s = &Scheduler{
		cron:        cron.New(),
		masterActor: pid,
	}

	return s, nil
}

// Register a job.
func (s *Scheduler) Register(id string, j *Job) {
	s.masterActor.Tell(&Register{id, j})
}

// AddTask schedule a run for the job.
func (s *Scheduler) AddTask(cron string, id string) {
	s.cron.AddFunc(cron, func() {
		s.masterActor.Tell(&StartJob{id})
	})
}

// // Start the cron
// func (s *Scheduler) Start() {
// 	s.cron.Start()
// }

// func (s *Scheduler) Stop() {
// 	s.cron.Stop()
// }

// DisableAll disables execution for all jobs.
func (s *Scheduler) DisableAll() {

}

// EnableAll enables execution for all jobs.
func (s *Scheduler) EnableAll() {

}

// Disable execution for the specified job.
func (s *Scheduler) Disable(jobID string) {

}

// Enable execution for the specified job.
func (s *Scheduler) Enable(jobID string) {

}

/* Actors */

type masterActor struct {
	workers map[string]*actor.PID
}
type workerActor struct {
	job *Job
}
type runnerActor struct{}

/* MasterActor Messages */

// RegisterJob message sent to MasterActor to register a job.
type registerJob struct {
	label string
	job   *Job
}

// StartJob message sent to MasterActor to specified job.
type startJob struct {
	label string
}

/* WorkerActor Messages */

type Execute struct {}



/* RunnerActor Messages */

type Status struct{}

type Heartbeat struct{}

/* Actor builders */

func newMasterActor() actor.Actor {
	return &masterActor{
		workers: make(map[string]*actor.PID),
	}
}

func newWorkerActorBuilder(j *Job) func() actor.Actor {
	return func() actor.Actor {
		return &workerActor{
			job: j,
		}
	}
}

func newRunnerActor() actor.Actor {
	return &runnerActor{}
}

/* Master Actor Logic*/

func (state *masterActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *registerJob:
		var props = actor.FromProducer(newWorkerActorBuilder(msg.job)).WithSupervisor(actor.NewOneForOneStrategy(10, 1000, decider))
		var worker = context.Spawn(props)
		state.workers[msg.label] = worker
	case *startJob:
		state.workers[msg.label].Tell(&Execute{})
	}
}

/* Worker Actor Logic*/

func (state *WorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Execute:
		// if DistributedLock
			// 1. CleanupPhase

			// 2. ReservationPhase

			// 3. ExecutionPhase

		// else
			// SwitchBehavior to OCCUPIED or Switch a flag in state of the actor
			//context.SetBehavior(state.ReceiveOccupied)

		// Set NormalExecutionTimeout
		context.SetReceiveTimeout(state.job.executionTimeout)
		// Spawn Runner
		// TODO with specific worker supervisor
		props := actor.FromProducer(newRunnerActor).WithSupervisor(actor.NewOneForOneStrategy(10, 1000, decider))
		runner := context.Spawn(props)
		// Tell Run to Runner
		runner.Tell(&Run{state.job})
	case *Status:
		fmt.Println("Status")

	case *Heartbeat:
		//context.SetBehavior(state.ReceiveOccupied)
		fmt.Println("Heartbeat")

	case *actor.ReceiveTimeout:
		//TODO store the kind of timeout (in state of the actor ?)
		// Check reason of timeout

		// if normal execution
		// Set timeout to Timeout - NormalExecution (Perform some check about the result of this ccomputation)
		context.SetReceiveTimeout(state.job.executionTimeout)
		// TODO Logging
		
		// if exec timeout
		// set timeout to KillTimeout - timeout -NormalExecution
		context.SetReceiveTimeout(state.job.executionTimeout)
		// Stop children
		context.Children()[0].Stop()

		// if kill timeout
		// KIll all !!!!! 
		panic("kill timeout")
	}
}


/* Runner Actor Logic*/

func (state *RunnerActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *Run:
		fmt.Println("Run received in RUNNER")
		err := RunJob(msg.job, context.Parent())
		if err == nil {
			context.Parent().Tell(&Status{})
		} else {
			context.Parent().Tell(&Status{})
		}
	case *actor.Stopping:
		fmt.Println("Stopping")
	default:
		fmt.Printf("Default %T", msg)
	}
}

func RunJob(j *Job, heartbeatTarget *actor.PID) error {
	heartbeatTarget.Tell(&Heartbeat{})
	var err error

	for _, step := range j.steps {
		err = step(nil, nil)
		heartbeatTarget.Tell(&Heartbeat{})

		if err != nil {
			break
		}
	}

	if err != nil {
		j.cleanupStep(nil, nil)
		heartbeatTarget.Tell(&Heartbeat{})
	}

	return err
}




/* Actor Utils */

func Logger(next actor.ActorFunc) actor.ActorFunc{
	fn := func(ctx actor.Context){
		message := c.Message()
		log.Printf("%v got %v %+v", c.Self(), reflect.TypeOf(message), message)
		next(c)
	}

	return fn
}


func masterActorSupervisorStrategy() actor.SupervisorStrategy{
	actor.NewOneForOneStrategy(10, 1000, masterActorDecider)
}

func masterActorDecider(reason interface{}) actor.Directive {
	switch reason {
	case "KillTimeoutExceeded":
		return actor.EscalateDirective
	default:
		return actor.RestartDirective
	}
}

func masterActorGuardianStrategy() actor.SupervisorStrategy{
	actor.NewOneForOneStrategy(10, 1000, alwaysPanicDecider)
}

func alwaysPanicDecider(reason interface{}) actor.Directive{
	panic("MasterActor guardian killed itself")
}