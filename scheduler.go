package job

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/victorcoder/dkron/cron"
)

const (
	normalExecution   = 0
	EXECUTION_TIMEOUT = 1
	SUICIDE_TIMEOUT   = 2
)

// Scheduler is main entry point.
type Scheduler struct {
	cron           *cron.Cron
	masterActor    *actor.PID
	lockType       int
	noDB           bool
	suicideTimeout time.Duration
}

// SchedulerOption is used to configure the Scheduler. It takes on argument: the Scheduler we are operating on.
type SchedulerOption func(*Scheduler) error

// NewScheduler returns a new scheduler.
// TODO options
// DB connection param
// Local lock mode vs DistributedLock mode
// Kill timeout
func NewScheduler(options ...SchedulerOption) (*Scheduler, error) {
	var props = actor.FromProducer(newMasterActor).WithSupervisor(masterActorSupervisorStrategy()).WithGuardian(masterActorGuardianStrategy())
	var pid = actor.Spawn(props)

	var s = &Scheduler{
		cron:        cron.New(),
		masterActor: pid,
	}

	// Apply options to the scheduler
	for _, opt := range options {
		var err = opt(s)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Register a job.
func (s *Scheduler) Register(id string, j *Job) {
	s.masterActor.Tell(&RegisterJob{id, j})
}

// AddTask schedule a run for the job.
func (s *Scheduler) AddTask(cron string, id string) {
	s.cron.AddFunc(cron, func() {
		s.masterActor.Tell(&StartJob{id})
	})
}

// Start the cron
func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}

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
	job                *Job
	currentTimeoutType int
	occupied           bool
}
type runnerActor struct{}

/* MasterActor Messages */

// RegisterJob message sent to MasterActor to register a job.
type RegisterJob struct {
	label string
	job   *Job
}

// StartJob message sent to MasterActor to specified job.
type StartJob struct {
	label string
}

/* WorkerActor Messages */

type Execute struct{}

type Status struct {
	status  string
	message string
}

type HeartBeat struct {
	StepInfos map[string]string
}

/* RunnerActor Messages */

type Run struct {
	job *Job
}

type NextStep struct {
	job       *Job
	i         int
	stepInfos map[string]string
}

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
	case *RegisterJob:
		var props = actor.FromProducer(newWorkerActorBuilder(msg.job)).WithSupervisor(masterActorSupervisorStrategy())
		var worker = context.Spawn(props)
		state.workers[msg.label] = worker
	case *StartJob:
		state.workers[msg.label].Tell(&Execute{})
	}
}

/* Worker Actor Logic*/

func (state *workerActor) Receive(context actor.Context) {
	switch message := context.Message().(type) {
	case *Execute:
		if state.occupied {
			fmt.Println("OCCUPIED")
			return
		}
		state.occupied = true
		// if DistributedLock
		// 1. CleanupPhase

		// 2. ReservationPhase

		// 3. ExecutionPhase

		// else
		// SwitchBehavior to OCCUPIED or Switch a flag in state of the actor
		//context.SetBehavior(state.ReceiveOccupied)

		// Set NormalExecutionTimeout
		state.currentTimeoutType = normalExecution
		context.SetReceiveTimeout(state.job.executionTimeout)
		// Spawn Runner
		// TODO with specific worker supervisor
		props := actor.FromProducer(newRunnerActor)
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
			state.currentTimeoutType = EXECUTION_TIMEOUT
			context.SetReceiveTimeout(state.job.executionTimeout)
		case EXECUTION_TIMEOUT:
			state.currentTimeoutType = SUICIDE_TIMEOUT
			context.SetReceiveTimeout(100 * time.Second)
			context.Children()[0].Stop()
		case SUICIDE_TIMEOUT:
			panic("SUICIDE_TIMEOUT")
		default:
			panic("UNKNOWN_TIMEOUT")
		}
	}
}

/* Runner Actor Logic*/

func (state *runnerActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *Run:

		//Initialize map Step Status

		//TODO est ce que on stocke stepInfo dans le state ou on le passe dans le message
		//-> voir si il y a un impact lors d'un restart?
		var stepInfos = make(map[string]string)

		for _, step := range msg.job.steps {
			//TODO check comment avoir le nom de la fonction
			fmt.Println(stepName(step))
			stepInfos[stepName(step)] = "Iddle"
		}

		i := 0
		context.Self().Tell(&NextStep{msg.job, i, stepInfos})

	case *NextStep:
		var step = msg.job.steps[msg.i]
		var infos = msg.stepInfos
		infos[stepName(step)] = "Running"
		context.Parent().Tell(&HeartBeat{infos})

		//TODO changer Step pour que retourne interface{}, error
		//-> Ainsi possibilité de transmettre des infos d'un step à l'autre
		//ça facilitera le découpage en step plus petit
		var _, err = step(nil, nil)

		if err != nil {
			infos[stepName(step)] = "Failed"
			context.Parent().Tell(&HeartBeat{infos})

			context.Parent().Tell(&Status{"Failure", "err"})
			return
		}

		infos[stepName(step)] = "Completed"
		context.Parent().Tell(&HeartBeat{infos})

		var i = msg.i + 1

		if i >= len(msg.job.steps) {
			context.Parent().Tell(&Status{"Done", "mess"})
			return
		}

		// TODO transamettre val (le retour de step) dans le message
		// Ajouter val au context car on veut un contexte tout le temps
		context.Self().Tell(&NextStep{msg.job, i, infos})
	}
}

/* Actor Utils */

// func Logger(next actor.ActorFunc) actor.ActorFunc {
// 	fn := func(ctx actor.Context) {
// 		message := c.Message()
// 		log.Printf("%v got %v %+v", c.Self(), reflect.TypeOf(message), message)
// 		next(c)
// 	}

// 	return fn
// }

func masterActorSupervisorStrategy() actor.SupervisorStrategy {
	return actor.NewOneForOneStrategy(10, 1000, masterActorDecider)
}

func masterActorDecider(reason interface{}) actor.Directive {
	switch reason {
	case "KillTimeoutExceeded":
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

func stepName(s Step) string {
	return runtime.FuncForPC(reflect.ValueOf(s).Pointer()).Name()
}
