package main

import (
	"fmt"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/victorcoder/dkron/cron"
)

type Scheduler struct {
	cron        *cron.Cron
	masterActor *actor.PID
}

/* Messages */
type Register struct {
	label string
	job   *Job
}

type StartJob struct {
	label string
}

type Run struct {
	job *Job
}

type Status struct{}

type Heartbeat struct{}

/* Actors */
type MasterActor struct {
	workers map[string]*actor.PID
}
type WorkerActor struct {
	job *Job
}
type RunnerActor struct{}

func newMasterActor() actor.Actor {
	return &MasterActor{
		workers: make(map[string]*actor.PID),
	}
}

func newWorkerActorBuilder(j *Job) func() actor.Actor {
	return func() actor.Actor {
		return &WorkerActor{
			job: j,
		}
	}
}

func newRunnerActor() actor.Actor {
	return &RunnerActor{}
}

func (state *MasterActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *Register:
		fmt.Println("Register")
		var props = actor.FromProducer(newWorkerActorBuilder(msg.job))
		var worker = context.Spawn(props)
		state.workers[msg.label] = worker
	case *StartJob:
		fmt.Println("StartJob")
		state.workers[msg.label].Tell(&Run{})

	default:
		//fmt.Printf("MasterActor - type Not found: %T\n", msg)
	}
}

func (state *WorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		fmt.Println("Run")
		// TODO
		//Reserve, Lock acquiere, etc ...

		context.SetReceiveTimeout(state.job.maxExecutionTime)
		props := actor.FromProducer(newRunnerActor)
		runner := context.Spawn(props)
		context.SetBehavior(state.ReceiveOccupied)
		runner.Tell(&Run{state.job})
	case *Status:
		fmt.Println("Status")
	case *Heartbeat:
		fmt.Println("Heartbeat")
	case *actor.ReceiveTimeout:
		fmt.Println("-------Timeout received")
		panic("Ouch")
	case *actor.Restarting:
		fmt.Println("Restarting")
	case *actor.Stopping:
		fmt.Println("StoppingChild")
	}
}

func (state *WorkerActor) ReceiveOccupied(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		fmt.Println("Occupied")
	case *Status:
		fmt.Println("Status")
		context.SetBehavior(state.Receive)
	case *Heartbeat:
		fmt.Println("Heartbeat")
	case *actor.ReceiveTimeout:
		fmt.Println("-------Timeout received")
	}

}

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

func NewScheduler() (*Scheduler, error) {
	decider := func(reason interface{}) actor.Directive{
		fmt.Println("handling failure of child")
		return actor.StopDirective
	}
	supervisor := actor.NewOneForOneStrategy(10, 1000, decider)
	var props = actor.FromProducer(newMasterActor).WithSupervisor(supervisor)
	var pid = actor.Spawn(props)
	var s = &Scheduler{
		cron:        cron.New(),
		masterActor: pid,
	}

	return s, nil
}

func (s *Scheduler) Register(id string, j *Job) {
	s.masterActor.Tell(&Register{id, j})
}

func (s *Scheduler) AddTask(cron string, id string) {
	fmt.Println("Add Task " + id)
	s.cron.AddFunc(cron, func() {
		fmt.Println("Tell called: Master.StartJob")
		s.masterActor.Tell(&StartJob{id})
	})
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}
