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

type Init struct{}

/* Messages */
type RegisterMessage struct {
	jobId string
	job   *Job
}

type StartJob struct {
	jobId string
}

type Run struct{}
type Done struct{}

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

func (state *MasterActor) Receive(context actor.Context) {
	fmt.Println("Master Received a message")

	switch msg := context.Message().(type) {
	case *Init:
		fmt.Println("Init")
	case *RegisterMessage:
		fmt.Println("Register")
		var props = actor.FromProducer(newWorkerActorBuilder(msg.job))
		var worker = context.Spawn(props)
		state.workers[msg.jobId] = worker
	case *StartJob:
		fmt.Println("StartJob")
		state.workers[msg.jobId].Tell(&Run{})
	default:
		fmt.Printf("type Not found: %T\n", msg)
	}
}

func (state *WorkerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		fmt.Println("Run")
		props := actor.FromProducer(newRunnerActor)
		runner := context.Spawn(props)
		runner.Tell(&Run{})
	case *Done:
		fmt.Println("Job finished")
	}
}

func newRunnerActor() actor.Actor {
	return &RunnerActor{}
}

func (state *RunnerActor) Receive(context actor.Context) {
	switch context.Message().(type) {
	case *Run:
		fmt.Println("Run received in RUNNER")
		context.Parent().Tell(&Done{})
	}
}

func NewScheduler() (*Scheduler, error) {

	var props = actor.FromProducer(newMasterActor)
	var pid = actor.Spawn(props)
	var s = &Scheduler{
		cron:        cron.New(),
		masterActor: pid,
	}

	pid.Tell(&Init{})

	return s, nil
}

func (s *Scheduler) Register(id string, j *Job) {
	s.masterActor.Tell(&RegisterMessage{id, j})
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
