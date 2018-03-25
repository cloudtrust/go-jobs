package main

import (
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/victorcoder/dkron/cron"
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
