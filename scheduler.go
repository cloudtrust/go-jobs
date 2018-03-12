package main

import (
	"github.com/victorcoder/dkron/cron"
)

type Scheduler struct {
	cron *cron.Cron
}

func NewScheduler() (*Scheduler, error) {
	var s = &Scheduler{
		cron: cron.New(),
	}

	return s, nil
}

func (s *Scheduler) AddTask(cron string, job *Job) {
	s.cron.AddFunc(cron, func() { job.Run() })
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}
