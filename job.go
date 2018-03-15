package main

import (
	"context"
	"time"
)

type Step func(context.Context, interface{}) error

type Job struct {
	// Unique id to identify the job
	jobId string
	// Steps of the job
	steps []Step
	// Step of cleaning called in case of failure of one of the steps
	cleanupStep Step
	// Max execution time of whole job
	maxExecutionTime time.Duration
}

func NewJob(id string, cleaning Step, duration time.Duration, s ...Step) *Job {
	var job = &Job{
		jobId:       id,
		steps:       s,
		cleanupStep: cleaning,
		maxExecutionTime: duration,
	}

	return job
}

func (j *Job) Id() string {
	return j.jobId
}
