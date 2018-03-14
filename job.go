package main

import (
	"context"
)

type Step func(context.Context, interface{}) error

type Job struct {
	// Unique id to identify the job
	jobId string
	// Steps of the job
	step Step
}

func NewJob(id string, s Step) *Job {
	var job = &Job{
		jobId: id,
		step:  s,
	}

	return job
}

func (j *Job) Id() string {
	return j.jobId
}