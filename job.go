package job

import (
	"context"
	"fmt"
	"time"
)

// Step is a unit of work of a Job.
type Step func(context.Context, interface{}) (interface{}, error)

// Job contains the definiton of the job to execute.
type Job struct {
	// Name of the job
	name string
	// Steps of the job
	steps []Step
	// Step of cleaning called in case of failure of one of the steps.
	// Step of cleanup is optional.
	// Note: This step of cleanup is called in best effort but without warranty.
	cleanupStep Step
	// Timeout of the job execution.
	// By default there is no timeout.
	// Note: the job will only be interrupted at the end of a Step.
	executionTimeout time.Duration
	// Expected duration of the job
	normalDuration time.Duration
}

// Option is used to configure the Job. It takes on argument: the Job we are operating on.
type Option func(*Job) error

// Steps is a syntaxic sugar for definition of a slice of Step.
func Steps(s ...Step) []Step {
	return s
}

// NewJob returns a new Job.
func NewJob(name string, steps []Step, options ...Option) (*Job, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("Job's name cannot be empty")
	}

	if steps == nil || len(steps) == 0 {
		return nil, fmt.Errorf("At least one Step must be specified")
	}

	var job = &Job{
		name:             name,
		steps:            steps,
		cleanupStep:      nil,
		executionTimeout: 0,
		normalDuration:   0,
	}

	// Apply options to the job
	for _, opt := range options {
		var err = opt(job)
		if err != nil {
			return nil, err
		}
	}

	return job, nil

}

// Name returns the name which identify the job.
func (j *Job) Name() string {
	return j.name
}

// Steps returns the slice of steps.
func (j *Job) Steps() []Step {
	return j.steps
}

// CleanupStep is the option used to set a step of cleanup
func CleanupStep(s Step) Option {
	return func(j *Job) error {
		j.cleanupStep = s
		return nil
	}
}

// NormalDuration is the option used to set the normal duration of job execution
func NormalDuration(d time.Duration) Option {
	return func(j *Job) error {
		j.normalDuration = d
		return nil
	}
}

// ExecutionTimeout is the option used to set the job execution timeout
func ExecutionTimeout(d time.Duration) Option {
	return func(j *Job) error {
		j.executionTimeout = d
		return nil
	}
}
