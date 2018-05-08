package job

import (
	"context"
	"fmt"
	"time"
)

// Step is a unit of work of a Job.
type Step func(context.Context, interface{}) (interface{}, error)

// StepFunc is executed before the first job step. It can put information
// in the context that will bi passed to each step.
type StepFunc func(context.Context) context.Context

// CleanupStep is the unit of work to execute in case of failure
type CleanupStep func(context.Context) (map[string]string, error)

// Job contains the definiton of the job to execute.
type Job struct {
	// Name of the job
	name string
	// Steps of the job
	steps []Step
	// Step of cleaning called in case of failure of one of the steps.
	// Step of cleanup is optional.
	// Note: This step of cleanup is called in best effort but without warranty.
	cleanupStep CleanupStep
	// Expected duration of the job
	normalDuration time.Duration
	// Timeout of the job execution, if exceeded, the job should be stopped.
	// By default there is no timeout.
	// Note: the job will only be interrupted at the end of a Step.
	executionTimeout time.Duration
	// safeguard againt infinite loop. If exceeded, the whole application can be killed.
	// By default there is no timeout.
	suicideTimeout time.Duration

	// before are executed on the Job before the first step.
	before []StepFunc

	// context passed to each step
	ctx context.Context
}

// Option is used to configure the Job. It takes on argument: the Job we are operating on.
type Option func(*Job)

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
		ctx:              context.Background(),
	}

	// Apply options to the job
	for _, opt := range options {
		opt(job)
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

func (j *Job) CleanupStep() CleanupStep {
	return j.cleanupStep
}

func (j *Job) NormalDuration() time.Duration {
	return j.normalDuration
}

func (j *Job) ExecutionTimeout() time.Duration {
	return j.executionTimeout
}

func (j *Job) SuicideTimeout() time.Duration {
	return j.suicideTimeout
}

func (j *Job) Before() []StepFunc {
	return j.before
}

func (j *Job) Context() context.Context {
	return j.ctx
}

// Cleanup is the option used to set a step of cleanup
func Cleanup(s CleanupStep) Option {
	return func(j *Job) {
		j.cleanupStep = s
	}
}

// WithContext is the option used to set a context that is passed to each job step
func WithContext(ctx context.Context) Option {
	return func(j *Job) {
		j.ctx = ctx
	}
}

// NormalDuration is the option used to set the normal duration of job execution
func NormalDuration(d time.Duration) Option {
	return func(j *Job) {
		j.normalDuration = d
	}
}

// ExecutionTimeout is the option used to set the job execution timeout
func ExecutionTimeout(d time.Duration) Option {
	return func(j *Job) {
		j.executionTimeout = d
	}
}

// SuicideTimeout is the option used to set the job suicide timeout
func SuicideTimeout(d time.Duration) Option {
	return func(j *Job) {
		j.suicideTimeout = d
	}
}

// Before functions are executed on the job before the first step.
func Before(before ...StepFunc) Option {
	return func(j *Job) {
		j.before = append(j.before, before...)
	}
}
