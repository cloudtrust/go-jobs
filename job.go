package main

import (
	"fmt"
)

type Job struct {
	// Unique id to identify the job
	jobId string
	// Steps of the job
	steps []func()
	// Function executed before exection of each step
	beforeFuncs []func()
	// Function executed before exection of each step
	afterFuncs []func()
}

func New(id string) (*Job, error) {
	var job = &Job{
		jobId:       id,
		steps:       make([]func(), 0),
		beforeFuncs: make([]func(), 0),
		afterFuncs:  make([]func(), 0),
	}

	return job, nil
}

func (j *Job) JobId() string {
	return j.jobId
}

func (j *Job) AddSteps(steps ...func()) {
	j.steps = append(j.steps, steps...)
}

func (j *Job) BeforeEachStep(beforeFunc ...func()) {
	j.beforeFuncs = append(j.beforeFuncs, beforeFunc...)
}

func (j *Job) AfterEachStep(afterFunc ...func()) {
	j.afterFuncs = append(j.afterFuncs, afterFunc...)
}

func (j *Job) Run() error {
	for _, f := range j.steps {
		fmt.Println("--BeforeSteps--")
		exec(j.beforeFuncs...)
		fmt.Println("--AfterSteps--")
		fmt.Println("--ExcuteStep Begin--")
		f()
		fmt.Println("--ExcuteStep Stop--")
		fmt.Println("--BeforeSteps--")
		exec(j.afterFuncs...)
		fmt.Println("--AfterSteps--")
	}
	return nil
}

func exec(funcs ...func()) {
	for _, f := range funcs {
		f()
	}
}
