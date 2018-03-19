package job

import (
	"time"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func step(context.Context, interface{}) (interface{}, error) {
	return nil, nil
}

func TestNewJob(t *testing.T) {
	var job, err = NewJob("ID", Steps(step))

	assert.Nil(t, err)
	assert.NotNil(t, job)
}

func TestNewJobWithInvalidId(t *testing.T) {
	var job, err = NewJob("", Steps(step))

	assert.NotNil(t, err)
	assert.Nil(t, job)
}

func TestNewJobWithInvalidSteps(t *testing.T) {
	var job, err = NewJob("ID", nil)

	assert.NotNil(t, err)
	assert.Nil(t, job)
}

func TestSetCleanupStep(t *testing.T) {
	var job, err = NewJob("ID", Steps(step), CleanupStep(step))

	assert.Nil(t, err)
	assert.NotNil(t, job)
}

func TestSetNormalDuration(t *testing.T) {
	var timeout = 500 * time.Millisecond
	var job, err = NewJob("ID", Steps(step), NormalDuration(timeout))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, timeout, job.normalDuration)
}

func TestSetExecutionTimeout(t *testing.T) {
	var timeout = 500 * time.Millisecond
	var job, err = NewJob("ID", Steps(step), ExecutionTimeout(timeout))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, timeout, job.executionTimeout)
}

func TestGetName(t *testing.T){
	var name = "ID"
	var job, err = NewJob(name, Steps(step))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, name, job.Name())
}

func TestGetSteps(t *testing.T){
	var name = "ID"
	var job, err = NewJob(name, Steps(step, step))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, 2, len(job.Steps()))
}
