package job

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func step(context.Context, interface{}) (interface{}, error) {
	return nil, nil
}

func cleanupStep(context.Context) (map[string]string, error) {
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

func TestGetName(t *testing.T) {
	var name = "ID"
	var job, err = NewJob(name, Steps(step))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, name, job.Name())
}

func TestSteps(t *testing.T) {
	var name = "ID"
	var job, err = NewJob(name, Steps(step, step))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, 2, len(job.Steps()))
}

func TestCleanup(t *testing.T) {
	var job, err = NewJob("ID", Steps(step, step), Cleanup(cleanupStep))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.NotNil(t, job.CleanupStep())
}

func TestNormalDuration(t *testing.T) {
	var d = 500 * time.Millisecond
	var job, err = NewJob("ID", Steps(step, step), NormalDuration(d))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, d, job.NormalDuration())
}

func TestExecutionTimeout(t *testing.T) {
	var d = 500 * time.Millisecond
	var job, err = NewJob("ID", Steps(step, step), ExecutionTimeout(d))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, d, job.ExecutionTimeout())
}

func TestSuicideTimeout(t *testing.T) {
	var d = 500 * time.Millisecond
	var job, err = NewJob("ID", Steps(step, step), SuicideTimeout(d))

	assert.Nil(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, d, job.SuicideTimeout())
}
