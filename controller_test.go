package main

import (
	"context"
	"testing"
	"time"

	"github.com/cloudtrust/go-jobs/job"
	"github.com/stretchr/testify/assert"
)

func TestNominalUsage(t *testing.T) {
	//new, register, schedule, now
	var jobController, err = NewController("componentName", &DummyIdGenerator{})

	assert.Nil(t, err)
	var count = 0

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		count = count + 1
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob("job", job.Steps(dummyStep))

	jobController.Register(job)
	jobController.Register(job)

	jobController.Schedule("* * * * * *", job.Name())
	//wg.Wait()
	time.Sleep(3 * time.Second)

	assert.True(t, count > 2)

	jobController.Stop()
	var countSnapshot = count
	time.Sleep(2 * time.Second)
	assert.Equal(t, countSnapshot, count)

	jobController.Start()
	time.Sleep(2 * time.Second)
	assert.True(t, count > countSnapshot)
}

// Enable/Disable specific

// Enable/Disable all

func TestDisabledFunctions(t *testing.T) {
	//new, register, schedule, now
	var jobController, err = NewController("componentName", &DummyIdGenerator{})

	assert.Nil(t, err)

	var dummyStep = func(context.Context, interface{}) (interface{}, error) {
		return map[string]string{"message": "done"}, nil
	}

	var job, _ = job.NewJob("job", job.Steps(dummyStep))

	jobController.Register(job)

	assert.NotNil(t, jobController.Schedule("* * * * *", "job2"))
	assert.NotNil(t, jobController.Enable("job2"))
	assert.NotNil(t, jobController.Disable("job2"))

	assert.NotNil(t, jobController.Enable("job"))
	assert.NotNil(t, jobController.Disable("job"))
	assert.NotNil(t, jobController.EnableAll())
	assert.NotNil(t, jobController.DisableAll())
	

}

/* Utils */

type DummyIdGenerator struct {
	i int
}

func (g *DummyIdGenerator) NextId() string {
	g.i = g.i + 1
	return string(g.i)
}
