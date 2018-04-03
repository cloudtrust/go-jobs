package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	job_actor "github.com/cloudtrust/go-jobs/actor"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/victorcoder/dkron/cron"
)

// DB is the interface of the DB.
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// LockManager is the lock policy to prevent concurrent job execution.
type LockManager interface {
	Enable(componentName string, jobName string) error
	Disable(componentName string, jobName string) error
	Unlock(componentName string, componentID string, jobName string, jobID string) error
	Lock(componentName string, componentID string, jobName string, jobID string, jobMaxDuration time.Duration) error
}

// StatusManager is the component used to persist information about job executions.
type StatusManager interface {
	Start(componentName, jobName string) error
	Update(componentName, jobName string, stepInfos map[string]string) error
	Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
	Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
}

// IDGenerator is used to compute a unique identifier for component instance and job execution instance.
type IDGenerator interface {
	NextID() string
}

type Controller struct {
	cron          *cron.Cron
	masterActor   *actor.PID
	componentName string
	componentID   string
	idGenerator   IDGenerator
	statusManager StatusManager
	lockManager   LockManager
	jobDirectory  map[string]string
}

// ControllerOption is used to configure the Controller. It takes on argument: the Controller we are operating on.
type ControllerOption func(*Controller)

// NewController returns a new Controller.
func NewController(componentName string, idGenerator IDGenerator, lockManager LockManager, options ...ControllerOption) (*Controller, error) {

	var componentID = idGenerator.NextID()

	var cron = cron.New()
	cron.Start()

	var props = job_actor.BuildMasterActorProps(componentName, componentID)
	var pid = actor.Spawn(props)

	var s = &Controller{
		cron:          cron,
		masterActor:   pid,
		componentName: componentName,
		componentID:   componentID,
		idGenerator:   idGenerator,
		lockManager:   lockManager,
		statusManager: nil,
		jobDirectory:  make(map[string]string),
	}

	// Apply options to the Controller
	for _, opt := range options {
		opt(s)
	}

	return s, nil
}

// EnableStatusStorage is an option to provide a component for job execution information storage.
func EnableStatusStorage(statusManager StatusManager) ControllerOption {
	return func(c *Controller) {
		c.statusManager = statusManager
	}
}

//ComponentName returns the ComponentName of the instance.
func (c *Controller) ComponentName() string {
	return c.componentName
}

//ComponentID returns the ComponentID of the instance. This ID was computed thanks to the IDGenerator.
func (c *Controller) ComponentID() string {
	return c.componentID
}

// Register a new job. This method must be called first to be able to Schedule/Execute a job.
func (c *Controller) Register(j *job.Job) {
	if _, ok := c.jobDirectory[j.Name()]; ok {
		//already registered
		return
	}

	c.jobDirectory[j.Name()] = ""

	c.masterActor.Tell(&job_actor.RegisterJob{Job: j, IDGenerator: c.idGenerator, StatusManager: c.statusManager, LockManager: c.lockManager})
}

// Schedule a job execution according to a cron expression.
// See https://godoc.org/github.com/victorcoder/dkron/cron for cron expression syntax.
func (c *Controller) Schedule(cron string, jobName string) error {
	_, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it")
	}

	c.cron.AddFunc(cron, func() {
		c.masterActor.Tell(&job_actor.StartJob{JobName: jobName})
	})

	return nil
}

// Execute trigger an immediate job execution. (Lock policy is still applied)
func (c *Controller) Execute(jobName string) error {
	_, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it")
	}

	c.masterActor.Tell(&job_actor.StartJob{JobName: jobName})
	return nil
}

// Start the cron.
func (c *Controller) Start() {
	c.cron.Start()
}

//Stop the cron. All the scheduled tasks will not be executed until Start() is called.
func (c *Controller) Stop() {
	c.cron.Stop()
}

// DisableAll disables execution for all jobs according to the LockManager policy implementation.
func (c *Controller) DisableAll() error {
	for jobName := range c.jobDirectory {
		var err = c.lockManager.Disable(c.componentName, jobName)

		if err != nil {
			return err
		}
	}

	return nil
}

// EnableAll enables execution for all jobs.
func (c *Controller) EnableAll() error {
	for jobName := range c.jobDirectory {
		var err = c.lockManager.Enable(c.componentName, jobName)

		if err != nil {
			return err
		}
	}

	return nil
}

// Disable execution for the specified job according to the LockManager policy implementation.
func (c *Controller) Disable(jobName string) error {
	_, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it")
	}

	return c.lockManager.Disable(c.componentName, jobName)
}

// Enable execution for the specified job.
func (c *Controller) Enable(jobName string) error {
	_, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it")
	}

	return c.lockManager.Enable(c.componentName, jobName)
}
