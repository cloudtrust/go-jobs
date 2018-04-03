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

type LockManager interface {
	Enable(componentName string, jobName string) error
	Disable(componentName string, jobName string) error
	Unlock(componentName string, componentID string, jobName string, jobID string) error
	Lock(componentName string, componentID string, jobName string, jobID string, jobMaxDuration time.Duration) error
}

type StatusManager interface {
	Start(componentName, jobName string) error
	Update(componentName, jobName string, stepInfos map[string]string) error
	Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
	Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
}

type IdGenerator interface {
	NextId() string
}

type Controller struct {
	cron          *cron.Cron
	masterActor   *actor.PID
	componentName string
	componentID   string
	idGenerator   IdGenerator
	statusManager StatusManager
	lockManager   LockManager
	jobDirectory  map[string]string
}

// ControllerOption is used to configure the Controller. It takes on argument: the Controller we are operating on.
type ControllerOption func(*Controller) error

// NewController returns a new Controller.
// TODO options
// DB connection param
// Local lock mode vs DistributedLock mode
// Kill timeout
func NewController(componentName string, idGenerator IdGenerator, lockManager LockManager, options ...ControllerOption) (*Controller, error) {

	var componentID = idGenerator.NextId()

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
		var err = opt(s)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func EnableStatusStorage(statusManager StatusManager) ControllerOption {
	return func(c *Controller) error {
		c.statusManager = statusManager
		return nil
	}
}

func (c *Controller) ComponentName() string {
	return c.componentName
}

func (c *Controller) ComponentID() string {
	return c.componentID
}

// Register a job.
func (c *Controller) Register(j *job.Job) {
	if _, ok := c.jobDirectory[j.Name()]; ok {
		//already registered
		return
	}

	var jobID = c.idGenerator.NextId()
	c.jobDirectory[j.Name()] = jobID

	c.masterActor.Tell(&job_actor.RegisterJob{JobID: jobID, Job: j, StatusManager: c.statusManager, LockManager: c.lockManager})
}

// AddTask schedule a run for the job.
func (c *Controller) Schedule(cron string, jobName string) error {
	id, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	c.cron.AddFunc(cron, func() {
		fmt.Print("exec")
		c.masterActor.Tell(&job_actor.StartJob{JobID: id})
	})

	return nil
}

func (c *Controller) Execute(jobName string) error {
	id, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	c.masterActor.Tell(&job_actor.StartJob{JobID: id})
	return nil
}

// Start the cron
func (c *Controller) Start() {
	c.cron.Start()
}

func (c *Controller) Stop() {
	c.cron.Stop()
}

// DisableAll disables execution for all jobs.
func (c *Controller) DisableAll() error {
	for jobName, jobID := range c.jobDirectory {
		var err = c.lockManager.Disable(c.componentName, jobName)

		if err != nil {
			return err
		}
	}

	return nil
}

// EnableAll enables execution for all jobs.
func (c *Controller) EnableAll() error {
	for jobName, jobID := range c.jobDirectory {
		var err = c.lockManager.Enable(c.componentName, jobName)

		if err != nil {
			return err
		}
	}

	return nil
}

// Disable execution for the specified job.
func (c *Controller) Disable(jobName string) error {
	jobID, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	return c.lockManager.Disable(c.componentName, jobName)
}

// Enable execution for the specified job.
func (c *Controller) Enable(jobName string) error {
	jobID, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	return c.lockManager.Enable(c.componentName, jobName)
}
