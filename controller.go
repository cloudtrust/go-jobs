package controller

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	job_actor "github.com/cloudtrust/go-jobs/actor"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/cloudtrust/go-jobs/status"
	"github.com/go-kit/kit/log"
	"github.com/victorcoder/dkron/cron"
)

// Storage is the interface of the storage.
type Storage interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// LockManager is the lock policy to prevent concurrent job execution.
type LockManager interface {
	Enable(componentName string, jobName string) error
	Disable(componentName string, jobName string) error
	Lock(componentName, componentID, jobName, jobID string, jobMaxDuration time.Duration) error
	Unlock(componentName, componentID, jobName, jobID string) error
}

// StatusManager is the component used to persist information about job executions.
type StatusManager interface {
	Start(componentName, componentID, jobName string) error
	Update(componentName, componentID, jobName string, stepInfos map[string]string) error
	Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
	Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error
	Register(componentName, componentID, jobName, jobID string)
}

// IDGenerator is used to compute a unique identifier for component instance and job execution instance.
type IDGenerator interface {
	NextID() string
}

// Logger is the Logging interface
type Logger interface {
	Log(...interface{}) error
}

type directory struct {
	m map[string]struct{}
}

func (d *directory) add(key string) {
	d.m[key] = struct{}{}
}

func (d *directory) contains(key string) bool {
	var _, ok = d.m[key]
	return ok
}

// Controller is the main point of this library.
type Controller struct {
	cron          *cron.Cron
	masterActor   *actor.PID
	componentName string
	componentID   string
	idGenerator   IDGenerator
	statusManager StatusManager
	lockManager   LockManager
	jobDirectory  *directory
	logger        Logger
}

// Option is used to configure the Controller. It takes on argument: the Controller we are operating on.
type Option func(*Controller)

// NewController returns a new Controller.
func NewController(componentName, componentID string, idGenerator IDGenerator, lockManager LockManager, options ...Option) *Controller {
	var cron = cron.New()
	cron.Start()

	var s = &Controller{
		cron:          cron,
		componentName: componentName,
		componentID:   componentID,
		idGenerator:   idGenerator,
		lockManager:   lockManager,
		statusManager: &status.NoopStatusManager{},
		jobDirectory:  &directory{make(map[string]struct{})},
		logger:        log.NewNopLogger(),
	}

	// Apply options to the Controller
	for _, opt := range options {
		opt(s)
	}

	var props = job_actor.BuildMasterActorProps(componentName, componentID, s.logger)
	s.masterActor = actor.Spawn(props)

	return s
}

// EnableStatusStorage is an option to provide a component for job execution information storage.
func EnableStatusStorage(statusManager StatusManager) Option {
	return func(c *Controller) {
		c.statusManager = statusManager
	}
}

// LogWith specify which Logger ot use.
func LogWith(logger Logger) Option {
	return func(c *Controller) {
		c.logger = logger
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
	if c.jobDirectory.contains(j.Name()) {
		//already registered
		return
	}

	c.jobDirectory.add(j.Name())
	c.masterActor.Tell(&job_actor.RegisterJob{Job: j, IDGenerator: c.idGenerator, StatusManager: c.statusManager, LockManager: c.lockManager})
}

// Schedule a job execution according to a cron expression.
// See https://godoc.org/github.com/victorcoder/dkron/cron for cron expression syntax.
func (c *Controller) Schedule(cron string, jobName string) error {
	if !c.jobDirectory.contains(jobName) {
		return fmt.Errorf("job '%v' does not exist", jobName)
	}

	c.cron.AddFunc(cron, func() {
		c.masterActor.Tell(&job_actor.StartJob{JobName: jobName})
	})

	return nil
}

// Execute trigger an immediate job execution. (Lock policy is still applied)
func (c *Controller) Execute(jobName string) error {
	if !c.jobDirectory.contains(jobName) {
		return fmt.Errorf("job '%v' does not exist", jobName)
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
	for jobName := range c.jobDirectory.m {
		var err = c.lockManager.Disable(c.componentName, jobName)

		if err != nil {
			return err
		}
	}
	return nil
}

// EnableAll enables execution for all jobs.
func (c *Controller) EnableAll() error {
	for jobName := range c.jobDirectory.m {
		var err = c.lockManager.Enable(c.componentName, jobName)

		if err != nil {
			return err
		}
	}
	return nil
}

// Disable execution for the specified job according to the LockManager policy implementation.
func (c *Controller) Disable(jobName string) error {
	if !c.jobDirectory.contains(jobName) {
		return fmt.Errorf("job '%v' does not exist", jobName)
	}

	return c.lockManager.Disable(c.componentName, jobName)
}

// Enable execution for the specified job.
func (c *Controller) Enable(jobName string) error {
	if !c.jobDirectory.contains(jobName) {
		return fmt.Errorf("job '%v' does not exist", jobName)
	}

	return c.lockManager.Enable(c.componentName, jobName)
}
