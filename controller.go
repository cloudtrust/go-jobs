package main

import (
	"database/sql"
	"fmt"

	"github.com/AsynkronIT/protoactor-go/actor"
	job_actor "github.com/cloudtrust/go-jobs/actor"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/cloudtrust/go-jobs/lock"
	"github.com/cloudtrust/go-jobs/status"
	"github.com/victorcoder/dkron/cron"
)

// DB is the interface of the DB.
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type LockManager interface {
	Lock() error
	Unlock() error
}

type StatusManager interface {
	Start() error
	Update(stepInfos map[string]string) error
	Complete(stepInfos, message map[string]string) error
	Fail(stepInfos, message map[string]string) error
}

type IdGenerator interface {
	NextId() string
}

type Controller struct {
	cron                 *cron.Cron
	masterActor          *actor.PID
	componentName        string
	componentID          string
	idGenerator          IdGenerator
	lockMode             lock.LockMode
	statusStorageEnabled bool
	dbStatus             DB
	dbLock               DB
	jobDirectory         map[string]string
}

// ControllerOption is used to configure the Controller. It takes on argument: the Controller we are operating on.
type ControllerOption func(*Controller) error

// NewController returns a new Controller.
// TODO options
// DB connection param
// Local lock mode vs DistributedLock mode
// Kill timeout
func NewController(componentName string, idGenerator IdGenerator, options ...ControllerOption) (*Controller, error) {

	var componentID = idGenerator.NextId()

	var cron = cron.New()
	cron.Start()

	var props = job_actor.BuildMasterActorProps(componentName, componentID)
	var pid = actor.Spawn(props)

	var s = &Controller{
		cron:                 cron,
		masterActor:          pid,
		componentName:        componentName,
		componentID:          componentID,
		idGenerator:          idGenerator,
		lockMode:             lock.Local,
		statusStorageEnabled: false,
		dbStatus:             nil,
		dbLock:               nil,
		jobDirectory:         make(map[string]string),
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

func EnableDistrutedLock(db DB) ControllerOption {
	return func(c *Controller) error {
		c.dbLock = db
		c.lockMode = lock.Distributed
		return nil
	}
}

func EnableStatusStorage(db DB) ControllerOption {
	return func(c *Controller) error {
		c.dbStatus = db
		c.statusStorageEnabled = true
		return nil
	}
}

func (c *Controller) LockMode() lock.LockMode {
	return c.lockMode
}

func (c *Controller) StatusStorageEnabled() bool {
	return c.statusStorageEnabled
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

	var l LockManager
	var s StatusManager

	if c.lockMode == lock.Distributed {
		l = lock.New(c.dbLock, c.componentName, c.componentID, j.Name(), jobID, 0)
	} else {
		l = lock.NewLocalLock()
	}

	if c.statusStorageEnabled {
		s = status.New(c.dbStatus, c.componentName, c.componentID, j.Name(), jobID)
	}

	c.masterActor.Tell(&job_actor.RegisterJob{JobID: jobID, Job: j, StatusManager: s, LockManager: l})
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
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}

	for jobName, jobID := range c.jobDirectory {
		var err = lock.New(c.dbLock, c.componentName, c.componentID, jobName, jobID, 0).Disable()

		if err != nil {
			return err
		}
	}

	return nil
}

// EnableAll enables execution for all jobs.
func (c *Controller) EnableAll() error {
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}

	for jobName, jobID := range c.jobDirectory {
		var err = lock.New(c.dbLock, c.componentName, c.componentID, jobName, jobID, 0).Enable()

		if err != nil {
			return err
		}
	}

	return nil
}

// Disable execution for the specified job.
func (c *Controller) Disable(jobName string) error {
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}

	jobID, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	return lock.New(c.dbLock, c.componentName, c.componentID, jobName, jobID, 0).Disable()
}

// Enable execution for the specified job.
func (c *Controller) Enable(jobName string) error {
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}

	jobID, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	return lock.New(c.dbLock, c.componentName, c.componentID, jobName, jobID, 0).Enable()
}

func (c *Controller) assertDistirbutedLock() (bool, error) {
	if c.lockMode == lock.Local {
		return false, fmt.Errorf("Feature not available with 'Local' lock mode.")
	}
	return true, nil
}
