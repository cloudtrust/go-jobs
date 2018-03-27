package main

import (
	"fmt"

	"github.com/AsynkronIT/protoactor-go/actor"
	job_actor "github.com/cloudtrust/go-jobs/actor"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/victorcoder/dkron/cron"
)

type LockMode int

const (
	// Local lock
	Local LockMode = iota
	// Lock distributed across instances via DB
	Distributed
)

func (l LockMode) String() string {
	var names = []string{"Local", "Distributed"}

	if l < Local || l > Distributed {
		panic("Unknown lock mode")
	}

	return names[l]
}

type DB interface{}

type IdGenerator interface {
	NextId() string
}

type Controller struct {
	cron                 *cron.Cron
	masterActor          *actor.PID
	componentName        string
	componentID          string
	idGenerator          IdGenerator
	lockMode             LockMode
	statusStorageEnabled bool
	db                   DB
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

	var props = job_actor.BuildMasterActorProps()
	var pid = actor.Spawn(props)

	var s = &Controller{
		cron:                 cron,
		masterActor:          pid,
		componentName:        componentName,
		componentID:          componentID,
		idGenerator:          idGenerator,
		lockMode:             Local,
		statusStorageEnabled: false,
		db:                   nil,
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
		c.db = db
		c.lockMode = Distributed
		return nil
	}
}

func EnableStatusStorage(db DB) ControllerOption {
	return func(c *Controller) error {
		c.db = db
		c.statusStorageEnabled = true
		return nil
	}
}

func (c *Controller) LockMode() LockMode {
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
	if _, ok := c.jobDirectory[j.Name()]; !ok {
		//already registered
		return
	}

	var jobID = c.idGenerator.NextId()
	c.jobDirectory[j.Name()] = jobID

	c.masterActor.Tell(&job_actor.RegisterJob{Label: jobID, Job: j, Statistics: nil, Lock: nil})
}

// AddTask schedule a run for the job.
func (c *Controller) Schedule(cron string, jobName string) error {
	id, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	c.cron.AddFunc(cron, func() {
		c.masterActor.Tell(&job_actor.StartJob{id})
	})

	return nil
}

func (c *Controller) Execute(jobName string) error {
	id, ok := c.jobDirectory[jobName]

	if !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	c.masterActor.Tell(&job_actor.StartJob{Label: id})
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
	//TODO
	return nil
}

// EnableAll enables execution for all jobs.
func (c *Controller) EnableAll() error {
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}
	//TODO
	return nil
}

// Disable execution for the specified job.
func (c *Controller) Disable(jobName string) error {
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}

	if id, ok := c.jobDirectory[jobName]; !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	//TODO
	return nil
}

// Enable execution for the specified job.
func (c *Controller) Enable(jobName string) error {
	if ok, err := c.assertDistirbutedLock(); !ok {
		return err
	}

	if id, ok := c.jobDirectory[jobName]; !ok {
		return fmt.Errorf("Unknown job. First register it.")
	}

	//TODO
	return nil
}

func (c *Controller) assertDistirbutedLock() (bool, error) {
	if c.lockMode == Local {
		return false, fmt.Errorf("Feature not available with 'Local' lock mode.")
	}
	return true, nil
}
