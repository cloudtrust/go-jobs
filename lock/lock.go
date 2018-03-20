package lock

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

const (
	createLocksTblStmt = `CREATE TABLE locks (
		component_name STRING,
		component_id STRING,
		job_name STRING,
		job_id STRING,
		enabled BOOL,
		status STRING,
		PRIMARY KEY (component_name, job_name))`
	insertLockStmt = `INSERT INTO locks (
		component_name,
		component_id,
		job_name,
		job_id,
		enabled,
		status)
		VALUES ($1, $2, $3, $4, $5, $6)`
	lockStmt = `UPDATE locks SET (
		component_id,
		job_id,
		status) = ($1, $2, 'LOCKED') 
		WHERE (component_name = $3 AND job_name = $4 AND enabled = true AND status = 'UNLOCKED')`
	unlockStmt = `UPDATE locks SET (status) = ('UNLOCKED') WHERE (
		component_name = $1 AND 
		component_id = $2 AND 
		job_name = $3 AND
		job_id = $4)`
	enableStmt = `UPDATE locks SET (enabled) = ($1) 
		WHERE (component_name = $2 AND job_name = $3)`
	selectLockStmt = `SELECT * FROM locks WHERE (component_name = $1 AND job_name = $2)`
)

// Lock is the locking module.
type Lock struct {
	db            DB
	componentName string
	componentID   string
	jobName       string
	jobID         string
}

// DB is the interface of the DB.
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type table struct {
	componentName string
	componentID   string
	jobName       string
	jobID         string
	enabled       bool
	status        string
}

// New returns a new locking module.
func New(db DB, componentName, componentID, jobName, jobID string) *Lock {
	var l = &Lock{
		db:            db,
		componentName: componentName,
		componentID:   componentID,
		jobName:       jobName,
		jobID:         jobID,
	}

	// Init DB: create table and lock entry for job.
	db.Exec(createLocksTblStmt)
	db.Exec(insertLockStmt, l.componentName, l.componentID, l.jobName, l.jobID, true, "UNLOCKED")

	return l
}

// Unauthorised is the error returned when a worker try to reserve a job and another worker is already
// doing it.
type Unauthorised struct {
	componentName    string
	componentID      string
	jobName          string
	jobID            string
	lckComponentName string
	lckComponentID   string
	lckJobName       string
	lckJobID         string
}

func (e *Unauthorised) Error() string {
	return fmt.Sprintf("component '%s:%s' could not lock job '%s:%s', component '%s:%s' job '%s:%s' has the lock",
		e.componentName, e.componentID, e.jobName, e.jobID, e.lckComponentName, e.lckComponentID, e.lckJobName, e.lckJobID)
}

// Disabled is the error returned when a job is disabled.
type Disabled struct {
	componentName string
	jobName       string
}

func (e *Disabled) Error() string {
	return fmt.Sprintf("job '%s' for component '%s' is disabled", e.jobName, e.componentName)
}

// Lock try to reserve and lock the job in the distributed DB. It returns a nil error if the reservation succeeded, and
// an error if it didn't
func (l *Lock) Lock() error {
	if !l.IsEnabled() {
		return &Disabled{componentName: l.componentName, jobName: l.jobName}
	}
	// To obtain distributed lock, update field in cockroach DB.
	l.db.Exec(lockStmt, l.componentID, l.jobID, l.componentName, l.jobName)

	var lck, err = l.getLock()
	switch {
	case err != nil:
		return err
	case lck.componentName == l.componentName && lck.componentID == l.componentID &&
		lck.jobName == l.jobName && lck.jobID == l.jobID &&
		lck.enabled == true && lck.status == "LOCKED":
		return nil
	default:
		return &Unauthorised{componentName: l.componentName, componentID: l.componentID, jobName: l.jobName, jobID: l.jobID,
			lckComponentName: lck.componentName, lckComponentID: lck.componentID, lckJobName: lck.jobName, lckJobID: lck.jobID}
	}
}

// Unlock unlock the job in the distributed DB. A job can be unlocked it it is disabled.
// Unlock returns a nil error if the operation succeeded, and an error if it didn't.
func (l *Lock) Unlock() error {
	l.db.Exec(unlockStmt, l.componentName, l.componentID, l.jobName, l.jobID)

	var lck, err = l.getLock()
	switch {
	case err != nil:
		return err
	case lck.componentName == l.componentName && lck.jobName == l.jobName &&
		lck.status == "UNLOCKED":
		return nil
	default:
		return &Unauthorised{componentName: l.componentName, componentID: l.componentID, jobName: l.jobName, jobID: l.jobID,
			lckComponentName: lck.componentName, lckComponentID: lck.componentID, lckJobName: lck.jobName, lckJobID: lck.jobID}
	}
}

// getLock returns the whole lock database entry for the current Job.
func (l *Lock) getLock() (*table, error) {
	var row = l.db.QueryRow(selectLockStmt, l.componentName, l.jobName)
	var t = table{}
	var err = row.Scan(&t.componentName, &t.componentID, &t.jobName, &t.jobID, &t.enabled, &t.status)
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock from DB")
	}
	return &t, nil
}

// OwningLock returns true if the component is owning the lock, false otherwise.
func (l *Lock) OwningLock() bool {
	var lck, err = l.getLock()
	switch {
	case err != nil:
		return false
	case lck.componentName == l.componentName && lck.componentID == l.componentID &&
		lck.jobName == l.jobName && lck.jobID == l.jobID &&
		lck.enabled == true && lck.status == "LOCKED":
		return true
	default:
		return false
	}
}

// Enable enable the job. It sets the 'enabled' boolean to true for the current job.
func (l *Lock) Enable() error {
	var _, err = l.db.Exec(enableStmt, true, l.componentName, l.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s:%s' could not enable job '%s:%s'", l.componentName, l.componentID, l.jobName, l.jobID)
	}
	return nil
}

// Disable disable the job. It sets the 'enabled' boolean to false for the current job.
func (l *Lock) Disable() error {
	var _, err = l.db.Exec(enableStmt, false, l.componentName, l.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s:%s' could not disable job '%s:%s'", l.componentName, l.componentID, l.jobName, l.jobID)
	}
	return nil
}

// IsEnabled return true if the job is enabled, false otherwise.
func (l *Lock) IsEnabled() bool {
	var lck, err = l.getLock()
	switch {
	case err != nil:
		return false
	case lck.componentName == l.componentName && lck.jobName == l.jobName &&
		lck.enabled == true:
		return true
	default:
		return false
	}
}
