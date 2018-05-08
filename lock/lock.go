// Package lock provides a mechanism for distributed locks.
// Several instances of components (same component name, different component IDs) can
// compete to execute Jobs. We want that a given job (recognised by its job name) is
// executed only once at a time.
// To achive that we use CockroachDB, a distributed ACID DB. Each component will
// execute a transaction to lock the job, the transaction that succeed will obtain the lock,
// and the component can execute the corresponding job.
// Also, when a lock is disabled, the Lock method will always return an ErrDisabled error.
package lock

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	createLocksTblStmt = `CREATE TABLE IF NOT EXISTS locks (
		component_name STRING,
		component_id STRING,
		job_name STRING,
		job_id STRING,
		enabled BOOL,
		status STRING,
		lock_time TIMESTAMPTZ,
		PRIMARY KEY (component_name, job_name))`
	insertLockStmt = `INSERT INTO locks (
		component_name,
		component_id,
		job_name,
		job_id,
		enabled,
		status,
		lock_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (component_name, job_name) DO NOTHING`
	lockStmt = `UPDATE locks SET (
		component_id,
		job_id,
		status,
		lock_time) = ($1, $2, 'LOCKED', $3) 
		WHERE (component_name = $4 AND job_name = $5 AND enabled = true AND status = 'UNLOCKED')`
	forceLockStmt = `UPDATE locks SET (
		component_id,
		job_id,
		status,
		lock_time) = ($1, $2, 'LOCKED', $3)
		WHERE (component_name = $4 AND job_name = $5 AND enabled = true)`
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
	db DB
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
	lockTime      time.Time
}

// New returns a new locking module.
func New(db DB) *Lock {
	var l = &Lock{
		db: db,
	}

	// Init DB: create table and lock entry for job.
	db.Exec(createLocksTblStmt)
	return l
}

// ErrUnauthorised is the error returned when there is a call to the Lock method, but there is already another component
// owning the lock.
type ErrUnauthorised struct {
	componentName    string
	componentID      string
	jobName          string
	jobID            string
	lckComponentName string
	lckComponentID   string
	lckJobName       string
	lckJobID         string
}

func (e *ErrUnauthorised) Error() string {
	return fmt.Sprintf("component '%s:%s' could not lock job '%s:%s', component '%s:%s' job '%s:%s' has the lock",
		e.componentName, e.componentID, e.jobName, e.jobID, e.lckComponentName, e.lckComponentID, e.lckJobName, e.lckJobID)
}

// ErrDisabled is the error returned when the lock is disabled.
type ErrDisabled struct {
	componentName string
	jobName       string
}

func (e *ErrDisabled) Error() string {
	return fmt.Sprintf("job '%s' for component '%s' is disabled", e.jobName, e.componentName)
}

// Lock try to reserve and lock the job in the distributed DB. It returns a nil error if the reservation succeeded, and
// an error if it didn't.
func (l *Lock) Lock(componentName, componentID, jobName, jobID string, jobMaxDuration time.Duration) error {
	l.createRowIfNotExists(componentName, jobName)

	var stmt string
	{
		var lck, err = l.getTable(componentName, jobName)
		switch {
		case err != nil:
			return err
		case lck.enabled == false:
			return &ErrDisabled{componentName: componentName, jobName: jobName}
		case time.Now().After(lck.lockTime.Add(jobMaxDuration)):
			// If the job exceed the job maxduration, we can force a lock.
			stmt = forceLockStmt
		default:
			stmt = lockStmt
		}
	}

	// Try locking.
	l.db.Exec(stmt, componentID, jobID, time.Now().UTC(), componentName, jobName)

	// Check if we got the lock.
	var lck, err = l.getTable(componentName, jobName)
	switch {
	case err != nil:
		return err
	case lck.componentName == componentName && lck.componentID == componentID &&
		lck.jobName == jobName && lck.jobID == jobID &&
		lck.enabled == true && lck.status == "LOCKED":
		return nil
	default:
		return &ErrUnauthorised{componentName: componentName, componentID: componentID, jobName: jobName, jobID: jobID,
			lckComponentName: lck.componentName, lckComponentID: lck.componentID, lckJobName: lck.jobName, lckJobID: lck.jobID}
	}
}

// Unlock unlocks the lock in the distributed DB. A job can be unlocked it it is disabled.
// Unlock returns a nil error if the operation succeeded, and an error if it didn't.
func (l *Lock) Unlock(componentName, componentID, jobName, jobID string) error {
	l.createRowIfNotExists(componentName, jobName)

	l.db.Exec(unlockStmt, componentName, componentID, jobName, jobID)

	var lck, err = l.getTable(componentName, jobName)
	switch {
	case err != nil:
		return err
	case lck.componentName == componentName && lck.jobName == jobName &&
		lck.status == "UNLOCKED":
		return nil
	default:
		return &ErrUnauthorised{componentName: componentName, componentID: componentID, jobName: jobName, jobID: jobID,
			lckComponentName: lck.componentName, lckComponentID: lck.componentID, lckJobName: lck.jobName, lckJobID: lck.jobID}
	}
}

// Enable enable the job. It sets the 'enabled' boolean to true for the current job.
func (l *Lock) Enable(componentName, jobName string) error {
	l.createRowIfNotExists(componentName, jobName)

	var _, err = l.db.Exec(enableStmt, true, componentName, jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not enable job '%s'", componentName, jobName)
	}
	return nil
}

// Disable disable the job. It sets the 'enabled' boolean to false for the current job.
func (l *Lock) Disable(componentName, jobName string) error {
	l.createRowIfNotExists(componentName, jobName)

	var _, err = l.db.Exec(enableStmt, false, componentName, jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not disable job '%s'", componentName, jobName)
	}
	return nil
}

// getTable returns the whole lock database entry for the current job.
func (l *Lock) getTable(componentName, jobName string) (*table, error) {
	var row = l.db.QueryRow(selectLockStmt, componentName, jobName)
	var (
		cName, cID, jName, jID, status string
		enabled                        bool
		lockTime                       time.Time
	)

	var err = row.Scan(&cName, &cID, &jName, &jID, &enabled, &status, &lockTime)
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock table from DB")
	}

	return &table{
		componentName: cName,
		componentID:   cID,
		jobName:       jName,
		jobID:         jID,
		enabled:       enabled,
		status:        status,
		lockTime:      lockTime.UTC(),
	}, nil
}

func (l *Lock) createRowIfNotExists(componentName, jobName string) {
	l.db.Exec(insertLockStmt, componentName, "", jobName, "", true, "UNLOCKED", time.Time{})
}

// NoopLocker is a lock that does nothing, it always accept lock requests. It should be used when no lock is required.
type NoopLocker struct{}

func (l *NoopLocker) Lock(componentName, componentID, jobName, jobID string, jobMaxDuration time.Duration) error {
	return nil
}
func (l *NoopLocker) Unlock(componentName, componentID, jobName, jobID string) error { return nil }
func (l *NoopLocker) Enable(componentName, jobName string) error                     { return nil }
func (l *NoopLocker) Disable(componentName, jobName string) error                    { return nil }
