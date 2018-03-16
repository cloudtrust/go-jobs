package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

const (
	createDBStmt      = "CREATE DATABASE IF NOT EXISTS jobs"
	createJobsTblStmt = "CREATE TABLE jobs (component_name STRING, component_id STRING, job_name STRING, job_id STRING, enabled BOOL, status STRING, start_time TIMESTAMP, last_update TIMESTAMP, message STRING, step_infos STRING, last_execution TIMESTAMP, last_execution_success TIMESTAMP, last_execution_duration INTERVAL, last_execution_status STRING, PRIMARY KEY (component_name, job_name))"
	insertJobStmt     = "INSERT INTO jobs (component_name, component_id, job_name, job_id, enabled, status, start_time, last_update, message, step_state, last_execution, last_execution_success, last_execution_duration, last_execution_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"
	lockStmt          = "UPDATE jobs SET (component_id, job_id, status, start_time) = ($1, $2, $3, $4) WHERE (component_name = $5 AND job_name = $6 AND status IN ('COMPLETED', 'CANCELED'))"
	owningLockStmt    = "SELECT * FROM jobs WHERE (component_name = $1 AND component_id = $2 AND job_name = $3 AND job_id = $4 AND status = 'RUNNING')"
	enableStmt        = "UPDATE jobs SET (enabled) = ($1) WHERE (component_name = $2 AND job_name = $3)"
)

type Lock struct {
	db            *sql.DB
	componentName string
	componentID   string
	jobName       string
	jobID         string
}

func New(db *sql.DB, componentName, componentID, jobName, jobID string) *Lock {
	var now = time.Now()
	var r, err = db.Exec(insertJobStmt, componentName, componentID, jobName, jobID, true, "COMPLETED", now, now, "", "", now, now, 0*time.Hour, "COMPLETED")
	fmt.Println(r, err)

	return &Lock{
		db:            db,
		componentName: componentName,
		componentID:   componentID,
		jobName:       jobName,
		jobID:         jobID,
	}
}

// Unauthorised is the error returned when a worker try to reserve a job and another worker is already
// doing it.
type Unauthorised struct {
	componentName string
	jobName       string
}

func (e *Unauthorised) Error() string {
	return fmt.Sprintf("component '%s' could not lock job '%s'", e.componentName, e.jobName)
}

func (l *Lock) Lock() error {
	// To obtain distributed lock, update field in cockroach DB. If the update is successfull, we can execute the job.
	var res, err = l.db.Exec(lockStmt, l.componentID, l.jobID, "RUNNING", time.Now(), l.componentName, l.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not lock job '%s'", l.componentName, l.jobName)
	}

	// If one row is affected, the lock was successfully aquired.
	if i, err := res.RowsAffected(); err == nil && i == 1 {
		return nil
	}
	return &Unauthorised{componentName: l.componentName, jobName: l.jobName}
}

// OwningLock returns true if the worker is owning the lock, false otherwise.
func (l *Lock) OwningLock() (bool, error) {
	var res, err = l.db.Exec(owningLockStmt, l.componentName, l.componentID, l.jobName, l.jobID)
	if err != nil {
		return false, errors.Wrapf(err, "component '%s' could not lock job '%s'", l.componentName, l.jobName)
	}
	// If one row is returned, we have the lock.
	if i, err := res.RowsAffected(); err == nil && i == 1 {
		return true, nil
	}
	return false, nil
}

func (l *Lock) Enable() error {
	var res, err = l.db.Exec(enableStmt, true, l.componentName, l.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not enable job '%s'", l.componentName, l.jobName)
	}
	return nil
}

func (l *Lock) Disable() error {
	var res, err = l.db.Exec(enableStmt, false, l.componentName, l.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not disable job '%s'", l.componentName, l.jobName)
	}
	return nil
}
