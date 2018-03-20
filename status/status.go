package status

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

const (
	createStatusTblStmt = `CREATE TABLE status (
		component_name STRING,
		component_id STRING,
		job_name STRING,
		job_id STRING,
		start_time TIMESTAMP WITHOUT TIME ZONE,
		step_infos STRING,
		last_update TIMESTAMP WITHOUT TIME ZONE,
		last_execution TIMESTAMP WITHOUT TIME ZONE,
		last_execution_status STRING,
		last_execution_message STRING,
		last_execution_duration INTERVAL,
		last_success TIMESTAMP WITHOUT TIME ZONE,
		PRIMARY KEY (component_name, job_name))`
	insertStatusStmt = `INSERT INTO status (
		component_name,
		component_id,
		job_name,
		job_id,
		start_time,
		step_infos,
		last_update,
		last_execution,
		last_execution_status,
		last_execution_message,
		last_execution_duration,
		last_success)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	selectStatusStmt = `SELECT * FROM status WHERE (component_name = $1 AND job_name = $2)`
	startStmt        = `UPDATE status SET (start_time) = ($1) WHERE (component_name = $2 AND job_name = $3)`
	updateStatusStmt = `UPDATE status SET (last_update, step_infos) = ($1, $2) WHERE (component_name = $3 AND job_name = $4)`
	finishStmt       = `UPDATE status SET (last_update, message, step_infos, last_execution, last_execution_success, last_execution_duration, last_execution_status) = ($1, $2, $3, $4, $5, $6, $7) WHERE (component_name = $8 AND job_name = $9)`
	cancelStmt       = `UPDATE status SET (last_update, message, step_infos, last_execution, last_execution_duration, last_execution_status) = ($1, $2, $3, $4, $5, $6) WHERE (component_name = $7 AND job_name = $8)`
)

// Status is the locking module.
type Status struct {
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

// Table is a struct representing a row of the database status table.
type Table struct {
	componentName         string
	componentID           string
	jobName               string
	jobID                 string
	startTime             time.Time
	stepInfos             string
	lastUpdate            time.Time
	lastExecution         time.Time
	lastExecutionStatus   string
	lastExecutionMessage  string
	lastExecutionDuration time.Duration
	lastSuccess           time.Time
}

// New returns a new status module.
func New(db DB, componentName, componentID, jobName, jobID string) *Status {
	var s = &Status{
		db:            db,
		componentName: componentName,
		componentID:   componentID,
		jobName:       jobName,
		jobID:         jobID,
	}

	// Init DB: create table and status entry for job.
	db.Exec(createStatusTblStmt)
	db.Exec(insertStatusStmt, s.componentName, s.componentID, s.jobName, s.jobID, time.Time{}, "", time.Time{}, time.Time{}, "", "", 0*time.Second, time.Time{})

	return s
}

// Start update the job start time in the DB.
func (s *Status) Start() error {
	var _, err = s.db.Exec(startStmt, time.Now(), s.componentName, s.jobName)
	return err
}

// GetStatus returns the whole status database entry for the current Job.
func (s *Status) GetStatus() (*Table, error) {
	var row = s.db.QueryRow(selectStatusStmt, s.componentName, s.jobName)
	var t = Table{}
	var interval string
	var err = row.Scan(&t.componentName, &t.componentID, &t.jobName, &t.jobID, &t.startTime, &t.stepInfos, &t.lastUpdate, &t.lastExecution, &t.lastExecutionStatus, &t.lastExecutionMessage, &interval, &t.lastSuccess)
	if err != nil {
		return nil, errors.Wrap(err, "could not get job from DB")
	}

	var duration time.Duration
	{
		var err error
		duration, err = time.ParseDuration(interval)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse duration")
		}
	}
	t.lastExecutionDuration = duration
	return &t, nil
}

// GetStartTime reads in DB and returns the time at which the job started.
func (s *Status) GetStartTime() (time.Time, error) {
	var t, err = s.GetStatus()
	if err != nil {
		return time.Time{}, err
	}
	return t.startTime, nil

}

// Update updates the job status.
func (s *Status) Update(stepInfos map[string]string) error {
	var infos []byte
	{
		var err error
		infos, err = json.Marshal(stepInfos)
		if err != nil {
			return errors.Wrap(err, "could not marshal json")
		}
	}

	var _, err = s.db.Exec(updateStatusStmt, time.Now(), string(infos), s.componentName, s.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not update status '%s'", s.componentName, s.jobName)
	}

	return nil
}

// Finish update the job infos when the job complete without errors.
func (s *Status) Finish(stepInfos, message map[string]string) error {
	var infos []byte
	{
		var err error
		infos, err = json.Marshal(stepInfos)
		if err != nil {
			return errors.Wrap(err, "could not marshal json")
		}
	}

	var msg []byte
	{
		var err error
		msg, err = json.Marshal(message)
		if err != nil {
			return errors.Wrap(err, "could not marshal json")
		}
	}

	var startTime time.Time
	{
		var err error
		startTime, err = s.GetStartTime()
		if err != nil {
			return err
		}
	}

	var now = time.Now()
	var _, err = s.db.Exec(finishStmt, now, string(msg), string(infos), now, now, time.Since(startTime), "SUCCESS", s.componentName, s.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not update status '%s'", s.componentName, s.jobName)
	}

	return nil
}

// Cancel update the job infos when the job finishes with errors.
func (s *Status) Cancel(stepInfos, message map[string]string) error {
	var infos []byte
	{
		var err error
		infos, err = json.Marshal(stepInfos)
		if err != nil {
			return errors.Wrap(err, "could not marshal json")
		}
	}

	var msg []byte
	{
		var err error
		msg, err = json.Marshal(message)
		if err != nil {
			return errors.Wrap(err, "could not marshal json")
		}
	}

	var startTime time.Time
	{
		var err error
		startTime, err = s.GetStartTime()
		if err != nil {
			return err
		}
	}

	var now = time.Now()
	var _, err = s.db.Exec(cancelStmt, now, string(msg), string(infos), now, time.Since(startTime), "FAILED", s.componentName, s.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not update status '%s'", s.componentName, s.jobName)
	}

	return nil
}
