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
		start_time TIMESTAMPTZ,
		last_update TIMESTAMPTZ,
		step_infos STRING,
		last_completed_component_id STRING,
		last_completed_job_id STRING,
		last_completed_start_time TIMESTAMPTZ,
		last_completed_end_time TIMESTAMPTZ,
		last_completed_step_infos STRING,
		last_completed_message STRING,
		last_failed_component_id STRING,
		last_failed_job_id STRING,
		last_failed_start_time TIMESTAMPTZ,
		last_failed_end_time TIMESTAMPTZ,
		last_failed_step_infos STRING,
		last_failed_message STRING,
		PRIMARY KEY (component_name, job_name))`
	insertStatusStmt = `INSERT INTO status (
		component_name,
		component_id,
		job_name,
		job_id,
		start_time,
		last_update,
		step_infos,
		last_completed_component_id,
		last_completed_job_id,
		last_completed_start_time,
		last_completed_end_time,
		last_completed_step_infos,
		last_completed_message,
		last_failed_component_id,
		last_failed_job_id,
		last_failed_start_time,
		last_failed_end_time,
		last_failed_step_infos,
		last_failed_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`
	selectStatusStmt = `SELECT * FROM status WHERE (component_name = $1 AND job_name = $2)`
	startStmt        = `UPDATE status SET (start_time) = ($1) WHERE (component_name = $2 AND job_name = $3)`
	updateStatusStmt = `UPDATE status SET (last_update, step_infos) = ($1, $2) WHERE (component_name = $3 AND job_name = $4)`
	completeStmt     = `UPDATE status SET (last_completed_component_id, last_completed_job_id, last_completed_start_time, last_completed_end_time, last_completed_step_infos, last_completed_message) = ($1, $2, status.start_time, $3, $4, $5) WHERE (component_name = $6 AND job_name = $7)`
	failStmt         = `UPDATE status SET (last_failed_component_id, last_failed_job_id, last_failed_start_time, last_failed_end_time, last_failed_step_infos, last_failed_message) = ($1, $2, status.start_time, $3, $4, $5) WHERE (component_name = $6 AND job_name = $7)`
)

// Status is the status module.
type Status struct {
	db DB
}

// DB is the interface of the DB.
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Table is a struct representing a row of the database status table.
type Table struct {
	componentName            string
	componentID              string
	jobName                  string
	jobID                    string
	startTime                time.Time
	lastUpdate               time.Time
	stepInfos                string
	lastCompletedComponentID string
	lastCompletedJobID       string
	lastCompletedStart       time.Time
	lastCompletedEnd         time.Time
	lastCompletedStepInfos   string
	lastCompletedMessage     string
	lastFailedComponentID    string
	lastFailedJobID          string
	lastFailedStart          time.Time
	lastFailedEnd            time.Time
	lastFailedStepInfos      string
	lastFailedMessage        string
}

// New returns a new status module.
func New(db DB) *Status {
	var s = &Status{
		db: db,
	}

	// Init DB: create table and status entry for job.
	db.Exec(createStatusTblStmt)

	return s
}

func (s *Status) Register(componentName, componentID, jobName, jobID string) {
	var t = time.Time{}
	s.db.Exec(insertStatusStmt, componentName, componentID, jobName, jobID, t, t, "", "", "", t, t, "", "", "", "", t, t, "", "")
}

// Start update the job start time in the DB.
func (s *Status) Start(componentName, jobName string) error {
	var _, err = s.db.Exec(startStmt, time.Now().UTC(), componentName, jobName)
	return err
}

// GetStatus returns the whole status database entry for the current Job.
func (s *Status) GetStatus(componentName, jobName string) (*Table, error) {
	var row = s.db.QueryRow(selectStatusStmt, componentName, jobName)
	var (
		cName, cID, jName, jID, stepInfos                                                           string
		lastCompletedComponentID, lastCompletedJobID, lastCompletedStepInfos, lastCompletedMessage  string
		lastFailedComponentID, lastFailedJobID, lastFailedStepInfos, lastFailedMessage              string
		startTime, lastUpdate, lastCompletedStart, lastCompletedEnd, lastFailedStart, lastFailedEnd time.Time
	)

	var err = row.Scan(&cName, &cID, &jName, &jID, &startTime, &lastUpdate, &stepInfos,
		&lastCompletedComponentID, &lastCompletedJobID, &lastCompletedStart, &lastCompletedEnd, &lastCompletedStepInfos, &lastCompletedMessage,
		&lastFailedComponentID, &lastFailedJobID, &lastFailedStart, &lastFailedEnd, &lastFailedStepInfos, &lastFailedMessage)
	if err != nil {
		return nil, errors.Wrap(err, "could not get job from DB")
	}

	return &Table{
		componentName:            cName,
		componentID:              cID,
		jobName:                  jName,
		jobID:                    jID,
		startTime:                startTime.UTC(),
		lastUpdate:               lastUpdate.UTC(),
		stepInfos:                stepInfos,
		lastCompletedComponentID: lastCompletedComponentID,
		lastCompletedJobID:       lastCompletedJobID,
		lastCompletedStart:       lastCompletedStart.UTC(),
		lastCompletedEnd:         lastCompletedEnd.UTC(),
		lastCompletedStepInfos:   lastCompletedStepInfos,
		lastCompletedMessage:     lastCompletedMessage,
		lastFailedComponentID:    lastFailedComponentID,
		lastFailedJobID:          lastFailedJobID,
		lastFailedStart:          lastFailedStart.UTC(),
		lastFailedEnd:            lastFailedEnd.UTC(),
		lastFailedStepInfos:      lastFailedStepInfos,
		lastFailedMessage:        lastFailedMessage,
	}, nil
}

// GetStartTime reads in DB and returns the time at which the job started.
func (s *Status) GetStartTime(componentName, jobName string) (time.Time, error) {
	var t, err = s.GetStatus(componentName, jobName)
	if err != nil {
		return time.Time{}, err
	}
	return t.startTime, nil
}

// Update updates the job status.
func (s *Status) Update(componentName, jobName string, stepInfos map[string]string) error {
	var infos []byte
	{
		var err error
		infos, err = json.Marshal(stepInfos)
		if err != nil {
			return errors.Wrap(err, "update failed, could not marshal stepInfos json")
		}
	}

	var _, err = s.db.Exec(updateStatusStmt, time.Now().UTC(), string(infos), componentName, jobName)
	if err != nil {
		return errors.Wrapf(err, "update failed, component '%s' could not update status '%s'", componentName, jobName)
	}

	return nil
}

// Complete update the job infos when the job finishes without errors.
func (s *Status) Complete(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error {
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

	var _, err = s.db.Exec(completeStmt, componentID, jobID, time.Now().UTC(), string(infos), string(msg), componentName, jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not update status '%s'", componentName, jobName)
	}
	return nil
}

// Fail update the job infos when the job finishes with errors.
func (s *Status) Fail(componentName, componentID, jobName, jobID string, stepInfos, message map[string]string) error {
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

	var _, err = s.db.Exec(failStmt, componentID, jobID, time.Now().UTC(), string(infos), string(msg), componentName, jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not update status '%s'", componentName, jobName)
	}

	return nil
}
