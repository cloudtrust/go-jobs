package main

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

const (
	updateStatusStmt = "UPDATE jobs SET (last_update, step_infos) = ($1, $2) WHERE (component_name = $3 AND job_name = $4)"
	startTimeStmt    = "SELECT start_time from jobs WHERE (component_name = $1 AND job_name = $1)"
	finishStmt       = "UPDATE jobs SET (status, last_update, message, step_infos, last_execution, last_execution_success, last_execution_duration, last_execution_status) = ($1, $2, $3, $4, $5, $6, $7, $8) WHERE (component_name = $9 AND job_name = $10)"
)

type StatusModule struct {
	db            *sql.DB
	componentName string
	jobName       string
}

func NewStatusModule(db *sql.DB, componentName, jobName string) *StatusModule {
	return &StatusModule{
		db:            db,
		componentName: componentName,
		jobName:       jobName,
	}
}

func (s *StatusModule) Update(stepInfos map[string]string) error {
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

func (s *StatusModule) Finish(stepInfos, message map[string]string) error {
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
		var rows, err = s.db.Query(startTimeStmt)
		if err != nil {
			return errors.Wrap(err, "could not get job start_time from DB")
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&startTime); err != nil {
				return errors.Wrap(err, "could not get job start_time from DB")
			}
		}
	}

	var now = time.Now()
	var _, err = s.db.Exec(finishStmt, "COMPLETED", now, string(msg), string(infos), now, now, time.Since(startTime), "COMPLETED", s.componentName, s.jobName)
	if err != nil {
		return errors.Wrapf(err, "component '%s' could not update status '%s'", s.componentName, s.jobName)
	}

	return nil
}

func (s *StatusModule) Cancel(stepInfos, message map[string]string) {

}
