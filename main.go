package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	_ "github.com/lib/pq"
)

func main() {

	// Logger.
	var logger = log.NewJSONLogger(os.Stdout)
	{
		logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	// Connect to the "jobs" database.
	db, err := sql.Open("postgres", "postgresql://job@172.19.0.2:26257/jobs?sslmode=disable")
	if err != nil {
		logger.Log("msg", "error connecting to the database: ", "error", err.Error())
		return
	}

	db.Exec("DROP table jobs")
	db.Exec(createJobsTblStmt)

	var lock = New(db, "keycloak_bridge", "bridgeID", "backup", "backupID")

	_ = lock
	/*
		if _, err := db.Exec(insertJobStmt, "keycloak", "backup", "1234", "5678", "COMPLETED", time.Now(), time.Duration(1*time.Hour), time.Now(), "message", "step", time.Now(), time.Now(), time.Duration(3*time.Hour), "OK"); err != nil {
			logger.Log("msg", "could not insert rows", "error", err.Error())
			return
		}

		// Update.
		if res, err := db.Exec(updateStmt, "STARTING", "keycloak", "backup"); err != nil {
			logger.Log("msg", "could not update", "error", err.Error())
			return
		} else {
			fmt.Println(res.RowsAffected())
		}
		print(db, logger)*/
}

func print(db *sql.DB, logger log.Logger) {
	// Print.
	rows, err := db.Query("SELECT * FROM jobs")
	if err != nil {
		logger.Log("msg", "could not read", "error", err.Error())
		return
	}
	defer rows.Close()
	fmt.Println("Jobs:")
	for rows.Next() {
		var component_name, job_name, job_id, worker_id, status, message, step_state, last_execution_status string
		var start_time, last_update, last_execution, last_execution_success time.Time
		var max_duration, last_execution_duration string
		if err := rows.Scan(&component_name, &job_name, &job_id, &worker_id, &status, &start_time, &max_duration, &last_update, &message, &step_state, &last_execution, &last_execution_success, &last_execution_duration, &last_execution_status); err != nil {
			logger.Log("msg", "could not scan", "error", err.Error())
			return
		}
		fmt.Printf("%v %v %v %v %v %v %v %v %v %v %v %v %v %v\n", component_name, job_name, job_id, worker_id, status, start_time, max_duration, last_update, message, step_state, last_execution, last_execution_success, last_execution_duration, last_execution_status)
	}
}
