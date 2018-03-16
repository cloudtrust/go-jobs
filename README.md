# go-jobs

Jobs library for Go

## Cockroach DB reservation api

```Go
NewReservationModule(*sql.DB, worker_id)
    Create row (with default empty values)

ReserveJob(component_name, job_name) error
```

column name | |
--- | ----------- | -------------
component_name | |
job_name | |
job_id (from snowflake) | |
worker_id (from snowflake, obtained at library setup) | |
status (STARTING, RUNNING, COMPLETED, CANCELLED) | |
start_time | |
max_duration | |
last_update | |
message(error, .... e.g. job failed for users x,y,z...) | |
step_state | |
last_execution | |
last_execution_success | |
last_execution_duration | |
last_execution_status | |
