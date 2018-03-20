# go-jobs

Jobs library for Go

## Cockroach DB reservation api

```Go
NewReservationModule(*sql.DB, worker_id)
    Create row (with default empty values)

ReserveJob(component_name, job_name) error
```

name | type | description
--- | ----------- | -------------
component_name | STRING | name of the component (e.g. 'keycloak_bridge')
component_id | STRING | component ID, obtained from flaki at startup. This ID can differentiate two instances of the same component.
job_name | STRING | name of the job
job_id | STRING | job ID, obtained from flaki when the job starts. The ID is associated with one job instance, so if a component exectute a job several times, e.g. a daily backup, each execution will have its own ID.
enabled | BOOL | use to disable jobs, for example during an upgrade we may want to disable some jobs.
status | STRING | status of the job ('RUNNING', 'IDLE')
start_time | TIMESTAMP | when the job started
step_infos | STRING | information on the current execution, updated regularly.
last_update | TIMESTAMP | when the step_infos field was last updated. If this field was not updated for a long time, we can guess that the job crashed.
message | STRING | message is updated with information about the job execution when the job finishes.
last_execution | TIMESTAMP | when the job was last executed
last_execution_success | TIMESTAMP | when the job was last executed successfully
last_execution_duration | INTERVAL | the duration of the last job execution
last_execution_status | STRING | the status of the last job execution ('SUCCESS', 'FAILED')
