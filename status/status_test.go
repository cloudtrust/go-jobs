package status

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var (
	hostPort    = flag.String("hostport", "172.19.0.2:26257", "cockroach host:port")
	user        = flag.String("user", "job", "user name")
	db          = flag.String("db", "jobs", "database name")
	integration = flag.Bool("integration", false, "run the integration tests")
)

func TestMain(m *testing.M) {
	flag.Parse()
	result := m.Run()
	os.Exit(result)
}

func TestNewStatus(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var s = New(db)
	s.Register(componentName, componentID, jobName, jobID)
	var tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.Zero(t, tbl.startTime)
	assert.Zero(t, tbl.lastUpdate)
	assert.Equal(t, "", tbl.stepInfos)
	assert.Equal(t, "", tbl.lastCompletedComponentID)
	assert.Equal(t, "", tbl.lastCompletedJobID)
	assert.Zero(t, tbl.lastCompletedStart)
	assert.Zero(t, tbl.lastCompletedEnd)
	assert.Equal(t, "", tbl.lastCompletedStepInfos)
	assert.Equal(t, "", tbl.lastCompletedMessage)
	assert.Equal(t, "", tbl.lastFailedComponentID)
	assert.Equal(t, "", tbl.lastFailedJobID)
	assert.Zero(t, tbl.lastFailedStart)
	assert.Zero(t, tbl.lastFailedEnd)
	assert.Equal(t, "", tbl.lastFailedStepInfos)
	assert.Equal(t, "", tbl.lastFailedMessage)
}

func TestStart(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var s = New(db)
	s.Register(componentName, componentID, jobName, jobID)
	var tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.Zero(t, tbl.startTime)

	// Start.
	assert.Nil(t, s.Start(componentName, jobName))
	tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)

	// start is modified.
	assert.NotZero(t, tbl.startTime)

	// The other fields stay the same.
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.Zero(t, tbl.lastUpdate)
	assert.Equal(t, "", tbl.stepInfos)
	assert.Equal(t, "", tbl.lastCompletedComponentID)
	assert.Equal(t, "", tbl.lastCompletedJobID)
	assert.Zero(t, tbl.lastCompletedStart)
	assert.Zero(t, tbl.lastCompletedEnd)
	assert.Equal(t, "", tbl.lastCompletedStepInfos)
	assert.Equal(t, "", tbl.lastCompletedMessage)
	assert.Equal(t, "", tbl.lastFailedComponentID)
	assert.Equal(t, "", tbl.lastFailedJobID)
	assert.Zero(t, tbl.lastFailedStart)
	assert.Zero(t, tbl.lastFailedEnd)
	assert.Equal(t, "", tbl.lastFailedStepInfos)
	assert.Equal(t, "", tbl.lastFailedMessage)
}

func TestGetStartTime(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var s = New(db)
	s.Register(componentName, componentID, jobName, jobID)
	var tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.Zero(t, tbl.startTime)

	// Start.
	assert.Nil(t, s.Start(componentName, jobName))
	tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.NotZero(t, tbl.startTime)
}

func TestUpdate(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var s = New(db)
	s.Register(componentName, componentID, jobName, jobID)
	var tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.Zero(t, tbl.lastUpdate)
	assert.Zero(t, tbl.stepInfos)

	// Update.
	assert.Nil(t, s.Update(componentName, jobName, map[string]string{"key": "val"}))
	tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)

	// lastUpdate and stepInfos are modified.
	assert.NotZero(t, tbl.lastUpdate)
	assert.Equal(t, "{\"key\":\"val\"}", tbl.stepInfos)

	// The other fields stay the same.
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.Zero(t, tbl.startTime)
	assert.Equal(t, "", tbl.lastCompletedComponentID)
	assert.Equal(t, "", tbl.lastCompletedJobID)
	assert.Zero(t, tbl.lastCompletedStart)
	assert.Zero(t, tbl.lastCompletedEnd)
	assert.Equal(t, "", tbl.lastCompletedStepInfos)
	assert.Equal(t, "", tbl.lastCompletedMessage)
	assert.Equal(t, "", tbl.lastFailedComponentID)
	assert.Equal(t, "", tbl.lastFailedJobID)
	assert.Zero(t, tbl.lastFailedStart)
	assert.Zero(t, tbl.lastFailedEnd)
	assert.Equal(t, "", tbl.lastFailedStepInfos)
	assert.Equal(t, "", tbl.lastFailedMessage)
}

func TestComplete(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
		stepInfos     = map[string]string{"key": "stepInfos"}
		msg           = map[string]string{"key": "message"}
	)

	var s = New(db)
	s.Register(componentName, componentID, jobName, jobID)
	var tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.Zero(t, tbl.lastCompletedComponentID)
	assert.Zero(t, tbl.lastCompletedJobID)
	assert.Zero(t, tbl.lastCompletedStart)
	assert.Zero(t, tbl.lastCompletedEnd)
	assert.Zero(t, tbl.lastCompletedStepInfos)
	assert.Zero(t, tbl.lastCompletedMessage)

	// Complete.
	assert.Nil(t, s.Start(componentName, jobName))
	assert.Nil(t, s.Complete(componentName, componentID, jobName, jobID, stepInfos, msg))
	tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)

	// The fields lastCompleted[ComponentID, JobID, Start, End, StepInfos, Message] and start time are modified.
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobID, tbl.jobID)
	assert.NotZero(t, tbl.lastCompletedStart)
	assert.NotZero(t, tbl.lastCompletedEnd)
	assert.Equal(t, "{\"key\":\"stepInfos\"}", tbl.lastCompletedStepInfos)
	assert.Equal(t, "{\"key\":\"message\"}", tbl.lastCompletedMessage)
	assert.Equal(t, tbl.startTime, tbl.lastCompletedStart)

	// The other fields stay the same.
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.Zero(t, tbl.lastUpdate)
	assert.Equal(t, "", tbl.stepInfos)
	assert.Equal(t, "", tbl.lastFailedComponentID)
	assert.Equal(t, "", tbl.lastFailedJobID)
	assert.Zero(t, tbl.lastFailedStart)
	assert.Zero(t, tbl.lastFailedEnd)
	assert.Equal(t, "", tbl.lastFailedStepInfos)
	assert.Equal(t, "", tbl.lastFailedMessage)
}

func TestFail(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
		stepInfos     = map[string]string{"key": "stepInfos"}
		msg           = map[string]string{"key": "message"}
	)

	var s = New(db)
	s.Register(componentName, componentID, jobName, jobID)
	var tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)
	assert.Zero(t, tbl.lastFailedComponentID)
	assert.Zero(t, tbl.lastFailedJobID)
	assert.Zero(t, tbl.lastFailedStart)
	assert.Zero(t, tbl.lastFailedEnd)
	assert.Zero(t, tbl.lastFailedStepInfos)
	assert.Zero(t, tbl.lastFailedMessage)

	// Complete.
	assert.Nil(t, s.Start(componentName, jobName))
	assert.Nil(t, s.Fail(componentName, componentID, jobName, jobID, stepInfos, msg))
	tbl, err = s.GetStatus(componentName, jobName)
	assert.Nil(t, err)

	// The fields lastFailed[ComponentID, JobID, Start, End, StepInfos, Message] and start time are modified.
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobID, tbl.jobID)
	assert.NotZero(t, tbl.lastFailedStart)
	assert.NotZero(t, tbl.lastFailedEnd)
	assert.Equal(t, "{\"key\":\"stepInfos\"}", tbl.lastFailedStepInfos)
	assert.Equal(t, "{\"key\":\"message\"}", tbl.lastFailedMessage)
	assert.Equal(t, tbl.startTime, tbl.lastFailedStart)

	// The other fields stay the same.
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.Zero(t, tbl.lastUpdate)
	assert.Equal(t, "", tbl.stepInfos)
	assert.Equal(t, "", tbl.lastCompletedComponentID)
	assert.Equal(t, "", tbl.lastCompletedJobID)
	assert.Zero(t, tbl.lastCompletedStart)
	assert.Zero(t, tbl.lastCompletedEnd)
	assert.Equal(t, "", tbl.lastCompletedStepInfos)
	assert.Equal(t, "", tbl.lastCompletedMessage)
}

func setupCleanDB(t *testing.T) *sql.DB {
	var db, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", *user, *hostPort, *db))
	assert.Nil(t, err)
	// Clean
	db.Exec("DROP table status")
	return db
}
