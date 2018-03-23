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

	var sts = &Table{
		componentName:         "cmp",
		componentID:           strconv.FormatUint(rand.Uint64(), 10),
		jobName:               "job",
		jobID:                 strconv.FormatUint(rand.Uint64(), 10),
		startTime:             refEpoch,
		stepInfos:             "",
		lastUpdate:            refEpoch,
		lastExecution:         refEpoch,
		lastExecutionStatus:   "",
		lastExecutionMessage:  "",
		lastExecutionDuration: 0 * time.Second,
		lastSuccess:           refEpoch,
	}

	var s = New(db, sts.componentName, sts.componentID, sts.jobName, sts.jobID)
	var stsTbl, err = s.GetStatus()
	assert.Nil(t, err)
	assert.Equal(t, sts, stsTbl)
}

func TestStart(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var sts = &Table{
		componentName:         "cmp",
		componentID:           strconv.FormatUint(rand.Uint64(), 10),
		jobName:               "job",
		jobID:                 strconv.FormatUint(rand.Uint64(), 10),
		startTime:             time.Time{},
		stepInfos:             "",
		lastUpdate:            time.Time{},
		lastExecution:         time.Time{},
		lastExecutionStatus:   "",
		lastExecutionMessage:  "",
		lastExecutionDuration: 0 * time.Second,
		lastSuccess:           time.Time{},
	}

	var s = New(db, sts.componentName, sts.componentID, sts.jobName, sts.jobID)

	// Get start time
	start, err := s.GetStartTime()
	assert.Nil(t, err)

	startUpdated, err := s.GetStartTime()
	assert.Nil(t, err)
	assert.Equal(t, start, startUpdated)

	// Update start time
	assert.Nil(t, s.Start())

	startUpdated, err = s.GetStartTime()
	assert.NotEqual(t, start, startUpdated)
}
func TestUpdate(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var sts = &Table{
		componentName:         "cmp",
		componentID:           strconv.FormatUint(rand.Uint64(), 10),
		jobName:               "job",
		jobID:                 strconv.FormatUint(rand.Uint64(), 10),
		startTime:             time.Time{},
		stepInfos:             "",
		lastUpdate:            time.Time{},
		lastExecution:         time.Time{},
		lastExecutionStatus:   "",
		lastExecutionMessage:  "",
		lastExecutionDuration: 0 * time.Second,
		lastSuccess:           time.Time{},
	}

	var s = New(db, sts.componentName, sts.componentID, sts.jobName, sts.jobID)

	// Get status table
	status, err := s.GetStatus()
	assert.Nil(t, err)
	assert.Zero(t, status.stepInfos)

	assert.Nil(t, s.Update(map[string]string{"key": "val"}))
	statusUpdated, err := s.GetStatus()
	assert.Nil(t, err)
	assert.Equal(t, "{\"key\":\"val\"}", statusUpdated.stepInfos)
}

func TestComplete(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var sts = &Table{
		componentName:         "cmp",
		componentID:           strconv.FormatUint(rand.Uint64(), 10),
		jobName:               "job",
		jobID:                 strconv.FormatUint(rand.Uint64(), 10),
		startTime:             time.Time{},
		stepInfos:             "",
		lastUpdate:            time.Time{},
		lastExecution:         time.Time{},
		lastExecutionStatus:   "",
		lastExecutionMessage:  "",
		lastExecutionDuration: 0 * time.Second,
		lastSuccess:           time.Time{},
	}

	var s = New(db, sts.componentName, sts.componentID, sts.jobName, sts.jobID)

	// Get status table
	status, err := s.GetStatus()
	assert.Nil(t, err)
	assert.Zero(t, status.stepInfos)

	assert.Nil(t, s.Complete(map[string]string{"key": "val"}, map[string]string{"msg": "message"}))
	statusUpdated, err := s.GetStatus()
	assert.Nil(t, err)
	assert.Equal(t, "{\"key\":\"val\"}", statusUpdated.stepInfos)
	assert.Equal(t, "{\"msg\":\"message\"}", statusUpdated.lastExecutionMessage)
}

func TestFail(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var sts = &Table{
		componentName:         "cmp",
		componentID:           strconv.FormatUint(rand.Uint64(), 10),
		jobName:               "job",
		jobID:                 strconv.FormatUint(rand.Uint64(), 10),
		startTime:             time.Time{},
		stepInfos:             "",
		lastUpdate:            time.Time{},
		lastExecution:         time.Time{},
		lastExecutionStatus:   "",
		lastExecutionMessage:  "",
		lastExecutionDuration: 0 * time.Second,
		lastSuccess:           time.Time{},
	}

	var s = New(db, sts.componentName, sts.componentID, sts.jobName, sts.jobID)

	// Get status table
	status, err := s.GetStatus()
	assert.Nil(t, err)
	assert.Zero(t, status.stepInfos)
	s.Start()
	time.Sleep(1 * time.Second)
	assert.Nil(t, s.Complete(map[string]string{"key": "val"}, map[string]string{"msg": "message"}))
	statusUpdated, err := s.GetStatus()
	assert.Nil(t, err)
	assert.Equal(t, "{\"key\":\"val\"}", statusUpdated.stepInfos)
	assert.Equal(t, "{\"msg\":\"message\"}", statusUpdated.lastExecutionMessage)
}

func setupCleanDB(t *testing.T) *sql.DB {
	var db, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", *user, *hostPort, *db))
	assert.Nil(t, err)
	// Clean
	db.Exec("DROP table status")
	return db
}