package lock

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

func TestNewLock(t *testing.T) {
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

	var l = New(db, componentName, componentID, jobName, jobID, 1*time.Hour)
	var tbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.True(t, tbl.enabled)
	assert.Equal(t, "UNLOCKED", tbl.status)
	assert.Zero(t, tbl.lockTime)
}

func TestEnable(t *testing.T) {
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

	var l = New(db, componentName, componentID, jobName, jobID, 1*time.Hour)

	// Initially enabled.
	assert.True(t, l.IsEnabled())
	var tbl, err = l.getLock()
	assert.Nil(t, err)
	assert.True(t, tbl.enabled)

	// Several calls to Enable have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Enable())
		assert.True(t, l.IsEnabled())
		tbl, err = l.getLock()
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, componentID, tbl.componentID)
		assert.Equal(t, jobName, tbl.jobName)
		assert.Equal(t, jobID, tbl.jobID)
		assert.True(t, tbl.enabled)
		assert.Equal(t, "UNLOCKED", tbl.status)
		assert.Zero(t, tbl.lockTime)
	}

	// Disable/enable
	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
}

func TestDisable(t *testing.T) {
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

	var l = New(db, componentName, componentID, jobName, jobID, 1*time.Hour)

	// Several calls to Disable have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Disable())
		assert.False(t, l.IsEnabled())
		var tbl, err = l.getLock()
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, componentID, tbl.componentID)
		assert.Equal(t, jobName, tbl.jobName)
		assert.Equal(t, jobID, tbl.jobID)
		assert.False(t, tbl.enabled)
		assert.Equal(t, "UNLOCKED", tbl.status)
		assert.Zero(t, tbl.lockTime)
	}

	// Enable/Disable.
	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
}

func TestEnableDisableMultipleLocks(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID1  = strconv.FormatUint(rand.Uint64(), 10)
		componentID2  = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID1        = strconv.FormatUint(rand.Uint64(), 10)
		jobID2        = strconv.FormatUint(rand.Uint64(), 10)
	)

	var l1 = New(db, componentName, componentID1, jobName, jobID1, 1*time.Hour)
	var l2 = New(db, componentName, componentID2, jobName, jobID2, 1*time.Hour)

	// Initially enabled.
	assert.True(t, l1.IsEnabled())
	assert.True(t, l2.IsEnabled())

	// Component 1 disable lock.
	assert.Nil(t, l1.Disable())
	assert.False(t, l1.IsEnabled())
	assert.False(t, l2.IsEnabled())

	// Component 2 disable lock.
	assert.Nil(t, l2.Disable())
	assert.False(t, l1.IsEnabled())
	assert.False(t, l2.IsEnabled())

	// Component 1 enable lock.
	assert.Nil(t, l1.Enable())
	assert.True(t, l1.IsEnabled())
	assert.True(t, l2.IsEnabled())
}

func TestLock(t *testing.T) {
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

	var l = New(db, componentName, componentID, jobName, jobID, 1*time.Hour)

	// Initially unlocked
	var tbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.True(t, tbl.enabled)
	assert.Equal(t, "UNLOCKED", tbl.status)
	assert.Equal(t, tbl.lockTime, time.Time{})
	assert.False(t, l.OwningLock())

	var oldlockTime = tbl.lockTime
	// Several calls to Lock have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Lock())
		assert.True(t, l.OwningLock())
		var tbl, err = l.getLock()
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, componentID, tbl.componentID)
		assert.Equal(t, jobName, tbl.jobName)
		assert.Equal(t, jobID, tbl.jobID)
		assert.True(t, tbl.enabled)
		assert.Equal(t, "LOCKED", tbl.status)
		assert.True(t, tbl.lockTime.After(oldlockTime))
	}

	// Unlock/Lock.
	assert.Nil(t, l.Unlock())
	assert.False(t, l.OwningLock())
	assert.Nil(t, l.Lock())
	assert.True(t, l.OwningLock())
}

func TestUnlock(t *testing.T) {
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

	var l = New(db, componentName, componentID, jobName, jobID, 1*time.Hour)

	// Several calls to Unlock have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Unlock())
		assert.False(t, l.OwningLock())
		var tbl, err = l.getLock()
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, componentID, tbl.componentID)
		assert.Equal(t, jobName, tbl.jobName)
		assert.Equal(t, jobID, tbl.jobID)
		assert.True(t, tbl.enabled)
		assert.Equal(t, "UNLOCKED", tbl.status)
	}

	// Lock/Unlock.
	assert.Nil(t, l.Lock())
	assert.True(t, l.OwningLock())
	assert.Nil(t, l.Unlock())
	assert.False(t, l.OwningLock())
}

func TestLockWhenDisabled(t *testing.T) {
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

	var l = New(db, componentName, componentID, jobName, jobID, 1*time.Hour)
	assert.Nil(t, l.Disable())
	var err = l.Lock()
	assert.IsType(t, &ErrDisabled{}, err)
}

func TestLockWithMultipleComponents(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID1  = strconv.FormatUint(rand.Uint64(), 10)
		componentID2  = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID1        = strconv.FormatUint(rand.Uint64(), 10)
		jobID2        = strconv.FormatUint(rand.Uint64(), 10)
	)

	var l1 = New(db, componentName, componentID1, jobName, jobID1, 1*time.Hour)
	var l2 = New(db, componentName, componentID2, jobName, jobID2, 1*time.Hour)

	// l1 locks.
	assert.Nil(t, l1.Lock())
	assert.True(t, l1.OwningLock())
	assert.False(t, l2.OwningLock())
	assert.IsType(t, &ErrUnauthorised{}, l2.Lock())
	assert.IsType(t, &ErrUnauthorised{}, l2.Unlock())

	// l1 unlocks.
	assert.Nil(t, l1.Unlock())
	assert.False(t, l1.OwningLock())
	assert.False(t, l2.OwningLock())

	// l2 locks.
	assert.Nil(t, l2.Lock())
	assert.True(t, l2.OwningLock())
	assert.False(t, l1.OwningLock())
	assert.IsType(t, &ErrUnauthorised{}, l1.Lock())
	assert.IsType(t, &ErrUnauthorised{}, l1.Unlock())
	assert.Nil(t, l2.Unlock())
}

func TestForceLock(t *testing.T) {
	if !*integration {
		t.Skip()
	}
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID1  = strconv.FormatUint(rand.Uint64(), 10)
		componentID2  = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID1        = strconv.FormatUint(rand.Uint64(), 10)
		jobID2        = strconv.FormatUint(rand.Uint64(), 10)
	)

	var maxDuration = 500 * time.Millisecond
	var l1 = New(db, componentName, componentID1, jobName, jobID1, maxDuration)
	var l2 = New(db, componentName, componentID2, jobName, jobID2, maxDuration)

	// L1 has lock.
	assert.Nil(t, l1.Lock())
	assert.IsType(t, &ErrUnauthorised{}, l2.Lock())
	assert.True(t, l1.OwningLock())
	assert.False(t, l2.OwningLock())

	// Wait till the job max duration exceed.
	time.Sleep(maxDuration)

	// L1 still has the lock, but because the maxDuration exceeded, L2 can take the lock by force.
	assert.True(t, l1.OwningLock())
	assert.False(t, l2.OwningLock())
	assert.Nil(t, l2.Lock())
	assert.False(t, l1.OwningLock())
	assert.True(t, l2.OwningLock())
}

func setupCleanDB(t *testing.T) *sql.DB {
	var db, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", *user, *hostPort, *db))
	assert.Nil(t, err)
	// Clean
	db.Exec("DROP table locks")
	return db
}
