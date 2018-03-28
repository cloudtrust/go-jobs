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
	hostPort    = flag.String("hostport", "127.0.0.1:26257", "cockroach host:port")
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
	// if !*integration {
	// 	t.Skip()
	// }
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var l = New(db)
	l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)
	l.Unlock(componentName, componentID, jobName, jobID)

	var tbl, err = l.getLock(componentName, jobName)
	assert.Nil(t, err)
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.True(t, tbl.enabled)
	assert.Equal(t, "UNLOCKED", tbl.status)
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

	var l = New(db)
	l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)

	// Initially enabled.
	assert.True(t, l.IsEnabled(componentName, jobName))
	var tbl, err = l.getLock(componentName, jobName)
	assert.Nil(t, err)
	assert.True(t, tbl.enabled)

	// Several calls to Enable have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Enable(componentName, jobName))
		assert.True(t, l.IsEnabled(componentName, jobName))
		tbl, err = l.getLock(componentName, jobName)
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
	assert.Nil(t, l.Disable(componentName, jobName))
	assert.False(t, l.IsEnabled(componentName, jobName))
	assert.Nil(t, l.Enable(componentName, jobName))
	assert.True(t, l.IsEnabled(componentName, jobName))
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

	var l = New(db)
	l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)

	// Several calls to Disable have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Disable(componentName, jobName))
		assert.False(t, l.IsEnabled(componentName, jobName))
		var tbl, err = l.getLock(componentName, jobName)
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
	assert.Nil(t, l.Enable(componentName, jobName))
	assert.True(t, l.IsEnabled(componentName, jobName))
	assert.Nil(t, l.Disable(componentName, jobName))
	assert.False(t, l.IsEnabled(componentName, jobName))
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

	var l1 = New(db)
	l1.Lock(componentName, componentID1, jobName, jobID1, 1*time.Hour)
	var l2 = New(db)
	l2.Lock(componentName, componentID2, jobName, jobID2, 1*time.Hour)

	// Initially enabled.
	assert.True(t, l1.IsEnabled(componentName, jobName))
	assert.True(t, l2.IsEnabled(componentName, jobName))

	// Component 1 disable lock.
	assert.Nil(t, l1.Disable(componentName, jobName))
	assert.False(t, l1.IsEnabled(componentName, jobName))
	assert.False(t, l2.IsEnabled(componentName, jobName))

	// Component 2 disable lock.
	assert.Nil(t, l2.Disable(componentName, jobName))
	assert.False(t, l1.IsEnabled(componentName, jobName))
	assert.False(t, l2.IsEnabled(componentName, jobName))

	// Component 1 enable lock.
	assert.Nil(t, l1.Enable(componentName, jobName))
	assert.True(t, l1.IsEnabled(componentName, jobName))
	assert.True(t, l2.IsEnabled(componentName, jobName))
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

	var l = New(db)
	l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)

	// Initially unlocked
	var tbl, err = l.getLock(componentName, jobName)
	assert.Nil(t, err)
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, componentID, tbl.componentID)
	assert.Equal(t, jobName, tbl.jobName)
	assert.Equal(t, jobID, tbl.jobID)
	assert.True(t, tbl.enabled)
	assert.Equal(t, "UNLOCKED", tbl.status)
	assert.Equal(t, tbl.lockTime, time.Time{})
	assert.False(t, l.OwningLock(componentName, componentID, jobName, jobID))

	var oldlockTime = tbl.lockTime
	// Several calls to Lock have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour))
		assert.True(t, l.OwningLock(componentName, componentID, jobName, jobID))
		var tbl, err = l.getLock(componentName, jobName)
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
	assert.Nil(t, l.Unlock(componentName, componentID, jobName, jobID))
	assert.False(t, l.OwningLock(componentName, componentID, jobName, jobID))
	assert.Nil(t, l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour))
	assert.True(t, l.OwningLock(componentName, componentID, jobName, jobID))
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

	var l = New(db)
	l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)

	// Several calls to Unlock have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Unlock(componentName, componentID, jobName, jobID))
		assert.False(t, l.OwningLock(componentName, componentID, jobName, jobID))
		var tbl, err = l.getLock(componentName, jobName)
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, componentID, tbl.componentID)
		assert.Equal(t, jobName, tbl.jobName)
		assert.Equal(t, jobID, tbl.jobID)
		assert.True(t, tbl.enabled)
		assert.Equal(t, "UNLOCKED", tbl.status)
	}

	// Lock/Unlock.
	assert.Nil(t, l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour))
	assert.True(t, l.OwningLock(componentName, componentID, jobName, jobID))
	assert.Nil(t, l.Unlock(componentName, componentID, jobName, jobID))
	assert.False(t, l.OwningLock(componentName, componentID, jobName, jobID))
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

	var l = New(db)
	l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)

	assert.Nil(t, l.Disable(componentName, jobName))
	var err = l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)
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

	var l1 = New(db)
	l1.Lock(componentName, componentID1, jobName, jobID1, 1*time.Hour)
	var l2 = New(db)
	l2.Lock(componentName, componentID2, jobName, jobID2, 1*time.Hour)

	// l1 locks.
	assert.Nil(t, l1.Lock(componentName, componentID1, jobName, jobID1, 1*time.Hour))
	assert.True(t, l1.OwningLock(componentName, componentID1, jobName, jobID1))
	assert.False(t, l2.OwningLock(componentName, componentID2, jobName, jobID2))
	assert.IsType(t, &ErrUnauthorised{}, l2.Lock(componentName, componentID2, jobName, jobID2, 1*time.Hour))
	assert.IsType(t, &ErrUnauthorised{}, l2.Unlock(componentName, componentID2, jobName, jobID2))

	// l1 unlocks.
	assert.Nil(t, l1.Unlock(componentName, componentID1, jobName, jobID1))
	assert.False(t, l1.OwningLock(componentName, componentID1, jobName, jobID1))
	assert.False(t, l2.OwningLock(componentName, componentID2, jobName, jobID2))

	// l2 locks.
	assert.Nil(t, l2.Lock(componentName, componentID2, jobName, jobID2, 1*time.Hour))
	assert.True(t, l2.OwningLock(componentName, componentID2, jobName, jobID2))
	assert.False(t, l1.OwningLock(componentName, componentID1, jobName, jobID1))
	assert.IsType(t, &ErrUnauthorised{}, l1.Lock(componentName, componentID1, jobName, jobID1, 1*time.Hour))
	assert.IsType(t, &ErrUnauthorised{}, l1.Unlock(componentName, componentID1, jobName, jobID1))
	assert.Nil(t, l2.Unlock(componentName, componentID2, jobName, jobID2))
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
	var l1 = New(db)
	l1.Lock(componentName, componentID1, jobName, jobID1, maxDuration)
	var l2 = New(db)
	l2.Lock(componentName, componentID2, jobName, jobID2, maxDuration)

	// L1 has lock.
	assert.Nil(t, l1.Lock(componentName, componentID1, jobName, jobID1, maxDuration))
	assert.IsType(t, &ErrUnauthorised{}, l2.Lock(componentName, componentID2, jobName, jobID2, maxDuration))
	assert.True(t, l1.OwningLock(componentName, componentID1, jobName, jobID1))
	assert.False(t, l2.OwningLock(componentName, componentID2, jobName, jobID2))

	// Wait till the job max duration exceed.
	time.Sleep(maxDuration)

	// L1 still has the lock, but because the maxDuration exceeded, L2 can take the lock by force.
	assert.True(t, l1.OwningLock(componentName, componentID1, jobName, jobID1))
	assert.False(t, l2.OwningLock(componentName, componentID2, jobName, jobID2))
	assert.Nil(t, l2.Lock(componentName, componentID2, jobName, jobID2, maxDuration))
	assert.False(t, l1.OwningLock(componentName, componentID1, jobName, jobID1))
	assert.True(t, l2.OwningLock(componentName, componentID2, jobName, jobID2))
}

func setupCleanDB(t *testing.T) *sql.DB {
	var db, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", *user, *hostPort, *db))
	assert.Nil(t, err)
	// Clean
	db.Exec("DROP table locks")
	return db
}
