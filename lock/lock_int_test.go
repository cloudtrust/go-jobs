// +build integration

package lock

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

var (
	hostPort = flag.String("hostport", "127.0.0.1:26258", "cockroach host:port")
	user     = flag.String("user", "cockroach8", "user name")
	db       = flag.String("db", "cockroach8", "database name")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestNewLock(t *testing.T) {
	var db = setupCleanDB(t)

	var c int

	// The table lock does not exist so the error is not nil.
	var row = db.QueryRow("SELECT count(*) from locks")
	var err = row.Scan(&c)
	assert.NotNil(t, err)

	New(db)

	// Check that New creates the 'locks' table in the DB.
	row = db.QueryRow("SELECT count(*) from locks")
	err = row.Scan(&c)
	assert.Nil(t, err)
	assert.Zero(t, c)
}

func TestEnable(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		jobName       = "job"
	)

	var l = New(db)

	// Several calls to Enable have the same result.
	for i := 0; i < 10; i++ {
		err := l.Enable(componentName, jobName)
		assert.Nil(t, err)

		assert.True(t, enabled(t, l, componentName, jobName))
	}
}

func TestDisable(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		jobName       = "job"
	)

	var l = New(db)

	// Several calls to Disable have the same result.
	for i := 0; i < 10; i++ {
		err := l.Disable(componentName, jobName)
		assert.Nil(t, err)

		assert.False(t, enabled(t, l, componentName, jobName))
	}
}

func TestEnableDisable(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		jobName       = "job"
	)

	var l = New(db)

	// Enable
	err := l.Enable(componentName, jobName)
	assert.Nil(t, err)

	assert.True(t, enabled(t, l, componentName, jobName))

	// Disable
	err = l.Disable(componentName, jobName)
	assert.Nil(t, err)

	assert.False(t, enabled(t, l, componentName, jobName))
}

func TestEnableDisableMultipleLocks(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		c1ID          = strconv.FormatUint(rand.Uint64(), 10)
		c2ID          = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		j1ID          = strconv.FormatUint(rand.Uint64(), 10)
		j2ID          = strconv.FormatUint(rand.Uint64(), 10)
		d             = 1 * time.Second
	)

	var l1 = New(db)
	var l2 = New(db)

	// Job is enabled (l1 can lock it).
	assert.Nil(t, l1.Lock(componentName, c1ID, jobName, j1ID, d))
	assert.Nil(t, l1.Unlock(componentName, c1ID, jobName, j1ID))

	// Disable job.
	assert.Nil(t, l1.Disable(componentName, jobName))
	assert.IsType(t, &ErrDisabled{}, l1.Lock(componentName, c1ID, jobName, j1ID, d))
	assert.IsType(t, &ErrDisabled{}, l2.Lock(componentName, c1ID, jobName, j1ID, d))

	// Enable job.
	assert.Nil(t, l2.Enable(componentName, jobName))
	assert.Nil(t, l1.Lock(componentName, c1ID, jobName, j1ID, d))
	assert.Nil(t, l1.Unlock(componentName, c1ID, jobName, j1ID))
	assert.Nil(t, l2.Lock(componentName, c2ID, jobName, j2ID, d))
	assert.Nil(t, l2.Unlock(componentName, c2ID, jobName, j2ID))
}

func TestLock(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
		d             = 1 * time.Second
	)

	var l = New(db)

	var oldlockTime = time.Time{}
	// Several calls to Lock have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Lock(componentName, componentID, jobName, jobID, d))
		var tbl, err = l.getTable(componentName, jobName)
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, componentID, tbl.componentID)
		assert.Equal(t, jobName, tbl.jobName)
		assert.Equal(t, jobID, tbl.jobID)
		assert.True(t, tbl.enabled)
		assert.Equal(t, "LOCKED", tbl.status)
		assert.True(t, tbl.lockTime.After(oldlockTime))
	}
}

func TestUnlock(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var l = New(db)

	// Several calls to Unlock have the same result.
	for i := 0; i < 10; i++ {
		assert.Nil(t, l.Unlock(componentName, componentID, jobName, jobID))
		var tbl, err = l.getTable(componentName, jobName)
		assert.Nil(t, err)
		assert.Equal(t, componentName, tbl.componentName)
		assert.Equal(t, jobName, tbl.jobName)
		assert.True(t, tbl.enabled)
		assert.Equal(t, "UNLOCKED", tbl.status)
	}
}

func TestLockWhenDisabled(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		componentID   = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		jobID         = strconv.FormatUint(rand.Uint64(), 10)
	)

	var l = New(db)

	assert.Nil(t, l.Disable(componentName, jobName))
	var err = l.Lock(componentName, componentID, jobName, jobID, 1*time.Hour)
	assert.IsType(t, &ErrDisabled{}, err)
}

func TestLockWithMultipleComponents(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		c1ID          = strconv.FormatUint(rand.Uint64(), 10)
		c2ID          = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		j1ID          = strconv.FormatUint(rand.Uint64(), 10)
		j2ID          = strconv.FormatUint(rand.Uint64(), 10)
		d             = 1 * time.Second
	)

	var l1 = New(db)
	var l2 = New(db)

	// l1 locks.
	assert.Nil(t, l1.Lock(componentName, c1ID, jobName, j1ID, d))
	assert.IsType(t, &ErrUnauthorised{}, l2.Lock(componentName, c2ID, jobName, j2ID, d))
	assert.IsType(t, &ErrUnauthorised{}, l2.Unlock(componentName, c2ID, jobName, j2ID))

	// l1 unlocks
	assert.Nil(t, l1.Unlock(componentName, c1ID, jobName, j1ID))

	// l2 locks.
	assert.Nil(t, l2.Lock(componentName, c2ID, jobName, j2ID, d))
	assert.IsType(t, &ErrUnauthorised{}, l1.Lock(componentName, c1ID, jobName, j1ID, d))
	assert.IsType(t, &ErrUnauthorised{}, l1.Unlock(componentName, c1ID, jobName, j1ID))
}

func TestForceLock(t *testing.T) {
	var db = setupCleanDB(t)

	var (
		componentName = "cmp"
		c1ID          = strconv.FormatUint(rand.Uint64(), 10)
		c2ID          = strconv.FormatUint(rand.Uint64(), 10)
		jobName       = "job"
		j1ID          = strconv.FormatUint(rand.Uint64(), 10)
		j2ID          = strconv.FormatUint(rand.Uint64(), 10)
		d             = 1 * time.Second
	)

	var l1 = New(db)
	var l2 = New(db)

	// l1 locks.
	assert.Nil(t, l1.Lock(componentName, c1ID, jobName, j1ID, d))

	// l2 try locking, it gets an error because l1 has the lock.
	assert.IsType(t, &ErrUnauthorised{}, l2.Lock(componentName, c2ID, jobName, j2ID, d))

	// l2 locks after the lock max duration exceed.
	time.Sleep(d)
	assert.Nil(t, l2.Lock(componentName, c2ID, jobName, j2ID, d))
}

func TestNoopLocker(t *testing.T) {
	var (
		componentName  = "cmp"
		componentID    = strconv.FormatUint(rand.Uint64(), 10)
		jobName        = "job"
		jobID          = strconv.FormatUint(rand.Uint64(), 10)
		jobMaxDuration = 1 * time.Second
	)

	var l = &NoopLocker{}
	assert.Nil(t, l.Lock(componentName, componentID, jobName, jobID, jobMaxDuration))
	assert.Nil(t, l.Unlock(componentName, componentID, jobName, jobID))
	assert.Nil(t, l.Enable(componentName, jobName))
	assert.Nil(t, l.Disable(componentName, jobName))
}

func TestLocalLock(t *testing.T) {
	var l = NewLocalLock()

	assert.Nil(t, l.Lock())
	assert.NotNil(t, l.Lock())
	assert.Nil(t, l.Unlock())
	assert.Nil(t, l.Unlock())
}

func enabled(t *testing.T, l *Lock, componentName, jobName string) bool {
	tbl, err := l.getTable(componentName, jobName)
	assert.Nil(t, err)
	assert.Equal(t, componentName, tbl.componentName)
	assert.Equal(t, jobName, tbl.jobName)
	return tbl.enabled
}

func setupCleanDB(t *testing.T) *sql.DB {
	flag.Parse()
	var db, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", *user, *hostPort, *db))
	assert.Nil(t, err)
	// Clean
	db.Exec("DROP table locks")
	return db
}
