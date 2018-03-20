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
	hostPort = flag.String("hostport", "172.19.0.2:26257", "cockroach host:port")
	user     = flag.String("user", "job", "user name")
	db       = flag.String("db", "jobs", "database name")
)

func TestMain(m *testing.M) {
	flag.Parse()
	result := m.Run()
	os.Exit(result)
}

func TestNewLock(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}

	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)
	var lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
}

func TestEnable(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}

	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)
	// Initially enabled
	assert.True(t, l.IsEnabled())
	var lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	// Several calls to Enable have the same result.
	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	// Disable/enable
	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
}
func TestDisable(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}

	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)

	// Several calls to Enable have the same result.
	tbl.enabled = false

	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
	var lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)

	// Enable/disable
	assert.Nil(t, l.Enable())
	assert.True(t, l.IsEnabled())
	assert.Nil(t, l.Disable())
	assert.False(t, l.IsEnabled())
}

func TestLock(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}

	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)
	// Initially unlocked
	var lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.False(t, l.OwningLock())

	// Several calls to Lock have the same result.
	tbl.status = "LOCKED"
	assert.Nil(t, l.Lock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.True(t, l.OwningLock())

	assert.Nil(t, l.Lock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.True(t, l.OwningLock())

	assert.Nil(t, l.Lock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.True(t, l.OwningLock())

	// Unlock/Lock
	assert.Nil(t, l.Unlock())
	assert.False(t, l.OwningLock())
	assert.Nil(t, l.Lock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.True(t, l.OwningLock())
}

func TestLockDisabled(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}
	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)
	assert.Nil(t, l.Disable())
	assert.IsType(t, &Disabled{}, l.Lock())
}

func TestMultipleLock(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}
	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)
	var tbl2 = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}
	var l2 = New(db, tbl2.componentName, tbl2.componentID, tbl2.jobName, tbl2.jobID)

	// l has lock
	assert.Nil(t, l.Lock())
	assert.True(t, l.OwningLock())
	assert.IsType(t, &Unauthorised{}, l2.Lock())
	assert.False(t, l2.OwningLock())
	assert.True(t, l.OwningLock())
	assert.IsType(t, &Unauthorised{}, l2.Unlock())

	// l2 has lock
	assert.Nil(t, l.Unlock())
	assert.False(t, l.OwningLock())
	assert.Nil(t, l2.Lock())
	assert.True(t, l2.OwningLock())
	assert.False(t, l.OwningLock())
}

func TestUnlock(t *testing.T) {
	var db = setupCleanDB(t)
	rand.Seed(time.Now().UnixNano())

	var tbl = &table{
		componentName: "cmp",
		componentID:   strconv.FormatUint(rand.Uint64(), 10),
		jobName:       "job",
		jobID:         strconv.FormatUint(rand.Uint64(), 10),
		enabled:       true,
		status:        "UNLOCKED",
	}

	var l = New(db, tbl.componentName, tbl.componentID, tbl.jobName, tbl.jobID)
	// Initially unlocked
	var lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.False(t, l.OwningLock())

	// Several calls to Unlock have the same result.
	assert.Nil(t, l.Unlock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.False(t, l.OwningLock())

	assert.Nil(t, l.Unlock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.False(t, l.OwningLock())

	assert.Nil(t, l.Unlock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.False(t, l.OwningLock())

	// Lock/Unlock
	assert.Nil(t, l.Lock())
	assert.True(t, l.OwningLock())
	assert.Nil(t, l.Unlock())
	lockTbl, err = l.getLock()
	assert.Nil(t, err)
	assert.Equal(t, tbl, lockTbl)
	assert.False(t, l.OwningLock())
}

func setupCleanDB(t *testing.T) *sql.DB {
	var db, err = sql.Open("postgres", fmt.Sprintf("postgresql://%s@%s/%s?sslmode=disable", *user, *hostPort, *db))
	assert.Nil(t, err)
	// Clean
	db.Exec("DROP table locks")
	return db
}
