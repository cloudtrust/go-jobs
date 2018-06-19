package lock

import (
	"fmt"
	"sync"
)

// NewLocalLock returns a lock local, that is a lock that does not use the distributed Storage to assure
// that only one instance of the job is running in the cluster.
func NewLocalLock() *LocalLock {
	return &LocalLock{
		mutex:  &sync.Mutex{},
		locked: false,
	}
}

// LocalLock is the local lock
type LocalLock struct {
	mutex  *sync.Mutex
	locked bool
}

// Lock locks locally.
func (l *LocalLock) Lock() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.locked == true {
		return fmt.Errorf("already locked")
	}

	l.locked = true
	return nil
}

// Unlock unlocks locally.
func (l *LocalLock) Unlock() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.locked = false
	return nil
}
