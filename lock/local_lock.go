package lock

import (
	"fmt"
	"sync"
)

type LocalLock struct {
	mutex  *sync.Mutex
	locked bool
}

func NewLocalLock() *LocalLock {
	return &LocalLock{
		mutex:  &sync.Mutex{},
		locked: false,
	}
}

func (l *LocalLock) Lock() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.locked == true {
		return fmt.Errorf("already locked")
	}

	l.locked = true
	return nil
}

func (l *LocalLock) Unlock() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.locked = false
	return nil
}
