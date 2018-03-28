package lock

import (
	"fmt"
)


type LocalLock struct {
	locked bool
}

func NewLocalLock() *LocalLock {
	return &LocalLock{
		locked: false,
	}
}

func (l *LocalLock) Lock() error {
	if l.locked == true {
		return fmt.Errorf("Already locked")
	}
	
	l.locked = true
	return nil
}

func (l *LocalLock) Unlock() error {
	l.locked = false
	return nil
}