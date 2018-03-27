package lock


type LocalLock struct {
	locked bool
}

func NewLocalLock() *LocalLock {
	return &LocalLock{
		locked: false,
	}
}

func (l *LocalLock) Lock() error {
	l.locked = true
	return nil
}

func (l *LocalLock) Unlock() error {
	l.locked = false
	return nil
}