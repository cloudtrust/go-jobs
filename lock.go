package job

import "database/sql"

type Lock struct{}
type StatusModule struct{}

func New(db *sql.DB, componentName, componentID, jobName, jobID string) *Lock {
	return nil
}

func (l *Lock) OwningLock() bool {
	return true
}

func (l *Lock) Lock() error {
	return nil
}

func (l *Lock) Disable() error {
	return nil
}

func (l *Lock) Enable() error {
	return nil
}

func NewStatusModule() *StatusModule {
	return nil
}
func (s *StatusModule) Update(stepState map[string]string) {

}

func (s *StatusModule) Finish(stepState, message map[string]string) {

}

func (s *StatusModule) Cancel(stepState, message map[string]string) {

}
