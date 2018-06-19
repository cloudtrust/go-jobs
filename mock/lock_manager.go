// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cloudtrust/go-jobs (interfaces: LockManager)

// Package mock is a generated GoMock package.
package mock

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
	time "time"
)

// LockManager is a mock of LockManager interface
type LockManager struct {
	ctrl     *gomock.Controller
	recorder *LockManagerMockRecorder
}

// LockManagerMockRecorder is the mock recorder for LockManager
type LockManagerMockRecorder struct {
	mock *LockManager
}

// NewLockManager creates a new mock instance
func NewLockManager(ctrl *gomock.Controller) *LockManager {
	mock := &LockManager{ctrl: ctrl}
	mock.recorder = &LockManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *LockManager) EXPECT() *LockManagerMockRecorder {
	return m.recorder
}

// Disable mocks base method
func (m *LockManager) Disable(arg0, arg1 string) error {
	ret := m.ctrl.Call(m, "Disable", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Disable indicates an expected call of Disable
func (mr *LockManagerMockRecorder) Disable(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Disable", reflect.TypeOf((*LockManager)(nil).Disable), arg0, arg1)
}

// Enable mocks base method
func (m *LockManager) Enable(arg0, arg1 string) error {
	ret := m.ctrl.Call(m, "Enable", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Enable indicates an expected call of Enable
func (mr *LockManagerMockRecorder) Enable(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Enable", reflect.TypeOf((*LockManager)(nil).Enable), arg0, arg1)
}

// Lock mocks base method
func (m *LockManager) Lock(arg0, arg1, arg2, arg3 string, arg4 time.Duration) error {
	ret := m.ctrl.Call(m, "Lock", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// Lock indicates an expected call of Lock
func (mr *LockManagerMockRecorder) Lock(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lock", reflect.TypeOf((*LockManager)(nil).Lock), arg0, arg1, arg2, arg3, arg4)
}

// Unlock mocks base method
func (m *LockManager) Unlock(arg0, arg1, arg2, arg3 string) error {
	ret := m.ctrl.Call(m, "Unlock", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// Unlock indicates an expected call of Unlock
func (mr *LockManagerMockRecorder) Unlock(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unlock", reflect.TypeOf((*LockManager)(nil).Unlock), arg0, arg1, arg2, arg3)
}
