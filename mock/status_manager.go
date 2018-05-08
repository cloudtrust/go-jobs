// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cloudtrust/go-jobs (interfaces: StatusManager)

// Package mock is a generated GoMock package.
package mock

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// StatusManager is a mock of StatusManager interface
type StatusManager struct {
	ctrl     *gomock.Controller
	recorder *StatusManagerMockRecorder
}

// StatusManagerMockRecorder is the mock recorder for StatusManager
type StatusManagerMockRecorder struct {
	mock *StatusManager
}

// NewStatusManager creates a new mock instance
func NewStatusManager(ctrl *gomock.Controller) *StatusManager {
	mock := &StatusManager{ctrl: ctrl}
	mock.recorder = &StatusManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *StatusManager) EXPECT() *StatusManagerMockRecorder {
	return m.recorder
}

// Complete mocks base method
func (m *StatusManager) Complete(arg0, arg1, arg2, arg3 string, arg4, arg5 map[string]string) error {
	ret := m.ctrl.Call(m, "Complete", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(error)
	return ret0
}

// Complete indicates an expected call of Complete
func (mr *StatusManagerMockRecorder) Complete(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Complete", reflect.TypeOf((*StatusManager)(nil).Complete), arg0, arg1, arg2, arg3, arg4, arg5)
}

// Fail mocks base method
func (m *StatusManager) Fail(arg0, arg1, arg2, arg3 string, arg4, arg5 map[string]string) error {
	ret := m.ctrl.Call(m, "Fail", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(error)
	return ret0
}

// Fail indicates an expected call of Fail
func (mr *StatusManagerMockRecorder) Fail(arg0, arg1, arg2, arg3, arg4, arg5 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Fail", reflect.TypeOf((*StatusManager)(nil).Fail), arg0, arg1, arg2, arg3, arg4, arg5)
}

// Register mocks base method
func (m *StatusManager) Register(arg0, arg1, arg2, arg3 string) {
	m.ctrl.Call(m, "Register", arg0, arg1, arg2, arg3)
}

// Register indicates an expected call of Register
func (mr *StatusManagerMockRecorder) Register(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Register", reflect.TypeOf((*StatusManager)(nil).Register), arg0, arg1, arg2, arg3)
}

// Start mocks base method
func (m *StatusManager) Start(arg0, arg1, arg2 string) error {
	ret := m.ctrl.Call(m, "Start", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start
func (mr *StatusManagerMockRecorder) Start(arg0, arg1, arg2 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*StatusManager)(nil).Start), arg0, arg1, arg2)
}

// Update mocks base method
func (m *StatusManager) Update(arg0, arg1, arg2 string, arg3 map[string]string) error {
	ret := m.ctrl.Call(m, "Update", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// Update indicates an expected call of Update
func (mr *StatusManagerMockRecorder) Update(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*StatusManager)(nil).Update), arg0, arg1, arg2, arg3)
}