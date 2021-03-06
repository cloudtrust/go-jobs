// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cloudtrust/go-jobs/actor (interfaces: IDGenerator)

// Package mock is a generated GoMock package.
package mock

import (
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// IDGenerator is a mock of IDGenerator interface
type IDGenerator struct {
	ctrl     *gomock.Controller
	recorder *IDGeneratorMockRecorder
}

// IDGeneratorMockRecorder is the mock recorder for IDGenerator
type IDGeneratorMockRecorder struct {
	mock *IDGenerator
}

// NewIDGenerator creates a new mock instance
func NewIDGenerator(ctrl *gomock.Controller) *IDGenerator {
	mock := &IDGenerator{ctrl: ctrl}
	mock.recorder = &IDGeneratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *IDGenerator) EXPECT() *IDGeneratorMockRecorder {
	return m.recorder
}

// NextID mocks base method
func (m *IDGenerator) NextID() string {
	ret := m.ctrl.Call(m, "NextID")
	ret0, _ := ret[0].(string)
	return ret0
}

// NextID indicates an expected call of NextID
func (mr *IDGeneratorMockRecorder) NextID() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NextID", reflect.TypeOf((*IDGenerator)(nil).NextID))
}
