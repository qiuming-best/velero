// Code generated by mockery v2.10.2. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	uploader "github.com/vmware-tanzu/velero/pkg/uploader"

	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

// RestorerFactory is an autogenerated mock type for the RestorerFactory type
type RestorerFactory struct {
	mock.Mock
}

// NewRestorer provides a mock function with given fields: _a0, _a1
func (_m *RestorerFactory) NewRestorer(_a0 context.Context, _a1 *v1.Restore) (uploader.Restorer, error) {
	ret := _m.Called(_a0, _a1)

	var r0 uploader.Restorer
	if rf, ok := ret.Get(0).(func(context.Context, *v1.Restore) uploader.Restorer); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(uploader.Restorer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *v1.Restore) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
