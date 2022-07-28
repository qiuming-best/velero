/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kopiaup

import (
	"context"
	"errors"
	"testing"

	"github.com/kopia/kopia/snapshot"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	repomocks "github.com/vmware-tanzu/velero/pkg/repository/mocks"
	uploadermocks "github.com/vmware-tanzu/velero/pkg/uploader/upimpl/kopiaup/mocks"
)

type snapshotMockes struct {
	policyMock     *uploadermocks.Policy
	snapshotMock   *uploadermocks.Snapshot
	uploderMock    *uploadermocks.Uploader
	repoWriterMock *repomocks.RepositoryWriter
}

type mockArgs struct {
	methodName string
	returns    []interface{}
}

func InjectSnapshotFuncs() *snapshotMockes {
	s := &snapshotMockes{
		policyMock:     &uploadermocks.Policy{},
		snapshotMock:   &uploadermocks.Snapshot{},
		uploderMock:    &uploadermocks.Uploader{},
		repoWriterMock: &repomocks.RepositoryWriter{},
	}

	setPolicyFunc = s.policyMock.SetPolicy
	treeForSourceFunc = s.policyMock.TreeForSource
	applyRetentionPolicyFunc = s.policyMock.ApplyRetentionPolicy
	loadSnapshotFunc = s.snapshotMock.LoadSnapshot
	saveSnapshotFunc = s.snapshotMock.SaveSnapshot
	return s
}

func MockFuncs(s *snapshotMockes, args []mockArgs) {
	s.snapshotMock.On("LoadSnapshot", mock.Anything, mock.Anything, mock.Anything).Return(args[0].returns...)
	s.snapshotMock.On("SaveSnapshot", mock.Anything, mock.Anything, mock.Anything).Return(args[1].returns...)
	s.policyMock.On("TreeForSource", mock.Anything, mock.Anything, mock.Anything).Return(args[2].returns...)
	s.policyMock.On("ApplyRetentionPolicy", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(args[3].returns...)
	s.policyMock.On("SetPolicy", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(args[4].returns...)
	s.uploderMock.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(args[5].returns...)
	s.repoWriterMock.On("Flush", mock.Anything).Return(args[6].returns...)
}

func TestSnapshotSource(t *testing.T) {
	s := InjectSnapshotFuncs()
	ctx := context.TODO()
	sourceInfo := snapshot.SourceInfo{
		UserName: "testUserName",
		Host:     "testHost",
		Path:     "/var",
	}
	rootDir, err := getLocalFSEntry(ctx, sourceInfo.Path)
	assert.NoError(t, err)
	log := logrus.New()
	manifest := &snapshot.Manifest{
		ID:        "test",
		RootEntry: &snapshot.DirEntry{},
	}

	testCases := []struct {
		name     string
		args     []mockArgs
		notError bool
	}{
		{
			name: "regular test",
			args: []mockArgs{
				{methodName: "LoadSnapshot", returns: []interface{}{manifest, nil}},
				{methodName: "SaveSnapshot", returns: []interface{}{manifest.ID, nil}},
				{methodName: "TreeForSource", returns: []interface{}{nil, nil}},
				{methodName: "ApplyRetentionPolicy", returns: []interface{}{nil, nil}},
				{methodName: "SetPolicy", returns: []interface{}{nil}},
				{methodName: "Upload", returns: []interface{}{manifest, nil}},
				{methodName: "Flush", returns: []interface{}{nil}},
			},
			notError: true,
		},
		{
			name: "failed to load snapshot",
			args: []mockArgs{
				{methodName: "LoadSnapshot", returns: []interface{}{manifest, errors.New("failed to load snapshot")}},
				{methodName: "SaveSnapshot", returns: []interface{}{manifest.ID, nil}},
				{methodName: "TreeForSource", returns: []interface{}{nil, nil}},
				{methodName: "ApplyRetentionPolicy", returns: []interface{}{nil, nil}},
				{methodName: "SetPolicy", returns: []interface{}{nil}},
				{methodName: "Upload", returns: []interface{}{manifest, nil}},
				{methodName: "Flush", returns: []interface{}{nil}},
			},
			notError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			MockFuncs(s, tc.args)
			_, _, err = SnapshotSource(ctx, s.repoWriterMock, s.uploderMock, sourceInfo, rootDir, "/", log, "TestSnapshotSource")
			if tc.notError {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}

}
