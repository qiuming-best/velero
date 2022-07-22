package mocks

import (
	"context"

	"github.com/sirupsen/logrus"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/util/command"
)

// FakeResticBackupExec represents an object that can run backups.
type FakeResticBackupExec struct{}

// RunBackup runs a Restic backup.
func (exec FakeResticBackupExec) RunBackup(ctx context.Context, cmd *command.Command, log logrus.FieldLogger, updateFn func(velerov1api.PodVolumeOperationProgress, string)) (string, string, error) {
	return "", "", nil
}

// GetSnapshotID gets the Restic snapshot ID.
func (exec FakeResticBackupExec) GetSnapshotID(cmd *command.Command) (string, error) {
	return "", nil
}
