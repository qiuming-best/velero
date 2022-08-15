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
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/restic"
	"github.com/vmware-tanzu/velero/pkg/util/exec"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

type resticProvider struct {
	repoIdentifier  string
	credentialsFile string
	caCertFile      string
	repoEnv         []string
	backupTags      map[string]string
	log             logrus.FieldLogger
	bsl             *velerov1api.BackupStorageLocation
}

func NewResticUploaderProvider(
	repoIdentifier string,
	bsl *velerov1api.BackupStorageLocation,
	credGetter *credentials.CredentialGetter,
	repoKeySelector *v1.SecretKeySelector,
	log logrus.FieldLogger,
) (Provider, error) {
	provider := resticProvider{
		repoIdentifier: repoIdentifier,
		log:            log,
		bsl:            bsl,
	}

	var err error

	provider.credentialsFile, err = credGetter.FromFile.Path(repoKeySelector)
	if err != nil {
		return nil, errors.Wrap(err, "error creating temp restic credentials file")
	}

	// if there's a caCert on the ObjectStorage, write it to disk so that it can be passed to restic
	if bsl.Spec.ObjectStorage != nil && bsl.Spec.ObjectStorage.CACert != nil {
		provider.caCertFile, err = restic.TempCACertFile(bsl.Spec.ObjectStorage.CACert, bsl.Name, filesystem.NewFileSystem())
		if err != nil {
			return nil, errors.Wrap(err, "error create temp cert file")
		}
	}

	provider.repoEnv, err = restic.CmdEnv(bsl, credGetter.FromFile)
	if err != nil {
		return nil, errors.Wrap(err, "error generating repository cmnd env")
	}

	return &provider, nil
}

//Not implement yet
func (rp *resticProvider) Cancel() {
}

func (rp *resticProvider) Close(ctx context.Context) {
	os.Remove(rp.credentialsFile)
	os.Remove(rp.caCertFile)
}

// RunBackup runs a `backup` command and watches the output to provide
// progress updates to the caller.
func (rp *resticProvider) RunBackup(
	ctx context.Context,
	path string,
	tags map[string]string,
	parentSnapshot string,
	updateFunc func(velerov1api.PodVolumeOperationProgress)) (string, error) {
	// buffers for copying command stdout/err output into
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	// create a channel to signal when to end the goroutine scanning for progress
	// updates
	quit := make(chan struct{})

	rp.backupTags = tags

	backupCmd := restic.BackupCommand(rp.repoIdentifier, rp.credentialsFile, path, tags)
	backupCmd.Env = rp.repoEnv
	backupCmd.CACertFile = rp.caCertFile

	if parentSnapshot != "" {
		backupCmd.ExtraFlags = append(backupCmd.ExtraFlags, fmt.Sprintf("--parent=%s", parentSnapshot))
	}

	// #4820: restrieve insecureSkipTLSVerify from BSL configuration for
	// AWS plugin. If nothing is return, that means insecureSkipTLSVerify
	// is not enable for Restic command.
	skipTLSRet := restic.GetInsecureSkipTLSVerifyFromBSL(rp.bsl, rp.log)
	if len(skipTLSRet) > 0 {
		backupCmd.ExtraFlags = append(backupCmd.ExtraFlags, skipTLSRet)
	}

	cmd := backupCmd.Cmd()
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	err := cmd.Start()
	if err != nil {
		rp.log.Errorf("failed to execute %v with stderr %v error %v", cmd, stderrBuf.String(), err)
		return "", err
	}

	go func() {
		ticker := time.NewTicker(backupProgressCheckInterval)
		for {
			select {
			case <-ticker.C:
				lastLine := restic.GetLastLine(stdoutBuf.Bytes())
				if len(lastLine) > 0 {
					stat, err := restic.DecodeBackupStatusLine(lastLine)
					if err != nil {
						rp.log.WithError(err).Errorf("error getting backup progress")
					}

					// if the line contains a non-empty bytes_done field, we can update the
					// caller with the progress
					if stat.BytesDone != 0 {
						updateFunc(velerov1api.PodVolumeOperationProgress{
							TotalBytes: stat.TotalBytes,
							BytesDone:  stat.BytesDone,
						})
					}
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	err = cmd.Wait()
	if err != nil {
		return "", errors.WithStack(fmt.Errorf("failed to wait execute %v with stderr %v error %v", cmd, stderrBuf.String(), err))
	}
	quit <- struct{}{}

	summary, err := restic.GetSummaryLine(stdoutBuf.Bytes())
	if err != nil {
		return "", errors.WithStack(fmt.Errorf("failed to get summary %v with error %v", stderrBuf.String(), err))
	}
	stat, err := restic.DecodeBackupStatusLine(summary)
	if err != nil {
		return "", errors.WithStack(fmt.Errorf("failed to decode summary %v with error %v", string(summary), err))
	}
	if stat.MessageType != "summary" {
		return "", errors.WithStack(fmt.Errorf("error getting backup summary: %s", string(summary)))
	}

	// update progress to 100%
	updateFunc(velerov1api.PodVolumeOperationProgress{
		TotalBytes: stat.TotalBytesProcessed,
		BytesDone:  stat.TotalBytesProcessed,
	})
	snapshotID, err := rp.GetSnapshotID()
	if err != nil {
		return "", errors.WithStack(fmt.Errorf("error getting snapshot id with error: %s", err))
	}
	rp.log.Debugf("Ran command=%s, stdout=%s, stderr=%s", cmd.String(), summary, stderrBuf.String())
	return snapshotID, nil
}

// GetSnapshotID runs provided 'snapshots' command to get the ID of a snapshot
// and an error if a unique snapshot cannot be identified.
func (rup *resticProvider) GetSnapshotID() (string, error) {
	cmd := restic.GetSnapshotCommand(rup.repoIdentifier, rup.credentialsFile, rup.backupTags)
	cmd.Env = rup.repoEnv
	cmd.CACertFile = rup.caCertFile

	stdout, stderr, err := exec.RunCommand(cmd.Cmd())
	if err != nil {
		return "", errors.Wrapf(err, "error running command, stderr=%s", stderr)
	}

	type snapshotID struct {
		ShortID string `json:"short_id"`
	}

	var snapshots []snapshotID
	if err := json.Unmarshal([]byte(stdout), &snapshots); err != nil {
		return "", errors.Wrap(err, "error unmarshalling snapshots result")
	}

	if len(snapshots) != 1 {
		return "", errors.Errorf("expected one matching snapshot, got %d", len(snapshots))
	}

	return snapshots[0].ShortID, nil
}

// RunRestore runs a `restore` command and monitors the volume size to
// provide progress updates to the caller.
func (rp *resticProvider) RunRestore(
	ctx context.Context,
	snapshotID string,
	volumePath string,
	updateFunc func(velerov1api.PodVolumeOperationProgress)) error {

	restoreCmd := restic.RestoreCommand(rp.repoIdentifier, rp.credentialsFile, snapshotID, volumePath)
	restoreCmd.Env = rp.repoEnv
	restoreCmd.CACertFile = rp.caCertFile

	// #4820: restrieve insecureSkipTLSVerify from BSL configuration for
	// AWS plugin. If nothing is return, that means insecureSkipTLSVerify
	// is not enable for Restic command.
	skipTLSRet := restic.GetInsecureSkipTLSVerifyFromBSL(rp.bsl, rp.log)
	if len(skipTLSRet) > 0 {
		restoreCmd.ExtraFlags = append(restoreCmd.ExtraFlags, skipTLSRet)
	}
	snapshotSize, err := restic.GetSnapshotSize(restoreCmd.RepoIdentifier, restoreCmd.PasswordFile, restoreCmd.CACertFile, restoreCmd.Args[0], restoreCmd.Env, restoreCmd.InsecureSkipTSLVerify)
	if err != nil {
		return errors.Wrap(err, "error getting snapshot size")
	}

	updateFunc(velerov1api.PodVolumeOperationProgress{
		TotalBytes: snapshotSize,
	})

	// create a channel to signal when to end the goroutine scanning for progress
	// updates
	quit := make(chan struct{})

	go func() {
		ticker := time.NewTicker(restoreProgressCheckInterval)
		for {
			select {
			case <-ticker.C:
				volumeSize, err := restic.GetVolumeSize(restoreCmd.Dir)
				if err != nil {
					rp.log.WithError(err).Errorf("error getting volume size for restore dir %v", restoreCmd.Dir)
					return
				}
				if volumeSize != 0 {
					updateFunc(velerov1api.PodVolumeOperationProgress{
						TotalBytes: snapshotSize,
						BytesDone:  volumeSize,
					})
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

	stdout, stderr, err := exec.RunCommand(restoreCmd.Cmd())
	quit <- struct{}{}
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to execute restore command %v with stderr %v", restoreCmd.Cmd().String(), stderr))
	}
	// update progress to 100%
	updateFunc(velerov1api.PodVolumeOperationProgress{
		TotalBytes: snapshotSize,
		BytesDone:  snapshotSize,
	})
	rp.log.Debugf("Ran command=%s, stdout=%s, stderr=%s", restoreCmd.Command, stdout, stderr)
	return err
}
