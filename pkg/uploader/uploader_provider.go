/*
Copyright the Velero contributors.

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

package uploader

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository"
)

const restoreProgressCheckInterval = 10 * time.Second
const backupProgressCheckInterval = 10 * time.Second

type UploaderProvider interface {
	GetSnapshotID() (string, error)

	RunBackup(
		ctx context.Context,
		path string,
		tags map[string]string,
		parentSnapshot string,
		updateFunc func(velerov1api.PodVolumeOperationProgress, string)) (string, string, error)

	RunRestore(
		ctx context.Context,
		snapshotID string,
		volumePath string,
		updateFunc func(velerov1api.PodVolumeOperationProgress)) (string, string, error)

	GetTaskName() string

	Cancel()

	Close()

	Cancel()
}

func NewUploaderProvider(
	ctx context.Context,
	legacyUploader bool,
	repoIdentifier string,
	namespace string,
	backupStorageLocation *velerov1api.BackupStorageLocation,
	credentialsFileStore credentials.FileStore,
	repoKeySelector *v1.SecretKeySelector,
	kbClient kbclient.Client,
	configFile string,
	log logrus.FieldLogger,
	action string,
) (UploaderProvider, error) {
	err := ensureRepoConnect(ctx, namespace, kbClient, credentialsFileStore, log, legacyUploader, repoIdentifier, backupStorageLocation.Name)
	if err != nil {
		return nil, err
	}

	if legacyUploader {
		return NewResticUploaderProvider(repoIdentifier, backupStorageLocation, credentialsFileStore, repoKeySelector, log)
	} else {
		return NewKopiaUploaderProvider(ctx, repoIdentifier, credentialsFileStore, repoKeySelector, configFile, log, action)
	}
}

func ensureRepoConnect(
	ctx context.Context, namespace string, kbClient kbclient.Client,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger, legacyRepo bool, repoID string, bsl string) error {
	repoProv, err := repository.NewRepositoryProvider(ctx, namespace, kbClient, credentialFileStore, log, legacyRepo)
	if err != nil {
		return err
	}

	return repoProv.ConnectToRepo(repoID, bsl)
}
