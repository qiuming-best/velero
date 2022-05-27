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

package repository

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/cache"

	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov1client "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned/typed/velero/v1"
	velerov1informers "github.com/vmware-tanzu/velero/pkg/generated/informers/externalversions/velero/v1"
)

const (
	// DefaultMaintenanceFrequency is the default time interval
	// at which restic prune is run.
	DefaultMaintenanceFrequency      = 7 * 24 * time.Hour
	DefaultQuickMaintenanceFrequency = 24 * time.Hour
)

// RepositoryManager manage repositories.
type RepositoryManager interface {
	// InitRepo initializes a repo with the specified name and identifier.
	InitRepo(repo *velerov1api.ResticRepository) error

	// ConnectToRepo tries to open the target repo,and returns an error
	// if it fails. This is intended to be used to ensure that the repo
	// exists/can be authenticated to.
	ConnectToRepo(repo *velerov1api.ResticRepository) error

	// PruneRepo deletes unused data and metadata from a repo.
	PruneRepo(repo *velerov1api.ResticRepository) error

	// PruneRepoQuick deletes unused data from a repo.
	PruneRepoQuick(repo *velerov1api.ResticRepository) error

	// EnsureUnlockRepo removes stale locks from a repo.
	EnsureUnlockRepo(repo *velerov1api.ResticRepository) error

	// LockRepoShared accquires a shared lock to a repo.
	LockRepoShared(name string)

	// UnlockRepoShared releases a shared lock to a repo.
	UnlockRepoShared(name string)

	// LockRepo accquires an exclusive lock to a repo.
	LockRepoExclusive(name string)

	// LockRepo releases an exclusive lock to a repo.
	UnlockRepoExclusive(name string)

	//EnsureRepo initialize the repo instance
	EnsureRepo(ctx context.Context, namespace, volumeNamespace, backupLocation string) (string, string, error)

	//GetRepoInformerSyncFlag return whether the repo informer has been synced
	GetRepoInformerSynced() cache.InformerSynced

	Forget(ctx context.Context, snapshotID, volumeNamespace, backupLocation string) error
}

type RepositoryProvider interface {
	InitRepo(repoID string, bsl string) error

	ConnectToRepo(repoID string, bsl string) error

	PruneRepo(repoID string, bsl string) error

	PruneRepoQuick(repoID string, bsl string) error

	EnsureUnlockRepo(repoID string, bsl string) error

	Forget(repoID, snapshotID, backupLocation string) error
}

type repositoryManager struct {
	namespace          string
	repoInformerSynced cache.InformerSynced
	kbClient           kbclient.Client
	log                logrus.FieldLogger
	repoLocker         *repoLocker
	repoEnsurer        *repositoryEnsurer
	ctx                context.Context

	repoProv RepositoryProvider
}

func NewRepositoryProvider(
	ctx context.Context,
	namespace string,
	kbClient kbclient.Client,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger,
	legacyRepo bool) (RepositoryProvider, error) {
	if legacyRepo {
		return NewResticRepoProvider(ctx, namespace, kbClient, credentialFileStore, log)
	} else {
		return NewUnifiedRepoProvider(ctx, namespace, kbClient, credentialFileStore, log)
	}
}

// NewRepositoryManager constructs a RepositoryManager.
func NewRepositoryManager(
	ctx context.Context,
	namespace string,
	repoInformer velerov1informers.ResticRepositoryInformer,
	repoClient velerov1client.ResticRepositoriesGetter,
	kbClient kbclient.Client,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger,
	legacyRepo bool,
) (RepositoryManager, error) {
	rm := &repositoryManager{
		namespace:          namespace,
		repoInformerSynced: repoInformer.Informer().HasSynced,
		kbClient:           kbClient,
		log:                log,
		ctx:                ctx,

		repoLocker:  newRepoLocker(),
		repoEnsurer: newRepositoryEnsurer(repoInformer, repoClient, log),
	}

	var err error
	rm.repoProv, err = NewRepositoryProvider(ctx, namespace, kbClient, credentialFileStore, log, legacyRepo)
	if err != nil {
		return nil, err
	}

	return rm, nil
}

func (rm *repositoryManager) InitRepo(repo *velerov1api.ResticRepository) error {
	rm.log.Debugln("Init repo %s", repo.Spec.ResticIdentifier)

	// Accquire an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.repoProv.InitRepo(repo.Spec.ResticIdentifier, repo.Spec.BackupStorageLocation)
}

func (rm *repositoryManager) ConnectToRepo(repo *velerov1api.ResticRepository) error {
	rm.log.Debugln("Connect to repo %s", repo.Spec.ResticIdentifier)

	// Accquire a non-exclusive lock
	rm.repoLocker.Lock(repo.Name)
	defer rm.repoLocker.Unlock(repo.Name)

	return rm.repoProv.ConnectToRepo(repo.Spec.ResticIdentifier, repo.Spec.BackupStorageLocation)
}

func (rm *repositoryManager) PruneRepo(repo *velerov1api.ResticRepository) error {
	// Accquire an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.repoProv.PruneRepo(repo.Spec.ResticIdentifier, repo.Spec.BackupStorageLocation)
}

func (rm *repositoryManager) PruneRepoQuick(repo *velerov1api.ResticRepository) error {
	// Accquire an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.repoProv.PruneRepoQuick(repo.Spec.ResticIdentifier, repo.Spec.BackupStorageLocation)
}

func (rm *repositoryManager) EnsureUnlockRepo(repo *velerov1api.ResticRepository) error {
	// Accquire a non-exclusive lock
	rm.repoLocker.Lock(repo.Name)
	defer rm.repoLocker.Unlock(repo.Name)

	return rm.repoProv.EnsureUnlockRepo(repo.Spec.ResticIdentifier, repo.Spec.BackupStorageLocation)
}

func (rm *repositoryManager) LockRepoShared(name string) {
	rm.repoLocker.Lock(name)
}

func (rm *repositoryManager) UnlockRepoShared(name string) {
	rm.repoLocker.Unlock(name)
}

func (rm *repositoryManager) LockRepoExclusive(name string) {
	rm.repoLocker.LockExclusive(name)
}

func (rm *repositoryManager) UnlockRepoExclusive(name string) {
	rm.repoLocker.UnlockExclusive(name)
}

func (rm *repositoryManager) EnsureRepo(ctx context.Context, namespace, volumeNamespace, backupLocation string) (string, string, error) {
	repo, err := rm.repoEnsurer.EnsureRepo(ctx, namespace, volumeNamespace, backupLocation)
	if err != nil {
		rm.log.WithError(errors.WithStack(err)).Error("Failed to ensure repo instance")
		return "", "", err
	}

	return repo.Name, repo.Spec.ResticIdentifier, nil
}

func (rm *repositoryManager) GetRepoInformerSynced() cache.InformerSynced {
	return rm.repoInformerSynced
}

func (rm *repositoryManager) Forget(ctx context.Context, snapshotID, volumeNamespace, backupLocation string) error {
	// We can't wait for this in the constructor, because this informer is coming
	// from the shared informer factory, which isn't started until *after* the repo
	// manager is instantiated & passed to the controller constructors. We'd get a
	// deadlock if we tried to wait for this in the constructor.
	if !cache.WaitForCacheSync(ctx.Done(), rm.repoInformerSynced) {
		return errors.New("timed out waiting for cache to sync")
	}

	repo, err := rm.repoEnsurer.EnsureRepo(ctx, rm.namespace, volumeNamespace, backupLocation)
	if err != nil {
		return err
	}

	// Accquire an exclusive lock
	rm.repoLocker.LockExclusive(repo.Name)
	defer rm.repoLocker.UnlockExclusive(repo.Name)

	return rm.repoProv.Forget(repo.Spec.ResticIdentifier, snapshotID, backupLocation)
}
