package uploader

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	clientset "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	velerov1informers "github.com/vmware-tanzu/velero/pkg/generated/informers/externalversions/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

// BackupManager manage uploaders.
type BackupRestoreManager interface {
	BackupperFactory

	RestorerFactory
}

// BackupperFactory can construct backuppers.
type BackupperFactory interface {
	// NewBackupper returns a backupper for use during a single
	// Velero backup.
	NewBackupper(context.Context, *velerov1api.Backup) (Backupper, error)
}

// RestorerFactory can construct restorers.
type RestorerFactory interface {
	// NewRestorer returns a restorer for use during a single
	// Velero restore.
	NewRestorer(context.Context, *velerov1api.Restore) (Restorer, error)
}

type podVolumeBackupManager struct {
	namespace            string
	veleroClient         clientset.Interface
	repoManager          repository.RepositoryManager
	log                  logrus.FieldLogger
	fileSystem           filesystem.Interface
	ctx                  context.Context
	pvcClient            corev1client.PersistentVolumeClaimsGetter
	pvClient             corev1client.PersistentVolumesGetter
	credentialsFileStore credentials.FileStore
}

// NewPodVolumeBackupManager constructs a BackupManager.
func NewPodVolumeBackupManager(
	ctx context.Context,
	namespace string,
	veleroClient clientset.Interface,
	repoManager repository.RepositoryManager,
	pvcClient corev1client.PersistentVolumeClaimsGetter,
	pvClient corev1client.PersistentVolumesGetter,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger,
) (BackupRestoreManager, error) {
	up := &podVolumeBackupManager{
		namespace:            namespace,
		veleroClient:         veleroClient,
		repoManager:          repoManager,
		pvcClient:            pvcClient,
		pvClient:             pvClient,
		credentialsFileStore: credentialFileStore,
		log:                  log,
		ctx:                  ctx,

		fileSystem: filesystem.NewFileSystem(),
	}

	return up, nil
}

func (up *podVolumeBackupManager) NewBackupper(ctx context.Context, backup *velerov1api.Backup) (Backupper, error) {
	informer := velerov1informers.NewFilteredPodVolumeBackupInformer(
		up.veleroClient,
		backup.Namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s=%s", velerov1api.BackupUIDLabel, backup.UID)
		},
	)

	b := newBackupper(ctx, up.veleroClient, up.repoManager, informer, up.pvcClient, up.pvClient, up.log)

	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced, up.repoManager.GetRepoInformerSynced()) {
		return nil, errors.New("timed out waiting for caches to sync")
	}

	return b, nil
}

func (up *podVolumeBackupManager) NewRestorer(ctx context.Context, restore *velerov1api.Restore) (Restorer, error) {
	informer := velerov1informers.NewFilteredPodVolumeRestoreInformer(
		up.veleroClient,
		restore.Namespace,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(opts *metav1.ListOptions) {
			opts.LabelSelector = fmt.Sprintf("%s=%s", velerov1api.RestoreUIDLabel, restore.UID)
		},
	)

	r := newRestorer(ctx, up.repoManager, up.veleroClient, informer, up.pvcClient, up.log)

	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced, up.repoManager.GetRepoInformerSynced()) {
		return nil, errors.New("timed out waiting for cache to sync")
	}

	return r, nil
}
