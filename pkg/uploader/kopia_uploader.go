package uploader

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/repository/udmreposrv"
	"github.com/vmware-tanzu/velero/pkg/uploader/upimpl"
	"github.com/vmware-tanzu/velero/pkg/uploader/upimpl/kopiaup"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
	"github.com/vmware-tanzu/velero/pkg/util/utfstring"
)

type kopiaUploaderProvider struct {
	ctx            context.Context
	repoIdentifier string
	bkRepo         udmrepo.BackupRepo
	snapshotInfo   *upimpl.SnapshotInfo
	taskName       string
	log            logrus.FieldLogger
}

func NewKopiaUploaderProvider(
	ctx context.Context,
	repoIdentifier string,
	credentialsFileStore credentials.FileStore,
	repoKeySelector *v1.SecretKeySelector,
	configFile string,
	log logrus.FieldLogger,
) (UploaderProvider, error) {
	kup := kopiaUploaderProvider{
		repoIdentifier: repoIdentifier,
		log:            log,
	}

	kup.ctx = logging.SetupKopiaLog(ctx, log)

	buf, err := credentialsFileStore.Buffer(repoKeySelector)
	if err != nil {
		log.WithError(err).Error("Failed to get password buffer")
		return nil, err
	}

	var password string
	password, err = utfstring.GetCredentialFromBuffer(buf)
	if err != nil {
		log.WithError(err).Error("Failed to read password from buffer")
		return nil, err
	}

	repoService := udmreposrv.CreateUdmrepoService(ctx, log)

	log.WithField("configFile", configFile).Info("Opening backup repo")

	kup.bkRepo, err = repoService.OpenBackupRepo(configFile, password)
	if err != nil {
		log.WithError(err).Error("Failed to find kopia repository")
		return nil, err
	}

	return &kup, nil
}

func (kup *kopiaUploaderProvider) Close() {
	kup.bkRepo.Close()
}

func (kup *kopiaUploaderProvider) GetSnapshotID() (string, error) {
	return kup.snapshotInfo.ID, nil
}

func (kup *kopiaUploaderProvider) RunBackup(
	path string,
	tags map[string]string,
	parentSnapshot string,
	updateFunc func(velerov1api.PodVolumeOperationProgress)) (string, string, error) {

	kup.taskName = "Kopia-Backup"

	log := kup.log.WithFields(logrus.Fields{
		"path":           path,
		"parentSnapshot": parentSnapshot,
		"taskName":       kup.taskName,
	})

	log.Info("Starting backup")

	snapshotInfo, err := kopiaup.Backup(kup.ctx, path, kup.bkRepo, parentSnapshot, kup.log, func(p upimpl.UploaderProgress) {
		updateFunc(velerov1api.PodVolumeOperationProgress{
			TotalBytes: p.TotalBytes,
			BytesDone:  p.BytesDone,
		})
	})

	if err != nil {
		log.WithError(err).Error("Failed to run kopia backup")
		return "", "", err
	}

	kup.snapshotInfo = snapshotInfo

	output := fmt.Sprintf("Kopia backup finished, snapshot ID %s, backup size %d", snapshotInfo.ID, snapshotInfo.Size)

	log.Info(output)

	return output, "", nil
}

func (kup *kopiaUploaderProvider) RunRestore(
	snapshotID string,
	volumePath string,
	updateFunc func(velerov1api.PodVolumeOperationProgress)) (string, string, error) {

	kup.taskName = "Kopia-Restore"

	log := kup.log.WithFields(logrus.Fields{
		"snapshotID": snapshotID,
		"volumePath": volumePath,
	})

	log.Info("Starting restore")

	size, fileCount, err := kopiaup.Restore(kup.ctx, kup.bkRepo, snapshotID, volumePath, kup.log, func(p upimpl.UploaderProgress) {
		updateFunc(velerov1api.PodVolumeOperationProgress{
			TotalBytes: p.TotalBytes,
			BytesDone:  p.BytesDone,
		})
	})

	if err != nil {
		log.WithError(err).Error("Failed to run kopia restore")
		return "", "", err
	}

	updateFunc(velerov1api.PodVolumeOperationProgress{
		TotalBytes: size,
		BytesDone:  size,
	})

	output := fmt.Sprintf("Kopia restore finished, restore size %d, file count %d", size, fileCount)

	log.Info(output)

	return output, "", nil
}

func (kup *kopiaUploaderProvider) SetCACert(caCert []byte, bsl string) error {
	return nil
}

func (kup *kopiaUploaderProvider) GetTaskName() string {
	return kup.taskName
}
