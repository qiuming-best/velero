package repository

import (
	"context"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo/storage/kopialib"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/utfstring"
)

type unifiedRepoProvider struct {
	ctx                  context.Context
	namespace            string
	kbClient             kbclient.Client
	fileSystem           filesystem.Interface
	credentialsFileStore credentials.FileStore
	log                  logrus.FieldLogger
	repoService          udmrepo.BackupRepoService
	repoPassword         string
}

func NewUnifiedRepoProvider(
	ctx context.Context,
	namespace string,
	kbClient kbclient.Client,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger,
) (RepositoryProvider, error) {
	repo := unifiedRepoProvider{
		ctx:                  ctx,
		namespace:            namespace,
		kbClient:             kbClient,
		credentialsFileStore: credentialFileStore,
		log:                  log,
		fileSystem:           filesystem.NewFileSystem(),
	}

	repo.repoService = kopialib.NewKopiaRepoService(ctx, log)

	log.Debug("Finished create unified repo service")

	return &repo, nil
}

func (urp *unifiedRepoProvider) ensureRepoPassword() error {
	if urp.repoPassword != "" {
		return nil
	}

	buf, err := urp.credentialsFileStore.Buffer(RepoKeySelector())
	if err != nil {
		urp.log.WithError(err).Error("Failed to get password buffer")
		return err
	}

	urp.repoPassword, err = utfstring.GetCredentialFromBuffer(buf)
	if err != nil {
		urp.log.WithError(err).Error("Failed to read password from buffer")
		return err
	}

	return nil
}

func (urp *unifiedRepoProvider) InitRepo(repoID string, backupLocation string) error {
	log := urp.log.WithFields(logrus.Fields{
		"repoID":         repoID,
		"backupLocation": backupLocation,
	})

	log.Debug("Start to init repo")

	repoName := repoName(repoID)

	err := urp.ensureRepoPassword()
	if err != nil {
		return err
	}

	repoOption, err := urp.getRepoOption(repoID, repoName, backupLocation, log)
	if err != nil {
		return err
	}

	err = urp.repoService.InitBackupRepo(repoOption, getFullConfigPath(repoName), true)
	if err != nil {
		log.WithError(err).Error("Failed to init backup repo")
	}

	return err
}

func (urp *unifiedRepoProvider) ConnectToRepo(repoID string, backupLocation string) error {
	log := urp.log.WithFields(logrus.Fields{
		"repoID":         repoID,
		"backupLocation": backupLocation,
	})

	log.Debug("Start to connect repo")

	repoName := repoName(repoID)

	err := urp.ensureRepoPassword()
	if err != nil {
		return err
	}

	bkRepo, err := urp.repoService.OpenBackupRepo(getFullConfigPath(repoName), urp.repoPassword)
	if err == nil {
		bkRepo.Close()
		log.Info("Repo %s has already been connected", repoID)

		return nil
	}

	repoOption, err := urp.getRepoOption(repoID, repoName, backupLocation, log)
	if err != nil {
		return err
	}

	err = urp.repoService.InitBackupRepo(repoOption, getFullConfigPath(repoName), false)
	if err != nil {
		log.WithError(err).Error("Failed to connect backup repo")
		return err
	}

	return nil
}

func (urp *unifiedRepoProvider) PruneRepo(repoID string, backupLocation string) error {
	log := urp.log.WithFields(logrus.Fields{
		"repoID":         repoID,
		"backupLocation": backupLocation,
	})

	log.Debug("Start to prune repo")

	repoName := repoName(repoID)

	err := urp.ensureRepoPassword()
	if err != nil {
		return err
	}

	err = urp.repoService.MaintainBackupRepo(getFullConfigPath(repoName), urp.repoPassword, true)
	if err != nil {
		log.WithError(err).Error("Failed to prune backup repo")
	}

	return err
}

func (urp *unifiedRepoProvider) PruneRepoQuick(repoID string, backupLocation string) error {
	log := urp.log.WithFields(logrus.Fields{
		"repoID":         repoID,
		"backupLocation": backupLocation,
	})

	log.Debug("Start to prune repo quick")

	repoName := repoName(repoID)

	err := urp.ensureRepoPassword()
	if err != nil {
		return err
	}

	err = urp.repoService.MaintainBackupRepo(getFullConfigPath(repoName), urp.repoPassword, false)
	if err != nil {
		log.WithError(err).Error("Failed to prune backup repo quick")
	}

	return err
}

func (urp *unifiedRepoProvider) EnsureUnlockRepo(repoID string, backupLocation string) error {
	return nil
}

func (urp *unifiedRepoProvider) Forget(repoID, snapshotID, backupLocation string) error {
	log := urp.log.WithFields(logrus.Fields{
		"repoID":         repoID,
		"backupLocation": backupLocation,
		"snapshotID":     snapshotID,
	})

	log.Debug("Start to forget snapshot")

	repoName := repoName(repoID)

	err := urp.ensureRepoPassword()
	if err != nil {
		return err
	}

	bkRepo, err := urp.repoService.OpenBackupRepo(getFullConfigPath(repoName), urp.repoPassword)
	if err != nil {
		log.WithError(err).Error("Failed to open backup repo")
		return err
	}

	defer bkRepo.Close()

	err = bkRepo.DeleteManifest(udmrepo.ID(snapshotID))
	if err != nil {
		log.WithError(err).Error("Failed to delete manifest")
		return err
	}

	return nil
}

func repoName(repoID string) string {
	if repoID == "" {
		return ""
	}

	return repoID[strings.LastIndex(repoID, "/")+1:]
}

func storageType(repoID string) string {
	if repoID == "" {
		return ""
	}

	//return "filesystem"
	return "s3"
}

func getFullRepoPath(repoName string) string {
	//path := "~/velero/kopia/"
	//filepath.Join(path, repoName)
	path := "/tmp"
	return path
}

func getFullConfigPath(repoName string) string {
	//repoPath := filepath.Join("~/velero/kopia", repoName)
	//path := "/tmp"
	//return filepath.Join(path, "repo.config")
	return ""
}

func (urp *unifiedRepoProvider) getRepoOption(repoID string, repoName string, backupLocation string, log logrus.FieldLogger) (udmrepo.RepoOptions, error) {
	storage := storageType(repoID)

	loc := &velerov1api.BackupStorageLocation{}
	if err := urp.kbClient.Get(context.Background(), kbclient.ObjectKey{
		Namespace: urp.namespace,
		Name:      backupLocation,
	}, loc); err != nil {
		return udmrepo.RepoOptions{}, errors.Wrap(err, "error getting backup storage location")
	}

	//connection.GetInsecureSkipTLSVerifyFromBSL(loc, log)
	generalOptionStr := `{"storage":"` + storage + `", "password":"` + urp.repoPassword + `"}`
	bucket := "velero-e2e-testing"
	region := "minio"
	endpoint := "minio.minio.svc:9000"
	accessKey := "minio"
	secret := "minio456"
	//SessionToken := os.Getenv("sessionToken")
	prefix := "unified-repo/" + repoName + "/"
	storageOptionStr := `{"bucket":"` + bucket + `",
	"Region":"` + region + `", "endpoint":"` + endpoint + `", "prefix":"` + prefix + `", 
	"accessKeyID":"` + accessKey + `", "secretAccessKey":"` + secret + `","doNotUseTLS":true}`

	log.Infof("Init repo param: repo name %s, storage option [%s], general Option [%s]", repoName, storageOptionStr, generalOptionStr)

	return udmrepo.RepoOptions{
		GeneralOptions: string(generalOptionStr),
		StorageOptions: storageOptionStr,
	}, nil
}
