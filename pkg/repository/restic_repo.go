package repository

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository/resticrepo"
	"github.com/vmware-tanzu/velero/pkg/util/command"
	"github.com/vmware-tanzu/velero/pkg/util/connection"
	veleroexec "github.com/vmware-tanzu/velero/pkg/util/exec"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

type resticRepoProvider struct {
	ctx                  context.Context
	namespace            string
	kbClient             kbclient.Client
	fileSystem           filesystem.Interface
	credentialsFileStore credentials.FileStore
	log                  logrus.FieldLogger
}

func NewResticRepoProvider(
	ctx context.Context,
	namespace string,
	kbClient kbclient.Client,
	credentialFileStore credentials.FileStore,
	log logrus.FieldLogger,
) (RepositoryProvider, error) {
	return &resticRepoProvider{
		ctx:                  ctx,
		namespace:            namespace,
		kbClient:             kbClient,
		credentialsFileStore: credentialFileStore,
		log:                  log,
		fileSystem:           filesystem.NewFileSystem(),
	}, nil
}

func (rp *resticRepoProvider) InitRepo(repoID string, backupLocation string) error {
	return rp.exec(command.InitCommand(repoID), backupLocation)
}

func (rp *resticRepoProvider) ConnectToRepo(repoID string, backupLocation string) error {
	snapshotsCmd := command.SnapshotsCommand(repoID)
	// use the '--latest=1' flag to minimize the amount of data fetched since
	// we're just validating that the repo exists and can be authenticated
	// to.
	// "--last" is replaced by "--latest=1" in restic v0.12.1
	snapshotsCmd.ExtraFlags = append(snapshotsCmd.ExtraFlags, "--latest=1")

	return rp.exec(snapshotsCmd, backupLocation)
}

func (rp *resticRepoProvider) PruneRepo(repoID string, backupLocation string) error {
	return rp.exec(command.PruneCommand(repoID), backupLocation)
}

func (rp *resticRepoProvider) PruneRepoQuick(repoID string, bsl string) error {
	return nil
}

func (rp *resticRepoProvider) EnsureUnlockRepo(repoID string, backupLocation string) error {
	return rp.exec(command.UnlockCommand(repoID), backupLocation)
}

func (rp *resticRepoProvider) Forget(repoID, snapshotID, backupLocation string) error {
	return rp.exec(command.ForgetCommand(repoID, snapshotID), backupLocation)
}

func (rp *resticRepoProvider) exec(cmd *command.Command, backupLocation string) error {
	file, err := rp.credentialsFileStore.Path(RepoKeySelector())
	if err != nil {
		return err
	}
	// ignore error since there's nothing we can do and it's a temp file.
	defer os.Remove(file)

	cmd.PasswordFile = file

	loc := &velerov1api.BackupStorageLocation{}
	if err := rp.kbClient.Get(context.Background(), kbclient.ObjectKey{
		Namespace: rp.namespace,
		Name:      backupLocation,
	}, loc); err != nil {
		return errors.Wrap(err, "error getting backup storage location")
	}

	// if there's a caCert on the ObjectStorage, write it to disk so that it can be passed to restic
	var caCertFile string
	if loc.Spec.ObjectStorage != nil && loc.Spec.ObjectStorage.CACert != nil {
		caCertFile, err = resticrepo.TempCACertFile(loc.Spec.ObjectStorage.CACert, backupLocation, rp.fileSystem)
		if err != nil {
			return errors.Wrap(err, "error creating temp cacert file")
		}
		// ignore error since there's nothing we can do and it's a temp file.
		defer os.Remove(caCertFile)
	}
	cmd.CACertFile = caCertFile

	env, err := resticrepo.CmdEnv(loc, rp.credentialsFileStore)
	if err != nil {
		return err
	}
	cmd.Env = env

	// #4820: restrieve insecureSkipTLSVerify from BSL configuration for
	// AWS plugin. If nothing is return, that means insecureSkipTLSVerify
	// is not enable for Restic command.
	cmd.InsecureSkipTSLVerify = connection.GetInsecureSkipTLSVerifyFromBSL(loc, rp.log)

	stdout, stderr, err := veleroexec.RunCommand(cmd.Cmd())
	rp.log.WithFields(logrus.Fields{
		"repository": cmd.RepoName(),
		"command":    cmd.String(),
		"stdout":     stdout,
		"stderr":     stderr,
	}).Debugf("Ran restic command")
	if err != nil {
		return errors.Wrapf(err, "error running command=%s, stdout=%s, stderr=%s", cmd.String(), stdout, stderr)
	}

	return nil
}
