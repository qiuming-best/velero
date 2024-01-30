package repomaintenance

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerocli "github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/repository"
	"github.com/vmware-tanzu/velero/pkg/repository/provider"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Options struct {
	VolumeNamespace          string
	BackupStorageLocation    string
	RepoType                 string
	KeepLatestMaitenanceJobs int
	LogLevelFlag             *logging.LevelFlag
	FormatFlag               *logging.FormatFlag
}

func (o *Options) BindFlags(flags *pflag.FlagSet) {
	flags.StringVar(&o.VolumeNamespace, "repo-name", "", "namespace of the pod/volume that the snapshot is for")
	flags.StringVar(&o.BackupStorageLocation, "backup-storage-location", "", "backup's storage location name")
	flags.StringVar(&o.RepoType, "repo-type", velerov1api.BackupRepositoryTypeKopia, "type of the repository where the snapshot is stored")
	flags.Var(o.LogLevelFlag, "log-level", fmt.Sprintf("The level at which to log. Valid values are %s.", strings.Join(o.LogLevelFlag.AllowedValues(), ", ")))
	flags.Var(o.FormatFlag, "log-format", fmt.Sprintf("The format for log output. Valid values are %s.", strings.Join(o.FormatFlag.AllowedValues(), ", ")))
}

func NewCommand(f velerocli.Factory) *cobra.Command {
	o := &Options{
		LogLevelFlag: logging.LogLevelFlag(logrus.InfoLevel),
		FormatFlag:   logging.NewFormatFlag(),
	}
	cmd := &cobra.Command{
		Use:    "repo-maintenance",
		Hidden: true,
		Short:  "VELERO SERVER COMMAND ONLY - not intended to be run directly by users",
		Run: func(c *cobra.Command, args []string) {
			o.Run(f)
		},
	}

	o.BindFlags(cmd.Flags())
	return cmd
}

func checkError(err error, log logrus.FieldLogger) {
	if err != nil {
		if err != context.Canceled {
			log.Errorf("An error occurred: %v", err)
		}
		os.Exit(1)
	}
}

func (o *Options) Run(f velerocli.Factory) {
	log.SetOutput(os.Stdout)
	logrus.SetOutput(os.Stdout)
	logger := logging.DefaultLogger(o.LogLevelFlag.Parse(), o.FormatFlag.Parse())

	errorFile, err := os.Create("/dev/termination-log")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create termination log file: %v\n", err)
		return
	}
	defer errorFile.Close()

	logger.AddHook(&logging.FileHook{
		File: errorFile})

	scheme := runtime.NewScheme()
	err = velerov1api.AddToScheme(scheme)
	checkError(err, logger)

	err = v1.AddToScheme(scheme)
	checkError(err, logger)

	config, err := f.ClientConfig()
	checkError(err, logger)

	cli, err := client.New(config, client.Options{
		Scheme: scheme,
	})
	checkError(err, logger)

	credentialFileStore, err := credentials.NewNamespacedFileStore(
		cli,
		f.Namespace(),
		"/tmp/credentials",
		filesystem.NewFileSystem(),
	)
	checkError(err, logger)

	credentialSecretStore, err := credentials.NewNamespacedSecretStore(cli, f.Namespace())
	checkError(err, logger)

	var repoProvider provider.Provider
	if o.RepoType == velerov1api.BackupRepositoryTypeRestic {
		repoProvider = provider.NewResticRepositoryProvider(credentialFileStore, filesystem.NewFileSystem(), logger)
	} else {
		repoProvider = provider.NewUnifiedRepoProvider(
			credentials.CredentialGetter{
				FromFile:   credentialFileStore,
				FromSecret: credentialSecretStore,
			}, o.RepoType, cli, logger)
	}

	// backupRepository
	repo, err := repository.GetBackupRepository(context.Background(), cli, f.Namespace(),
		repository.BackupRepositoryKey{
			VolumeNamespace: o.VolumeNamespace,
			BackupLocation:  o.BackupStorageLocation,
			RepositoryType:  o.RepoType,
		}, true)
	checkError(err, logger)

	// bsl
	bsl := &velerov1api.BackupStorageLocation{}
	cli.Get(context.Background(), client.ObjectKey{Namespace: f.Namespace(), Name: repo.Spec.BackupStorageLocation}, bsl)
	checkError(err, logger)

	para := provider.RepoParam{
		BackupRepo:     repo,
		BackupLocation: bsl,
	}

	err = repoProvider.BoostRepoConnect(context.Background(), para)
	checkError(err, logger)

	err = repoProvider.PruneRepo(context.Background(), para)
	checkError(err, logger)
}
