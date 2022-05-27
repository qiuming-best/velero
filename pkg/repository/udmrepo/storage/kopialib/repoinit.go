package kopialib

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/repo/splitter"
	"github.com/pkg/errors"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
)

type kopiaStorageFlags interface {
	Setup(flags string) error
	Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error)
}

type kopiaRepoOptions struct {
	name         string
	description  string
	storageFlags kopiaStorageFlags
}

type genKopiaOptions struct {
	Storage                       string        `json:"storage"`
	RepoVersion                   int           `json:"repoVersion"`
	Password                      string        `json:"password"`
	HashAlgo                      string        `json:"hashAlgo"`
	EncryptionAlgo                string        `json:"encAlgo"`
	SplitAlgo                     string        `json:"splitAlgo"`
	RetentionMode                 string        `json:"retentionMode"`
	RetentionPeriod               time.Duration `json:"retentionPeriod"`
	CreateOnly                    bool          `json:"createOnly"`
	ConnectCacheDirectory         string        `json:"connectCacheDirectory"`
	ConnectMaxCacheSizeMB         int64         `json:"connectMaxCacheSizeMB"`
	ConnectMaxMetadataCacheSizeMB int64         `json:"connectMaxMetadataCacheSizeMB"`
	ConnectMaxListCacheDuration   time.Duration `json:"connectMaxListCacheDuration"`
	ConnectHostname               string        `json:"connectHostname"`
	ConnectUsername               string        `json:"connectUsername"`
	ConnectReadonly               bool          `json:"connectReadonly"`
	ConnectDescription            string        `json:"connectDescription"`
	ConnectEnableActions          bool          `json:"connectEnableActions"`
	DisableFormatBlobCache        bool          `json:"disableFormatBlobCache"`
	FormatBlobCacheDuration       time.Duration `json:"formatBlobCacheDuration"`
}

func CreateBackupRepo(ctx context.Context, repoOption udmrepo.RepoOptions, outputConfigFile string) error {
	outputConfigFile = RepoConfigFileName(outputConfigFile)

	var genOptions genKopiaOptions
	err := json.Unmarshal([]byte(repoOption.GeneralOptions), &genOptions)
	if err != nil {
		return errors.Wrap(err, "Failed to unmarshal kopia general options")
	}

	setupDefaultOptionValues(&genOptions)

	options := findStorageOptions(genOptions.Storage)
	if options == nil {
		return errors.New("Failed to find storage options")
	}

	options.storageFlags.Setup(repoOption.StorageOptions)

	st, err := options.storageFlags.Connect(ctx, true, genOptions.RepoVersion)
	if err != nil {
		return errors.Wrap(err, "Failed to connect to storage")
	}

	err = createWithStorage(ctx, st, genOptions)
	if err != nil {
		return errors.Wrap(err, "Failed to create with storage")
	}

	return connectWithStorage(ctx, st, genOptions, outputConfigFile)
}

func ConnectBackupRepo(ctx context.Context, repoOption udmrepo.RepoOptions, outputConfigFile string) error {
	outputConfigFile = RepoConfigFileName(outputConfigFile)

	var genOptions genKopiaOptions
	err := json.Unmarshal([]byte(repoOption.GeneralOptions), &genOptions)
	if err != nil {
		return errors.Wrap(err, "Failed to unmarshal kopia general options")
	}

	setupDefaultOptionValues(&genOptions)

	options := findStorageOptions(genOptions.Storage)
	if options == nil {
		return errors.New("Failed to find storage options")
	}

	options.storageFlags.Setup(repoOption.StorageOptions)

	st, err := options.storageFlags.Connect(ctx, false, genOptions.RepoVersion)
	if err != nil {
		return errors.Wrap(err, "Failed to connect to storage")
	}

	return connectWithStorage(ctx, st, genOptions, outputConfigFile)
}

func initStorageOptions() []kopiaRepoOptions {
	return []kopiaRepoOptions{
		{"azure", "an Azure blob storage", &kopiaAzureFlags{}},
		{"filesystem", "a filesystem", &kopiaFilesystemFlags{}},
		{"gcs", "a Google Cloud Storage bucket", &kopiaGCSFlags{}},
		{"s3", "an S3 bucket", &kopiaS3Flags{}},
	}
}

func findStorageOptions(storage string) *kopiaRepoOptions {
	list := initStorageOptions()

	for _, options := range list {
		if strings.EqualFold(options.name, storage) {
			return &options
		}
	}

	return nil
}

func setupDefaultOptionValues(genOptions *genKopiaOptions) {
	if genOptions.HashAlgo == "" {
		genOptions.HashAlgo = hashing.DefaultAlgorithm
	}

	if genOptions.EncryptionAlgo == "" {
		genOptions.EncryptionAlgo = encryption.DefaultAlgorithm
	}

	if genOptions.SplitAlgo == "" {
		genOptions.SplitAlgo = splitter.DefaultAlgorithm
	}
}

func createWithStorage(ctx context.Context, st blob.Storage, genOptions genKopiaOptions) error {
	err := ensureEmpty(ctx, st)
	if err != nil {
		return errors.Wrap(err, "unable to get repository storage")
	}

	options := newRepositoryOptionsFromFlags(genOptions)

	if err := repo.Initialize(ctx, st, options, genOptions.Password); err != nil {
		return errors.Wrap(err, "cannot initialize repository")
	}

	if genOptions.CreateOnly {
		return nil
	}

	return nil
}

func connectWithStorage(ctx context.Context, st blob.Storage, genOptions genKopiaOptions, outputConfigFile string) error {
	if err := repo.Connect(ctx, outputConfigFile, st, genOptions.Password, newRepoConnectOptions(genOptions)); err != nil {
		return errors.Wrap(err, "error connecting to repository")
	}

	return nil
}

func ensureEmpty(ctx context.Context, s blob.Storage) error {
	hasDataError := errors.Errorf("has data")

	err := s.ListBlobs(ctx, "", func(cb blob.Metadata) error {
		return hasDataError
	})

	if errors.Is(err, hasDataError) {
		return errors.New("found existing data in storage location")
	}

	return errors.Wrap(err, "error listing blobs")
}

func newRepositoryOptionsFromFlags(genOptions genKopiaOptions) *repo.NewRepositoryOptions {
	return &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			MutableParameters: content.MutableParameters{
				Version: content.FormatVersion(genOptions.RepoVersion),
			},
			Hash:       genOptions.HashAlgo,
			Encryption: genOptions.EncryptionAlgo,
		},

		ObjectFormat: object.Format{
			Splitter: genOptions.SplitAlgo,
		},

		RetentionMode:   blob.RetentionMode(genOptions.RetentionMode),
		RetentionPeriod: genOptions.RetentionPeriod,
	}
}

func getFormatBlobCacheDuration(genOptions genKopiaOptions) time.Duration {
	if genOptions.DisableFormatBlobCache {
		return -1
	}

	return genOptions.FormatBlobCacheDuration
}

func newRepoConnectOptions(genOptions genKopiaOptions) *repo.ConnectOptions {
	return &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory:            genOptions.ConnectCacheDirectory,
			MaxCacheSizeBytes:         genOptions.ConnectMaxCacheSizeMB << 20,         //nolint:gomnd
			MaxMetadataCacheSizeBytes: genOptions.ConnectMaxMetadataCacheSizeMB << 20, //nolint:gomnd
			MaxListCacheDuration:      content.DurationSeconds(genOptions.ConnectMaxListCacheDuration.Seconds()),
		},
		ClientOptions: repo.ClientOptions{
			Hostname:                genOptions.ConnectHostname,
			Username:                genOptions.ConnectUsername,
			ReadOnly:                genOptions.ConnectReadonly,
			Description:             genOptions.ConnectDescription,
			EnableActions:           genOptions.ConnectEnableActions,
			FormatBlobCacheDuration: getFormatBlobCacheDuration(genOptions),
		},
	}
}
