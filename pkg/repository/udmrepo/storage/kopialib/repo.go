package kopialib

import (
	"context"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/util/logging"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

type kopiaRepoService struct {
	fullMaintenance bool
	ctx             context.Context
	logger          logrus.FieldLogger
}

type kopiaRepository struct {
	ctx       context.Context
	rawWriter repo.RepositoryWriter
}

type kopiaObjectReader struct {
	rawReader object.Reader
}

type kopiaObjectWriter struct {
	rawWriter object.Writer
}

func NewKopiaRepoService(ctx context.Context, logger logrus.FieldLogger) udmrepo.BackupRepoService {
	ks := &kopiaRepoService{
		ctx:    ctx,
		logger: logger,
	}

	ks.ctx = logging.SetupKopiaLog(ks.ctx, logger)

	return ks
}

func (ks *kopiaRepoService) InitBackupRepo(repoOption udmrepo.RepoOptions, outputConfigFile string, createNew bool) error {
	if createNew {
		return CreateBackupRepo(ks.ctx, repoOption, outputConfigFile)
	} else {
		return ConnectBackupRepo(ks.ctx, repoOption, outputConfigFile)
	}
}

func (ks *kopiaRepoService) OpenBackupRepo(configFile, password string) (udmrepo.BackupRepo, error) {
	repoConfig := RepoConfigFileName(configFile)
	if _, err := os.Stat(repoConfig); os.IsNotExist(err) {
		return nil, err
	}

	r, err := repo.Open(ks.ctx, repoConfig, password, &repo.Options{})
	if os.IsNotExist(err) {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	_, rw, err := r.NewWriter(ks.ctx, repo.WriteSessionOptions{
		Purpose:  "",
		OnUpload: func(i int64) {},
	})

	return &kopiaRepository{
		ctx:       ks.ctx,
		rawWriter: rw,
	}, nil
}

func (ks *kopiaRepoService) MaintainBackupRepo(configFile, password string, full bool) error {
	ks.fullMaintenance = full

	repoConfig := RepoConfigFileName(configFile)
	if _, err := os.Stat(repoConfig); os.IsNotExist(err) {
		return err
	}

	r, err := repo.Open(ks.ctx, repoConfig, password, &repo.Options{})
	if os.IsNotExist(err) {
		return err
	}

	if err != nil {
		return err
	}

	defer r.Close(ks.ctx)

	return repo.DirectWriteSession(ks.ctx, r.(repo.DirectRepository), repo.WriteSessionOptions{
		Purpose:  "Maintenance",
		OnUpload: func(i int64) {},
	}, func(ctx context.Context, dw repo.DirectRepositoryWriter) error { return ks.runMaintenance(ctx, dw) })
}

func (ks *kopiaRepoService) runMaintenance(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	return nil
	// mode := maintenance.ModeQuick
	// _, supportsEpochManager := rep.ContentManager().EpochManager()

	// if ks.fullMaintenance || supportsEpochManager {
	// 	mode = maintenance.ModeFull
	// }

	// return snapshotmaintenance.Run(ctx, rep, mode, false, maintenance.SafetyFull)
}

func (kr *kopiaRepository) OpenObject(id udmrepo.ID) (udmrepo.ObjectReader, error) {
	reader, err := kr.rawWriter.OpenObject(kr.ctx, object.ID(id))
	if err != nil {
		return nil, err
	}

	return &kopiaObjectReader{
		rawReader: reader,
	}, err
}

func (kr *kopiaRepository) GetManifest(id udmrepo.ID, mani *udmrepo.RepoManifest) error {
	metadata, err := kr.rawWriter.GetManifest(kr.ctx, manifest.ID(id), mani.Payload)
	if err != nil {
		return err
	}

	mani.Metadata = GetManifestEntryFromKopia(metadata)

	return nil
}

func (kr *kopiaRepository) FindManifests(filter udmrepo.ManifestFilter) ([]*udmrepo.ManifestEntryMetadata, error) {
	metadata, err := kr.rawWriter.FindManifests(kr.ctx, filter.Labels)
	return GetManifestEntriesFromKopia(metadata), err
}

func GetManifestEntryFromKopia(kMani *manifest.EntryMetadata) *udmrepo.ManifestEntryMetadata {
	var ret udmrepo.ManifestEntryMetadata

	ret.ID = udmrepo.ID(kMani.ID)
	ret.Labels = kMani.Labels
	ret.Length = kMani.Length
	ret.ModTime = kMani.ModTime

	return &ret
}

func GetManifestEntriesFromKopia(kMani []*manifest.EntryMetadata) []*udmrepo.ManifestEntryMetadata {
	var ret []*udmrepo.ManifestEntryMetadata

	for _, entry := range kMani {
		var e udmrepo.ManifestEntryMetadata
		e.ID = udmrepo.ID(entry.ID)
		e.Labels = entry.Labels
		e.Length = entry.Length
		e.ModTime = entry.ModTime

		ret = append(ret, &e)
	}

	return ret
}

func (kr *kopiaRepository) Time() time.Time {
	return kr.rawWriter.Time()
}

// func (kr *kopiaRepository) ClientOptions() udmrepo.RepoClientOptions {
// 	option := kr.rawWriter.ClientOptions()
// 	return GetClientOptionFromKopia(option)
// }

// func GetClientOptionFromKopia(option repo.ClientOptions) udmrepo.RepoClientOptions {
// 	var ret udmrepo.RepoClientOptions

// 	ret.Description = option.Description
// 	ret.EnableActions = option.EnableActions
// 	ret.FormatBlobCacheDuration = option.FormatBlobCacheDuration
// 	ret.Hostname = option.Hostname
// 	ret.ReadOnly = option.ReadOnly
// 	ret.Username = option.Username

// 	return ret
// }

// func (kr *kopiaRepository) Refresh() error {
// 	return kr.rawWriter.Refresh(kr.ctx)
// }

func (kr *kopiaRepository) Close() error {
	return kr.rawWriter.Close(kr.ctx)
}

func (kr *kopiaRepository) NewObjectWriter(opt udmrepo.ObjectWriteOptions) udmrepo.ObjectWriter {
	writer := kr.rawWriter.NewObjectWriter(kr.ctx, object.WriterOptions{
		Description: opt.Description,
		Prefix:      index.ID(opt.Prefix),
	})
	if writer == nil {
		return nil
	}

	return &kopiaObjectWriter{
		rawWriter: writer,
	}
}

func (kr *kopiaRepository) PutManifest(manifest udmrepo.RepoManifest) (udmrepo.ID, error) {
	id, err := kr.rawWriter.PutManifest(kr.ctx, manifest.Metadata.Labels, manifest.Payload)
	return udmrepo.ID(id), err
}

func (kr *kopiaRepository) DeleteManifest(id udmrepo.ID) error {
	return kr.rawWriter.DeleteManifest(kr.ctx, manifest.ID(id))
}

func (kr *kopiaRepository) Flush() error {
	return kr.rawWriter.Flush(kr.ctx)
}

func (kor *kopiaObjectReader) Read(p []byte) (n int, err error) {
	return kor.rawReader.Read(p)
}

func (kor *kopiaObjectReader) Seek(offset int64, whence int) (int64, error) {
	return kor.rawReader.Seek(offset, whence)
}

func (kor *kopiaObjectReader) Close() error {
	return kor.rawReader.Close()
}

func (kor *kopiaObjectReader) Length() int64 {
	return kor.rawReader.Length()
}

func (kow *kopiaObjectWriter) Write(p []byte) (n int, err error) {
	return kow.rawWriter.Write(p)
}

func (kow *kopiaObjectWriter) Seek(offset int64, whence int) (int64, error) {
	return -1, nil
}

func (kow *kopiaObjectWriter) Checkpoint() (udmrepo.ID, error) {
	id, err := kow.rawWriter.Checkpoint()
	return udmrepo.ID(id), err
}

func (kow *kopiaObjectWriter) Result() (udmrepo.ID, error) {
	id, err := kow.rawWriter.Result()
	return udmrepo.ID(id), err
}

func (kow *kopiaObjectWriter) Close() error {
	return kow.rawWriter.Close()
}
