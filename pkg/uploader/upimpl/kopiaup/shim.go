package kopiaup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

type shimRepository struct {
	udmRepo udmrepo.BackupRepo
}

type shimObjectWriter struct {
	repoWriter udmrepo.ObjectWriter
}

type shimObjectReader struct {
	repoReader udmrepo.ObjectReader
}

func NewShimRepo(repo udmrepo.BackupRepo) repo.RepositoryWriter {
	return &shimRepository{
		udmRepo: repo,
	}
}

func (sr *shimRepository) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	reader, err := sr.udmRepo.OpenObject(udmrepo.ID(id))
	if reader == nil {
		return nil, err
	}

	return &shimObjectReader{
		repoReader: reader,
	}, err
}

func (sr *shimRepository) VerifyObject(ctx context.Context, id object.ID) ([]content.ID, error) {
	return nil, nil
}

func (sr *shimRepository) GetManifest(ctx context.Context, id manifest.ID, payload interface{}) (*manifest.EntryMetadata, error) {
	repoMani := udmrepo.RepoManifest{
		Payload: payload,
	}

	err := sr.udmRepo.GetManifest(udmrepo.ID(id), &repoMani)
	if err != nil {
		return nil, fmt.Errorf("failed to find manifest id %v from repository with err %v", id, err)
	}
	return GetKopiaManifestEntry(repoMani.Metadata), err
}

func (sr *shimRepository) FindManifests(ctx context.Context, labels map[string]string) ([]*manifest.EntryMetadata, error) {
	metadata, err := sr.udmRepo.FindManifests(udmrepo.ManifestFilter{
		Labels: labels,
	})

	return GetKopiaManifestEntries(metadata), err
}

func GetKopiaManifestEntry(uMani *udmrepo.ManifestEntryMetadata) *manifest.EntryMetadata {
	var ret manifest.EntryMetadata

	ret.ID = manifest.ID(uMani.ID)
	ret.Labels = uMani.Labels
	ret.Length = uMani.Length
	ret.ModTime = uMani.ModTime

	return &ret
}

func GetKopiaManifestEntries(uMani []*udmrepo.ManifestEntryMetadata) []*manifest.EntryMetadata {
	var ret []*manifest.EntryMetadata

	for _, entry := range uMani {
		var e manifest.EntryMetadata
		e.ID = manifest.ID(entry.ID)
		e.Labels = entry.Labels
		e.Length = entry.Length
		e.ModTime = entry.ModTime

		ret = append(ret, &e)
	}

	return ret
}

func (sr *shimRepository) Time() time.Time {
	return sr.udmRepo.Time()
}

func (sr *shimRepository) ClientOptions() repo.ClientOptions {
	// option := sr.udmRepo.ClientOptions()
	// return GetClientOptionFromKopia(option)
	return repo.ClientOptions{}
}

// func GetClientOptionFromKopia(option udmrepo.RepoClientOptions) repo.ClientOptions {
// 	var ret repo.ClientOptions

// 	ret.Description = option.Description
// 	ret.EnableActions = option.EnableActions
// 	ret.FormatBlobCacheDuration = option.FormatBlobCacheDuration
// 	ret.Hostname = option.Hostname
// 	ret.ReadOnly = option.ReadOnly
// 	ret.Username = option.Username

// 	return ret
// }

func (sr *shimRepository) Refresh(ctx context.Context) error {
	return nil
}

func (sr *shimRepository) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	return nil, nil
}

func (sr *shimRepository) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	return nil
}

func (sr *shimRepository) PrefetchObjects(ctx context.Context, objectIDs []object.ID, hint string) ([]content.ID, error) {
	return nil, nil
}

func (sr *shimRepository) UpdateDescription(d string) {
}

func (sr *shimRepository) NewWriter(ctx context.Context, option repo.WriteSessionOptions) (context.Context, repo.RepositoryWriter, error) {
	return nil, nil, nil
}

func (sr *shimRepository) Close(ctx context.Context) error {
	return sr.udmRepo.Close()
}

func (sr *shimRepository) NewObjectWriter(ctx context.Context, option object.WriterOptions) object.Writer {
	var opt udmrepo.ObjectWriteOptions
	opt.Description = option.Description
	opt.Prefix = udmrepo.ID(option.Prefix)
	opt.FullPath = ""
	opt.AccessMode = udmrepo.OBJECT_DATA_ACCESS_MODE_FILE

	if strings.HasPrefix(option.Description, "DIR:") {
		opt.DataType = udmrepo.OBJECT_DATA_TYPE_METADATA
	} else {
		opt.DataType = udmrepo.OBJECT_DATA_TYPE_DATA
	}

	writer := sr.udmRepo.NewObjectWriter(opt)
	if writer == nil {
		return nil
	}

	return &shimObjectWriter{
		repoWriter: writer,
	}
}

func (sr *shimRepository) PutManifest(ctx context.Context, labels map[string]string, payload interface{}) (manifest.ID, error) {
	id, err := sr.udmRepo.PutManifest(udmrepo.RepoManifest{
		Payload: payload,
		Metadata: &udmrepo.ManifestEntryMetadata{
			Labels: labels,
		},
	})

	return manifest.ID(id), err
}

func (sr *shimRepository) DeleteManifest(ctx context.Context, id manifest.ID) error {
	return sr.udmRepo.DeleteManifest(udmrepo.ID(id))
}

func (sr *shimRepository) Flush(ctx context.Context) error {
	return sr.udmRepo.Flush()
}

func (sr *shimObjectReader) Read(p []byte) (n int, err error) {
	return sr.repoReader.Read(p)
}

func (sr *shimObjectReader) Seek(offset int64, whence int) (int64, error) {
	return sr.repoReader.Seek(offset, whence)
}

func (sr *shimObjectReader) Close() error {
	return sr.repoReader.Close()
}

func (sr *shimObjectReader) Length() int64 {
	return sr.repoReader.Length()
}

func (sr *shimObjectWriter) Write(p []byte) (n int, err error) {
	return sr.repoWriter.Write(p)
}

func (sr *shimObjectWriter) Checkpoint() (object.ID, error) {
	id, err := sr.repoWriter.Checkpoint()
	return object.ID(id), err

	//return "", nil
}

func (sr *shimObjectWriter) Result() (object.ID, error) {
	id, err := sr.repoWriter.Result()
	return object.ID(id), err
}

func (sr *shimObjectWriter) Close() error {
	return sr.repoWriter.Close()
}
