package kopiaup

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/sirupsen/logrus"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/pkg/errors"
	"github.com/vmware-tanzu/velero/pkg/uploader/upimpl"
)

const MaxParallelFileReads = 4
const ParallelWorkersRestore = 1

var treeForSourceFunc = policy.TreeForSource
var applyRetentionPolicyFunc = policy.ApplyRetentionPolicy
var setPolicyFunc = policy.SetPolicy
var saveSnapshotFunc = snapshot.SaveSnapshot
var loadSnapshotFunc = snapshot.LoadSnapshot

func newOptionalInt(b policy.OptionalInt) *policy.OptionalInt {
	return &b
}

func setupPolicy(ctx context.Context, rep repo.RepositoryWriter, sourceInfo snapshot.SourceInfo) {
	setPolicyFunc(ctx, rep, sourceInfo, &policy.Policy{
		RetentionPolicy: policy.RetentionPolicy{
			KeepLatest: newOptionalInt(math.MaxInt32),
		},
		CompressionPolicy: policy.CompressionPolicy{
			CompressorName: "none",
		},
		UploadPolicy: policy.UploadPolicy{
			MaxParallelFileReads: newOptionalInt(MaxParallelFileReads),
		},
		SchedulingPolicy: policy.SchedulingPolicy{
			Manual: true,
		},
	})
}

func getParallelWorksRestore(wokers int) int {
	maxNum := runtime.NumCPU()
	if wokers <= 0 || wokers > maxNum {
		return maxNum
	}
	return wokers
}

func Backup(ctx context.Context, uploader *snapshotfs.Uploader, repoWriter repo.RepositoryWriter, sourcePath string, parentSnapshot string, log logrus.FieldLogger, upFunc func(upimpl.UploaderProgress)) (*upimpl.SnapshotInfo, error) {
	if uploader == nil {
		return nil, fmt.Errorf("get empty kopia uploader")
	}
	dir, err := filepath.Abs(sourcePath)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid source path '%s'", sourcePath)
	}

	sourceInfo := snapshot.SourceInfo{
		UserName: upimpl.GetDefaultUserName(),
		Host:     upimpl.GetDefaultHostName(),
		Path:     filepath.Clean(dir),
	}

	rootDir, err := getLocalFSEntry(ctx, sourceInfo.Path)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to get local filesystem entry")
	}
	snapID, snapshotSize, err := SnapshotSource(ctx, repoWriter, uploader, sourceInfo, rootDir, parentSnapshot, log, "Kopia Uploader")
	if err != nil {
		return nil, err
	}

	snapshotInfo := &upimpl.SnapshotInfo{
		ID:   snapID,
		Size: snapshotSize,
	}

	return snapshotInfo, nil
}

func getLocalFSEntry(path0 string) (fs.Entry, error) {
	path, err := resolveSymlink(path0)
	if err != nil {
		return nil, errors.Wrap(err, "resolveSymlink")
	}

	e, err := localfs.NewEntry(path)
	if err != nil {
		return nil, errors.Wrap(err, "can't get local fs entry")
	}

	return e, nil
}

func resolveSymlink(path string) (string, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return "", errors.Wrap(err, "stat")
	}

	if (st.Mode() & os.ModeSymlink) == 0 {
		return path, nil
	}

	return filepath.EvalSymlinks(path)
}

type SnapshotUploader interface {
	Upload(
		ctx context.Context,
		source fs.Entry,
		policyTree *policy.Tree,
		sourceInfo snapshot.SourceInfo,
		previousManifests ...*snapshot.Manifest,
	) (*snapshot.Manifest, error)
}

func SnapshotSource(
	ctx context.Context,
	rep repo.RepositoryWriter,
	u SnapshotUploader,
	sourceInfo snapshot.SourceInfo,
	rootDir fs.Entry,
	parentSnapshot string,
	log logrus.FieldLogger,
	description string,
	upFunc func(upimpl.UploaderProgress),
) (string, int64, error) {
	log.Info("Start to snapshot...")
	snapshotStartTime := time.Now()
	var previous []*snapshot.Manifest
	if parentSnapshot != "" {
		mani, err := loadSnapshotFunc(ctx, rep, manifest.ID(parentSnapshot))
		if err != nil {
			log.WithError(err).Error("Failed to load previous snapshot from kopia")
			return "", 0, err
		}
		previous = append(previous, mani)
	} else {
		pre, err := findPreviousSnapshotManifest(ctx, rep, sourceInfo, nil)
		if err != nil {
			log.WithError(err).Error("Failed to find previous kopia snapshot manifests")
			return "", 0, err
		}
		previous = pre
	}
	setupPolicy(ctx, rep, sourceInfo)
	policyTree, err := treeForSourceFunc(ctx, rep, sourceInfo)
	if err != nil {
		log.Error("Failed to get kopia policy tree")
		return "", 0, errors.Wrap(err, "unable to create policy getter")
	}
	outputPolicy(ctx, rep, sourceInfo)
	log.Info("Start to snapshot1...")
	manifest, err = u.Upload(ctx, rootDir, policyTree, sourceInfo, previous...)
	if err != nil {
		log.WithError(err).Error("Failed to upload the kopia snapshot")
		return "", 0, err
	}
	log.Info("Start to snapshot2...")
	manifest.Description = description

	if _, err = saveSnapshotFunc(ctx, rep, manifest); err != nil {
		log.WithError(err).Error("Failed to save kopia manifest")
		return "", 0, err
	}

	_, err = applyRetentionPolicyFunc(ctx, rep, sourceInfo, true)
	if err != nil {
		log.WithError(err).Error("Failed to apply kopia retention policy")
		return "", 0, err
	}
	if err = rep.Flush(ctx); err != nil {
		log.WithError(err).Error("Failed to flush kopia repository")
		return "", 0, err
	}
	log.Info("Start to snapshot5...")
	outputPolicy(ctx, rep, sourceInfo)
	log.Infof("Created snapshot with root %v and ID %v in %v", manifest.RootObjectID(), manifest.ID, time.Since(snapshotStartTime).Truncate(time.Second))
	return reportSnapshotStatus(manifest)
	//}

	//err := repo.WriteSession(ctx, rep, writeSessionOpt, cb)
	/*err := cb(ctx, rep)
	if err != nil {
		return "", 0, err
	} else if manifest == nil {
		return "", 0, fmt.Errorf("failed to snapshot data with empty manifest")
	} else {
		return reportSnapshotStatus(manifest)
	}*/
}

	return reportSnapshotStatus(manifest)
}

func reportSnapshotStatus(manifest *snapshot.Manifest) (string, int64, error) {
	manifestID := manifest.ID
	snapSize := manifest.Stats.TotalFileSize

	var errs []string
	if ds := manifest.RootEntry.DirSummary; ds != nil {
		for _, ent := range ds.FailedEntries {
			errs = append(errs, ent.Error)
		}
	}
	if len(errs) != 0 {
		return "", 0, errors.New(strings.Join(errs, "\n"))
	}

	return string(manifestID), snapSize, nil
}

func findPreviousSnapshotManifest(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, noLaterThan *time.Time) ([]*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, err
	}

	var previousComplete *snapshot.Manifest
	var result []*snapshot.Manifest

	for _, p := range man {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}

		if p.IncompleteReason == "" && (previousComplete == nil || p.StartTime.After(previousComplete.StartTime)) {
			previousComplete = p
		}
	}

	if previousComplete != nil {
		result = append(result, previousComplete)
	}

	return result, nil
}

func Restore(ctx context.Context, rep repo.RepositoryWriter, progress *KopiaProgress, snapshotID, dest string, log logrus.FieldLogger, cancleCh chan struct{}) (int64, int32, error) {
	log.Info("Start to restore...")

	rootEntry, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, snapshotID, false)
	if err != nil {
		log.WithError(err).Error("Unable to get filesystem entry")
		return 0, 0, err
	}

	path, err := filepath.Abs(dest)
	if err != nil {
		log.WithError(err).Error("Unable to resolve path")
		return 0, 0, err
	}

	output := &restore.FilesystemOutput{
		TargetPath:             path,
		OverwriteDirectories:   true,
		OverwriteFiles:         true,
		OverwriteSymlinks:      true,
		IgnorePermissionErrors: true,
	}

	stat, err := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
		Parallel:               getParallelWorksRestore(ParallelWorkersRestore),
		RestoreDirEntryAtDepth: math.MaxInt32,
		Cancel:                 cancleCh,
		ProgressCallback: func(ctx context.Context, stats restore.Stats) {
			log.Debugf("vae updateRestoreProgressFunc %v", stats)
			progress.ProgressBytes(stats.RestoredTotalFileSize, stats.EnqueuedTotalFileSize)
		},
	})

	if err != nil {
		log.WithError(err).Error("Failed to copy snapshot data to the target")
		return 0, 0, err
	}
	return stat.RestoredTotalFileSize, stat.RestoredFileCount, nil
}
