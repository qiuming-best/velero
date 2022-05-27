package kopialib

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/pkg/errors"
)

type fileSystemOptions struct {
	Path            string `json:"path"`
	ListParallelism int    `json:"ListParallelism"`
	ConnectOwnerUID string `json:"connectOwnerUID"`
	ConnectOwnerGID string `json:"connectOwnerGID"`
	ConnectFileMode string `json:"connectFileMode"`
	ConnectDirMode  string `json:"connectDirMode"`
	ConnectFlat     bool   `json:"connectFlat"`
}

type kopiaFilesystemFlags struct {
	options fileSystemOptions
}

const (
	defaultFileMode = 0o600
	defaultDirMode  = 0o700
)

func (c *kopiaFilesystemFlags) Setup(flags string) error {
	err := json.Unmarshal([]byte(flags), &c.options)
	if err != nil {
		return errors.Wrap(err, "Failed to unmarshal file system options")
	}

	return nil
}

func (c *kopiaFilesystemFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	fso := filesystem.Options{}

	fso.Path = ResolveUserFriendlyPath(c.options.Path, false)

	if !filepath.IsAbs(fso.Path) {
		return nil, errors.Errorf("filesystem repository path must be absolute")
	}

	if v := c.options.ConnectOwnerUID; v != "" {
		// nolint:gomnd
		fso.FileUID = getIntPtrValue(v, 10)
	}

	if v := c.options.ConnectOwnerGID; v != "" {
		// nolint:gomnd
		fso.FileGID = getIntPtrValue(v, 10)
	}

	fso.FileMode = getFileModeValue(c.options.ConnectFileMode, defaultFileMode)
	fso.DirectoryMode = getFileModeValue(c.options.ConnectDirMode, defaultDirMode)
	fso.DirectoryShards = initialDirectoryShards(c.options.ConnectFlat, formatVersion)

	// nolint:wrapcheck
	return filesystem.New(ctx, &fso, isCreate)
}

func initialDirectoryShards(flat bool, formatVersion int) []int {
	if flat {
		return []int{}
	}

	// when creating a repository for version 1, use fixed {3,3} sharding,
	// otherwise old client can't read it.
	if formatVersion == 1 {
		return []int{3, 3}
	}

	return nil
}

func getIntPtrValue(value string, base int) *int {
	// nolint:gomnd
	if int64Val, err := strconv.ParseInt(value, base, 32); err == nil {
		intVal := int(int64Val)
		return &intVal
	}

	return nil
}

func getFileModeValue(value string, def os.FileMode) os.FileMode {
	// nolint:gomnd
	if uint32Val, err := strconv.ParseUint(value, 8, 32); err == nil {
		return os.FileMode(uint32Val)
	}

	return def
}

// ResolveUserFriendlyPath replaces ~ in a path with a home directory.
func ResolveUserFriendlyPath(path string, relativeToHome bool) string {
	home, _ := os.UserHomeDir()
	if home != "" && strings.HasPrefix(path, "~") {
		return home + path[1:]
	}

	if filepath.IsAbs(path) {
		return path
	}

	if relativeToHome {
		return filepath.Join(home, path)
	}

	return path
}
