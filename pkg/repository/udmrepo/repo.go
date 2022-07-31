/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package udmrepo

import (
	"context"
	"io"
	"time"
)

type ID string

// ManifestEntryMetadata is the metadata describing one manifest data
type ManifestEntryMetadata struct {
	ID      ID                // The ID of the manifest data
	Length  int32             // The data size of the manifest data
	Labels  map[string]string // Labels saved together with the manifest data
	ModTime time.Time         // Modified time of the manifest data
}

type RepoManifest struct {
	Payload  interface{}            // The user data of manifest
	Metadata *ManifestEntryMetadata // The metadata data of manifest
}

type ManifestFilter struct {
	Labels map[string]string
}

const (
	// Below consts descrbe the data type of one object.
	// Metadata: This type describes how the data is organized.
	// For a file system backup, the Metadata describes a Dir or File.
	// For a block backup, the Metadata describes a Disk and its incremental link.
	ObjectDataTypeUnknown  int = 0
	ObjectDataTypeMetadata int = 1
	ObjectDataTypeData     int = 2

	// Below consts defines the access mode when creating an object for write
	ObjectDataAccessModeUnknown int = 0
	ObjectDataAccessModeFile    int = 1
	ObjectDataAccessModeBlock   int = 2

	ObjectDataBackupModeUnknown int = 0
	ObjectDataBackupModeFull    int = 1
	ObjectDataBackupModeInc     int = 2
)

// ObjectWriteOptions defines the options when creating an object for write
type ObjectWriteOptions struct {
	FullPath    string // Full logical path of the object
	DataType    int    // OBJECT_DATA_TYPE_*
	Description string // A description of the object, could be empty
	Prefix      ID     // A prefix of the name used to save the object
	AccessMode  int    // OBJECT_DATA_ACCESS_*
	BackupMode  int    // OBJECT_DATA_BACKUP_*
}

// OwnershipOptions is used to add some access control to the unified repository.
// For example, some privileged operations of the unified repository can be done by the
// repository owner only; the data of a backup may be manipulated by the backup owner
// who created it only. It is optional for a backup repository to support this ownership control.
type OwnershipOptions struct {
	Username      string
	DomainName    string
	FullQualified string
}

type RepoOptions struct {
	// A repository specific string to identify a backup storage, i.e., "s3", "filesystem"
	StorageType string
	// Backup repository password, if any
	RepoPassword string
	// A custom path to save the repository's configuration, if any
	ConfigFilePath string
	// The ownership for the current repository operation
	Ownership OwnershipOptions
	// Other repository specific options
	GeneralOptions map[string]string
	// Storage specific options
	StorageOptions map[string]string

	// Description of the backup repository
	Description string
}

// BackupRepoService is used to initialize, open or maintain a backup repository
type BackupRepoService interface {
	// Create a backup repository or connect to an existing backup repository.
	// repoOption: option to the backup repository and the underlying backup storage.
	// createNew: indicates whether to create a new or connect to an existing backup repository.
	Init(ctx context.Context, repoOption RepoOptions, createNew bool) error

	// Open an backup repository that has been created/connected.
	// repoOption: options to open the backup repository and the underlying storage.
	Open(ctx context.Context, repoOption RepoOptions) (BackupRepo, error)

	// Periodically called to maintain the backup repository to eliminate redundant data and improve performance.
	// repoOption: options to maintain the backup repository.
	Maintain(ctx context.Context, repoOption RepoOptions) error
}

// BackupRepo provides the access to the backup repository
type BackupRepo interface {
	// Open an existing object for read.
	// id: the object's unified identifier.
	OpenObject(ctx context.Context, id ID) (ObjectReader, error)

	// Get a manifest data.
	GetManifest(ctx context.Context, id ID, mani *RepoManifest) error

	// Get one or more manifest data that match the given labels
	FindManifests(ctx context.Context, filter ManifestFilter) ([]*ManifestEntryMetadata, error)

	// Create a new object and return the object's writer interface.
	// return: A unified identifier of the object on success.
	NewObjectWriter(ctx context.Context, opt ObjectWriteOptions) ObjectWriter

	// Save a manifest object
	PutManifest(ctx context.Context, mani RepoManifest) (ID, error)

	// Delete a manifest object
	DeleteManifest(ctx context.Context, id ID) error

	// Flush all the backup repository data
	Flush(ctx context.Context) error

	// Get the local time of the backup repository. It may be different from the time of the caller
	Time() time.Time

	// Close the backup repository
	Close(ctx context.Context) error
}

type ObjectReader interface {
	io.ReadCloser
	io.Seeker

	// Length returns the logical size of the object
	Length() int64
}

type ObjectWriter interface {
	io.WriteCloser

	// For some cases, i.e. block incremental, the object is not written sequentially
	io.Seeker

	// Periodically called to preserve the state of data written to the repo so far.
	// Return a unified identifier that represent the current state.
	// An empty ID could be returned on success if the backup repository doesn't support this.
	Checkpoint() (ID, error)

	// Wait for the completion of the object write.
	// Result returns the object's unified identifier after the write completes.
	Result() (ID, error)
}
