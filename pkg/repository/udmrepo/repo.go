package udmrepo

import (
	"io"
	"time"
)

type ID string

///ManifestEntryMetadata is the metadata descrbing one manifest data
type ManifestEntryMetadata struct {
	ID      ID                `json:"id"`     ///The ID of the manifest data
	Length  int               `json:"length"` ///The data size of the manifest data
	Labels  map[string]string `json:"labels"` ///Labels saved together with the manifest data
	ModTime time.Time         `json:"mtime"`  ///Modified time of the manifest data
}

type RepoManifest struct {
	Payload  interface{}            ///The user data of manifest
	Metadata *ManifestEntryMetadata ///The metadata data of manifest
}

type ManifestFilter struct {
	Labels map[string]string
}

const (
	///Below consts descrbe the data type of one object.
	///Metadata: This type describes how the data is organized.
	///For a file system backup, the Metadata describes a Dir or File
	///For a block backup, the Metadata describes a Disk and its incremental link
	OBJECT_DATA_TYPE_UNKNOWN  int = 0
	OBJECT_DATA_TYPE_METADATA int = 1
	OBJECT_DATA_TYPE_DATA     int = 2

	///Below consts defines the access mode when creating an object for write
	OBJECT_DATA_ACCESS_MODE_UNKNOWN int = 0
	OBJECT_DATA_ACCESS_MODE_FILE    int = 1
	OBJECT_DATA_ACCESS_MODE_BLOCK   int = 2
)

///ObjectWriteOptions defines the options when creating an object for write
type ObjectWriteOptions struct {
	FullPath    string `json:"fullpath"`    ///Full logical path of the object
	DataType    int    `json:"datatype"`    ///OBJECT_DATA_TYPE_*
	Description string `json:"description"` ///A description of the object, could be empty
	Prefix      ID     `json:"prefix"`      ///A prefix of the name used to save the object
	AccessMode  int    `json:"accessmode"`  ///OBJECT_DATA_ACCESS_*
}

type RepoOptions struct {
	GeneralOptions string
	StorageOptions string
}

///BackupRepoService is used to create or open a backup repository
type BackupRepoService interface {
	///Create a backup repository or connect to an existing backup repository
	///repoOption: option to create/connect to the backup repository and the underlying backup storage
	///outputConfigFile: a path to generate the config file
	///createNew: indicates whether to create a new or connect to an existing backup repository
	InitBackupRepo(repoOption RepoOptions, outputConfigFile string, createNew bool) error

	///Open an backup repository that has been created/connected
	///configFile: includes all the varabiles to open the backup repository and the underlying storage
	///password: the password for the backup repository
	OpenBackupRepo(configFile, password string) (BackupRepo, error)

	///Periodically called to maintain the backup repository to eliminate redundant data and improve performance
	MaintainBackupRepo(configFile, password string, full bool) error
}

///BackupRepo provides the access to the backup repository
type BackupRepo interface {
	///Open an existing object for read
	///id: the object's unified identifier
	OpenObject(id ID) (ObjectReader, error)

	///Get a manifest data
	GetManifest(id ID, mani *RepoManifest) error

	///Get one or more manifest data that match the given labels
	FindManifests(filter ManifestFilter) ([]*ManifestEntryMetadata, error)

	///Create a new object and return the object's writer interface
	///return: A unified identifier of the object on success
	NewObjectWriter(opt ObjectWriteOptions) ObjectWriter

	///Save a manifest object
	PutManifest(mani RepoManifest) (ID, error)

	///Delete a manifest object
	DeleteManifest(id ID) error

	///Flush all the backup repository data
	Flush() error

	///Get the local time of the backup repository. It may be different from the time of the caller
	Time() time.Time

	///Close the backup repository
	Close() error

	//NewWriter(ctx context.Context, option repo.WriteSessionOptions) (context.Context, repo.RepositoryWriter, error)
}

type ObjectReader interface {
	io.ReadCloser
	io.Seeker

	///Length returns the logical size of the object
	Length() int64
}

type ObjectWriter interface {
	io.WriteCloser

	///For some cases, i.e. block incremental, the object is not written sequentially
	io.Seeker
	Checkpoint() (ID, error)
	///Wait for the completion of the object write
	///Result returns the object's unified identifier after the write completes
	Result() (ID, error)
}
