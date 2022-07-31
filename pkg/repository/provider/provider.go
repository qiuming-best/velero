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

package provider

import (
	"context"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

// RepoParam includes the parameters to manipulate a backup repository
// SubDir is used to generate the path in the backup storage
type RepoParam struct {
	SubDir         string
	BackupLocation *velerov1api.BackupStorageLocation
}

type Provider interface {
	//InitRepo is to initialize a repository from a new storage place
	InitRepo(ctx context.Context, param RepoParam) error

	//ConnectToRepo is to establish the connection to a
	//storage place that a repository is already initialized
	ConnectToRepo(ctx context.Context, param RepoParam) error

	//PrepareRepo is a combination of InitRepo and ConnectToRepo,
	//it may do initializing + connecting, connecting only if the repository
	//is already initialized, or do nothing if the repository is already connected
	PrepareRepo(ctx context.Context, param RepoParam) error

	//PruneRepo does a full prune/maintenance of the repository
	PruneRepo(ctx context.Context, param RepoParam) error

	//PruneRepoQuick does a quick prune/maintenance of the repository if available
	PruneRepoQuick(ctx context.Context, param RepoParam) error

	//EnsureUnlockRepo esures to remove any stale file locks in the storage
	EnsureUnlockRepo(ctx context.Context, param RepoParam) error

	//Forget is to delete a snapshot from the repository
	Forget(ctx context.Context, snapshotID string, param RepoParam) error
}
