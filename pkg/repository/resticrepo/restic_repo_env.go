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

package resticrepo

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/vmware-tanzu/velero/internal/credentials"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/repository/repoconfig"
	"github.com/vmware-tanzu/velero/pkg/util/filesystem"
)

const (
	// credentialsFileKey is the key within a BSL config that is checked to see if
	// the BSL is using its own credentials, rather than those in the environment
	credentialsFileKey = "credentialsFile"
)

// cmdEnv returns a list of environment variables (in the format var=val) that
// should be used when running a repository command for a particular backend provider.
// This list is the current environment, plus any provider-specific variables repository needs.
func CmdEnv(backupLocation *velerov1api.BackupStorageLocation, credentialFileStore credentials.FileStore) ([]string, error) {
	env := os.Environ()
	customEnv := map[string]string{}
	var err error

	config := backupLocation.Spec.Config
	if config == nil {
		config = map[string]string{}
	}

	if backupLocation.Spec.Credential != nil {
		credsFile, err := credentialFileStore.Path(backupLocation.Spec.Credential)
		if err != nil {
			return []string{}, errors.WithStack(err)
		}
		config[credentialsFileKey] = credsFile
	}

	backendType := repoconfig.GetBackendType(backupLocation.Spec.Provider)

	switch backendType {
	case repoconfig.AWSBackend:
		customEnv, err = GetS3RepositoryEnvVars(config)
		if err != nil {
			return []string{}, err
		}
	case repoconfig.AzureBackend:
		customEnv, err = GetAzureRepositoryEnvVars(config)
		if err != nil {
			return []string{}, err
		}
	case repoconfig.GCPBackend:
		customEnv, err = GetGCPRepositoryEnvVars(config)
		if err != nil {
			return []string{}, err
		}
	}

	for k, v := range customEnv {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	return env, nil
}

// TempCACertFile creates a temp file containing a CA bundle
// and returns its path. The caller should generally call os.Remove()
// to remove the file when done with it.
func TempCACertFile(caCert []byte, bsl string, fs filesystem.Interface) (string, error) {
	file, err := fs.TempFile("", fmt.Sprintf("cacert-%s", bsl))
	if err != nil {
		return "", errors.WithStack(err)
	}

	if _, err := file.Write(caCert); err != nil {
		// nothing we can do about an error closing the file here, and we're
		// already returning an error about the write failing.
		file.Close()
		return "", errors.WithStack(err)
	}

	name := file.Name()

	if err := file.Close(); err != nil {
		return "", errors.WithStack(err)
	}

	return name, nil
}
