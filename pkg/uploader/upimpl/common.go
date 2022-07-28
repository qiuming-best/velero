/*
Copyright The Velero Contributors.

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

package upimpl

import (
	"encoding/json"
	"os"
	"os/user"
	"strings"

	"github.com/pkg/errors"
)

type SnapshotInfo struct {
	ID   string `json:"id"`
	Size int64  `json:"Size"`
}

type UploaderProgress struct {
	TotalBytes int64 `json:"totalBytes,omitempty"`
	BytesDone  int64 `json:"doneBytes,omitempty"`
}

func MarshalSnapshotInfo(snapInfo *SnapshotInfo) (string, error) {
	snap, err := json.Marshal(snapInfo)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal snapshot info")
	}

	return string(snap), nil
}

func GetDefaultUserName() string {
	currentUser, err := user.Current()
	if err != nil {
		return "nobody"
	}

	u := currentUser.Username

	return u
}

func GetDefaultHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "nohost"
	}

	hostname = strings.ToLower(strings.Split(hostname, ".")[0])

	return hostname
}
