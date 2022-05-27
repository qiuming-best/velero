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
