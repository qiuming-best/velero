package kopialib

import (
	"os"
	"path/filepath"
)

func RepoConfigFileName(configFile string) string {
	if configFile != "" {
		return configFile
	}
	return filepath.Join(os.Getenv("HOME"), ".config", "kopia", "repository.config")
}
