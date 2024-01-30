package logging

import (
	"os"

	"github.com/sirupsen/logrus"
)

// FileHook logs errors into a specified file.
type FileHook struct {
	File *os.File
}

func (hook *FileHook) Levels() []logrus.Level {
	return []logrus.Level{logrus.ErrorLevel}
}

func (hook *FileHook) Fire(entry *logrus.Entry) error {
	_, err := hook.File.WriteString(entry.Message + "\n")
	return err
}
