package logging

import (
	"context"

	"github.com/kopia/kopia/repo/logging"
	"github.com/sirupsen/logrus"
)

type kopiaLogImpl struct {
	logger logrus.FieldLogger
}

func SetupKopiaLog(ctx context.Context, logger logrus.FieldLogger) context.Context {
	return logging.WithLogger(ctx, func(module string) logging.Logger {
		return &kopiaLogImpl{
			logger: logger,
		}
	})
}

func (kl *kopiaLogImpl) Debugf(msg string, args ...interface{}) {
	kl.logger.Debugf(msg, args...)
}

func (kl *kopiaLogImpl) Debugw(msg string, keyValuePairs ...interface{}) {
	m := map[string]interface{}{}
	for i := 0; i+1 < len(keyValuePairs); i += 2 {
		s, ok := keyValuePairs[i].(string)
		if !ok {
			panic("not a string key")
		}

		m[s] = keyValuePairs[i+1]
	}

	kl.logger.WithFields(m).Debug(msg)
}

func (kl *kopiaLogImpl) Infof(msg string, args ...interface{}) {
	kl.logger.Infof(msg, args...)
}

func (kl *kopiaLogImpl) Warnf(msg string, args ...interface{}) {
	kl.logger.Warnf(msg, args...)
}

func (kl *kopiaLogImpl) Errorf(msg string, args ...interface{}) {
	kl.logger.Errorf(msg, args...)
}
