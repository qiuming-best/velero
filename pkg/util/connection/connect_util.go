package connection

import (
	"strconv"

	"github.com/sirupsen/logrus"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

const (
	// insecureSkipTLSVerifyKey is the flag in BackupStorageLocation's config
	// to indicate whether to skip TLS verify to setup insecure HTTPS connection.
	insecureSkipTLSVerifyKey = "insecureSkipTLSVerify"
)

// GetInsecureSkipTLSVerifyFromBSL get insecureSkipTLSVerify flag from BSL configuration,
// Then return --insecure-tls flag with boolean value as result.
func GetInsecureSkipTLSVerifyFromBSL(backupLocation *velerov1api.BackupStorageLocation, logger logrus.FieldLogger) bool {
	if backupLocation == nil {
		logger.Info("bsl is nil. return empty.")
		return false
	}

	if insecure, _ := strconv.ParseBool(backupLocation.Spec.Config[insecureSkipTLSVerifyKey]); insecure {
		logger.Debugf("set insecure-tls is set to true according to BSL %s config", backupLocation.Name)
		return true
	}

	return false
}
