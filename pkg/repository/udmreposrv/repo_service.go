package udmreposrv

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo"
	"github.com/vmware-tanzu/velero/pkg/repository/udmrepo/storage/kopialib"
)

func CreateUdmrepoService(ctx context.Context, logger logrus.FieldLogger) udmrepo.BackupRepoService {
	return kopialib.NewKopiaRepoService(ctx, logger)
}
