package kopialib

import (
	"context"
	"encoding/json"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
	"github.com/pkg/errors"
)

type kopiaAzureFlags struct {
	azOptions azure.Options
}

func (c *kopiaAzureFlags) Setup(flags string) error {
	err := json.Unmarshal([]byte(flags), &c.azOptions)
	if err != nil {
		return errors.Wrap(err, "Failed to unmarshal Azure options")
	}

	return nil
}

func (c *kopiaAzureFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	return azure.New(ctx, &c.azOptions)
}
