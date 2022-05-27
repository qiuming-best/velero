package kopialib

import (
	"context"
	"encoding/json"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
	"github.com/pkg/errors"
)

type kopiaGCSFlags struct {
	options gcs.Options
}

func (c *kopiaGCSFlags) Setup(flags string) error {
	err := json.Unmarshal([]byte(flags), &c.options)
	if err != nil {
		return errors.Wrap(err, "Failed to unmarshal gcs options")
	}

	return nil
}

func (c *kopiaGCSFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	return gcs.New(ctx, &c.options)
}
