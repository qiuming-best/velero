package kopialib

import (
	"context"
	"encoding/json"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/s3"
	"github.com/pkg/errors"
)

type kopiaS3Flags struct {
	s3options s3.Options
}

func (c *kopiaS3Flags) Setup(flags string) error {
	err := json.Unmarshal([]byte(flags), &c.s3options)
	if err != nil {
		return errors.Wrap(err, "Failed to unmarshal s3 options")
	}

	return nil
}

func (c *kopiaS3Flags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	return s3.New(ctx, &c.s3options)
}
