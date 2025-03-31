package remote

import (
	"context"
	"io"
)

const (
	outputIDMetadataKey = "outputid"
)

type Storage interface {
	Kind() string
	Start(ctx context.Context) error
	Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error)
	Close() error
	Summary() string
}
