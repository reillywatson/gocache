package local

import (
	"context"
	"io"
)

type Storage interface {
	Kind() string
	Start(ctx context.Context) error
	Get(ctx context.Context, actionID string) (outputID, diskPath string, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error)
	Close() error
	Summary() string
}
