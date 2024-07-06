package storage

import (
	"context"
	"io"
	"time"
)

type Storage interface {
	io.Closer
	InsertPath(ctx context.Context, path string, updatedAt time.Time) (*time.Time, error)
	UpdateAccess(ctx context.Context, path string) error
	UpdateModified(ctx context.Context, path string) error
}

type ModifiedUpdater interface {
	UpdateModified(ctx context.Context) error
}

var storage Storage

func Default() Storage {
	return storage
}
