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
}

var storage Storage

func Default() Storage {
	return storage
}
