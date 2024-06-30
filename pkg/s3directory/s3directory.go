package s3directory

import (
	"context"
	"log/slog"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gqgs/s3fs/pkg/s3file"
	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/gqgs/s3fs/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var _ = (directoryInterface)((*directory)(nil))

type directoryInterface interface {
	fs.NodeGetattrer
	fs.NodeCreater
	fs.NodeMkdirer
	fs.NodeUnlinker
}

type directory struct {
	fs.Inode
	s3wrapper  s3wrapper.Wrapper
	updateTime uint64
	path       string
	logger     *slog.Logger
}

func New(ctx context.Context, path string, s3wrapper s3wrapper.Wrapper) (*directory, error) {
	logger := slog.Default().WithGroup("s3directory")
	logger.Debug("creating new directory", "path", path)

	lastUpdated, err := storage.Default().InsertPath(ctx, path, time.Now())
	if err != nil {
		logger.Error("error creating new directory", "path", path, "err", err)
		return nil, err
	}

	logger.Debug("created directory inode", "path", path, "lastUpdated", lastUpdated)
	return &directory{
		updateTime: uint64(lastUpdated.Unix()),
		path:       path,
		s3wrapper:  s3wrapper,
		logger:     logger,
	}, nil
}

func (d *directory) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	d.logger.Debug("directory getattr call", "path", d.path)

	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = d.updateTime
	out.Atime = d.updateTime
	out.Ctime = d.updateTime
	return fs.OK
}

func (d *directory) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	d.logger.Debug("directory create call", "name", name, "flags", flags, "mode", mode, "d.path", d.path)

	path := filepath.Join(d.path, name)
	key := path
	size := int64(0)
	lastModified := time.Now()

	file, err := s3file.New(ctx, key, lastModified, size, d.s3wrapper)
	if err != nil {
		d.logger.Error("error creating new file", "key", key, "err", err)
		return nil, nil, 0, fs.ToErrno(err)
	}

	child := d.NewPersistentInode(ctx, file, fs.StableAttr{})
	d.AddChild(name, child, true)
	return child, nil, 0, fs.OK
}

func (d *directory) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.logger.Debug("directory mkdir call", "name", name, "mode", mode, "d.path", d.path)

	path := filepath.Join(d.path, name)
	newDirectory, err := New(ctx, path, d.s3wrapper)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	child := d.NewPersistentInode(ctx, newDirectory, fs.StableAttr{Mode: syscall.S_IFDIR})
	d.AddChild(name, child, true)
	return child, fs.OK
}

func (d *directory) Unlink(ctx context.Context, name string) syscall.Errno {
	d.logger.Debug("directory unlink call", "name", name)

	child := d.Inode.GetChild(name)
	if child == nil {
		d.logger.Warn("child not found", "name", name)
		return fs.OK
	}

	// TODO: handle unlink of directories
	key := filepath.Join(d.path, name)
	if err := d.s3wrapper.DeleteObject(ctx, key); err != nil {
		d.logger.Error("error unlinking file", "key", key, "name", name)
		return fs.ToErrno(err)
	}

	child.ForgetPersistent()

	return fs.OK
}
