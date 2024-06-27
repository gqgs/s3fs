package s3directory

import (
	"context"
	"log/slog"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gqgs/s3fs/pkg/s3file"
	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var _ = (directoryInterface)((*directory)(nil))

type directoryInterface interface {
	fs.NodeGetattrer
	fs.NodeCreater
	fs.NodeMkdirer
}

type directory struct {
	fs.Inode
	s3wrapper  s3wrapper.Wrapper
	updateTime uint64
	path       string
	logger     *slog.Logger
}

func New(path string, lastUpdated time.Time, s3wrapper s3wrapper.Wrapper) *directory {
	return &directory{
		updateTime: uint64(lastUpdated.Unix()),
		path:       path,
		s3wrapper:  s3wrapper,
		logger:     slog.Default().WithGroup("s3directory"),
	}
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

	file := s3file.New(key, lastModified, size, d.s3wrapper)
	child := d.NewPersistentInode(ctx, file, fs.StableAttr{})
	d.AddChild(name, child, true)
	return child, nil, 0, fs.OK
}

func (d *directory) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.logger.Debug("directory mkdir call", "name", name, "mode", mode, "d.path", d.path)

	path := filepath.Join(d.path, name)
	child := d.NewPersistentInode(ctx, New(path, time.Now(), d.s3wrapper),
		fs.StableAttr{Mode: syscall.S_IFDIR})
	d.AddChild(name, child, true)
	return child, fs.OK
}
