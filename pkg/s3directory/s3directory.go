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

var (
	_ = (fs.NodeGetattrer)((*directory)(nil))
	_ = (fs.NodeCreater)((*directory)(nil))
	_ = (fs.NodeMkdirer)((*directory)(nil))
)

type directory struct {
	fs.Inode
	s3wrapper  s3wrapper.Wrapper
	updateTime uint64
	path       string
}

func New(path string, lastUpdated time.Time, s3wrapper s3wrapper.Wrapper) *directory {
	return &directory{
		updateTime: uint64(lastUpdated.Unix()),
		path:       path,
		s3wrapper:  s3wrapper,
	}
}

func (d *directory) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("directory getattr call", "path", d.path)
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = d.updateTime
	out.Atime = d.updateTime
	out.Ctime = d.updateTime
	return fs.OK
}

func (f *directory) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("directory create call", "name", name, "flags", flags, "mode", mode, "f.path", f.path)

	path := filepath.Join(f.path, name)

	key := path
	size := int64(0)
	lastModified := time.Now()
	file := s3file.New(key, lastModified, size, f.s3wrapper)

	child := f.NewPersistentInode(ctx, file, fs.StableAttr{})

	f.AddChild(name, child, true)

	return child, nil, 0, fs.OK
}

func (f *directory) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	slog.Debug("directory mkdir call", "name", name, "mode", mode, "f.path", f.path)

	path := filepath.Join(f.path, name)

	child := f.NewPersistentInode(ctx, New(path, time.Now(), f.s3wrapper),
		fs.StableAttr{Mode: syscall.S_IFDIR})

	f.AddChild(name, child, true)
	return child, fs.OK
}
