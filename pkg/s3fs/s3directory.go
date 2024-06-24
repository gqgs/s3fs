package s3fs

import (
	"context"
	"log/slog"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	_ = (fs.NodeGetattrer)((*s3Directory)(nil))
	_ = (fs.NodeCreater)((*s3Directory)(nil))
)

type s3Directory struct {
	fs.Inode
	updateTime uint64
	path       string
	s3Client   *s3.S3
	bucket     string
}

func (d *s3Directory) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("directory getattr call", "path", d.path)
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = d.updateTime
	out.Atime = d.updateTime
	out.Ctime = d.updateTime
	return fs.OK
}

func (f *s3Directory) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("directory create call", "name", name, "flags", flags, "mode", mode, "out", out)

	path := filepath.Join(f.path, name)

	// TODO: handle directories
	//ch = f.NewPersistentInode(ctx, &s3Directory{updateTime: uint64(time.Now().Unix()), path: path},
	//	fs.StableAttr{Mode: syscall.S_IFDIR})

	key := path
	size := int64(0)
	lastModified := time.Now()
	file := newS3File(key, f.bucket, size, lastModified, f.s3Client)

	child := f.NewPersistentInode(ctx, file, fs.StableAttr{})

	return child, nil, 0, fs.OK
}
