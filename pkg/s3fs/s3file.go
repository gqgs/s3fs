package s3fs

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	_ "github.com/mattn/go-sqlite3"
)

var (
	_ = (fs.NodeOpener)((*s3File)(nil))
	_ = (fs.NodeGetattrer)((*s3File)(nil))
	_ = (fs.NodeReader)((*s3File)(nil))
)

type s3File struct {
	*sync.Mutex
	fs.Inode
	*s3.Object
	s3Client *s3.S3
	reader   io.Reader
	bucket   string
}

// Open lazily unpacks zip data
func (f *s3File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if f.reader != nil {
		return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
	}
	slog.Debug("file open call", "key", *f.Object.Key)
	object, err := f.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    f.Key,
	})
	if err != nil {
		slog.Error("file open error", "key", *f.Object.Key, "err", err.Error())
		return nil, 0, syscall.EIO
	}

	f.reader = object.Body
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *s3File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.Lock()
	defer f.Unlock()

	size := min(int(aws.Int64Value(f.Object.Size)-off), len(dest))

	slog.Debug("file read call", "key", *f.Object.Key, "offset", off, "len(dest)", len(dest), "object_size", *f.Object.Size, "size", size)

	if len(dest) > size {
		dest = dest[:size]
	}

	n, err := io.ReadFull(f.reader, dest)
	if err != nil {
		slog.Error("file read error", "key", *f.Object.Key, "err", err.Error(), "read_bytes", n, "len(dest)", len(dest))
		return nil, syscall.EIO
	}

	slog.Debug("file read executed", "key", *f.Object.Key, "read_bytes", n, "len(dest)", len(dest))
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (f *s3File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("file getattr call", "key", *f.Object.Key)
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = uint64(f.LastModified.Unix())
	out.Atime = uint64(f.LastModified.Unix())
	out.Ctime = uint64(f.LastModified.Unix())
	out.Size = uint64(aws.Int64Value(f.Size))
	const bs = 4096
	out.Blksize = bs
	out.Blocks = (out.Size + bs - 1) / bs
	return 0
}
