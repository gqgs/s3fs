package s3fs

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	_ = (fs.NodeOpener)((*s3File)(nil))
	_ = (fs.NodeGetattrer)((*s3File)(nil))
	_ = (fs.NodeReader)((*s3File)(nil))
	_ = (fs.NodeWriter)((*s3File)(nil))
)

type s3File struct {
	fs.Inode
	s3Client     *s3.S3
	mu           *sync.Mutex
	reader       io.Reader
	key          string
	bucket       string
	data         []byte
	size         uint64
	lastModified uint64
}

func newS3File(key, bucket string, size int64, lastModified time.Time, s3client *s3.S3) *s3File {
	return &s3File{
		mu:           new(sync.Mutex),
		key:          key,
		bucket:       bucket,
		size:         uint64(size),
		lastModified: uint64(lastModified.Unix()),
		s3Client:     s3client,
	}
}

func (f *s3File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("file open call", "key", f.key)
	if f.reader != nil {
		slog.Debug("file open call with open reader. returning", "key", f.key)
		return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
	}

	object, err := f.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    &f.key,
	})
	if err != nil {
		slog.Error("file open error", "key", f.key, "err", err.Error())
		return nil, 0, fs.ToErrno(err)
	}

	f.reader = object.Body
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *s3File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	size := min(int(int64(f.size)-off), len(dest))

	slog.Debug("file read call", "key", f.key, "offset", off, "len(dest)", len(dest), "object_size", f.size, "size", size)

	if len(dest) > size {
		dest = dest[:size]
	}

	n, err := io.ReadFull(f.reader, dest)
	if err != nil {
		slog.Error("file read error", "key", f.key, "err", err.Error(), "read_bytes", n, "len(dest)", len(dest))
		return nil, fs.ToErrno(err)
	}

	slog.Debug("file read executed", "key", f.key, "read_bytes", n, "len(dest)", len(dest))
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (f *s3File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("file getattr call", "key", f.key)
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = f.lastModified
	out.Atime = f.lastModified
	out.Ctime = f.lastModified
	out.Size = f.size
	return 0
}

func (f *s3File) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	slog.Debug("write file call", "off", off, "len(data)", len(data))

	f.data = append(f.data, data...)

	return uint32(len(data)), fs.OK
}

func (f *s3File) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	slog.Debug("write flush call", "fh", fh)

	if len(f.data) == 0 {
		return fs.OK
	}

	if _, err := f.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Key:    &f.key,
		Bucket: &f.bucket,
		Body:   bytes.NewReader(f.data),
	}); err != nil {
		slog.Error("error flushing file put object error", "err", err)
		return fs.ToErrno(err)
	}

	f.size = uint64(len(f.data))
	f.data = nil
	return fs.OK
}
