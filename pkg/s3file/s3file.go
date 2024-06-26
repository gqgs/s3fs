package s3file

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	_ = (fs.NodeOpener)((*file)(nil))
	_ = (fs.NodeGetattrer)((*file)(nil))
	_ = (fs.NodeReader)((*file)(nil))
	_ = (fs.NodeWriter)((*file)(nil))
)

type file struct {
	fs.Inode
	s3wrapper s3wrapper.Wrapper
	mu        *sync.Mutex
	// reader       io.Reader
	key          string
	data         []byte
	size         uint64
	lastModified uint64
}

func New(key string, lastModified time.Time, size int64, s3wrapper s3wrapper.Wrapper) *file {
	return &file{
		mu:           new(sync.Mutex),
		key:          key,
		lastModified: uint64(lastModified.Unix()),
		size:         uint64(size),
		s3wrapper:    s3wrapper,
	}
}

func (f *file) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("file open call", "key", f.key)
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *file) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	size := min(int(int64(f.size)-off), len(dest))

	slog.Debug("file read call", "key", f.key, "offset", off, "len(dest)", len(dest), "object_size", f.size, "size", size)

	if len(dest) > size {
		dest = dest[:size]
	}

	n, err := f.s3wrapper.DownloadRange(ctx, f.key, dest, int(off), size)
	if err != nil {
		slog.Error("file download error", "key", f.key, "err", err.Error(), "read_bytes", n, "len(dest)", len(dest))
		return nil, fs.ToErrno(err)
	}

	slog.Debug("file read executed", "key", f.key, "read_bytes", n, "len(dest)", len(dest))
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (f *file) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("file getattr call", "key", f.key)
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = f.lastModified
	out.Atime = f.lastModified
	out.Ctime = f.lastModified
	out.Size = f.size
	return fs.OK
}

func (f *file) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	slog.Debug("write file call", "off", off, "len(data)", len(data))

	f.data = append(f.data, data...)

	return uint32(len(data)), fs.OK
}

func (f *file) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	slog.Debug("write flush call", "fh", fh, "key", f.key)

	if len(f.data) == 0 {
		return fs.OK
	}

	if err := f.s3wrapper.UploadFile(ctx, f.key, bytes.NewReader(f.data)); err != nil {
		slog.Error("error flushing file put object error", "err", err)
		return fs.ToErrno(err)
	}

	f.size = uint64(len(f.data))
	f.data = nil
	return fs.OK
}
