package s3file

import (
	"bytes"
	"context"
	"log/slog"
	"syscall"
	"time"

	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/gqgs/s3fs/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var _ = (fileInterface)((*file)(nil))

type fileInterface interface {
	fs.NodeOpener
	fs.NodeGetattrer
	fs.NodeReader
	fs.NodeWriter
	fs.NodeSetattrer
}

type file struct {
	fs.Inode
	s3wrapper    s3wrapper.Wrapper
	key          string
	data         []byte
	size         uint64
	modifiedTime uint64
	logger       *slog.Logger
	db           storage.Storage
}

func New(ctx context.Context, key string, modifiedTime time.Time, size int64, s3wrapper s3wrapper.Wrapper) (*file, error) {
	logger := slog.Default().WithGroup("s3file")
	logger.Debug("creating new file", "key", key, "size", size)

	db := storage.Default()
	_, err := db.InsertPath(ctx, key, modifiedTime)
	if err != nil {
		logger.Error("error creating new file", "key", key, "err", err)
		return nil, err
	}

	logger.Debug("created new file", "key", key, "size", size)

	return &file{
		key:          key,
		modifiedTime: uint64(modifiedTime.Unix()),
		size:         uint64(size),
		s3wrapper:    s3wrapper,
		logger:       logger,
		db:           db,
	}, nil
}

func (f *file) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	f.logger.Debug("file open call", "key", f.key)
	if err := f.db.UpdateAccess(ctx, f.key); err != nil {
		f.logger.Error("failed updating accessed at", "key", f.key, "err", err)
	}
	return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
}

func (f *file) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	size := min(int(int64(f.size)-off), len(dest))

	f.logger.Debug("file read call", "key", f.key, "offset", off, "len(dest)", len(dest), "object_size", f.size, "size", size)

	if len(dest) > size {
		dest = dest[:size]
	}

	n, err := f.s3wrapper.DownloadRange(ctx, f.key, dest, int(off), size)
	if err != nil {
		f.logger.Error("file download error", "key", f.key, "err", err, "read_bytes", n, "len(dest)", len(dest))
		return nil, fs.ToErrno(err)
	}

	f.logger.Debug("file read executed", "key", f.key, "read_bytes", n, "len(dest)", len(dest))
	return fuse.ReadResultData(dest[:n]), fs.OK
}

func (f *file) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.logger.Debug("file getattr call", "key", f.key)

	// TODO: set more strict permissions here
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = f.modifiedTime
	out.Atime = f.modifiedTime
	out.Ctime = f.modifiedTime
	out.Size = f.size
	out.SetTimeout(time.Second * 5)
	return fs.OK
}

func (f *file) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	f.logger.Debug("write file call", "off", off, "len(data)", len(data))

	f.data = append(f.data, data...)
	return uint32(len(data)), fs.OK
}

func (f *file) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.logger.Debug("write flush call", "fh", fh, "key", f.key)

	if len(f.data) == 0 {
		return fs.OK
	}

	if err := f.s3wrapper.UploadFile(ctx, f.key, bytes.NewReader(f.data)); err != nil {
		f.logger.Error("error flushing file put object error", "err", err)
		return fs.ToErrno(err)
	}

	if err := f.updateModified(ctx); err != nil {
		f.logger.Error("failed to update modified at", "err", err)
	}

	f.size = uint64(len(f.data))
	f.data = nil
	return fs.OK
}

func (f *file) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	f.logger.Debug("file setattr call", "key", f.key)

	out.Mode = in.Mode
	out.Mtime = in.Mtime
	out.Atime = in.Atime
	out.Ctime = in.Ctime
	out.Size = in.Size
	out.SetTimeout(time.Minute)

	return fs.OK
}

func (f *file) updateModified(ctx context.Context) error {
	f.logger.Debug("file updating mofidied at", "key", f.key)

	if err := f.db.UpdateModified(ctx, f.key); err != nil {
		return err
	}

	f.modifiedTime = uint64(time.Now().Unix())

	_, parent := f.Inode.Parent()
	if parent == nil {
		return nil
	}

	if p, ok := parent.Operations().(storage.ModifiedUpdater); ok {
		return p.UpdateModified(ctx)
	}

	return nil
}
