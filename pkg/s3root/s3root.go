package s3root

import (
	"context"
	"log"
	"log/slog"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/gqgs/s3fs/pkg/s3directory"
	"github.com/gqgs/s3fs/pkg/s3file"
	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/gqgs/s3fs/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fs"
)

var _ = (rootInterface)((*root)(nil))

type rootInterface interface {
	directory
	fs.NodeOnAdder
}

type directory interface {
	fs.InodeEmbedder
	fs.NodeCreater
	fs.NodeMkdirer
	fs.NodeGetattrer
	fs.NodeUnlinker
	storage.ModifiedUpdater
}

type root struct {
	directory
	s3wrapper s3wrapper.Wrapper
	logger    *slog.Logger
}

func New(ctx context.Context, s3wrapper s3wrapper.Wrapper) (*root, error) {
	logger := slog.Default().WithGroup("s3root")
	logger.Debug("creating new root")

	directory, err := s3directory.New(ctx, "", s3wrapper)
	if err != nil {
		logger.Error("error creating new root directory", "err", err)
		return nil, err
	}

	logger.Debug("created new root")
	return &root{
		directory: directory,
		s3wrapper: s3wrapper,
		logger:    logger,
	}, nil
}

func (r *root) OnAdd(ctx context.Context) {
	r.logger.Debug("onAdd called")

	objects, err := r.s3wrapper.ListObjects(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, object := range objects {
		dir, base := filepath.Split(aws.ToString(object.Key))

		p := r.EmbeddedInode()

		var path string
		// Add directories leading up to the file.
		for i, component := range strings.Split(dir, "/") {
			if len(component) == 0 {
				continue
			}

			if i == 0 {
				path = component
			} else {
				path = filepath.Join(path, component)
			}

			child := p.GetChild(component)
			if child == nil {
				directory, err := s3directory.New(ctx, path, r.s3wrapper)
				if err != nil {
					r.logger.Error("failed creating new directory", "err", err)
					log.Fatal(err)
				}

				// Create a directory
				child = p.NewPersistentInode(ctx, directory, fs.StableAttr{Mode: syscall.S_IFDIR})
				// Add it
				p.AddChild(component, child, false)

			}

			p = child
		}

		key := aws.ToString(object.Key)
		size := aws.ToInt64(object.Size)
		lastModified := aws.ToTime(object.LastModified)
		file, err := s3file.New(ctx, key, lastModified, size, r.s3wrapper)
		if err != nil {
			r.logger.Error("failed creating new file", "err", err)
			log.Fatal(err)
		}

		// Create the file. The Inode must be persistent,
		// because its life time is not under control of the
		// kernel.
		child := p.NewPersistentInode(ctx, file, fs.StableAttr{})

		// And add it
		p.AddChild(base, child, false)
	}
}
