package s3root

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"log"
	"log/slog"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gqgs/s3fs/pkg/s3directory"
	"github.com/gqgs/s3fs/pkg/s3file"
	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/hanwen/go-fuse/v2/fs"
)

var (
	_ = (fs.NodeOnAdder)((*root)(nil))
	_ = (fs.NodeCreater)((*root)(nil))
	_ = (fs.NodeMkdirer)((*root)(nil))
)

type root struct {
	fs.InodeEmbedder
	fs.NodeCreater
	fs.NodeMkdirer

	db        *sql.DB
	s3Client  *s3.S3
	bucket    string
	s3wrapper s3wrapper.Wrapper
	logger    *slog.Logger
}

func New(db *sql.DB, s3wrapper s3wrapper.Wrapper) (*root, error) {
	return &root{
		InodeEmbedder: s3directory.New("", time.Now(), s3wrapper),
		db:            db,
		s3wrapper:     s3wrapper,
		logger:        slog.Default().WithGroup("s3root"),
	}, nil
}

func (r *root) OnAdd(ctx context.Context) {
	r.logger.Debug("onAdd called")

	objects, err := r.s3wrapper.ListObjects(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, object := range objects {
		dir, base := filepath.Split(aws.StringValue(object.Key))

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

			ch := p.GetChild(component)
			if ch == nil {
				digest := sha1.Sum([]byte(path))
				hexdigest := hex.EncodeToString(digest[:])

				if _, err := r.db.ExecContext(ctx, "INSERT OR IGNORE INTO files (id, path) VALUES (?, ?)", hexdigest, path); err != nil {
					r.logger.Error("failed inserting data into database", "err", err.Error())
					log.Fatal(err)
				}
				var timestamp time.Time
				row := r.db.QueryRowContext(ctx, "SELECT updated_at FROM files WHERE id = ?", hexdigest)
				if err := row.Scan(&timestamp); err != nil {
					r.logger.Error("failed query database", "err", err.Error())
					log.Fatal(err)
				}

				r.logger.Debug("creating directory inode", "dir", dir, "hexdigest", hexdigest, "updated_at", timestamp)

				// Create a directory
				ch = p.NewPersistentInode(ctx, s3directory.New(path, timestamp, r.s3wrapper),
					fs.StableAttr{Mode: syscall.S_IFDIR})
				// Add it
				p.AddChild(component, ch, true)

			}

			p = ch
		}

		key := aws.StringValue(object.Key)
		size := aws.Int64Value(object.Size)
		lastModified := aws.TimeValue(object.LastModified)
		file := s3file.New(key, lastModified, size, r.s3wrapper)

		// Create the file. The Inode must be persistent,
		// because its life time is not under control of the
		// kernel.
		child := p.NewPersistentInode(ctx, file, fs.StableAttr{})

		// And add it
		p.AddChild(base, child, true)
	}
}
