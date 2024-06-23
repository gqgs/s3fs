package s3fs

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
)

var _ = (fs.NodeOnAdder)((*s3FS)(nil))

type s3FS struct {
	fs.Inode
	db       *sql.DB
	s3Client *s3.S3
	bucket   string
}

func NewFS(db *sql.DB, s3Client *s3.S3, bucket string) (*s3FS, error) {
	return &s3FS{
		db:       db,
		s3Client: s3Client,
		bucket:   bucket,
	}, nil
}

func (root *s3FS) OnAdd(ctx context.Context) {
	slog.Debug("onAdd called")

	objects, err := root.listObjects()
	if err != nil {
		panic(err)
	}

	for _, object := range objects {
		dir, base := filepath.Split(strings.TrimRight(aws.StringValue(object.Key), "/"))

		p := &root.Inode

		// Add directories leading up to the file.
		for _, component := range strings.Split(dir, "/") {
			if len(component) == 0 {
				continue
			}

			ch := p.GetChild(component)
			if ch == nil {
				digest := sha1.Sum([]byte(dir))
				hexdigest := hex.EncodeToString(digest[:])

				if _, err := root.db.Exec("INSERT OR IGNORE INTO files (id, key) VALUES (?, ?)", hexdigest, dir); err != nil {
					slog.Error("failed inserting data into database", "err", err.Error())
					panic(err)
				}
				var timestamp time.Time
				row := root.db.QueryRow("SELECT updated_at FROM files WHERE id = ?", hexdigest)
				if err := row.Scan(&timestamp); err != nil {
					slog.Error("failed query database", "err", err.Error())
					panic(err)
				}

				slog.Debug("creating directory inode", "dir", dir, "hexdigest", hexdigest, "updated_at", timestamp)

				// Create a directory
				ch = p.NewPersistentInode(ctx, &s3Directory{updateTime: uint64(timestamp.Unix()), key: dir},
					fs.StableAttr{Mode: syscall.S_IFDIR})
				// Add it
				p.AddChild(component, ch, true)

			}

			p = ch
		}

		// Make a file out of the content bytes. This type
		// provides the open/read/flush methods.
		file := &s3File{
			Mutex:    new(sync.Mutex),
			Object:   object,
			s3Client: root.s3Client,
			bucket:   root.bucket,
		}

		// Create the file. The Inode must be persistent,
		// because its life time is not under control of the
		// kernel.
		child := p.NewPersistentInode(ctx, file, fs.StableAttr{})

		// And add it
		p.AddChild(base, child, true)
	}
}

func (root *s3FS) listObjects() ([]*s3.Object, error) {
	var objects []*s3.Object
	for {
		var continuationToken *string
		listObjectsResult, err := root.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            &root.bucket,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}

		objects = append(objects, listObjectsResult.Contents...)
		continuationToken = listObjectsResult.ContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return objects, nil
}
