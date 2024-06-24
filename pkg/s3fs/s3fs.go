package s3fs

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
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	_ = (fs.NodeOnAdder)((*s3FS)(nil))
	_ = (fs.NodeCreater)((*s3FS)(nil))
	_ = (fs.NodeMkdirer)((*s3FS)(nil))
)

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

	objects, err := root.listObjects(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, object := range objects {
		dir, base := filepath.Split(aws.StringValue(object.Key))

		p := &root.Inode

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

				if _, err := root.db.ExecContext(ctx, "INSERT OR IGNORE INTO files (id, path) VALUES (?, ?)", hexdigest, path); err != nil {
					slog.Error("failed inserting data into database", "err", err.Error())
					log.Fatal(err)
				}
				var timestamp time.Time
				row := root.db.QueryRowContext(ctx, "SELECT updated_at FROM files WHERE id = ?", hexdigest)
				if err := row.Scan(&timestamp); err != nil {
					slog.Error("failed query database", "err", err.Error())
					log.Fatal(err)
				}

				slog.Debug("creating directory inode", "dir", dir, "hexdigest", hexdigest, "updated_at", timestamp)

				// Create a directory
				ch = p.NewPersistentInode(ctx, &s3Directory{updateTime: uint64(timestamp.Unix()), path: path, s3Client: root.s3Client, bucket: root.bucket},
					fs.StableAttr{Mode: syscall.S_IFDIR})
				// Add it
				p.AddChild(component, ch, true)

			}

			p = ch
		}

		key := aws.StringValue(object.Key)
		size := aws.Int64Value(object.Size)
		lastModified := aws.TimeValue(object.LastModified)
		file := newS3File(key, root.bucket, size, lastModified, root.s3Client)

		// Create the file. The Inode must be persistent,
		// because its life time is not under control of the
		// kernel.
		child := p.NewPersistentInode(ctx, file, fs.StableAttr{})

		// And add it
		p.AddChild(base, child, true)
	}
}

func (root *s3FS) listObjects(ctx context.Context) ([]*s3.Object, error) {
	var objects []*s3.Object
	for {
		var continuationToken *string
		listObjectsResult, err := root.s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
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

func (root *s3FS) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	slog.Debug("root create call")
	panic("not implemented")
}

func (root *s3FS) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	slog.Debug("root mkdir call", "name", name, "mode", mode)

	child := root.NewPersistentInode(ctx, &s3Directory{updateTime: uint64(time.Now().Unix()), path: name, s3Client: root.s3Client, bucket: root.bucket},
		fs.StableAttr{Mode: syscall.S_IFDIR})

	root.AddChild(name, child, true)
	return child, fs.OK
}
