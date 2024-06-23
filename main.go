package main

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	_ "github.com/mattn/go-sqlite3"
)

var (
	storageBucket = aws.String("gqgs-s3fs")
)

// Should be closed after being used
func newSqliteStorage(dbName string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbName+"?_synchronous=off&_journal_mode=off&cache=shared")
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS files (
			id TEXT PRIMARY KEY,
			key string,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return nil, err
	}

	return db, nil
}

// inMemoryFS is the root of the tree
type s3FS struct {
	fs.Inode
	db       *sql.DB
	s3Client *s3.S3
}

func News3FS() (*s3FS, error) {
	db, err := newSqliteStorage("./files.db")
	if err != nil {
		return nil, err
	}

	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)

	return &s3FS{
		db:       db,
		s3Client: s3Client,
	}, nil
}

// Ensure that we implement NodeOnAdder
var _ = (fs.NodeOnAdder)((*s3FS)(nil))

func (root *s3FS) listPaths() ([]*s3.CommonPrefix, error) {
	var prefixes []*s3.CommonPrefix
	for {
		var continuationToken *string
		listObjectsResult, err := root.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            storageBucket,
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}

		prefixes = append(prefixes, listObjectsResult.CommonPrefixes...)
		continuationToken = listObjectsResult.ContinuationToken
		if continuationToken == nil {
			break
		}
	}

	return prefixes, nil
}

func (root *s3FS) listObjects() ([]*s3.Object, error) {
	var objects []*s3.Object
	for {
		var continuationToken *string
		listObjectsResult, err := root.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            storageBucket,
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

// OnAdd is called on mounting the file system. Use it to populate
// the file system tree.
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

				if _, err := root.db.Exec("INSERT OR IGNORE INTO files (id, key) VALUES (?, ?)", hexdigest, aws.StringValue(object.Key)); err != nil {
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
				ch = p.NewPersistentInode(ctx, &s3Directory{updateTime: uint64(timestamp.Unix())},
					fs.StableAttr{Mode: syscall.S_IFDIR})
				// Add it
				p.AddChild(component, ch, true)

			}

			p = ch
		}

		// Make a file out of the content bytes. This type
		// provides the open/read/flush methods.
		embedder := &s3File{
			Mutex:    new(sync.Mutex),
			Object:   object,
			s3Client: root.s3Client,
		}

		// Create the file. The Inode must be persistent,
		// because its life time is not under control of the
		// kernel.
		child := p.NewPersistentInode(ctx, embedder, fs.StableAttr{})

		// And add it
		p.AddChild(base, child, true)
	}
}

var _ = (fs.NodeGetattrer)((*s3Directory)(nil))

type s3Directory struct {
	fs.Inode
	updateTime uint64
}

func (d *s3Directory) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	slog.Debug("directory getattr call")
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = d.updateTime
	out.Atime = d.updateTime
	out.Ctime = d.updateTime
	return 0
}

var _ = (fs.NodeOpener)((*s3File)(nil))
var _ = (fs.NodeGetattrer)((*s3File)(nil))
var _ = (fs.NodeReader)((*s3File)(nil))

type s3File struct {
	*sync.Mutex
	fs.Inode
	*s3.Object
	s3Client *s3.S3
	reader   io.Reader
	data     []byte
}

// Open lazily unpacks zip data
func (f *s3File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if f.reader != nil {
		return nil, fuse.FOPEN_KEEP_CACHE, fs.OK
	}
	slog.Debug("file open call", "key", *f.Object.Key)
	object, err := f.s3Client.GetObject(&s3.GetObjectInput{
		Bucket: storageBucket,
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

// This demonstrates how to build a file system in memory. The
// read/write logic for the file is provided by the MemRegularFile type.
func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "enable debug logs")
	flag.Parse()
	if flag.NArg() < 1 {
		_, prog := filepath.Split(os.Args[0])
		fmt.Printf("usage: %s MOUNTPOINT\n", prog)
		os.Exit(2)
	}

	if debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	root, err := News3FS()
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}

	server, err := fs.Mount(flag.Arg(0), root, &fs.Options{
		MountOptions: fuse.MountOptions{Debug: false},
	})
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Mounted on %s", flag.Arg(0))
	log.Printf("Unmount by calling 'fusermount -u %s'", flag.Arg(0))

	// Wait until unmount before exiting
	server.Wait()
}
