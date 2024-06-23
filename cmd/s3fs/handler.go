package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gqgs/s3fs/pkg/s3fs"
	"github.com/gqgs/s3fs/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	_ "github.com/mattn/go-sqlite3"
)

func handler(o options) error {
	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)

	storage, err := storage.NewSqliteDB(o.db)
	if err != nil {
		return err
	}

	rootInode, err := s3fs.NewFS(storage, s3Client, o.bucket)
	if err != nil {
		return err
	}

	server, err := fs.Mount(o.mountpoint, rootInode, &fs.Options{
		MountOptions: fuse.MountOptions{Debug: o.verbose},
	})
	if err != nil {
		return err
	}

	log.Printf("Mounted on %s", o.mountpoint)
	log.Printf("Unmount by calling 'fusermount -u %s'", o.mountpoint)

	server.Wait()
	return nil
}
