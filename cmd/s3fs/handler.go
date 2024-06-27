package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gqgs/s3fs/pkg/s3root"
	"github.com/gqgs/s3fs/pkg/s3wrapper"
	"github.com/gqgs/s3fs/pkg/storage"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func handler(o options) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return err
	}

	s3client := s3.NewFromConfig(cfg)
	s3wrapper := s3wrapper.New(s3client, o.bucket, o.concurrency)

	storage, err := storage.NewSqliteDB(o.db)
	if err != nil {
		return err
	}
	defer storage.Close()

	rootInode, err := s3root.New(storage, s3wrapper)
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
