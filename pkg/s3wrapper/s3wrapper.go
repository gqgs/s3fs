package s3wrapper

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var _ = (Wrapper)((*s3wrapper)(nil))

type Wrapper interface {
	ListObjects(ctx context.Context) ([]*s3.Object, error)
	DownloadRange(ctx context.Context, key string, dest []byte, off, size int) (n int64, err error)
	UploadFile(ctx context.Context, key string, reader io.Reader) error
}

type s3wrapper struct {
	*s3manager.Downloader
	*s3manager.Uploader
	s3client    *s3.S3
	logger      *slog.Logger
	bucket      string
	concurrency int
}

func New(s3client *s3.S3, bucket string, concurrency int) *s3wrapper {
	return &s3wrapper{
		Downloader:  s3manager.NewDownloaderWithClient(s3client),
		Uploader:    s3manager.NewUploaderWithClient(s3client),
		s3client:    s3client,
		bucket:      bucket,
		logger:      slog.Default().WithGroup("s3wrapper"),
		concurrency: concurrency,
	}
}

func (w *s3wrapper) UploadFile(ctx context.Context, key string, reader io.Reader) error {
	w.logger.Debug("upload file call", "key", key)

	_, err := w.Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Key:    &key,
		Bucket: &w.bucket,
		Body:   reader,
	}, func(d *s3manager.Uploader) {
		d.Concurrency = w.concurrency
	})
	return err
}

func (w *s3wrapper) DownloadRange(ctx context.Context, key string, dest []byte, off, size int) (n int64, err error) {
	w.logger.Debug("download range call", "key", key, "len(dest)", len(dest), "off", off, "size", size)

	return w.Downloader.DownloadWithContext(ctx, aws.NewWriteAtBuffer(dest), &s3.GetObjectInput{
		Bucket: &w.bucket,
		Key:    &key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+size-1)),
	}, func(d *s3manager.Downloader) {
		d.Concurrency = w.concurrency
	})
}

func (w *s3wrapper) ListObjects(ctx context.Context) ([]*s3.Object, error) {
	w.logger.Debug("list objects call")

	var objects []*s3.Object
	for {
		var continuationToken *string
		listObjectsResult, err := w.s3client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            &w.bucket,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			w.logger.Error("list objects call error", "err", err)
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
