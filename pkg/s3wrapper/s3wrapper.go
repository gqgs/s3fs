package s3wrapper

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var _ = (Wrapper)((*s3wrapper)(nil))

type Wrapper interface {
	ListObjects(ctx context.Context) ([]types.Object, error)
	DownloadRange(ctx context.Context, key string, dest []byte, off, size int) (n int64, err error)
	UploadFile(ctx context.Context, key string, reader io.Reader) error
	DeleteObject(ctx context.Context, key string) error
}

type s3wrapper struct {
	*manager.Downloader
	*manager.Uploader
	s3client *s3.Client
	logger   *slog.Logger
	bucket   string
}

func New(s3client *s3.Client, bucket string, concurrency int) *s3wrapper {
	return &s3wrapper{
		Downloader: manager.NewDownloader(s3client, func(o *manager.Downloader) {
			o.Concurrency = concurrency
		}),
		Uploader: manager.NewUploader(s3client, func(o *manager.Uploader) {
			o.Concurrency = concurrency
		}),
		s3client: s3client,
		bucket:   bucket,
		logger:   slog.Default().WithGroup("s3wrapper"),
	}
}

func (w *s3wrapper) UploadFile(ctx context.Context, key string, reader io.Reader) error {
	w.logger.Debug("upload file call", "key", key)

	_, err := w.Uploader.Upload(ctx, &s3.PutObjectInput{
		Key:    &key,
		Bucket: &w.bucket,
		Body:   reader,
	})
	return err
}

func (w *s3wrapper) DownloadRange(ctx context.Context, key string, dest []byte, off, size int) (n int64, err error) {
	w.logger.Debug("download range call", "key", key, "len(dest)", len(dest), "off", off, "size", size)

	return w.Downloader.Download(ctx, manager.NewWriteAtBuffer(dest), &s3.GetObjectInput{
		Bucket: &w.bucket,
		Key:    &key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+size-1)),
	})
}

func (w *s3wrapper) ListObjects(ctx context.Context) ([]types.Object, error) {
	w.logger.Debug("list objects call")

	var objects []types.Object
	paginator := s3.NewListObjectsV2Paginator(w.s3client, &s3.ListObjectsV2Input{
		Bucket: &w.bucket,
	})
	for paginator.HasMorePages() {
		result, err := paginator.NextPage(ctx)
		if err != nil {
			w.logger.Error("next page call error", "err", err)
			return nil, err
		}
		objects = append(objects, result.Contents...)
	}

	return objects, nil
}

func (w *s3wrapper) DeleteObject(ctx context.Context, key string) error {
	w.logger.Debug("delete object call", "key", key)

	_, err := w.s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &w.bucket,
		Key:    &key,
	})
	return err
}
