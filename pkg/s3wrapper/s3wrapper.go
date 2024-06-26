package s3wrapper

import (
	"context"
	"fmt"
	"io"

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
	s3client *s3.S3
	bucket   string
}

func New(s3client *s3.S3, bucket string) *s3wrapper {
	return &s3wrapper{
		Downloader: s3manager.NewDownloaderWithClient(s3client),
		Uploader:   s3manager.NewUploaderWithClient(s3client),
		s3client:   s3client,
		bucket:     bucket,
	}
}

func (w *s3wrapper) UploadFile(ctx context.Context, key string, reader io.Reader) error {
	_, err := w.Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Key:    &key,
		Bucket: &w.bucket,
		Body:   reader,
	})
	return err
}

func (w *s3wrapper) DownloadRange(ctx context.Context, key string, dest []byte, off, size int) (n int64, err error) {
	return w.Downloader.DownloadWithContext(ctx, aws.NewWriteAtBuffer(dest), &s3.GetObjectInput{
		Bucket: &w.bucket,
		Key:    &key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+size-1)),
	}, func(d *s3manager.Downloader) {
		d.Concurrency = 1
	})
}

func (w *s3wrapper) ListObjects(ctx context.Context) ([]*s3.Object, error) {
	var objects []*s3.Object
	for {
		var continuationToken *string
		listObjectsResult, err := w.s3client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            &w.bucket,
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
