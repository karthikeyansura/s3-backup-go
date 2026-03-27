package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config holds the configuration for connecting to an S3-compatible service.
type S3Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

// S3Store implements ObjectStore using MinIO's S3 client.
type S3Store struct {
	client *minio.Client
	bucket string
}

// NewS3Store creates a new S3-backed object store.
func NewS3Store(cfg S3Config) (*S3Store, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("store: init S3 client: %w", err)
	}

	return &S3Store{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// GetRange retrieves a specific byte range of an object.
func (s *S3Store) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+length-1); err != nil {
		return nil, fmt.Errorf("store: set range: %w", err)
	}

	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("store: get %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()

	buf := make([]byte, length)
	n, err := io.ReadFull(obj, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("store: read %s: %w", key, err)
	}
	return buf[:n], nil
}

// Size returns the total size of the object in bytes.
func (s *S3Store) Size(ctx context.Context, key string) (int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("store: stat %s: %w", key, err)
	}
	return info.Size, nil
}

// NewWriter initializes a new buffered writer for an S3 object.
func (s *S3Store) NewWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	return &s3Writer{
		ctx:   ctx,
		store: s,
		key:   key,
		buf:   &bytes.Buffer{},
	}, nil
}

type s3Writer struct {
	ctx   context.Context
	store *S3Store
	key   string
	buf   *bytes.Buffer
}

// Write appends data to the internal memory buffer.
func (w *s3Writer) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

// Close executes the multipart upload of the buffered data to S3.
func (w *s3Writer) Close() error {
	reader := bytes.NewReader(w.buf.Bytes())
	size := int64(w.buf.Len())

	_, err := w.store.client.PutObject(
		w.ctx,
		w.store.bucket,
		w.key,
		reader,
		size,
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
	)
	if err != nil {
		return fmt.Errorf("store: upload %s: %w", w.key, err)
	}
	return nil
}
