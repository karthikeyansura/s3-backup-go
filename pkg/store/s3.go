package store

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

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

// uploadBufSize is the buffering layer between backup traversal writes (often
// small, 512-byte sector-aligned chunks) and the pipe feeding PutObject.
// Batching into 1 MiB reduces write syscall overhead significantly.
const uploadBufSize = 1 << 20

// NewWriter streams data to S3 via multipart upload. A 1 MiB bufio.Writer sits
// between the caller and an io.Pipe; PutObject reads the pipe with size=-1 so
// minio-go handles automatic multipart chunking. The backup never needs to hold
// the entire object in RAM.
func (s *S3Store) NewWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)

	go func() {
		_, err := s.client.PutObject(
			ctx,
			s.bucket,
			key,
			pr,
			-1,
			minio.PutObjectOptions{
				ContentType: "application/octet-stream",
			},
		)
		if err != nil {
			_ = pr.CloseWithError(err)
		} else {
			_ = pr.Close()
		}
		done <- err
	}()

	return &s3Writer{
		key:   key,
		rawPw: pw,
		bufw:  bufio.NewWriterSize(pw, uploadBufSize),
		done:  done,
	}, nil
}

type s3Writer struct {
	key       string
	rawPw     *io.PipeWriter
	bufw      *bufio.Writer
	done      chan error
	closeOnce sync.Once
	closeErr  error
}

func (w *s3Writer) Write(p []byte) (int, error) {
	return w.bufw.Write(p)
}

func (w *s3Writer) Close() error {
	w.closeOnce.Do(func() {
		if err := w.bufw.Flush(); err != nil {
			_ = w.rawPw.CloseWithError(err)
			if uerr := <-w.done; uerr != nil {
				w.closeErr = fmt.Errorf("store: flush %s: %w (upload: %v)", w.key, err, uerr)
				return
			}
			w.closeErr = fmt.Errorf("store: flush %s: %w", w.key, err)
			return
		}
		if err := w.rawPw.Close(); err != nil {
			w.closeErr = err
			return
		}
		w.closeErr = <-w.done
	})
	return w.closeErr
}
