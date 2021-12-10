package fetcher

import (
	"context"
	"io"
	"net/url"
	"path"

	"cloud.google.com/go/storage"
)

var _ BinaryCacheReader = GCSFetcher{}

// GCSFetcher ...
type GCSFetcher struct {
	url    *url.URL
	bucket *storage.BucketHandle
	prefix string
}

// NewGCSFetcher --
func NewGCSFetcher(ctx context.Context, u *url.URL) (*GCSFetcher, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSFetcher{
		url:    u,
		bucket: client.Bucket(u.Host),
		prefix: u.Path,
	}, nil
}

// getObject composes the path with the prefix to return an ObjectHandle.
func (c GCSFetcher) getObject(p string) *storage.ObjectHandle {
	objectPath := path.Join(c.prefix, p)
	if objectPath[0] == '/' {
		objectPath = objectPath[1:]
	}
	return c.bucket.Object(objectPath)
}

// FileExists returns true if the file is already in the store.
// err is used for transient issues like networking errors.
func (c GCSFetcher) FileExists(ctx context.Context, path string) (bool, error) {
	obj := c.getObject(path)
	_, err := obj.Attrs(ctx)
	if err == nil {
		return true, nil
	}
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	return false, err
}

// GetFile returns a file stream from the store if the file exists
func (c GCSFetcher) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	obj := c.getObject(path)
	return obj.NewReader(ctx)
}

// URL returns the fetcher URI
func (c GCSFetcher) URL() string {
	return c.url.String()
}
