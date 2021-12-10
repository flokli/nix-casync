package fetcher

import (
	"context"
	"fmt"
	"io"
	"net/url"
)

// Fetcher points to our beloved https://cache.nixos.org
func DefaultCache() HTTPFetcher {
	u, _ := url.Parse("https://cache.nixos.org")
	return HTTPFetcher{u}
}

// Fetcher represents a read-only binary cache store
type Fetcher interface {
	FileExists(ctx context.Context, path string) (bool, error)
	GetFile(ctx context.Context, path string) (io.ReadCloser, error)
	URL() string
}

// NewFetcher parses the url and returns the proper store
// reader for it.
func NewBinaryCacheReader(ctx context.Context, fetcherURL string) (Fetcher, error) {
	u, err := url.Parse(fetcherURL)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "http", "https":
		return NewHTTPFetcher(u), nil
	case "gs":
		return NewGCSFetcher(ctx, u)
	case "s3":
		return NewS3Fetcher(u)
	case "file":
		return NewFileFetcher(u), nil
	default:
		return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
	}
}
