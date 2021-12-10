package fetcher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

var _ BinaryCacheReader = HTTPFetcher{}

// HTTPFetcher ...
type HTTPFetcher struct {
	url *url.URL // assumes the URI doesn't end with '/'
}

// NewHTTPFetcher ---
func NewHTTPFetcher(u *url.URL) HTTPFetcher {
	return HTTPFetcher{u}
}

// getURL composes the path with the prefix to return an URL.
func (c HTTPFetcher) getURL(p string) string {
	newPath := path.Join(c.url.Path, p)
	x, _ := c.url.Parse(newPath)
	return x.String()
}

// FileExists returns true if the file is already in the store.
// err is used for transient issues like networking errors.
func (c HTTPFetcher) FileExists(ctx context.Context, path string) (bool, error) {
	resp, err := http.Head(c.getURL(path))
	if err != nil {
		return false, err
	}
	return (resp.StatusCode == 200), nil
}

// GetFile returns a file stream from the store if the file exists
func (c HTTPFetcher) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	resp, err := http.Get(c.getURL(path))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected file status '%s'", resp.Status)
	}
	return resp.Body, nil
}

// URL returns the fetcher URI
func (c HTTPFetcher) URL() string {
	return c.url.String()
}
