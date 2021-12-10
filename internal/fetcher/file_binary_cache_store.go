package fetcher

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var _ BinaryCacheReader = FileFetcher{}

type FileFetcher struct {
	path string
}

func NewFileFetcher(u *url.URL) FileFetcher {
	return FileFetcher{u.Path}
}

func (c FileFetcher) checkPath(p string) error {
	if strings.HasPrefix(filepath.Clean(p), ".") {
		return errors.New("relative paths are not allowed")
	}
	return nil
}

func (c FileFetcher) FileExists(ctx context.Context, p string) (bool, error) {
	if err := c.checkPath(p); err != nil {
		return false, err
	}
	_, err := os.Open(path.Join(c.path, p))
	return !os.IsNotExist(err), err
}

func (c FileFetcher) GetFile(ctx context.Context, p string) (io.ReadCloser, error) {
	if err := c.checkPath(p); err != nil {
		return nil, err
	}
	return os.Open(path.Join(c.path, p))
}

func (c FileFetcher) URL() string {
	return "file://" + c.path
}
