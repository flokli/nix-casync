package store

import (
	"context"
	"errors"
	"io"

	"github.com/numtide/go-nix/nar/narinfo"
)

// BinaryCacheStore describes the interface all Stores implement
type BinaryCacheStore interface {
	GetNarInfo(ctx context.Context, outputhash []byte) (*narinfo.NarInfo, error)
	GetNar(ctx context.Context, narhash []byte) (io.ReadCloser, error)

	PutNarInfo(ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error

	// PutNar is called on upload. It returns a writer. Once the writer is closed, the content hash is validated to be narhash
	// TODO: add validation
	PutNar(ctx context.Context, narhash []byte) (io.WriteCloser, error)
}

var (
	ErrNotFound = errors.New("not found")
)
