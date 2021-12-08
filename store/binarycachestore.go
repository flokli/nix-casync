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
	PutNarInfo(ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error

	GetNar(ctx context.Context, narhash []byte) (io.ReadCloser, int64, error)
	// TODO: add validation for narhash to match content
	PutNar(ctx context.Context, narhash []byte) (io.WriteCloser, error)
}

var (
	ErrNotFound = errors.New("not found")
)
