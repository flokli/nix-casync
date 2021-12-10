package store

import (
	"context"
	"errors"
	"io"

	"github.com/numtide/go-nix/nar/narinfo"
)

// NarinfoStore can store and retrieve narinfo.NarInfo structs
type NarinfoStore interface {
	GetNarInfo(ctx context.Context, outputhash []byte) (*narinfo.NarInfo, error)
	PutNarInfo(ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error
}

// NarStore can store and retrieve .nar files
type NarStore interface {
	GetNar(ctx context.Context, narhash []byte) (io.ReadCloser, int64, error)
	// TODO: add validation for narhash to match content?
	PutNar(ctx context.Context, narhash []byte) (io.WriteCloser, error)
}

// BinaryCacheStore can store and retrieve both .nar files, and narinfo.NarInfo structs
type BinaryCacheStore interface {
	NarinfoStore
	NarStore
}

var (
	ErrNotFound = errors.New("not found")
)
