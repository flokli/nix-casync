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
	io.Closer
}

// WriteWriteCloserHashSum is a io.WriteCloser, which you can ask for a checksum
type WriteCloseHasher interface {
	io.WriteCloser
	Sha256Sum() []byte
}

// NarStore can store and retrieve .nar files
type NarStore interface {
	GetNar(ctx context.Context, narhash []byte) (io.ReadCloser, int64, error)
	PutNar(ctx context.Context) (WriteCloseHasher, error)
	io.Closer
}

// BinaryCacheStore can store and retrieve both .nar files, and narinfo.NarInfo structs
type BinaryCacheStore interface {
	NarinfoStore
	NarStore
}

var (
	ErrNotFound = errors.New("not found")
)
