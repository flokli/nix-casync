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

	GetNar(ctx context.Context, narhash []byte, w io.Writer) error
	// TODO: add validation for narhash to match content
	PutNar(ctx context.Context, narhash []byte, r io.Reader) error
}

var (
	ErrNotFound = errors.New("not found")
)
