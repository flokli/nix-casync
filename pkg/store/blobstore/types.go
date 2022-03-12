// Package blobstore implements some content-addressed blob stores.
// You can store whatever you want in there, but need to address things by their hash to get them out.
package blobstore

import (
	"context"
	"io"
)

// BlobStore describes the interface of a blob store.
type BlobStore interface {
	PutBlob(ctx context.Context) (WriteCloseHasher, error)
	GetBlob(ctx context.Context, sha256 []byte) (io.ReadCloser, int64, error)
	io.Closer
}

// WriteWriteCloserHashSum is a io.WriteCloser, which you can ask for a checksum.
type WriteCloseHasher interface {
	io.WriteCloser
	Sha256Sum() []byte
	BytesWritten() uint64
}
