package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"sync"

	"github.com/flokli/nix-casync/pkg/store"
)

// MemoryStore implements BlobStore
var _ BlobStore = &MemoryStore{}

type MemoryStore struct {
	// Go can't use []bytes as a map key
	blobs   map[string][]byte
	muBlobs sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		blobs: make(map[string][]byte),
	}
}

func (m *MemoryStore) Close() error {
	return nil
}

func (m *MemoryStore) PutBlob(ctx context.Context) (WriteCloseHasher, error) {
	return &memoryStoreWriter{
		hash:        sha256.New(),
		memoryStore: m,
	}, nil
}

func (m *MemoryStore) GetBlob(ctx context.Context, sha256 []byte) (io.ReadCloser, int64, error) {
	m.muBlobs.Lock()
	v, ok := m.blobs[hex.EncodeToString(sha256)]
	m.muBlobs.Unlock()
	if ok {
		return io.NopCloser(bytes.NewReader(v)), int64(len(v)), nil
	}
	return nil, 0, store.ErrNotFound
}

// memoryStoreWriter implements WriteCloseHasher
var _ WriteCloseHasher = &memoryStoreWriter{}

type memoryStoreWriter struct {
	memoryStore  *MemoryStore
	contents     []byte
	bytesWritten uint64
	hash         hash.Hash
}

func (msw *memoryStoreWriter) Write(p []byte) (n int, err error) {
	msw.contents = append(msw.contents, p...)
	msw.hash.Write(p)
	msw.bytesWritten += uint64(len(p))
	return len(p), nil
}

func (msw *memoryStoreWriter) Close() error {
	msw.memoryStore.muBlobs.Lock()
	msw.memoryStore.blobs[hex.EncodeToString(msw.hash.Sum([]byte{}))] = msw.contents
	msw.memoryStore.muBlobs.Unlock()
	return nil
}

func (msw *memoryStoreWriter) Sha256Sum() []byte {
	return msw.hash.Sum([]byte{})
}

func (msw *memoryStoreWriter) BytesWritten() uint64 {
	return msw.bytesWritten
}
