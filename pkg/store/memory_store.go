package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"
	"io"
	"sync"

	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

// MemoryStore implements both NarStore and NarinfoStore
var _ BinaryCacheStore = &MemoryStore{}

type MemoryStore struct {
	narinfo   map[string]*narinfo.NarInfo
	muNarinfo sync.Mutex
	nar       map[string][]byte
	muNar     sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		narinfo: make(map[string]*narinfo.NarInfo),
		nar:     make(map[string][]byte),
	}
}

func (m *MemoryStore) GetNarInfo(_ctx context.Context, outputhash []byte) (*narinfo.NarInfo, error) {
	// TODO: check if we need this for reads
	m.muNarinfo.Lock()
	defer m.muNarinfo.Unlock()
	v, ok := m.narinfo[nixbase32.EncodeToString(outputhash)]
	if ok {
		return v, nil
	}
	return nil, ErrNotFound
}

func (m *MemoryStore) PutNarInfo(_ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error {
	// TODO: what to do if it already exists?
	m.muNarinfo.Lock()
	defer m.muNarinfo.Unlock()
	m.narinfo[nixbase32.EncodeToString(outputhash)] = contents
	return nil
}

func (m *MemoryStore) GetNar(_ctx context.Context, narhash []byte) (io.ReadCloser, int64, error) {
	m.muNar.Lock()
	v, ok := m.nar[nixbase32.EncodeToString(narhash)]
	m.muNar.Unlock()
	if ok {
		return io.NopCloser(bytes.NewReader(v)), int64(len(v)), nil
	}
	return nil, 0, ErrNotFound
}

func (m *MemoryStore) PutNar(ctx context.Context) (WriteCloseHasher, error) {
	return &memoryStoreNarWriter{
		hash:        sha256.New(),
		memoryStore: m,
	}, nil
}

func (m *MemoryStore) Close() error {
	return nil
}

type memoryStoreNarWriter struct {
	memoryStore *MemoryStore
	contents    []byte
	hash        hash.Hash
}

func (msnw *memoryStoreNarWriter) Write(p []byte) (n int, err error) {
	msnw.contents = append(msnw.contents, p...)
	msnw.hash.Write(p)
	return len(p), nil
}

func (msnw *memoryStoreNarWriter) Close() error {
	// retrieve hash
	narhash := msnw.hash.Sum([]byte{})
	// TODO: add handling to not call close two times
	msnw.memoryStore.muNar.Lock()
	msnw.memoryStore.nar[nixbase32.EncodeToString(narhash)] = msnw.contents
	msnw.memoryStore.muNar.Unlock()
	return nil
}

func (msnw *memoryStoreNarWriter) Sha256Sum() []byte {
	return msnw.hash.Sum([]byte{})
}
