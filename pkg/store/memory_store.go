package store

import (
	"bytes"
	"context"
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

func (m *MemoryStore) PutNar(ctx context.Context, narhash []byte) (io.WriteCloser, error) {
	return &memoryStoreNarWriter{
		narhash:     narhash,
		memoryStore: m,
	}, nil
}

type memoryStoreNarWriter struct {
	narhash     []byte
	memoryStore *MemoryStore
	contents    []byte
}

func (msnw *memoryStoreNarWriter) Write(p []byte) (n int, err error) {
	msnw.contents = append(msnw.contents, p...)
	return len(p), nil
}

func (msnw *memoryStoreNarWriter) Close() error {
	// TODO: verify hash
	// TODO: add handling to not call close two times
	// TODO: what to do if it already exists?
	msnw.memoryStore.muNar.Lock()
	msnw.memoryStore.nar[nixbase32.EncodeToString(msnw.narhash)] = msnw.contents
	msnw.memoryStore.muNar.Unlock()
	return nil
}
