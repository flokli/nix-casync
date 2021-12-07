package store

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

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

func (m *MemoryStore) GetNar(_ctx context.Context, narhash []byte, w io.Writer) error {
	m.muNar.Lock()
	v, ok := m.nar[nixbase32.EncodeToString(narhash)]
	m.muNar.Unlock()
	if ok {
		_, err := w.Write(v)
		return err
	}
	return ErrNotFound
}

func (m *MemoryStore) PutNar(ctx context.Context, narhash []byte, r io.Reader) error {
	bb := bytes.NewBuffer(nil)
	_, err := io.Copy(bb, r)
	if err != nil {
		return err
	}

	// TODO: what to do if it already exists?
	m.muNar.Lock()
	m.nar[nixbase32.EncodeToString(narhash)] = bb.Bytes()
	m.muNar.Unlock()
	return nil
}
