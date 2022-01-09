package metadatastore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
)

// MemoryStore implements MetadataStore
var _ MetadataStore = &MemoryStore{}

type MemoryStore struct {
	pathInfo   map[string]PathInfo
	muPathInfo sync.Mutex
	narMeta    map[string]NarMeta
	muNarMeta  sync.Mutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		pathInfo: make(map[string]PathInfo),
		narMeta:  make(map[string]NarMeta),
	}
}

func (ms *MemoryStore) Close() error {
	return nil
}

func (ms *MemoryStore) GetPathInfo(ctx context.Context, outputHash []byte) (*PathInfo, error) {
	ms.muPathInfo.Lock()
	v, ok := ms.pathInfo[hex.EncodeToString(outputHash)]
	ms.muPathInfo.Unlock()
	if ok {
		return &v, nil
	}
	return nil, os.ErrNotExist
}

func (ms *MemoryStore) PutPathInfo(ctx context.Context, pathinfo *PathInfo) error {
	err := pathinfo.Check()
	if err != nil {
		return err
	}

	// foreign key constraint: referred NarMeta needs to exist
	_, err = ms.GetNarMeta(ctx, pathinfo.NarHash)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("referred nar doesn't exist: %w", err)
		}
		return err
	}

	ms.muPathInfo.Lock()
	ms.pathInfo[hex.EncodeToString(pathinfo.OutputHash)] = *pathinfo
	ms.muPathInfo.Unlock()
	return nil
}

func (ms *MemoryStore) GetNarMeta(ctx context.Context, narHash []byte) (*NarMeta, error) {
	ms.muNarMeta.Lock()
	v, ok := ms.narMeta[hex.EncodeToString(narHash)]
	ms.muNarMeta.Unlock()
	if ok {
		return &v, nil
	}
	return nil, os.ErrNotExist
}

func (ms *MemoryStore) PutNarMeta(ctx context.Context, narMeta *NarMeta) error {
	err := narMeta.Check()
	if err != nil {
		return err
	}

	// foreign key constraint: all references need to exist
	for i, reference := range narMeta.References {
		_, err := ms.GetPathInfo(ctx, reference)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("referred reference %v doesn't exist: %w", narMeta.ReferencesStr[i], err)
			}
			return err
		}
	}

	ms.muNarMeta.Lock()
	ms.narMeta[hex.EncodeToString(narMeta.NarHash)] = *narMeta
	ms.muNarMeta.Unlock()
	return nil
}

func (ms *MemoryStore) DropAll(ctx context.Context) error {
	ms.muNarMeta.Lock()
	ms.muPathInfo.Lock()
	for k := range ms.narMeta {
		delete(ms.narMeta, k)
	}
	for k := range ms.pathInfo {
		delete(ms.pathInfo, k)
	}
	ms.muNarMeta.Unlock()
	ms.muPathInfo.Unlock()
	return nil
}
