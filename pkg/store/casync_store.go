package store

import (
	"context"
	"io"
	"os"

	"github.com/folbricht/desync"
	"github.com/numtide/go-nix/nixbase32"
)

var _ NarStore = &CasyncStore{}

type CasyncStore struct {
	localStore      desync.WriteStore
	localIndexStore desync.IndexWriteStore
	concurrency     int

	chunkSizeAvgDefault uint64
	chunkSizeMinDefault uint64
	chunkSizeMaxDefault uint64

	// TODO: remote store(s)?
}

func NewCasyncStore(localStoreDir, localIndexStoreDir string) (*CasyncStore, error) {

	// TODO: maybe use MultiStoreWithCache?
	err := os.MkdirAll(localStoreDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	localStore, err := desync.NewLocalStore(localStoreDir, desync.StoreOptions{})
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(localIndexStoreDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	localIndexStore, err := desync.NewLocalIndexStore(localIndexStoreDir)
	if err != nil {
		return nil, err
	}

	return &CasyncStore{
		localStore:      localStore,
		localIndexStore: localIndexStore,
		concurrency:     1, // TODO: make configurable

		// values stolen from chunker_test.go
		// TODO: make configurable
		chunkSizeAvgDefault: 64 * 1024,
		chunkSizeMinDefault: 64 * 1024 / 4,
		chunkSizeMaxDefault: 64 * 1024 * 4,
	}, nil
}

func (c *CasyncStore) Close() error {
	err := c.localStore.Close()
	if err != nil {
		return err
	}
	return c.localIndexStore.Close()
}

func (c *CasyncStore) GetNar(ctx context.Context, narhash []byte) (io.ReadCloser, int64, error) {
	narhashStr := nixbase32.EncodeToString(narhash)
	// retrieve .caidx
	caidx, err := c.localIndexStore.GetIndex(narhashStr + ".nar")
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}

	csnr, err := NewCasyncStoreNarReader(
		ctx,
		caidx,
		c.localStore,
		[]desync.Seed{},
		1,
		nil,
	)
	if err != nil {
		return nil, 0, err
	}
	return csnr, caidx.Length(), nil
}

func (c *CasyncStore) PutNar(ctx context.Context) (WriteCloseHasher, error) {
	return NewCasyncStoreNarWriter(
		ctx,

		c.localStore,
		c.localIndexStore,

		c.concurrency,
		c.chunkSizeMinDefault,
		c.chunkSizeAvgDefault,
		c.chunkSizeMaxDefault,
	)
}