package blobstore

import (
	"context"
	"encoding/hex"
	"io"
	"os"

	"github.com/folbricht/desync"
)

var _ BlobStore = &CasyncStore{}

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

func (c *CasyncStore) GetBlob(ctx context.Context, sha256 []byte) (io.ReadCloser, int64, error) {
	// retrieve .caidx
	caidx, err := c.localIndexStore.GetIndex(hex.EncodeToString(sha256))
	if err != nil {
		return nil, 0, err
	}

	csnr, err := NewCasyncStoreReader(
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

func (c *CasyncStore) PutBlob(ctx context.Context) (WriteCloseHasher, error) {
	return NewCasyncStoreWriter(
		ctx,

		c.localStore,
		c.localIndexStore,

		c.concurrency,
		c.chunkSizeMinDefault,
		c.chunkSizeAvgDefault,
		c.chunkSizeMaxDefault,
	)
}
