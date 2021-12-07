package store

import (
	"context"
	"io"
	"io/ioutil"
	"os"

	"github.com/folbricht/desync"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

type CasyncStore struct {
	localStore      desync.WriteStore
	localIndexStore desync.IndexWriteStore
	concurrency     int

	chunkSizeAvgDefault uint64
	chunkSizeMinDefault uint64
	chunkSizeMaxDefault uint64

	// TODO: remote store(s)?

	// TODO: how do we store .narinfo files?
	memoryStore BinaryCacheStore
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

		memoryStore: NewMemoryStore(),
	}, nil
}

// TODO: right now we just abuse a memory store for .narinfo files
// this should be something more persistent.
func (c *CasyncStore) GetNarInfo(ctx context.Context, outputhash []byte) (*narinfo.NarInfo, error) {
	return c.memoryStore.GetNarInfo(ctx, outputhash)
}

func (c *CasyncStore) PutNarInfo(ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error {
	return c.memoryStore.PutNarInfo(ctx, outputhash, contents)
}

func (c *CasyncStore) GetNar(ctx context.Context, narhash []byte, w io.Writer) error {
	narhashStr := nixbase32.EncodeToString(narhash)
	// retrieve .caidx
	caidx, err := c.localIndexStore.GetIndex(narhashStr)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}

	tmpFile, err := ioutil.TempFile("", narhashStr+".nar")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	// run AssembleFile into a temporary file
	_, err = desync.AssembleFile(ctx, tmpFile.Name(), caidx, c.localStore, []desync.Seed{}, c.concurrency, nil)
	if err != nil {
		return err
	}

	tmpFileReader, err := os.Open(tmpFile.Name())
	if err != nil {
		return err
	}
	defer tmpFileReader.Close()

	io.Copy(w, tmpFileReader)
	return nil
}

func (c *CasyncStore) PutNar(ctx context.Context, narhash []byte, r io.Reader) error {
	narhashStr := nixbase32.EncodeToString(narhash)

	chunker, err := desync.NewChunker(
		r,
		c.chunkSizeMinDefault,
		c.chunkSizeAvgDefault,
		c.chunkSizeMaxDefault,
	)
	if err != nil {
		return err
	}
	caidx, err := desync.ChunkStream(ctx, chunker, c.localStore, c.concurrency)
	if err != nil {
		return err
	}

	err = c.localIndexStore.StoreIndex(narhashStr, caidx)
	if err != nil {
		return err
	}
	return nil
}
