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

type cleanCloser struct {
	f *os.File
	io.ReadCloser
}

func (c *cleanCloser) Read(p []byte) (n int, err error) {
	return c.f.Read(p)
}

func (c *cleanCloser) Close() error {
	defer os.Remove(c.f.Name())
	return c.f.Close()
}

func (c *CasyncStore) GetNar(ctx context.Context, narhash []byte) (io.ReadCloser, int64, error) {
	narhashStr := nixbase32.EncodeToString(narhash)
	// retrieve .caidx
	caidx, err := c.localIndexStore.GetIndex(narhashStr)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}

	tmpFile, err := ioutil.TempFile("", narhashStr+".nar")
	if err != nil {
		return nil, 0, err
	}

	// TODO: this still does all the heavy lifting before returning.
	// We should move this to the first Read() call.

	tmpFile2 := cleanCloser{f: tmpFile}

	// run AssembleFile into a temporary file
	_, err = desync.AssembleFile(ctx, tmpFile.Name(), caidx, c.localStore, []desync.Seed{}, c.concurrency, nil)
	if err != nil {
		return nil, 0, err
	}

	// flush and seek
	err = tmpFile.Sync()
	if err != nil {
		return nil, 0, err
	}
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		return nil, 0, err
	}
	return &tmpFile2, 0, nil
}

func (c *CasyncStore) PutNar(ctx context.Context, narhash []byte) (io.WriteCloser, error) {
	narhashStr := nixbase32.EncodeToString(narhash)
	// TODO: can we do better and do this in memory?
	tempfile, err := ioutil.TempFile("", narhashStr+".nar")
	if err != nil {
		return nil, err
	}

	return &casyncStoreNarWriter{
		ctx:         ctx,
		casyncStore: c,
		tempfile:    tempfile,
		narhashStr:  narhashStr,
	}, nil
}

type casyncStoreNarWriter struct {
	ctx         context.Context
	casyncStore *CasyncStore
	tempfile    *os.File
	narhashStr  string
}

func (csnw *casyncStoreNarWriter) Write(p []byte) (int, error) {
	return csnw.tempfile.Write(p)
}

func (csnw *casyncStoreNarWriter) Close() error {
	// at the end, we want to remove the tempfile
	defer os.Remove(csnw.tempfile.Name())
	// flush the tempfile and seek to the start
	err := csnw.tempfile.Sync()
	if err != nil {
		return err
	}
	_, err = csnw.tempfile.Seek(0, 0)
	if err != nil {
		return err
	}

	chunker, err := desync.NewChunker(
		csnw.tempfile,
		csnw.casyncStore.chunkSizeMinDefault,
		csnw.casyncStore.chunkSizeAvgDefault,
		csnw.casyncStore.chunkSizeMaxDefault,
	)
	if err != nil {
		return err
	}
	caidx, err := desync.ChunkStream(csnw.ctx, chunker, csnw.casyncStore.localStore, csnw.casyncStore.concurrency)
	if err != nil {
		return err
	}

	err = csnw.casyncStore.localIndexStore.StoreIndex(csnw.narhashStr, caidx)
	if err != nil {
		return err
	}
	return nil
}
