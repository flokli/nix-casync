package store

import (
	"context"
	"io"
	"io/ioutil"
	"os"

	"github.com/folbricht/desync"
)

// casyncStoreNarWriter provides a io.WriteCloser interface
// The whole content of the .nar file is written to it.
// Internally, it'll write it to a temporary file.
// On close, its contents will be chunked,
// the index added to the index store, and the chunks added to the chunk store.
type casyncStoreNarWriter struct {
	io.WriteCloser

	name string

	ctx context.Context

	desyncStore      desync.WriteStore
	desyncIndexStore desync.IndexWriteStore

	concurrency         int
	chunkSizeMinDefault uint64
	chunkSizeAvgDefault uint64
	chunkSizeMaxDefault uint64

	f *os.File
}

// init needs to be called by everything writing
// It ensures the tempfile is set up
func (csnw *casyncStoreNarWriter) init() error {
	tmpFile, err := ioutil.TempFile("", csnw.name+".nar")
	if err != nil {
		return err
	}
	// Cleanup is handled in csnw.Close(), or whenever there's an error during init
	csnw.f = tmpFile
	return nil
}

func (csnw *casyncStoreNarWriter) Write(p []byte) (int, error) {
	if csnw.f == nil {
		err := csnw.init()
		if err != nil {
			return 0, err
		}
	}
	return csnw.f.Write(p)
}

func (csnw *casyncStoreNarWriter) Close() error {
	// at the end, we want to remove the tempfile
	defer os.Remove(csnw.f.Name())

	// flush the tempfile and seek to the start
	err := csnw.f.Sync()
	if err != nil {
		return err
	}
	_, err = csnw.f.Seek(0, 0)
	if err != nil {
		return err
	}

	// Run the chunker on the tempfile
	chunker, err := desync.NewChunker(
		csnw.f,
		csnw.chunkSizeMinDefault,
		csnw.chunkSizeAvgDefault,
		csnw.chunkSizeMaxDefault,
	)
	if err != nil {
		return err
	}

	// upload all chunks into the store
	caidx, err := desync.ChunkStream(csnw.ctx,
		chunker,
		csnw.desyncStore,
		csnw.concurrency,
	)
	if err != nil {
		return err
	}

	// upload index into the index store
	err = csnw.desyncIndexStore.StoreIndex(csnw.name, caidx)
	if err != nil {
		return err
	}
	return nil
}
