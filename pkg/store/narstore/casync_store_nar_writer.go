package narstore

import (
	"context"
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/folbricht/desync"
	"github.com/numtide/go-nix/nixbase32"
)

// casyncStoreNarWriter provides a io.WriteCloser interface
// The whole content of the .nar file is written to it.
// Internally, it'll write it to a temporary file.
// On close, its contents will be chunked,
// the index added to the index store, and the chunks added to the chunk store.
type casyncStoreNarWriter struct {
	io.WriteCloser

	ctx context.Context

	desyncStore      desync.WriteStore
	desyncIndexStore desync.IndexWriteStore

	concurrency         int
	chunkSizeMinDefault uint64
	chunkSizeAvgDefault uint64
	chunkSizeMaxDefault uint64

	f    *os.File
	hash hash.Hash
}

// init needs to be called by everything writing
// It ensures the tempfile is set up
func (csnw *casyncStoreNarWriter) init() error {
	tmpFile, err := ioutil.TempFile("", ".nar")
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
	csnw.hash.Write(p)
	return csnw.f.Write(p)
}

func (csnw *casyncStoreNarWriter) Close() error {
	// at the end, we want to remove the tempfile
	defer os.Remove(csnw.f.Name())

	// calculate how the file will be called
	indexName := nixbase32.EncodeToString(csnw.Sha256Sum()) + ".nar"

	// check if that same file has already been uploaded.
	_, err := csnw.desyncIndexStore.GetIndex(indexName)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if err == nil {
		// if the file already exists in the index, we're done.
		return nil
	}

	// flush the tempfile and seek to the start
	err = csnw.f.Sync()
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
	// name it after the narhash
	err = csnw.desyncIndexStore.StoreIndex(indexName, caidx)
	if err != nil {
		return err
	}
	return nil
}

func (csnw *casyncStoreNarWriter) Sha256Sum() []byte {
	return csnw.hash.Sum([]byte{})
}
