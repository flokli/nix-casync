package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"io/ioutil"
	"os"

	"github.com/folbricht/desync"
)

// CasyncStoreWriter provides a io.WriteCloser interface
// The whole content of the blob is written to it.
// Internally, it'll write it to a temporary file.
// On close, its contents will be chunked,
// the index added to the index store, and the chunks added to the chunk store.
type CasyncStoreWriter struct {
	io.WriteCloser

	ctx context.Context

	desyncStore      desync.WriteStore
	desyncIndexStore desync.IndexWriteStore

	concurrency         int
	chunkSizeMinDefault uint64
	chunkSizeAvgDefault uint64
	chunkSizeMaxDefault uint64

	f            *os.File
	bytesWritten uint64
	hash         hash.Hash
}

// NewCasyncStoreWriter returns a properly initialized casyncStoreWriter.
func NewCasyncStoreWriter(
	ctx context.Context,
	desyncStore desync.WriteStore,
	desyncIndexStore desync.IndexWriteStore,
	concurrency int,
	chunkSizeMinDefault uint64,
	chunkSizeAvgDefault uint64,
	chunkSizeMaxDefault uint64,
) (*CasyncStoreWriter, error) {
	tmpFile, err := ioutil.TempFile("", "blob")
	if err != nil {
		return nil, err
	}
	// Cleanup is handled in Close()

	return &CasyncStoreWriter{
		ctx: ctx,

		desyncStore:      desyncStore,
		desyncIndexStore: desyncIndexStore,

		concurrency:         concurrency,
		chunkSizeMinDefault: chunkSizeMinDefault,
		chunkSizeAvgDefault: chunkSizeAvgDefault,
		chunkSizeMaxDefault: chunkSizeMaxDefault,

		f:    tmpFile,
		hash: sha256.New(),
	}, nil
}

func (csw *CasyncStoreWriter) Write(p []byte) (int, error) {
	csw.hash.Write(p)
	csw.bytesWritten += uint64(len(p))

	return csw.f.Write(p)
}

func (csw *CasyncStoreWriter) Close() error {
	// at the end, we want to remove the tempfile
	defer os.Remove(csw.f.Name())

	// calculate how the file will be called
	indexName := hex.EncodeToString(csw.Sha256Sum())

	// check if that same file has already been uploaded.
	_, err := csw.desyncIndexStore.GetIndex(indexName)

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if err == nil {
		// if the file already exists in the index, we're done.
		return nil
	}

	// flush the tempfile and seek to the start
	err = csw.f.Sync()
	if err != nil {
		return err
	}

	_, err = csw.f.Seek(0, 0)
	if err != nil {
		return err
	}

	// Run the chunker on the tempfile
	chunker, err := desync.NewChunker(
		csw.f,
		csw.chunkSizeMinDefault,
		csw.chunkSizeAvgDefault,
		csw.chunkSizeMaxDefault,
	)
	if err != nil {
		return err
	}

	// upload all chunks into the store
	caidx, err := desync.ChunkStream(csw.ctx,
		chunker,
		csw.desyncStore,
		csw.concurrency,
	)
	if err != nil {
		return err
	}

	// upload index into the index store
	// name it after the hash
	err = csw.desyncIndexStore.StoreIndex(indexName, caidx)
	if err != nil {
		return err
	}

	return nil
}

func (csw *CasyncStoreWriter) Sha256Sum() []byte {
	return csw.hash.Sum([]byte{})
}

func (csw *CasyncStoreWriter) BytesWritten() uint64 {
	return csw.bytesWritten
}
