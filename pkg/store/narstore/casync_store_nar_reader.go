package narstore

import (
	"context"
	"io"
	"io/ioutil"
	"os"

	"github.com/folbricht/desync"
)

// casyncStoreNarReader provides a io.ReadCloser
// on the first read, it creates a tempfile, assembles the contents into it,
// then reads into that file.
type casyncStoreNarReader struct {
	io.ReadCloser

	ctx         context.Context
	caidx       desync.Index
	desyncStore desync.Store
	seeds       []desync.Seed
	concurrency int
	pb          desync.ProgressBar

	f             *os.File
	fileAssembled bool // whether AssembleFile was already run
}

// NewCasyncStoreNarReader returns a properly initialized casyncStoreNarReader
func NewCasyncStoreNarReader(
	ctx context.Context,
	caidx desync.Index,
	desyncStore desync.Store,
	seeds []desync.Seed,
	concurrency int,
	pb desync.ProgressBar,
) (*casyncStoreNarReader, error) {
	tmpFile, err := ioutil.TempFile("", "nar")
	if err != nil {
		return nil, err
	}
	// Cleanup is handled in csnr.Close(), or whenever there's an error during init

	return &casyncStoreNarReader{
		ctx:         ctx,
		caidx:       caidx,
		desyncStore: desyncStore,
		seeds:       seeds,
		concurrency: concurrency,
		pb:          pb,
		f:           tmpFile,
	}, nil
}

func (csnr *casyncStoreNarReader) Read(p []byte) (n int, err error) {
	// if this is the first read, we need to run AssembleFile into f
	// if there's any error, we return it.
	// It's up to the caller to also run Close(), which will clean up the tmpfile
	if !csnr.fileAssembled {
		_, err = desync.AssembleFile(csnr.ctx, csnr.f.Name(), csnr.caidx, csnr.desyncStore, csnr.seeds, csnr.concurrency, csnr.pb)
		if err != nil {
			return 0, err
		}

		// flush and seek to the beginning
		err = csnr.f.Sync()
		if err != nil {
			return 0, err
		}
		_, err = csnr.f.Seek(0, 0)
		if err != nil {
			return 0, err
		}
		// we successfully went till here
		csnr.fileAssembled = true
	}
	return csnr.f.Read(p)
}

func (csnr *casyncStoreNarReader) Close() error {
	defer os.Remove(csnr.f.Name())
	return csnr.f.Close()
}
