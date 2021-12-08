package store

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

	f *os.File
}

// init needs to be called by everything reading
// It ensures the tempfile is set up and populated with the assembled data
func (csnr *casyncStoreNarReader) init() error {
	tmpFile, err := ioutil.TempFile("", "nar")
	if err != nil {
		return err
	}
	// Cleanup is handled in csnr.Close(), or whenever there's an error during init

	// run AssembleFile into a temporary file
	_, err = desync.AssembleFile(csnr.ctx, tmpFile.Name(), csnr.caidx, csnr.desyncStore, csnr.seeds, csnr.concurrency, csnr.pb)
	if err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	// flush and seek to the beginning
	err = tmpFile.Sync()
	if err != nil {
		os.Remove(tmpFile.Name())
		return err
	}
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	// set csnr.f to the successfully assembled file
	csnr.f = tmpFile
	return nil
}

func (csnr *casyncStoreNarReader) Read(p []byte) (n int, err error) {
	if csnr.f == nil {
		err := csnr.init()
		if err != nil {
			return 0, err
		}
	}
	return csnr.f.Read(p)
}

func (csnr *casyncStoreNarReader) Close() error {
	defer os.Remove(csnr.f.Name())
	return csnr.f.Close()
}
