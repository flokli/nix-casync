package store

import (
	"context"
	"io/ioutil"
	"os"
	"path"

	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

// FileStore implements NarinfoStore
var _ NarinfoStore = &FileStore{}

type FileStore struct {
	directory string
}

func NewFileStore(directory string) (*FileStore, error) {
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &FileStore{directory: directory}, nil
}

// narinfoPath constructs the name of the .narinfo file
func (fs *FileStore) narinfoPath(outputhash []byte) string {
	return path.Join(fs.directory, nixbase32.EncodeToString(outputhash)+".narinfo")
}

func (fs *FileStore) GetNarInfo(ctx context.Context, outputhash []byte) (*narinfo.NarInfo, error) {
	p := fs.narinfoPath(outputhash)

	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	ni, err := narinfo.Parse(f)
	if err != nil {
		return nil, err
	}
	return ni, nil
}

func (fs *FileStore) PutNarInfo(ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error {
	p := fs.narinfoPath(outputhash)

	// create a tempfile (in the same directory), write to it, then move it to where we want it to be
	// this is to ensure an atomic write/replacement.
	tmpFile, err := ioutil.TempFile(fs.directory, "narinfo")
	if err != nil {
		return err
	}

	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write([]byte(contents.String()))
	if err != nil {
		return err
	}

	err = tmpFile.Sync()
	if err != nil {
		return err
	}
	err = tmpFile.Close()
	if err != nil {
		return err
	}

	return os.Rename(tmpFile.Name(), p)
}

func (fs *FileStore) Close() error {
	return nil
}
