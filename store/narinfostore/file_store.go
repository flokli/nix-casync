package narinfostore

import (
	"context"
	"os"
	"path"

	"github.com/flokli/nix-casync/store"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

// FileStore implements NarinfoStore
var _ store.NarinfoStore = &FileStore{}

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

// NarinfoPath constructs the name of the .narinfo file
func (fs *FileStore) NarinfoPath(outputhash []byte) string {
	return path.Join(fs.directory, nixbase32.EncodeToString(outputhash)+".narinfo")
}

func (fs *FileStore) GetNarInfo(ctx context.Context, outputhash []byte) (*narinfo.NarInfo, error) {
	p := fs.NarinfoPath(outputhash)

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	ni, err := narinfo.Parse(f)
	if err != nil {
		return nil, err
	}
	return ni, nil
}

func (fs *FileStore) PutNarInfo(ctx context.Context, outputhash []byte, contents *narinfo.NarInfo) error {
	p := fs.NarinfoPath(outputhash)

	f, err := os.Create(p)
	if err != nil {
		return err
	}
	// TODO: this should be made transactional
	// (written to a temporary file that's moved to the final location)
	// so (re)uploading a .narinfo doesn't expose a empty,
	// or half-written .narinfo
	// TODO: also check on whether we remove the file properly in error cases
	err = f.Truncate(0)
	if err != nil {
		defer os.Remove(f.Name())
		return err
	}

	_, err = f.Write([]byte(contents.String()))
	if err != nil {
		defer os.Remove(f.Name())
		return err
	}

	err = f.Sync()
	if err != nil {
		defer os.Remove(f.Name())
		return err
	}
	err = f.Close()
	if err != nil {
		defer os.Remove(f.Name())
		return err
	}
	return nil
}
