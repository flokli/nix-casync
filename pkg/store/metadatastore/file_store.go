package metadatastore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/flokli/nix-casync/pkg/store"
)

// FileStore implements MetadataStore
var _ MetadataStore = &FileStore{}

type FileStore struct {
	pathInfoDirectory string
	narMetaDirectory  string
}

func NewFileStore(baseDirectory string) (*FileStore, error) {
	pathInfoDirectory := path.Join(baseDirectory, "pathinfo")
	err := os.MkdirAll(pathInfoDirectory, os.ModePerm)
	if err != nil {
		return nil, err
	}
	narMetaDirectory := path.Join(baseDirectory, "narmeta")
	err = os.MkdirAll(narMetaDirectory, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &FileStore{
		pathInfoDirectory: pathInfoDirectory,
		narMetaDirectory:  narMetaDirectory,
	}, nil
}

func (fs *FileStore) pathInfoPath(outputHash []byte) string {
	encodedHash := hex.EncodeToString(outputHash)
	return path.Join(fs.pathInfoDirectory, encodedHash[:4], encodedHash+".json")
}

func (fs *FileStore) narMetaPath(narHash []byte) string {
	encodedHash := hex.EncodeToString(narHash)
	return path.Join(fs.narMetaDirectory, encodedHash[:4], encodedHash+".json")
}

func (fs *FileStore) GetPathInfo(ctx context.Context, outputHash []byte) (*PathInfo, error) {
	p := fs.pathInfoPath(outputHash)

	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var pathInfo PathInfo
	err = json.Unmarshal(b, &pathInfo)
	if err != nil {
		return nil, err
	}
	return &pathInfo, nil
}

func (fs *FileStore) PutPathInfo(ctx context.Context, pathinfo *PathInfo) error {
	err := pathinfo.Check()
	if err != nil {
		return err
	}
	// foreign key constraint: referred NarMeta needs to exist
	_, err = fs.GetNarMeta(ctx, pathinfo.NarHash)
	if err != nil {
		if err == store.ErrNotFound {
			return fmt.Errorf("referred nar doesn't exist")
		}
		return err
	}

	p := fs.pathInfoPath(pathinfo.OutputHash)
	dir := path.Dir(p)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// create a tempfile (in the same directory), write to it, then move it to where we want it to be
	// this is to ensure an atomic write/replacement.
	tmpFile, err := ioutil.TempFile(path.Dir(p), "narinfo")
	if err != nil {
		return err
	}

	defer os.Remove(tmpFile.Name())

	// serialize the pathinfo to json
	b, err := json.Marshal(pathinfo)
	if err != nil {
		return err
	}
	_, err = tmpFile.Write(b)
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

func (fs *FileStore) GetNarMeta(ctx context.Context, narHash []byte) (*NarMeta, error) {
	p := fs.narMetaPath(narHash)

	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var narMeta NarMeta
	err = json.Unmarshal(b, &narMeta)
	if err != nil {
		return nil, err
	}
	return &narMeta, nil
}

func (fs *FileStore) PutNarMeta(ctx context.Context, narMeta *NarMeta) error {
	err := narMeta.Check()
	if err != nil {
		return err
	}

	// foreign key constraint: all references need to exist
	for i, reference := range narMeta.References {
		_, err := fs.GetPathInfo(ctx, reference)
		if err != nil {
			if err == store.ErrNotFound {
				return fmt.Errorf("referred reference %v doesn't exist", narMeta.ReferencesStr[i])
			}
			return err
		}
	}

	p := fs.narMetaPath(narMeta.NarHash)

	dir := path.Dir(p)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// create a tempfile (in the same directory), write to it, then move it to where we want it to be
	// this is to ensure an atomic write/replacement.
	tmpFile, err := ioutil.TempFile(path.Dir(p), "narmeta")
	if err != nil {
		return err
	}

	defer os.Remove(tmpFile.Name())

	// serialize the pathinfo to json
	b, err := json.Marshal(narMeta)
	if err != nil {
		return err
	}
	_, err = tmpFile.Write(b)
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

func (fs *FileStore) DropAll(ctx context.Context) error {
	err := os.RemoveAll(fs.narMetaDirectory)
	if err != nil {
		return err
	}
	err = os.RemoveAll(fs.pathInfoDirectory)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fs.narMetaDirectory, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.MkdirAll(fs.pathInfoDirectory, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}
