package blobstore_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/flokli/nix-casync/pkg/store/blobstore"
	"github.com/flokli/nix-casync/test"
	"github.com/stretchr/testify/assert"
)

func TestCasyncStore(t *testing.T) {
	// populate castr dir
	castrDir, err := ioutil.TempDir("", "castr")
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		os.RemoveAll(castrDir)
	})

	// populate caidx dir
	caidxDir, err := ioutil.TempDir("", "caidx")
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		os.RemoveAll(caidxDir)
	})

	// init casync store
	caStore, err := blobstore.NewCasyncStore(castrDir, caidxDir, 65536)
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		caStore.Close()
	})

	testBlobStore(t, caStore)
}

func TestMemoryStore(t *testing.T) {
	memoryStore := blobstore.NewMemoryStore()

	t.Cleanup(func() {
		memoryStore.Close()
	})

	testBlobStore(t, memoryStore)
}

// testBlobStore runs all nar tests, with a Narstore generated by storeGenerator.
func testBlobStore(t *testing.T, blobStore blobstore.BlobStore) {
	testDataT := test.GetTestDataTable()

	tdA, exists := testDataT["a"]
	if !exists {
		panic("testData[a] doesn't exist")
	}

	tdANarHash := tdA.Narinfo.NarHash.Digest
	tdANarSize := tdA.Narinfo.NarSize

	t.Run("GetBlobNotFound", func(t *testing.T) {
		_, _, err := blobStore.GetBlob(context.Background(), tdANarHash)
		if assert.Error(t, err) {
			assert.ErrorIsf(t,
				err,
				os.ErrNotExist,
				"on a non-existent blob, there should be a os.ErrNotExist in the error chain",
			)
		}
	})

	t.Run("PutBlob", func(t *testing.T) {
		w, err := blobStore.PutBlob(context.Background())
		assert.NoError(t, err)
		defer w.Close()

		n, err := io.Copy(w, bytes.NewReader(tdA.NarContents))

		assert.NoError(t, err)
		assert.Equal(t, tdANarSize, uint64(n))
		assert.NoError(t, w.Close())

		assert.Equal(t, tdANarHash, w.Sha256Sum(), "narhash should be correctly calculated")
	})

	t.Run("PutBlob again", func(t *testing.T) {
		w, err := blobStore.PutBlob(context.Background())
		assert.NoError(t, err)
		defer w.Close()

		n, err := io.Copy(w, bytes.NewReader(tdA.NarContents))
		assert.NoError(t, err)

		assert.Equal(t, tdANarSize, uint64(n))
		assert.NoError(t, w.Close())

		assert.Equal(t, tdANarHash, w.Sha256Sum(), "narhash should still be correctly calculated")
	})

	t.Run("PutNar,then abort", func(t *testing.T) {
		w, err := blobStore.PutBlob(context.Background())
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
	})

	t.Run("GetBlob", func(t *testing.T) {
		r, n, err := blobStore.GetBlob(context.Background(), tdANarHash)

		assert.NoError(t, err)
		assert.Equal(t, tdANarSize, uint64(n))

		actualContents, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, tdA.NarContents, actualContents)
	})

	t.Run("GetNar,then abort", func(t *testing.T) {
		r, _, err := blobStore.GetBlob(context.Background(), tdANarHash)

		assert.NoError(t, err)
		assert.NoError(t, r.Close())
	})
}
