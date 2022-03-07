package metadatastore

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/flokli/nix-casync/test"
	"github.com/stretchr/testify/assert"
)

func TestMemoryStore(t *testing.T) {
	memoryStore := NewMemoryStore()
	t.Cleanup(func() {
		memoryStore.Close()
	})
	testMetadataStore(t, memoryStore)
}

func TestFileStore(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "narinfo")
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})
	fileStore, err := NewFileStore(tmpDir)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		fileStore.Close()
	})
	testMetadataStore(t, fileStore)
}

// testMetadataStore runs all metadata store tests against the passed store.
func testMetadataStore(t *testing.T, metadataStore MetadataStore) {
	testDataT := test.GetTestData()

	tdA, exists := testDataT["a"]
	if !exists {
		panic("testData[a] doesn't exist")
	}
	tdAPathInfo, tdANarMeta, err := ParseNarinfo(tdA.Narinfo)
	if err != nil {
		t.Fatal(err)
	}
	tdB, exists := testDataT["b"]
	if !exists {
		panic("testData[b] doesn't exist")
	}
	tdBPathInfo, tdBNarMeta, err := ParseNarinfo(tdB.Narinfo)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("NarMeta", func(t *testing.T) {
		t.Run("GetNarMetaNotFound", func(t *testing.T) {
			_, err := metadataStore.GetNarMeta(context.Background(), tdANarMeta.NarHash)
			if assert.Error(t, err) {
				assert.ErrorIsf(t, err, os.ErrNotExist, "on a non-existent NarMeta, there should be a os.ErrNotExist in the error chain")
			}
		})

		t.Run("PutNarMeta", func(t *testing.T) {
			err := metadataStore.PutNarMeta(context.Background(), tdANarMeta)
			assert.NoError(t, err)
		})

		t.Run("PutNarMeta again", func(t *testing.T) {
			err := metadataStore.PutNarMeta(context.Background(), tdANarMeta)
			assert.NoError(t, err)
		})

		t.Run("GetNarMeta", func(t *testing.T) {
			narMeta, err := metadataStore.GetNarMeta(context.Background(), tdANarMeta.NarHash)
			assert.NoError(t, err)
			assert.Equal(t, *tdANarMeta, *narMeta)
		})
	})

	t.Run("PathInfo", func(t *testing.T) {
		t.Run("GetPathInfoNotFound", func(t *testing.T) {
			_, err := metadataStore.GetPathInfo(context.Background(), tdAPathInfo.OutputHash)
			if assert.Error(t, err) {
				assert.ErrorIsf(t, err, os.ErrNotExist, "on a non-existent PathInfo, there should be a os.ErrNotExist in the error chain")
			}
		})

		t.Run("PutPathInfo", func(t *testing.T) {
			err := metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.NoError(t, err)
		})

		t.Run("PutPathInfo again", func(t *testing.T) {
			err := metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.NoError(t, err)
		})

		t.Run("GetPathInfo", func(t *testing.T) {
			pathInfo, err := metadataStore.GetPathInfo(context.Background(), tdAPathInfo.OutputHash)
			if assert.NoError(t, err) {
				assert.Equal(t, *tdAPathInfo, *pathInfo)
			}
		})
	})

	t.Run("Integrity Tests", func(t *testing.T) {
		err := metadataStore.DropAll(context.Background())
		if err != nil {
			panic(err)
		}

		// Test it's not possible to upload A PathInfo without uploading A NarMeta first
		t.Run("require NarMeta first", func(t *testing.T) {
			err = metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.Error(t, err)

			err = metadataStore.PutNarMeta(context.Background(), tdANarMeta)
			assert.NoError(t, err)

			err = metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.NoError(t, err)
		})

		err = metadataStore.DropAll(context.Background())
		if err != nil {
			panic(err)
		}
		// Try to upload B, which refers to A (which is not uploaded) should fail,
		// until we upload A (and it's pathinfo)
		t.Run("require References to be uploaded first", func(t *testing.T) {
			// upload NarMeta for B
			err = metadataStore.PutNarMeta(context.Background(), tdBNarMeta)
			assert.Error(t, err, "uploading NarMeta with references to non-existing PathInfo should fail")

			// upload PathInfo for A, which should also fail without NarMeta for A
			err = metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.Error(t, err, "uploading PathInfo with references to non-existing NarMeta should fail")

			// now try to upload NarMeta for A, then PathInfo for A, then NarMeta for B, then PathInfo for B, which should succeed
			err = metadataStore.PutNarMeta(context.Background(), tdANarMeta)
			assert.NoError(t, err)
			err = metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.NoError(t, err)
			err = metadataStore.PutNarMeta(context.Background(), tdBNarMeta)
			assert.NoError(t, err)
			err = metadataStore.PutPathInfo(context.Background(), tdBPathInfo)
			assert.NoError(t, err)
		})

		err = metadataStore.DropAll(context.Background())
		if err != nil {
			panic(err)
		}
		// upload NarMeta for A, then PathInfo for A, then a broken NarMeta for B
		t.Run("PutNarMeta with broken inconsistent references", func(t *testing.T) {
			err = metadataStore.PutNarMeta(context.Background(), tdANarMeta)
			assert.NoError(t, err)
			err = metadataStore.PutPathInfo(context.Background(), tdAPathInfo)
			assert.NoError(t, err)

			brokenNarMeta := *tdBNarMeta
			brokenNarMeta.References = [][]byte{}
			err = metadataStore.PutNarMeta(context.Background(), &brokenNarMeta)
			assert.Error(t, err, "uploading NarMeta with inconsistent References[Str] should fail")
		})
	})
}
