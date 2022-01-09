package metadatastore

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/numtide/go-nix/nar/narinfo"
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

type testDataT map[string]struct {
	pathInfo *PathInfo
	narMeta  *NarMeta
}

// testMetadataStore runs all metadata store tests against the passed store
func testMetadataStore(t *testing.T, metadataStore MetadataStore) {
	// assemble some test data.
	// a is a simple store path without any references.
	// b has c in its references (created by //test/generator/default.nix)
	testData := make(testDataT, 3)

	for _, testItem := range []struct {
		name       string
		outhashStr string
	}{
		{name: "a", outhashStr: "dr76fsw7d6ws3pymafx0w0sn4rzbw7c9"},
		{name: "b", outhashStr: "7cwx623saf2h3z23wsn26icszvskk4iy"},
		{name: "c", outhashStr: "x236iz9shqypbnm64qgqisz0jr4wmj2b"},
	} {
		ni, err := narinfo.Parse(readTestData("../../../test/compression_none/" + testItem.outhashStr + ".narinfo"))
		if err != nil {
			panic(err)
		}
		pathInfo, narMeta, err := ParseNarinfo(ni)
		if err != nil {
			panic(err)
		}
		testData[testItem.name] = struct {
			pathInfo *PathInfo
			narMeta  *NarMeta
		}{
			pathInfo: pathInfo,
			narMeta:  narMeta,
		}
	}

	tdA, exists := testData["a"]
	if !exists {
		panic("testData[a] doesn't exist")
	}
	tdB, exists := testData["b"]
	if !exists {
		panic("testData[b] doesn't exist")
	}
	tdC, exists := testData["c"]
	if !exists {
		panic("testData[c] doesn't exist")
	}

	t.Run("NarMeta", func(t *testing.T) {
		t.Run("GetNarMetaNotFound", func(t *testing.T) {
			_, err := metadataStore.GetNarMeta(context.Background(), tdA.narMeta.NarHash)
			if assert.Error(t, err) {
				assert.ErrorIsf(t, err, os.ErrNotExist, "on a non-existent NarMeta, there should be a os.ErrNotExist in the error chain")
			}
		})

		t.Run("PutNarMeta", func(t *testing.T) {
			err := metadataStore.PutNarMeta(context.Background(), tdA.narMeta)
			assert.NoError(t, err)
		})

		t.Run("PutNarMeta again", func(t *testing.T) {
			err := metadataStore.PutNarMeta(context.Background(), tdA.narMeta)
			assert.NoError(t, err)
		})

		t.Run("GetNarMeta", func(t *testing.T) {
			narMeta, err := metadataStore.GetNarMeta(context.Background(), tdA.narMeta.NarHash)
			assert.NoError(t, err)
			assert.Equal(t, *tdA.narMeta, *narMeta)
		})
	})

	t.Run("PathInfo", func(t *testing.T) {
		t.Run("GetPathInfoNotFound", func(t *testing.T) {
			_, err := metadataStore.GetPathInfo(context.Background(), tdA.pathInfo.OutputHash)
			if assert.Error(t, err) {
				assert.ErrorIsf(t, err, os.ErrNotExist, "on a non-existent PathInfo, there should be a os.ErrNotExist in the error chain")
			}
		})

		t.Run("PutPathInfo", func(t *testing.T) {
			err := metadataStore.PutPathInfo(context.Background(), tdA.pathInfo)
			assert.NoError(t, err)
		})

		t.Run("PutPathInfo again", func(t *testing.T) {
			err := metadataStore.PutPathInfo(context.Background(), tdA.pathInfo)
			assert.NoError(t, err)
		})

		t.Run("GetPathInfo", func(t *testing.T) {
			pathInfo, err := metadataStore.GetPathInfo(context.Background(), tdA.pathInfo.OutputHash)
			if assert.NoError(t, err) {
				assert.Equal(t, *tdA.pathInfo, *pathInfo)
			}
		})
	})

	t.Run("Integrity Tests", func(t *testing.T) {
		err := metadataStore.DropAll(context.Background())
		if err != nil {
			panic(err)
		}

		// Test it's not possible to upload C PathInfo without uploading C NarInfo first
		t.Run("require NarMeta first", func(t *testing.T) {
			err = metadataStore.PutPathInfo(context.Background(), tdC.pathInfo)
			assert.Error(t, err)

			err = metadataStore.PutNarMeta(context.Background(), tdC.narMeta)
			assert.NoError(t, err)

			err = metadataStore.PutPathInfo(context.Background(), tdC.pathInfo)
			assert.NoError(t, err)
		})

		err = metadataStore.DropAll(context.Background())
		if err != nil {
			panic(err)
		}
		// Try to upload B, which refers to C (which is not uploaded) should fail,
		// until we upload C (and it's pathinfo)
		t.Run("require References to be uploaded first", func(t *testing.T) {
			// upload NarMeta for B
			err = metadataStore.PutNarMeta(context.Background(), tdB.narMeta)
			assert.Error(t, err, "uploading NarMeta with references to non-existing PathInfo should fail")

			// upload PathInfo for C, which should also fail without NarMeta for C
			err = metadataStore.PutPathInfo(context.Background(), tdC.pathInfo)
			assert.Error(t, err, "uploading PathInfo with references to non-existing NarMeta should fail")

			// now try to upload NarMeta for C, then PathInfo for C, then NarMeta for B, then PathInfo for B, which should succeed
			err = metadataStore.PutNarMeta(context.Background(), tdC.narMeta)
			assert.NoError(t, err)
			err = metadataStore.PutPathInfo(context.Background(), tdC.pathInfo)
			assert.NoError(t, err)
			err = metadataStore.PutNarMeta(context.Background(), tdB.narMeta)
			assert.NoError(t, err)
			err = metadataStore.PutPathInfo(context.Background(), tdB.pathInfo)
			assert.NoError(t, err)
		})

		err = metadataStore.DropAll(context.Background())
		if err != nil {
			panic(err)
		}
		// upload NarMeta for C, then PathInfo for C, then a broken NarMeta for B
		t.Run("PutNarMeta with broken inconsistent references", func(t *testing.T) {
			err = metadataStore.PutNarMeta(context.Background(), tdC.narMeta)
			assert.NoError(t, err)
			err = metadataStore.PutPathInfo(context.Background(), tdC.pathInfo)
			assert.NoError(t, err)

			brokenNarMeta := *tdB.narMeta
			brokenNarMeta.References = [][]byte{}
			err = metadataStore.PutNarMeta(context.Background(), &brokenNarMeta)
			assert.Error(t, err, "uploading NarMeta with inconsistent References[Str] should fail")
		})
	})
}

// readTestData reads a test file and returns a io.Reader to it
// if there's an error acessing the file, it panics
func readTestData(path string) io.ReadSeekCloser {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	return f
}
