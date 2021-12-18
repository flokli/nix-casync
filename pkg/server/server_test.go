package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/datadog/zstd"
	"github.com/flokli/nix-casync/pkg/store/blobstore"
	"github.com/flokli/nix-casync/pkg/store/metadatastore"
	"github.com/numtide/go-nix/nixbase32"
	"github.com/stretchr/testify/assert"
)

// readTestData reads a test file and returns a io.Reader to it
// if there's an error acessing the file, it panics
func readTestData(path string) io.ReadSeekCloser {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	return f
}

// TestHandler tests the handler
func TestHandler(t *testing.T) {
	blobStore := blobstore.NewMemoryStore()
	defer blobStore.Close()
	metadataStore := metadatastore.NewMemoryStore()
	defer metadataStore.Close()
	server := NewServer(blobStore, metadataStore, "zstd")

	t.Run("Nar tests", func(t *testing.T) {
		narhashStr := "0mw6qwsrz35cck0wnjgmfnjzwnjbspsyihnfkng38kxghdc9k9zd"
		narpath := "/nar/" + narhashStr + ".nar"
		testFilePath := "../../test/compression_none/nar/" + narhashStr + ".nar"

		t.Run("GET non-existent .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", narpath, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
		})

		t.Run("PUT .nar", func(t *testing.T) {
			tdr := readTestData(testFilePath)
			defer tdr.Close()
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", narpath, tdr)
			if err != nil {
				t.Fatal(err)
			}

			server.Handler.ServeHTTP(rr, req)

			// expect status to be ok
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

			// expect body to be empty
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, []byte{}, actualContents)
		})

		tdr := readTestData(testFilePath)
		defer tdr.Close()
		expectedContents, err := io.ReadAll(tdr)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("GET .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", narpath, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"application/x-nix-nar"}, rr.Result().Header["Content-Type"])
			assert.Equal(t, []string{fmt.Sprintf("%d", len(expectedContents))}, rr.Result().Header["Content-Length"])

			// read in the retrieved body
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, expectedContents, actualContents)
		})

		// get compressed .nar, which should match the uncompressed .nar after decompressing with zstd
		t.Run("GET compressed .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", narpath+".zst", nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"application/x-nix-nar"}, rr.Result().Header["Content-Type"])
			assert.Equal(t, []string{fmt.Sprintf("%d", len(expectedContents))}, rr.Result().Header["Content-Length"])

			// read in the retrieved body
			zstdReader := zstd.NewReader(rr.Result().Body)
			defer zstdReader.Close()
			actualContents, err := io.ReadAll(zstdReader)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, expectedContents, actualContents)
		})

		// TODO: once we support removing .nar files, remove it before re-uploading
		t.Run("PUT compressed .nar", func(t *testing.T) {
			narhashStr := "0mw6qwsrz35cck0wnjgmfnjzwnjbspsyihnfkng38kxghdc9k9zd"
			narhash := nixbase32.MustDecodeString(narhashStr)
			narpathXz := "/nar/1qv1l5zhzgqc66l0vjy2aw7z50fhga16anlyn2c1yp975aafmz93.nar.xz"
			narTestFilePath := "../../test/compression_xz" + narpathXz
			// upload compressed nar file
			tdr := readTestData(narTestFilePath)
			defer tdr.Close()
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", narpathXz, tdr)
			assert.NoError(t, err)

			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

			// check it exists in the store
			narMeta, err := metadataStore.GetNarMeta(context.Background(), narhash)
			assert.NoError(t, err)
			assert.NotEqual(t, 0, narMeta.Size)
		})
	})

	t.Run("Narinfo tests", func(t *testing.T) {
		outputhashStr := "dr76fsw7d6ws3pymafx0w0sn4rzbw7c9"
		testFilePath := "../../test/compression_none/" + outputhashStr + ".narinfo"
		path := "/" + outputhashStr + ".narinfo"

		t.Run("GET non-existent .narinfo", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", path, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
		})

		t.Run("PUT .narinfo", func(t *testing.T) {
			tdr := readTestData(testFilePath)
			defer tdr.Close()

			rr := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", path, tdr)
			if err != nil {
				t.Fatal(err)
			}

			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

			// expect body to be empty
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, []byte{}, actualContents)
		})

		t.Run("PUT compressed .narinfo", func(t *testing.T) {
			testFileCompressedPath := "../../test/compression_xz/" + outputhashStr + ".narinfo"
			tdr := readTestData(testFileCompressedPath)
			defer tdr.Close()

			rr := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", path, tdr)
			if err != nil {
				t.Fatal(err)
			}

			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

			// expect body to be empty
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, []byte{}, actualContents)
		})

		// read in the text fixture
		tdr := readTestData("../../test/compression_none/" + outputhashStr + "_returned_zstd.narinfo")
		defer tdr.Close()
		expectedContents, err := io.ReadAll(tdr)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("GET .narinfo", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("GET", path, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"text/x-nix-narinfo"}, rr.Result().Header["Content-Type"])
			assert.Equal(t, []string{fmt.Sprintf("%d", len(expectedContents))}, rr.Result().Header["Content-Length"])

			// read in the retrieved body
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, expectedContents, actualContents)
		})
	})
}
