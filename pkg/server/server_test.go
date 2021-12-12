package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/flokli/nix-casync/pkg/store"
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

// TestHandlerNar tests the narfile-specific parts of the handler
func TestHandlerNar(t *testing.T) {
	s := store.NewMemoryStore()
	defer s.Close()
	server := NewServer()
	server.MountNarStore(s)

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
}

// testHandlerNarinfo receives an intialized NarinfoStore and tests the handler against it
func TestHandlerNarinfo(t *testing.T) {
	server := NewServer()
	server.MountNarinfoStore(store.NewMemoryStore())

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

	// read in the text fixture
	tdr := readTestData(testFilePath)
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
}

// TODO: test .narinfo rewriting, test .nar decompression
