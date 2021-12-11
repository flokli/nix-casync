package server

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/flokli/nix-casync/pkg/store"
	"github.com/flokli/nix-casync/pkg/store/narinfostore"
	"github.com/flokli/nix-casync/pkg/store/narstore"
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
	caStore, err := narstore.NewCasyncStore(castrDir, caidxDir)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		caStore.Close()
	})

	testHandlerNar(t, caStore)
}

func TestNarinfoFilestore(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "narinfo")
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	narinfoFilestore, err := narinfostore.NewFileStore(tmpDir)
	if err != nil {
		panic(err)
	}
	t.Cleanup(func() {
		narinfoFilestore.Close()
	})
	testHandlerNarinfo(t, narinfoFilestore)
}

func TestMemoryStore(t *testing.T) {
	memoryStore := store.NewMemoryStore()
	t.Cleanup(func() {
		memoryStore.Close()
	})
	testHandlerNar(t, memoryStore)
	testHandlerNarinfo(t, memoryStore)
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

// testHandlerNar receives an intialized NarStore and tests the handler against it
func testHandlerNar(t *testing.T, narStore store.NarStore) {
	server := NewServer()
	server.MountNarStore(narStore)

	path := "/nar/0mw6qwsrz35cck0wnjgmfnjzwnjbspsyihnfkng38kxghdc9k9zd.nar"
	// read in the text fixture
	tdr := readTestData("../../test/compression_none" + path)
	defer tdr.Close()

	t.Run("PUT .nar", func(t *testing.T) {
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

	tdr.Seek(0, 0)
	expectedContents, err := io.ReadAll(tdr)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("GET .nar", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req, err := http.NewRequest("GET", path, nil)
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

	t.Run("GET non-existent .nar", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/nar/0mw6qwsrz35cck0wnjgmfnjzwnjbspsyihnfkng38kxghdc9k9zc.nar", nil)
		if err != nil {
			t.Fatal(err)
		}
		server.Handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
	})
}

// testHandlerNarinfo receives an intialized NarinfoStore and tests the handler against it
func testHandlerNarinfo(t *testing.T, narinfoStore store.NarinfoStore) {
	server := NewServer()
	server.MountNarinfoStore(narinfoStore)

	path := "/dr76fsw7d6ws3pymafx0w0sn4rzbw7c9.narinfo"
	// read in the text fixture
	tdr := readTestData("../../test/compression_none" + path)
	defer tdr.Close()

	t.Run("PUT .narinfo", func(t *testing.T) {
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

	tdr.Seek(0, 0)
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

	t.Run("GET non-existent .narinfo", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req, err := http.NewRequest("GET", "/dr76fsw7d6ws3pymafx0w0sn4rzbw7c8.narinfo", nil)
		if err != nil {
			t.Fatal(err)
		}
		server.Handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
	})
}
