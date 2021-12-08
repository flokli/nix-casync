package server

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/flokli/nix-casync/store"
	"github.com/flokli/nix-casync/store/narstore"
	"github.com/stretchr/testify/assert"
)

func mustNewCastore() store.NarStore {
	castrDir, err := ioutil.TempDir("", "castr")
	if err != nil {
		panic(err)
	}
	//defer os.RemoveAll(castrDir)

	caIdxDir, err := ioutil.TempDir("", "caidx")
	if err != nil {
		panic(err)
	}
	//defer os.RemoveAll(caIdxDir)

	caStore, err := narstore.NewCasyncStore(castrDir, caIdxDir)
	if err != nil {
		panic(err)
	}
	return caStore
}

var (
	memoryStore = store.NewMemoryStore()

	// TODO: split handler tests?
	tt = []struct {
		storeName string
		handler   *Handler
	}{
		{"MemoryStore", &Handler{
			NarinfoStore: memoryStore,
			NarStore:     memoryStore,
		}},
		{"CasyncStore", &Handler{
			NarinfoStore: store.NewMemoryStore(),
			NarStore:     mustNewCastore(),
		}},
	}

	// tp describes the test plan.
	// We describe where we HTTP PUT, then HTTP GET from, and where on the filesystem the testdata resides.
	tp = []struct {
		path     string
		filepath string
	}{
		{"/dr76fsw7d6ws3pymafx0w0sn4rzbw7c9.narinfo", "../testdata/compression_none/dr76fsw7d6ws3pymafx0w0sn4rzbw7c9.narinfo"},
		{"/nar/0mw6qwsrz35cck0wnjgmfnjzwnjbspsyihnfkng38kxghdc9k9zd.nar", "../testdata/compression_none/nar/0mw6qwsrz35cck0wnjgmfnjzwnjbspsyihnfkng38kxghdc9k9zd.nar"},
	}
)

// readTestData reads a test file and returns a io.Reader to it
// if there's an error acessing the file, it panics
func readTestData(path string) io.ReadCloser {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	return f
}

func TestBinaryCacheStores(t *testing.T) {
	for _, v := range tt {
		t.Run(v.storeName, func(t *testing.T) {
			for _, vv := range tp {
				// upload the file
				t.Run("PUT "+vv.path, func(t *testing.T) {
					// read in the text fixture
					f := readTestData(vv.filepath)
					defer f.Close()

					rr := httptest.NewRecorder()
					req, err := http.NewRequest("PUT", vv.path, f)
					if err != nil {
						t.Fatal(err)
					}
					v.handler.ServeHTTP(rr, req)
					assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
				})

				// retrieve back the file
				t.Run("GET "+vv.path, func(t *testing.T) {
					rr := httptest.NewRecorder()
					req, err := http.NewRequest("GET", vv.path, nil)
					if err != nil {
						t.Fatal(err)
					}
					v.handler.ServeHTTP(rr, req)

					// read in the text fixture
					f := readTestData(vv.filepath)
					defer f.Close()
					expectedContents, err := io.ReadAll(f)
					if err != nil {
						t.Fatal(err)
					}

					if strings.HasSuffix(vv.path, ".narinfo") {
						assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
						assert.Equal(t, []string{"text/x-nix-narinfo"}, rr.Result().Header["Content-Type"])
						assert.Equal(t, []string{fmt.Sprintf("%d", len(expectedContents))}, rr.Result().Header["Content-Length"])
					} else if strings.HasSuffix(vv.path, ".nar") {
						assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
						assert.Equal(t, []string{"application/x-nix-nar"}, rr.Result().Header["Content-Type"])
					}

					// read in the retrieved body
					actualContents, err := io.ReadAll(rr.Result().Body)
					if err != nil {
						t.Fatal(err)
					}

					assert.Equal(t, expectedContents, actualContents)
				})
			}
		})
	}
}
