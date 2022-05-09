package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flokli/nix-casync/pkg/server"
	"github.com/flokli/nix-casync/pkg/server/compression"
	"github.com/flokli/nix-casync/pkg/store/blobstore"
	"github.com/flokli/nix-casync/pkg/store/metadatastore"
	"github.com/flokli/nix-casync/pkg/util"
	"github.com/flokli/nix-casync/test"
	"github.com/klauspost/compress/zstd"
	"github.com/nix-community/go-nix/pkg/nar/narinfo"
	"github.com/nix-community/go-nix/pkg/nixbase32"
	"github.com/stretchr/testify/assert"
)

// TestHandler tests the handler.
func TestHandler(t *testing.T) {
	blobStore := blobstore.NewMemoryStore()
	defer blobStore.Close()

	metadataStore := metadatastore.NewMemoryStore()
	defer metadataStore.Close()

	server := server.NewServer(blobStore, metadataStore, "zstd", 40)

	testDataT := test.GetTestDataTable()

	tdA, exists := testDataT["a"]
	if !exists {
		panic("testData[a] doesn't exist")
	}

	tdAOutputHash, err := util.GetHashFromStorePath(tdA.Narinfo.StorePath)
	if !exists {
		panic(err)
	}

	tdB, exists := testDataT["b"]
	if !exists {
		panic("testData[b] doesn't exist")
	}

	tdBOutputHash, err := util.GetHashFromStorePath(tdB.Narinfo.StorePath)
	if !exists {
		panic(err)
	}

	tdC, exists := testDataT["c"]
	if !exists {
		panic("testData[c] doesn't exist")
	}

	tdCOutputHash, err := util.GetHashFromStorePath(tdC.Narinfo.StorePath)
	if !exists {
		panic(err)
	}

	t.Run("Nar tests", func(t *testing.T) {
		narpath := "/nar/" + nixbase32.EncodeToString(tdA.Narinfo.NarHash.Digest) + ".nar"

		t.Run("GET non-existent .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", narpath, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
		})

		t.Run("PUT .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", narpath, bytes.NewReader(tdA.NarContents))
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

		t.Run("GET .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", narpath, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"application/x-nix-nar"}, rr.Result().Header["Content-Type"])
			assert.Equal(t, []string{fmt.Sprintf("%d", tdA.Narinfo.NarSize)}, rr.Result().Header["Content-Length"])

			// read in the retrieved body
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tdA.NarContents, actualContents)
		})

		// get compressed .nar, which should match the uncompressed .nar after decompressing with zstd
		t.Run("GET compressed .nar", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", narpath+".zst", nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"application/x-nix-nar"}, rr.Result().Header["Content-Type"])
			// We don't send the Content-Length header here, as we compress on the fly and don't know upfront

			// read the body into a buffer
			buf, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}

			// read in the retrieved body
			zstdReader, err := zstd.NewReader(bytes.NewReader(buf))
			if err != nil {
				t.Fatal(err)
			}
			defer zstdReader.Close()
			actualContents, err := io.ReadAll(zstdReader)
			if err != nil {
				t.Fatal(err)
			}

			// decompressed, it should look like the Nar we initially wrote
			assert.Equal(t, tdA.NarContents, actualContents)
		})

		// TODO: remove Nar file to ensure we don't just no-op the upload
		// blobStore.DropAll(context.Background())

		t.Run("PUT compressed .nar", func(t *testing.T) {
			// What name we upload it as doesn't really matter
			// (we still use the narhash here, even though Nix would use the file hash)
			// The only thing that matters is the extension.
			narpathZstd := "/nar/" + nixbase32.EncodeToString(tdA.Narinfo.NarHash.Digest) + ".nar.zst"

			// compress the .nar file on the fly, store in nb
			var b bytes.Buffer
			wc, err := compression.NewCompressor(&b, "zstd")
			assert.NoError(t, err, "creating a new compressor shouldn't error")
			_, err = wc.Write(tdA.NarContents)
			assert.NoError(t, err, "writing to compressor shouldn't error")
			err = wc.Close()
			assert.NoError(t, err, "closing compressor shouldn't error")

			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "PUT", narpathZstd, bytes.NewReader(b.Bytes()))
			assert.NoError(t, err)

			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

			// check it exists in the store
			narMeta, err := metadataStore.GetNarMeta(context.Background(), tdA.Narinfo.NarHash.Digest)
			assert.NoError(t, err)
			assert.NotEqual(t, 0, narMeta.Size)
		})
	})

	t.Run("Narinfo tests", func(t *testing.T) {
		path := "/" + nixbase32.EncodeToString(tdAOutputHash) + ".narinfo"

		// synthesize a minimal narinfo for a compressed version
		// This is what we get served from the handler,
		// as zstd compression is configured.
		// We also use it later in the test to upload a compressed version.
		smallNarinfo := tdA.Narinfo
		smallNarinfo.URL += ".zst"

		smallNarinfo.FileHash = nil
		smallNarinfo.FileSize = 0
		smallNarinfo.Compression = "zstd"
		b := bytes.NewBufferString(smallNarinfo.String())
		smallNarinfoContents := b.Bytes()

		t.Run("GET non-existent .narinfo", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusNotFound, rr.Result().StatusCode)
		})

		t.Run("PUT .narinfo", func(t *testing.T) {
			rr := httptest.NewRecorder()
			req, err := http.NewRequest("PUT", path, bytes.NewReader(tdA.NarinfoContents))
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

		// when we retrieve it back, it should be served compressed
		// (as we initialize the handler with zstd compression)
		t.Run("GET .narinfo", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"text/x-nix-narinfo"}, rr.Result().Header["Content-Type"])
			assert.Equal(t, []string{fmt.Sprintf("%d", len(smallNarinfoContents))}, rr.Result().Header["Content-Length"])

			// read in the retrieved body
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, smallNarinfoContents, actualContents)
		})

		t.Run("PUT .narinfo referring to compressed NAR", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "PUT", path, bytes.NewReader(smallNarinfoContents))
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

		// when we retrieve it back, it should still look like the minimal narinfo
		t.Run("GET .narinfo", func(t *testing.T) {
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
			if err != nil {
				t.Fatal(err)
			}
			server.Handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusOK, rr.Result().StatusCode)
			assert.Equal(t, []string{"text/x-nix-narinfo"}, rr.Result().Header["Content-Type"])
			assert.Equal(t, []string{fmt.Sprintf("%d", len(smallNarinfoContents))}, rr.Result().Header["Content-Length"])

			// read in the retrieved body
			actualContents, err := io.ReadAll(rr.Result().Body)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, smallNarinfoContents, actualContents)
		})

		t.Run("PUT .nar for B", func(t *testing.T) {
			narpath := "/nar/" + nixbase32.EncodeToString(tdB.Narinfo.NarHash.Digest) + ".nar"
			rr := httptest.NewRecorder()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "PUT", narpath, bytes.NewReader(tdB.NarContents))
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
	})

	bNarinfoPath := "/" + nixbase32.EncodeToString(tdBOutputHash) + ".narinfo"

	t.Run("PUT .narinfo for B", func(t *testing.T) {
		rr := httptest.NewRecorder()

		req, err := http.NewRequest("PUT", bNarinfoPath, bytes.NewReader(tdB.NarinfoContents))
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

	t.Run("GET .narinfo for B", func(t *testing.T) {
		rr := httptest.NewRecorder()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", bNarinfoPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		server.Handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

		// parse the .narinfo file we get back
		ni, err := narinfo.Parse(rr.Result().Body)
		assert.NoError(t, err)

		// assert references are preserved
		assert.Equal(t, tdB.Narinfo.References, ni.References)
	})

	t.Run("PUT .nar for C", func(t *testing.T) {
		narpath := "/nar/" + nixbase32.EncodeToString(tdC.Narinfo.NarHash.Digest) + ".nar"
		rr := httptest.NewRecorder()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "PUT", narpath, bytes.NewReader(tdC.NarContents))
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

	cNarinfoPath := "/" + nixbase32.EncodeToString(tdCOutputHash) + ".narinfo"

	t.Run("PUT .narinfo for C (contains self-reference)", func(t *testing.T) {
		rr := httptest.NewRecorder()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "PUT", cNarinfoPath, bytes.NewReader(tdC.NarinfoContents))
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

	t.Run("GET .narinfo for C (contains self-reference)", func(t *testing.T) {
		rr := httptest.NewRecorder()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", cNarinfoPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		server.Handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Result().StatusCode)

		// parse the .narinfo file we get back
		ni, err := narinfo.Parse(rr.Result().Body)
		assert.NoError(t, err)

		// assert references are preserved
		assert.Equal(t, tdC.Narinfo.References, ni.References)
	})
}
