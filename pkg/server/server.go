package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/flokli/nix-casync/pkg/server/compression"
	"github.com/flokli/nix-casync/pkg/store"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	Handler *chi.Mux

	narStore     store.NarStore
	narinfoStore store.NarinfoStore
	io.Closer
}

func NewServer() *Server {
	r := chi.NewRouter()
	//r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("nix-casync"))
	})

	r.Get("/nix-cache-info", func(w http.ResponseWriter, r *http.Request) {
		// TODO: make configurable
		w.Write([]byte("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40"))
	})

	return &Server{Handler: r}
}

func (s *Server) Close() error {
	err := s.narStore.Close()
	if err != nil {
		return err
	}
	return s.narinfoStore.Close()
}

func (s *Server) MountNarinfoStore(narinfoStore store.NarinfoStore) {
	s.narinfoStore = narinfoStore
	pattern := "/{outputhash:^[" + nixbase32.Alphabet + "]{32}}.narinfo"
	s.Handler.Get(pattern, s.handleNarinfo)
	s.Handler.Head(pattern, s.handleNarinfo)
	s.Handler.Put(pattern, s.handleNarinfo)
}

func (s *Server) handleNarinfo(w http.ResponseWriter, r *http.Request) {
	outputhashStr := chi.URLParam(r, "outputhash")
	outputhash, err := nixbase32.DecodeString(outputhashStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusBadRequest)
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		narinfo, err := s.narinfoStore.GetNarInfo(r.Context(), outputhash)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), status)
			return
		}

		// render narinfo
		narinfoContent := narinfo.String()

		w.Header().Add("Content-Type", "text/x-nix-narinfo")
		w.Header().Add("Content-Length", fmt.Sprintf("%d", len(narinfoContent)))

		w.Write([]byte(narinfoContent))
		return
	}
	if r.Method == http.MethodPut {
		ni, err := narinfo.Parse(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusBadRequest)
			return
		}
		err = s.narinfoStore.PutNarInfo(r.Context(), outputhash, ni)
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusInternalServerError)
			return
		}
		return
	}
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (s *Server) MountNarStore(narStore store.NarStore) {
	s.narStore = narStore
	patternPlain := "/nar/{narhash:^[" + nixbase32.Alphabet + "]{52}$}.nar"
	patternCompressed := patternPlain + `{compressionSuffix:^(\.\w+)$}`

	// We only serve plain Narfiles
	s.Handler.Get(patternPlain, s.handleNar)
	s.Handler.Head(patternPlain, s.handleNar)

	// When Nix uploads compressed paths (if compression=none is not set),
	// we simply can't know if a file exists or not.
	// Nix uploads /nar/$filehash.nar.$compressionType, not /nar/$narhash.nar.$compressionType,
	// but we content-address the decompressed contents.
	// Register a dumb HEAD handler that returns a 404 for all compressed paths.
	// This will cause Nix to unnecessarily upload Narfiles multiple times.
	// It's not as bad as it sounds, as this only affects multiple Narinfo files
	// referencing the same Narfile.
	// Nix first checks the Narinfo files for existence, and doesn't update the Narfile.
	s.Handler.Head(patternCompressed, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Can't know for compressed Narfiles", http.StatusNotFound)
	})

	s.Handler.Put(patternPlain, s.handleNar)
	s.Handler.Put(patternCompressed, s.handleNar)
}

func (s *Server) handleNar(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		narhashStr := chi.URLParam(r, "narhash")
		narhash, err := nixbase32.DecodeString(narhashStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusBadRequest)
		}
		r, size, err := s.narStore.GetNar(r.Context(), narhash)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, fmt.Sprintf("GET handle-nar: %v", err), status)
			return
		}
		defer r.Close()

		w.Header().Add("Content-Type", "application/x-nix-nar")
		w.Header().Add("Content-Length", fmt.Sprintf("%d", size))
		io.Copy(w, r)

		return
	}

	if r.Method == http.MethodPut {
		narWriter, err := s.narStore.PutNar(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
			return
		}

		// There might be suffixes indicating compression, wrap the request body via the generic decompressor
		reader, err := compression.NewDecompressorBySuffix(r.Body, chi.URLParam(r, "compressionSuffix"))
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
		}

		// copy the body of the request into narwriter
		_, err = io.Copy(narWriter, reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
		}
		err = narWriter.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
		}

		return
	}

	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}
