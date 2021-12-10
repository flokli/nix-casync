package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/flokli/nix-casync/pkg/store"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	Handler *chi.Mux

	narStore     store.NarStore
	narinfoStore store.NarinfoStore
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
	pattern := "/nar/{narhash:^[" + nixbase32.Alphabet + "]{52}}.nar"
	s.Handler.Get(pattern, s.handleNar)
	s.Handler.Head(pattern, s.handleNar)
	s.Handler.Put(pattern, s.handleNar)
}

func (s *Server) handleNar(w http.ResponseWriter, r *http.Request) {
	narhashStr := chi.URLParam(r, "narhash")
	narhash, err := nixbase32.DecodeString(narhashStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusBadRequest)
	}

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
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
		w2, err := s.narStore.PutNar(r.Context(), narhash)
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
			return
		}

		// copy the body of the request into w2
		_, err = io.Copy(w2, r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
		}
		err = w2.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("PUT handle-nar: %v", err), http.StatusInternalServerError)
		}

		return
	}

	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}
