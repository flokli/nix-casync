package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/flokli/nix-casync/store"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

type Handler struct {
	BinaryCacheStore store.BinaryCacheStore
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/nix-cache-info" {
		h.handleNixCacheInfo(w, r)
		return
	}
	// TODO: should we keep looking at r.URL.Path in downstream methods? Or pass in the path?
	if strings.HasPrefix(r.URL.Path, "/nar/") {
		h.handleNar(w, r)
		return
	}
	if len(r.URL.Path) == 41 && strings.HasSuffix(r.URL.Path, ".narinfo") {
		h.handleNarinfo(w, r)
		return
	}
}

func (h *Handler) handleNixCacheInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		// TODO: make configurable
		w.Write([]byte("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40"))
		return
	}
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (h *Handler) handleNarinfo(w http.ResponseWriter, r *http.Request) {
	outputhash, err := nixbase32.DecodeString(r.URL.Path[1:33])
	if err != nil {
		http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusBadRequest)
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		narinfo, err := h.BinaryCacheStore.GetNarInfo(r.Context(), outputhash)
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
		err = h.BinaryCacheStore.PutNarInfo(r.Context(), outputhash, ni)
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusInternalServerError)
			return
		}
		return
	}
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (h *Handler) handleNar(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("%v %v -> %v\n", r.Method, r.URL.Path, r.URL.Path[5:57])
	narhash, err := nixbase32.DecodeString(r.URL.Path[5:57])
	if err != nil {
		http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusBadRequest)
	}
	if r.URL.Path[57:] != ".nar" {
		// only handle .nar files right now
		http.Error(w, fmt.Sprintf("handle-nar: invalid path extension (%v) - only .nar supported", r.URL.Path[57:]), http.StatusBadRequest)
		return
	}

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		narReader, err := h.BinaryCacheStore.GetNar(r.Context(), narhash)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, fmt.Sprintf("handle-nar: %v", err), status)
			return
		}

		w.Header().Add("Content-Type", "application/x-nix-nar")
		// TODO: calc and send content-length
		io.Copy(w, narReader)
		return
	}

	if r.Method == http.MethodPut {
		nw, err := h.BinaryCacheStore.PutNar(r.Context(), narhash)
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-nar: %v", err), http.StatusInternalServerError)
			return
		}

		// write payload into writer
		_, err = io.Copy(nw, r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-nar: copy: %v", err), http.StatusInternalServerError)
			return
		}

		// check for errors during close
		err = nw.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("handle-nar: close: %v", err), http.StatusInternalServerError)
			return
		}

		return
	}

	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}
