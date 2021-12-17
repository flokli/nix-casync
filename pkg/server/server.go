package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/flokli/nix-casync/pkg/server/compression"
	"github.com/flokli/nix-casync/pkg/store"
	"github.com/flokli/nix-casync/pkg/store/blobstore"
	"github.com/flokli/nix-casync/pkg/store/metadatastore"
	"github.com/numtide/go-nix/hash"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"

	"github.com/go-chi/chi/v5"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	Handler *chi.Mux

	blobStore     blobstore.BlobStore
	metadataStore metadatastore.MetadataStore

	narServeCompression string // zstd,gzip,brotli,none

	io.Closer
}

func NewServer(blobStore blobstore.BlobStore, metadataStore metadatastore.MetadataStore, narServeCompression string) *Server {
	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("nix-casync"))
	})

	r.Get("/nix-cache-info", func(w http.ResponseWriter, r *http.Request) {
		// TODO: make configurable
		w.Write([]byte("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40"))
	})

	s := &Server{
		Handler:             r,
		blobStore:           blobStore,
		metadataStore:       metadataStore,
		narServeCompression: narServeCompression,
	}

	s.RegisterNarHandlers()
	s.RegisterNarinfoHandlers()
	return s
}

func (s *Server) Close() error {
	// TODO: how do we ensure we close both?
	err := s.blobStore.Close()
	if err != nil {
		return err
	}
	return s.metadataStore.Close()
}

func (s *Server) RegisterNarinfoHandlers() {
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
		// get PathInfo
		pathInfo, err := s.metadataStore.GetPathInfo(r.Context(), outputhash)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), status)
			return
		}

		narhashStr := nixbase32.EncodeToString(pathInfo.NarHash)

		// get NarMeta
		narMeta, err := s.metadataStore.GetNarMeta(r.Context(), pathInfo.NarHash)
		if err != nil {
			// if we can't retrieve the NarMeta, that's a inconsistency.
			log.Errorf(
				"Unable to find NarMeta for NarHash %s, referenced in PathInfo %s",
				narhashStr,
				nixbase32.EncodeToString(pathInfo.OutputHash),
			)
			http.Error(w, fmt.Sprintf("handle-narinfo: %v", err), http.StatusInternalServerError)
		}

		// render the narinfo
		narHash := &hash.Hash{
			HashType: hash.HashTypeSha256,
			Digest:   narMeta.NarHash,
		}
		narInfo := &narinfo.NarInfo{
			StorePath:   pathInfo.StorePath(),
			URL:         "nar/" + narhashStr + ".nar",
			Compression: s.narServeCompression,

			NarHash: narHash,
			NarSize: narMeta.Size,

			References: narMeta.ReferencesStr,

			Deriver: pathInfo.Deriver,

			System: pathInfo.System,

			Signatures: pathInfo.NarinfoSignatures,

			CA: pathInfo.CA,
		}

		if s.narServeCompression != "none" {
			if s.narServeCompression == "zstd" {
				narInfo.URL = narInfo.URL + ".zst"
			} else {
				narInfo.URL = narInfo.URL + "." + s.narServeCompression
			}
		}

		// render narinfo
		narinfoContent := narInfo.String()

		w.Header().Add("Content-Type", "text/x-nix-narinfo")
		w.Header().Add("Content-Length", fmt.Sprintf("%d", len(narinfoContent)))

		w.Write([]byte(narinfoContent))
		return
	}
	if r.Method == http.MethodPut {
		ni, err := narinfo.Parse(r.Body)
		if err != nil {
			log.Errorf("Error parsing .narinfo: %v", err)
			http.Error(w, fmt.Sprintf("Error parsing narinfo: %v", err), http.StatusBadRequest)
			return
		}

		// retrieve the NarMeta
		narMeta, err := s.metadataStore.GetNarMeta(r.Context(), ni.NarHash.Digest)
		if err == store.ErrNotFound {
			log.Error("Rejected uploading a .narinfo pointing to a non-existent narhash")
			http.Error(w, "narinfo points to non-existent narhash", http.StatusBadRequest)
			return
		}

		// Parse the .narinfo into a PathInfo and NarMeta struct
		sentPathInfo, sentNarMeta, err := metadatastore.ParseNarinfo(ni)
		if err != nil {
			log.Errorf("Unable to parse .narinfo: %v", err)
			http.Error(w, "Unable to parse .narinfo: %v", http.StatusBadRequest)
		}

		// Compare narMeta generated out of the .narinfo with the one in the store
		if !narMeta.IsEqualTo(sentNarMeta, false) {
			log.Error("Sent .narinfo with conflicting NarMeta")
			http.Error(w, "Nar Metadata is conflicting", http.StatusBadRequest)
		}

		// HACK: until we implement our own reference scanner on NAR upload, we
		// populate NarMeta.References[Str] on .narinfo upload,
		// if it's empty right now.
		if len(narMeta.References) == 0 && len(sentNarMeta.References) != 0 {
			narMeta.ReferencesStr = sentNarMeta.ReferencesStr
			narMeta.References = sentNarMeta.References
			s.metadataStore.PutNarMeta(r.Context(), narMeta)
		}

		// Do full comparison of NarMeta, including references
		if !narMeta.IsEqualTo(sentNarMeta, true) {
			log.Error("Sent .narinfo with conflicting NarMeta (References)")
			http.Error(w, "Nar Metadata (References) is conflicting", http.StatusBadRequest)
		}

		err = s.metadataStore.PutPathInfo(r.Context(), sentPathInfo)
		if err != nil {
			http.Error(w, fmt.Sprintf("PutPathInfo: %v", err), http.StatusInternalServerError)
			return
		}
		return
	}
	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}

func (s *Server) RegisterNarHandlers() {
	patternPlain := "/nar/{narhash:^[" + nixbase32.Alphabet + "]{52}$}.nar"
	patternCompressed := patternPlain + `{compressionSuffix:^(\.\w+)$}`

	s.Handler.Get(patternPlain, s.handleNar)
	s.Handler.Head(patternPlain, s.handleNar)
	s.Handler.Get(patternCompressed, s.handleNar)
	s.Handler.Head(patternCompressed, s.handleNar)

	// When Nix uploads compressed paths (if compression=none is not set),
	// we simply can't know if a file exists or not.
	// Nix uploads (and checks for existence of) /nar/$filehash.nar.$compressionType,
	// not /nar/$narhash.nar.$compressionType (which is what we use)
	// We content-hash the decompressed contents and discard the compressed uploaded payload,
	// so there's no way to know if /nar/$filehash.nar.$compressionType was uploaded
	// This means we will return 404 whenever Nix tries to upload a compressed NAR file
	// This will cause Nix to unnecessarily upload Narfiles multiple times.
	// It's not as bad as it sounds, as this only affects multiple Narinfo files
	// referencing the same Narfile (and Nix might locally cache the fact it already uploaded
	// that Narfile)

	s.Handler.Put(patternPlain, s.handleNar)
	s.Handler.Put(patternCompressed, s.handleNar)
}

func (s *Server) handleNar(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		narhashStr := chi.URLParam(r, "narhash")
		narhash, err := nixbase32.DecodeString(narhashStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Unable to decode narHash %v: %v", narhashStr, err), http.StatusBadRequest)
		}
		blobReader, size, err := s.blobStore.GetBlob(r.Context(), narhash)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, fmt.Sprintf("Error retrieving narfile with hash %v: %v", narhashStr, err), status)
			return
		}
		defer blobReader.Close()

		// check compression suffix, and serve a compressed file depending on that.
		compressionSuffix := chi.URLParam(r, "compressionSuffix")

		// We only support zstd, gzip, brotli and none, as the others are way too CPU-intensive,
		// and never advertised anyways.
		compressedWriter, err := compression.NewCompressorBySuffix(w, compressionSuffix)
		if err != nil {
			// We still serve a 404 (as Nix might send a HEAD request while trying to upload xz, for example)
			http.Error(w, fmt.Sprintf("Unsupported compression suffix: %v", compressionSuffix), http.StatusNotFound)
		}

		w.Header().Add("Content-Type", "application/x-nix-nar")
		w.Header().Add("Content-Length", fmt.Sprintf("%d", size))

		io.Copy(compressedWriter, blobReader)
		defer compressedWriter.Close()
		return
	}

	if r.Method == http.MethodPut {
		blobWriter, err := s.blobStore.PutBlob(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf("Error initializing blobWriter: %v", err), http.StatusInternalServerError)
			return
		}
		defer blobWriter.Close()

		// There might be suffixes indicating compression, wrap the request body via the generic decompressor
		reader, err := compression.NewDecompressorBySuffix(r.Body, chi.URLParam(r, "compressionSuffix"))
		if err != nil {
			http.Error(w, fmt.Sprintf("Error initializing decompressor: %v", err), http.StatusInternalServerError)
		}

		// copy the body of the request into blobWriter
		_, err = io.Copy(blobWriter, reader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error copying to blobWriter: %v", err), http.StatusInternalServerError)
		}
		err = blobWriter.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("Error closing blobWriter: %v", err), http.StatusInternalServerError)
		}

		// Store NarMeta
		narMeta := &metadatastore.NarMeta{
			NarHash: blobWriter.Sha256Sum(),
			Size:    blobWriter.BytesWritten(),

			// TODO: Scan for references, add them here instead of filling on the first .narinfo file upload
		}
		err = s.metadataStore.PutNarMeta(r.Context(), narMeta)
		if err != nil {
			http.Error(w, fmt.Sprintf("PutNarMeta: %v", err), http.StatusInternalServerError)
		}

		return
	}

	http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}
