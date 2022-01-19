package main

import (
	"net/http"
	"os"
	"os/signal"
	"path"
	"time"

	"github.com/alecthomas/kong"
	"github.com/flokli/nix-casync/pkg/server"
	"github.com/flokli/nix-casync/pkg/store/blobstore"
	"github.com/flokli/nix-casync/pkg/store/metadatastore"
	"github.com/go-chi/chi/middleware"
	log "github.com/sirupsen/logrus"
)

var CLI struct {
	Serve struct {
		CachePath      string `name:"cache-path" help:"Path to use for a local cache, containing castr, caibx and narinfo files." type:"path" default:"/var/cache/nix-casync"`
		NarCompression string `name:"nar-compression" help:"The compression algorithm to advertise .nar files with (zstd,gzip,brotli,none)" enum:"zstd,gzip,brotli,none" type:"string" default:"zstd"`
		ListenAddr     string `name:"listen-addr" help:"The address this service listens on" type:"string" default:"[::]:9000"`
		Priority       int    `name:"priority" help:"What priority to advertise in nix-cache-info. Defaults to 40." type:"int" default:40`
		AvgChunkSize   int    `name:"avg-chunk-size" help:"The average chunking size to use when chunking NAR files, in bytes. Max is 4 times that, Min is a quarter of this value." type:"int" default:65536`
		AccessLog      bool   `name:"access-log" help:"Enable access logging" type:"bool" default:true negatable:""`
	} `cmd serve:"Serve a local nix cache."`
}

func main() {
	ctx := kong.Parse(&CLI)
	switch ctx.Command() {
	case "serve":
		// initialize casync store
		castrPath := path.Join(CLI.Serve.CachePath, "castr")
		caibxPath := path.Join(CLI.Serve.CachePath, "caibx")
		blobStore, err := blobstore.NewCasyncStore(castrPath, caibxPath, CLI.Serve.AvgChunkSize)
		if err != nil {
			log.Fatal(err)
		}

		// initialize narinfo store
		narinfoPath := path.Join(CLI.Serve.CachePath, "narinfo")
		metadataStore, err := metadatastore.NewFileStore(narinfoPath)
		if err != nil {
			log.Fatal(err)
		}

		s := server.NewServer(blobStore, metadataStore, CLI.Serve.NarCompression, CLI.Serve.Priority)
		defer s.Close()

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			for range c {
				log.Info("Received Signal, shutting down…")
				s.Close()
				os.Exit(1)
			}
		}()

		log.Printf("Starting Server at %v", CLI.Serve.ListenAddr)
		srv := &http.Server{
			Addr:         CLI.Serve.ListenAddr,
			Handler:      s.Handler,
			ReadTimeout:  50 * time.Second,
			WriteTimeout: 100 * time.Second,
			IdleTimeout:  150 * time.Second,
		}
		if CLI.Serve.AccessLog {
			srv.Handler = middleware.Logger(s.Handler)
		}
		log.Fatal(srv.ListenAndServe())
	default:
		panic(ctx.Command())
	}
}
