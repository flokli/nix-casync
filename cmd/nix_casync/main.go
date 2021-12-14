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
		CachePath  string `name:"cache-path" help:"Path to use for a local cache, containing castr, caibx and narinfo files." type:"path" default:"/var/cache/nix-casync"`
		ListenAddr string `name:"listen-addr" help:"The address this service listens on" type:"string" default:"[::]:9000"`
	} `cmd serve:"Serve a local nix cache."`
}

func main() {
	ctx := kong.Parse(&CLI)
	switch ctx.Command() {
	case "serve":
		// initialize casync store
		castrPath := path.Join(CLI.Serve.CachePath, "castr")
		caibxPath := path.Join(CLI.Serve.CachePath, "caibx")
		blobStore, err := blobstore.NewCasyncStore(castrPath, caibxPath) // TODO: ask for more parameters?
		if err != nil {
			log.Fatal(err)
		}

		// initialize narinfo store
		narinfoPath := path.Join(CLI.Serve.CachePath, "narinfo")
		metadataStore, err := metadatastore.NewFileStore(narinfoPath)
		if err != nil {
			log.Fatal(err)
		}

		s := server.NewServer(blobStore, metadataStore)
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
			Handler:      middleware.Logger(s.Handler),
			ReadTimeout:  50 * time.Second,
			WriteTimeout: 100 * time.Second,
			IdleTimeout:  150 * time.Second,
		}
		log.Fatal(srv.ListenAndServe())
	default:
		panic(ctx.Command())
	}
}
