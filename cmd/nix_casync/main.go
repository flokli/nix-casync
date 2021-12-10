package main

import (
	"net/http"
	"path"
	"time"

	"github.com/alecthomas/kong"
	"github.com/flokli/nix-casync/pkg/server"
	"github.com/flokli/nix-casync/pkg/store/narinfostore"
	"github.com/flokli/nix-casync/pkg/store/narstore"
	log "github.com/sirupsen/logrus"
)

var CLI struct {
	Serve struct {
		//Remote(s)     string `arg name:"casync-remote" help:"URL to the remote casync store." type:"url"`
		RemoteCAStrs []string `name:"remote-castr" help:"List of URLs to remote castr" type:"string"`
		CachePath    string   `name:"cache-path" help:"Path to use for a local cache, containing castr, caibx and narinfo files." type:"path" default:"/var/cache/nix-casync"`
		ListenAddr   string   `name:"listen-addr" help:"The address this service listens on" type:"string" default:"[::]:9000"`
	} `cmd serve:"Serve a local nix cache."`
}

func main() {
	ctx := kong.Parse(&CLI)
	switch ctx.Command() {
	case "serve":
		s := server.NewServer()

		// initialize casync store
		castrPath := path.Join(CLI.Serve.CachePath, "castr")
		caibxPath := path.Join(CLI.Serve.CachePath, "caibx")
		casyncStore, err := narstore.NewCasyncStore(castrPath, caibxPath) // TODO: ask for more parameters?
		if err != nil {
			log.Fatal(err)
		}
		s.MountNarStore(casyncStore)

		// initialize narinfo store
		narinfoPath := path.Join(CLI.Serve.CachePath, "narinfo")
		narinfoStore, err := narinfostore.NewFileStore(narinfoPath)
		if err != nil {
			log.Fatal(err)
		}
		s.MountNarinfoStore(narinfoStore)

		log.Printf("Starting Server at %v", CLI.Serve.ListenAddr)
		srv := &http.Server{
			Addr:         CLI.Serve.ListenAddr,
			Handler:      s.Handler,
			ReadTimeout:  50 * time.Second,
			WriteTimeout: 100 * time.Second,
			IdleTimeout:  150 * time.Second,
		}
		log.Fatal(srv.ListenAndServe())
	default:
		panic(ctx.Command())
	}
}
