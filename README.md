# nix-casync
A more efficient way to store and substitute Nix store paths.

Docs are a bit sparse right now, please refer to
https://flokli.de/posts/2021-12-10-nix-casync-intro/ for a description
on how this works.

## Build

```sh
$ go build ./cmd/nix_casync/
```

## Run
```sh
./nix_casync serve --cache-path=path/to/local
```
