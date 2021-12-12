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

### Uploading store paths
```
nix copy \
  --extra-experimental-features nix-command \
  --to "http://localhost:9000?compression=none" $storePath
```

#### Note on compression
While it's also possible to upload with narfile compression, this is only used
to compress *uploads* into the binary cache.

`nix-casync` will decompress compressed narfile uploads on the fly, chunk the
uncompressed payload, and serve those as `/nar/$narhash.nar`. It will also
rewrite uploaded `.narinfo` files accordingly.

This will have some unintuitive implications - if you upload a to
`/nar/$filehash.nar.zst`, the upload won't be available on that location (but
at `/nar/$narhash.nar`).

This also means `HTTP HEAD` requests to "compressed locations" will `404`, and
as a result, Nix clients might end up uploading the same `.nar` files multiple
times. [^1]

Generally, you only want to use compression if connectivity to the binary cache
is bad, and if you do, use a fast compression algorithm, such as `zstd`.


[^1]: Nix won't upload the same store path multiple times, as it checks
  `$outhash.narinfo` for existence first - so this only applies to multiple
  `.narinfo` files referring to the same `.nar` file.
