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

### Binary Cache
As of now, `nix-casync` can be used as a space-efficient binary cache.

You probably want to put some reverse proxy doing SSL in front of it, and add
some protection on the `PUT` endpoints.

The following section describes some internal behaviour of `nix-casync`, and
how it treats Narfiles and Narinfo files.

#### Narfiles
Narfiles can be uploaded with most of the compression mechanisms Nix supports.

The path it's uploaded at `HTTP PUT /nar/….nar[.$suffix]` doesn't really matter.

Files will be decompressed, chunked, and put in a content-addressed store.

Subsequently uploaded `.narinfo` files can refer to that file via the `NarHash`
attribute, and downloads can happen via `HTTP GET /nar/$narhash.nar[.$suffix]`.

For downloads, only a subset of compression algorithms (fast ones) are
supported, as those are assembled on the fly and should really only be
considered a poor-man's Content-Encoding.

##### Another note on compression
While it's possible to upload with narfile compression, as written above, this
is only used to compress *uploads* into the binary cache.

This will have some unintuitive implications - if you upload a to
`/nar/$filehash.nar.zst`, the upload won't be available on that location (but
at `/nar/$narhash.nar`).

This also means `HTTP HEAD` requests to "compressed locations" will `404`, and
as a result, Nix clients might end up uploading the same `.nar` files multiple
times. [^1]

If the binary cache is remote, it is preferable to use compression during
upload to reduce bandwidth usage. In that case, using a fast compression
algorithm, such as `zstd` is recommended.

By default, downloads are served with ZSTD Compression. This can be tweaked via
the `--nar-compression` command line parameter.

#### Narinfo files
Narinfo files describe information about a store path, as well as some
(redundant) information about the referred .nar file.

Internally, `nix-casync` splits data from `.narinfo` into `NarMeta` and
`PathInfo` models.

When a Narfile is uploaded, the following checks are made:

 - The .narinfo file refers to a Narfile (via NarHash) that already exists in
   `nix-casync`.
 - The `References` field matches with what `nix-casync`'s internal bookkeeping
   of References in `NarMeta` matches.
   Right now, that field is populated on the first `.narinfo` upload , but as
   it's possible to determine the references just by looking at the `.nar` file
   itself, a reference scanner could be added to `nix-casync` directly.
 - All `References` in the uploaded `.narinfo` refer to `PathInfo` (aka
 - `.narinfo` files) that were already uploaded to `nix-casync`.



[^1]: Nix won't upload the same store path multiple times, as it checks
  `$outhash.narinfo` for existence first - so this only applies to multiple
  `.narinfo` files referring to the same `.nar` file.
