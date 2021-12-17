package compression

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
	"github.com/datadog/zstd"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
	"github.com/ulikunitz/xz/lzma"
)

// NewDecompressor decompresses contents from an io.Reader
// The compression type needs to be specified upfront.
// It's the callers responsibility to close the reader when done.
func NewDecompressor(r io.Reader, compressionType string) (io.ReadCloser, error) {
	// Nix seems to support the following compressions:
	// - none
	// - br
	// - bzip2, compress, grzip, gzip, lrzip, lz4, lzip, lzma, lzop, xz, zstd (via libarchive)
	switch compressionType {
	case "none":
		return io.NopCloser(r), nil
	case "br":
		return io.NopCloser(brotli.NewReader(r)), nil
	case "bzip2":
		return io.NopCloser(bzip2.NewReader(r)), nil
	case "gzip":
		gzipReader, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return gzipReader, nil
	case "lz4":
		return io.NopCloser(lz4.NewReader(r)), nil
	case "lzma":
		lzmaReader, err := lzma.NewReader(r)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(lzmaReader), nil
	case "xz":
		xzReader, err := xz.NewReader(r)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(xzReader), nil
	case "zstd":
		return zstd.NewReader(r), nil
	}

	// compress, grzip, lzrzip, lzip, lzop
	return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
}

func NewDecompressorBySuffix(r io.Reader, compressionSuffix string) (io.ReadCloser, error) {
	if compressionSuffix == "" || compressionSuffix[1:] == "" {
		return NewDecompressor(r, "none")
	}
	if compressionSuffix[1:] == "zst" {
		return NewDecompressor(r, "zstd")
	}
	if compressionSuffix[1:] == "gz" {
		return NewDecompressor(r, "gzip")
	}
	if compressionSuffix[1:] == "bz2" {
		return NewDecompressor(r, "bzip2")
	}
	return NewDecompressor(r, compressionSuffix[1:])
}
