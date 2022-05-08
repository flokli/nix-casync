package compression

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"github.com/ulikunitz/xz"
)

// compressionSuffixToType maps from the compression suffix Nix uses when uploading to the compression type.
var compressionSuffixToType = map[string]string{ //nolint:gochecknoglobals
	"":      "none",
	".br":   "br",
	".bz2":  "bzip2",
	".gz":   "gzip", // keep in mind nix defaults to gzip if Compression: field is unset or empty string
	".lz4":  "lz4",
	".lzip": "lzip",
	".xz":   "xz",
	".zst":  "zstd",
}

func TypeToSuffix(compressionType string) (string, error) {
	for compressionSuffix, aCompressionType := range compressionSuffixToType {
		if aCompressionType == compressionType {
			return compressionSuffix, nil
		}
	}

	return "", fmt.Errorf("unknown compression type: %v", compressionType)
}

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
	case "xz":
		xzReader, err := xz.NewReader(r)
		if err != nil {
			return nil, err
		}

		return io.NopCloser(xzReader), nil
	case "zstd":
		zstdr, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}

		return io.NopCloser(zstdr), nil
	}

	// compress, grzip, lzrzip, lzip, lzop, lzma
	return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
}

func NewDecompressorBySuffix(r io.Reader, compressionSuffix string) (io.ReadCloser, error) {
	// try to lookup the compression type from compressionSuffixToType
	if compressionType, ok := compressionSuffixToType[compressionSuffix]; ok {
		return NewDecompressor(r, compressionType)
	}

	return nil, fmt.Errorf("unknown compression suffix: %v", compressionSuffix)
}
