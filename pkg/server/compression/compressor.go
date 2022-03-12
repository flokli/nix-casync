package compression

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
	"github.com/datadog/zstd"
)

// NewCompressor returns an io.WriteCloser that compresses its input.
// The compression type needs to be specified upfront.
// Only cheap compression is supported, as this is assembled on the fly, and acts as a poorman's content-encoding.
// It's the callers responsibility to close the reader when done.
func NewCompressor(w io.Writer, compressionType string) (io.WriteCloser, error) {
	switch compressionType {
	case "br":
		b := brotli.NewWriterLevel(w, brotli.BestSpeed)

		return b, nil
	case "gzip":
		return gzip.NewWriterLevel(w, gzip.BestSpeed)
	case "zstd":
		z := zstd.NewWriterLevel(w, zstd.BestSpeed)

		return z, nil
	}

	return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
}

// NewCompressorBySuffix returns an io.WriteCloser that compresses its input.
func NewCompressorBySuffix(w io.Writer, compressionSuffix string) (io.WriteCloser, error) {
	// try to lookup the compression type from compressionSuffixToType
	if compressionType, ok := CompressionSuffixToType[compressionSuffix]; ok {
		return NewCompressor(w, compressionType)
	}

	return nil, fmt.Errorf("unknown compression suffix: %v", compressionSuffix)
}
