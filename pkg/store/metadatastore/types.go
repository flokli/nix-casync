// Package metadatastore contains some datastructures that are part of a binary cache.
package metadatastore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/flokli/nix-casync/pkg/server/compression"
	"github.com/flokli/nix-casync/pkg/util"
	"github.com/numtide/go-nix/hash"
	"github.com/numtide/go-nix/nar/narinfo"
	"github.com/numtide/go-nix/nixbase32"
)

type MetadataStore interface {
	GetPathInfo(ctx context.Context, outputHash []byte) (*PathInfo, error)
	PutPathInfo(ctx context.Context, pathInfo *PathInfo) error

	// TODO: once we have reference scanning, it shouldn't be possible to mutate existing NarMetas
	GetNarMeta(ctx context.Context, narHash []byte) (*NarMeta, error)
	PutNarMeta(ctx context.Context, narMeta *NarMeta) error
	DropAll(ctx context.Context) error
	io.Closer
}

type PathInfo struct {
	OutputHash []byte
	Name       string

	NarHash []byte

	Deriver           string
	System            string
	NarinfoSignatures []*narinfo.Signature

	CA string
}

// ParseNarinfo parses a narinfo.NarInfo struct
// and returns a PathInfo and NarMeta struct, or an error.
func ParseNarinfo(narinfo *narinfo.NarInfo) (*PathInfo, *NarMeta, error) {
	// Ensure sha256 is used for hashing.
	if narinfo.NarHash.HashType != hash.HashTypeSha256 {
		return nil, nil, fmt.Errorf("unexpected hashtype: %v", narinfo.NarHash)
	}

	// Try to parse the StorePath field.
	// We need it later, but there's no need to lookup in NarMeta
	// if the StorePath field is already invalid.
	outputHash, err := util.GetHashFromStorePath(narinfo.StorePath)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid StorePath field: %v", narinfo.StorePath)
	}

	pathInfo := &PathInfo{
		OutputHash: outputHash,
		Name:       util.GetNameFromStorePath(narinfo.StorePath),

		NarHash: narinfo.NarHash.Digest,

		Deriver:           narinfo.Deriver,
		System:            narinfo.System,
		NarinfoSignatures: narinfo.Signatures,

		CA: narinfo.CA,
	}

	// Construct References
	references := make([][]byte, 0, len(narinfo.References))

	for _, referenceStr := range narinfo.References {
		hashRef, err := nixbase32.DecodeString(referenceStr[0:32])
		if err != nil {
			return nil, nil, fmt.Errorf("unable to decode hash %v in reference %v: %w", referenceStr, narinfo.References, err)
		}

		references = append(references, hashRef)
	}

	narMeta := &NarMeta{
		NarHash:       narinfo.NarHash.Digest,
		Size:          narinfo.NarSize,
		ReferencesStr: narinfo.References,
		References:    references,
	}

	return pathInfo, narMeta, nil
}

// RenderNarinfo renders a minimal .narinfo from a PathInfo and NarMeta.
// The URL is synthesized to /nar/$narhash.nar[$compressionSuffix].
func RenderNarinfo(pathInfo *PathInfo, narMeta *NarMeta, compressionType string) (string, error) {
	// render the narinfo
	narHash := &hash.Hash{
		HashType: hash.HashTypeSha256,
		Digest:   narMeta.NarHash,
	}
	narhashStr := nixbase32.EncodeToString(pathInfo.NarHash)
	narInfo := &narinfo.NarInfo{
		StorePath:   pathInfo.StorePath(),
		URL:         "nar/" + narhashStr + ".nar",
		Compression: compressionType,

		NarHash: narHash,
		NarSize: narMeta.Size,

		References: narMeta.ReferencesStr,

		Deriver: pathInfo.Deriver,

		System: pathInfo.System,

		Signatures: pathInfo.NarinfoSignatures,

		CA: pathInfo.CA,
	}

	suffix, err := compression.TypeToSuffix(compressionType)
	if err != nil {
		return "", err
	}

	narInfo.URL = narInfo.URL + suffix

	return narInfo.String(), nil
}

func (pi *PathInfo) StorePath() string {
	return util.StoreDir + "/" + nixbase32.EncodeToString(pi.OutputHash) + "-" + pi.Name
}

func (pi *PathInfo) Check() error {
	if len(pi.OutputHash) != 20 {
		return fmt.Errorf("invalid outputhash: %v", nixbase32.EncodeToString(pi.OutputHash))
	}

	if len(pi.Name) == 0 {
		return fmt.Errorf("invalid name: %v", pi.Name)
	}

	if len(pi.NarHash) != 32 {
		return fmt.Errorf("invalid narhash: %v", nixbase32.EncodeToString(pi.NarHash))
	}
	// Derivers can be empty (when importing store paths),
	// but when they're not, they need to be at least long enough to
	// hold the output hash (base32 encoded), a dash and the name
	if !(len(pi.Deriver) == 0 || (strings.HasSuffix(pi.Deriver, ".drv") && len(pi.Deriver) > 32+1+1)) {
		return fmt.Errorf("invalid deriver: %v", pi.Deriver)
	}

	return nil
}

type NarMeta struct {
	NarHash []byte
	Size    uint64

	References    [][]byte // this refers to multiple PathInfo.OutputHash
	ReferencesStr []string // we still keep the strings around, so we don't need to look up all other PathInfo objects
}

// Check provides some sanity checking on values in the NarMeta struct.
func (n *NarMeta) Check() error {
	if len(n.NarHash) != 32 { // 32 bytes = 256bits
		return fmt.Errorf("invalid narhash length: %v, must be 32", len(n.NarHash))
	}

	if n.Size == 0 {
		return fmt.Errorf("invalid Size: %v", n.Size)
	}

	if len(n.References) != len(n.ReferencesStr) {
		return fmt.Errorf("inconsistent number of References[Str]")
	}

	// We need to be able to decode all store paths in ReferencesStr
	// and they should match the hashes stored in References
	for i, refStr := range n.ReferencesStr {
		hash, err := nixbase32.DecodeString(refStr[:32])
		if err != nil {
			return fmt.Errorf("unable to encode hash from store path: %v", refStr)
		}

		if !bytes.Equal(hash, n.References[i]) {
			return fmt.Errorf(
				"inconsistent References and ReferencesStr at position %v: %v != %v",
				i,
				hash,
				n.References[i],
			)
		}
	}

	return nil
}

// IsEqualTo returns true if the other NarMeta is equal to it
// The compareReferences parameter controls whether references should be compared.
func (n *NarMeta) IsEqualTo(other *NarMeta, compareReferences bool) bool {
	if !(n.Size == other.Size) {
		return false
	}

	if !bytes.Equal(n.NarHash, other.NarHash) {
		return false
	}

	if compareReferences {
		for i, refStr := range n.ReferencesStr {
			if refStr != other.ReferencesStr[i] {
				return false
			}
		}

		for i, refBytes := range n.References {
			if !bytes.Equal(refBytes, other.References[i]) {
				return false
			}
		}
	}

	return true
}
