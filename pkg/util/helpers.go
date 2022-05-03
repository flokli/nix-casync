package util

import (
	"github.com/nix-community/go-nix/pkg/nixbase32"
)

const (
	StoreDir = "/nix/store" // hardcoded for now
)

// GetHashFromStorePath extracts the outputhash of a Nix Store path, and returns it decoded.
func GetHashFromStorePath(storePath string) ([]byte, error) {
	offset := len(StoreDir) + 1

	return nixbase32.DecodeString(storePath[offset : offset+32])
}

func GetNameFromStorePath(storePath string) string {
	offset := len(StoreDir) + 1 + 32 + 1

	return storePath[offset:]
}
