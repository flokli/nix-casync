package util_test

import (
	"testing"

	"github.com/flokli/nix-casync/pkg/util"
	"github.com/nix-community/go-nix/pkg/nixbase32"
	"github.com/stretchr/testify/assert"
)

func TestStorePathHelpers(t *testing.T) {
	storePath := "/nix/store/dr76fsw7d6ws3pymafx0w0sn4rzbw7c9-etc-os-release"

	t.Run("getHashFromStorePath", func(t *testing.T) {
		hash, err := util.GetHashFromStorePath(storePath)
		assert.NoError(t, err)
		assert.Equal(t, "dr76fsw7d6ws3pymafx0w0sn4rzbw7c9", nixbase32.EncodeToString(hash))
	})

	t.Run("getNameFromStorePath", func(t *testing.T) {
		name := util.GetNameFromStorePath(storePath)
		assert.Equal(t, "etc-os-release", name)
	})
}
