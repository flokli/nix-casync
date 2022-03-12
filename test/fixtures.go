package test

import (
	"bytes"
	_ "embed"
	"fmt"

	"github.com/numtide/go-nix/nar/narinfo"
)

//go:embed x236iz9shqypbnm64qgqisz0jr4wmj2b.narinfo
var aNarinfoContents []byte

//go:embed nar/0xmvxmsmmc6n79sk2h3r6db3yp8drmxps61mdk7iqnvc6vcsww60.nar
var aNarContents []byte

//go:embed 7cwx623saf2h3z23wsn26icszvskk4iy.narinfo
var bNarinfoContents []byte

//go:embed nar/0rcdxyw7kjpxshv7wb1am0nvjfjbjq67cvrc8dmbsy1slc2ycbxp.nar
var bNarContents []byte

//go:embed qp5h1cjd5ykcl4hyvsjhrlv68bbx8fan.narinfo
var cNarinfoContents []byte

//go:embed nar/0z2vk40phzzgsg14516mfs79l9fvl276b993mlqlb4rf0fd7hnwp.nar
var cNarContents []byte

type Data struct {
	NarinfoContents []byte
	Narinfo         *narinfo.NarInfo
	NarContents     []byte
}

type DataTable map[string]Data

// GetTestDataTable returns testdata from //test
// it's a map with the following store paths:
// a is a store path without any references.
// b refers to it.
// c contains a self-reference.
func GetTestDataTable() DataTable {
	testDataT := make(DataTable, 2)

	for _, item := range []struct {
		name            string
		narinfoContents []byte
		narContents     []byte
	}{
		{name: "a", narinfoContents: aNarinfoContents, narContents: aNarContents},
		{name: "b", narinfoContents: bNarinfoContents, narContents: bNarContents},
		{name: "c", narinfoContents: cNarinfoContents, narContents: cNarContents},
	} {
		// parse narinfo file
		narinfo, err := narinfo.Parse(bytes.NewReader(item.narinfoContents))
		if err != nil {
			panic(fmt.Errorf("error parsing narinfo contents: %w", err))
		}

		testDataT[item.name] = Data{
			NarinfoContents: item.narinfoContents,
			Narinfo:         narinfo,
			NarContents:     item.narContents,
		}
	}

	return testDataT
}
