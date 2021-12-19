package metadatastore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/flokli/nix-casync/pkg/store"
	"github.com/numtide/go-nix/nar/narinfo"

	"github.com/uptrace/bun/extra/bundebug"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

var _ MetadataStore = &DatabaseStore{}

type DatabaseStore struct {
	db *bun.DB
}

type DatabaseStorePathInfo struct {
	bun.BaseModel `bun:"table:pathinfo,alias:pi"`

	OutputHash []byte `bun:"outputhash,pk"`
	Name       string

	NarHash []byte
	Deriver string
	System  string
	CA      string

	NarinfoSignatures []*DatabaseStoreNarinfoSignature `bun:"rel:has-many,join:outputhash=outputhash"`
}

type DatabaseStoreNarinfoSignature struct {
	bun.BaseModel `bun:"table:narinfo_signatures,alias:ni_sig"`

	OutputHash []byte `bun:"outputhash,pk"`
	KeyName    string `bun:",pk"`
	Digest     []byte
}

type DatabaseStoreNarMeta struct {
	bun.BaseModel `bun:"table:nar,alias:nar"`

	NarHash    []byte `bun:"narhash,pk"`
	Size       uint64
	References []DatabaseStoreNarReference `bun:"m2m:nar_references,join:NarHash=NarHash"`
}

type DatabaseStoreNarReference struct {
	bun.BaseModel `bun:"table:nar_references"`

	NarMeta    *DatabaseStoreNarMeta  `bun:"rel:belongs-to,join:narhash=narhash"`
	NarHash    []byte                 `bun:"narhash,pk"`
	StorePath  *DatabaseStorePathInfo `bun:"rel:belongs-to,join:outputhash=outputhash"`
	OutputHash []byte                 `bun:"outputhash,pk"`
}

func NewDatabaseStore(ctx context.Context, driverName, dsn string) (*DatabaseStore, error) {
	// TODO: don't hardcode sqlite
	sqldb, err := sql.Open(sqliteshim.ShimName, "file::memory:?cache=shared")
	if err != nil {
		return nil, fmt.Errorf("unable to use data source name: %v", err)
	}

	sqldb.SetConnMaxLifetime(0)
	sqldb.SetMaxIdleConns(3)
	sqldb.SetMaxOpenConns(3)

	db := bun.NewDB(sqldb, sqlitedialect.New())

	db.AddQueryHook(bundebug.NewQueryHook(
		bundebug.WithVerbose(true),
		bundebug.FromEnv("BUNDEBUG"),
	))

	db.RegisterModel((*DatabaseStoreNarReference)(nil))
	db.RegisterModel((*DatabaseStorePathInfo)(nil))
	db.RegisterModel((*DatabaseStoreNarinfoSignature)(nil))
	//db.RegisterModel((*databaseStorNarMeta)(nil))

	_, err = db.NewCreateTable().Model((*DatabaseStorePathInfo)(nil)).Exec(ctx)
	if err != nil {
		return nil, err
	}
	_, err = db.NewCreateTable().Model((*DatabaseStoreNarinfoSignature)(nil)).Exec(ctx)
	if err != nil {
		return nil, err
	}
	_, err = db.NewCreateTable().Model((*DatabaseStoreNarReference)(nil)).Exec(ctx)
	if err != nil {
		return nil, err
	}
	_, err = db.NewCreateTable().Model((*DatabaseStoreNarMeta)(nil)).Exec(ctx)
	if err != nil {
		return nil, err
	}

	return &DatabaseStore{
		db: db,
	}, nil
}

func (ds *DatabaseStore) GetPathInfo(ctx context.Context, outputHash []byte) (*PathInfo, error) {
	dsPathInfo := new(DatabaseStorePathInfo)

	err := ds.db.NewSelect().
		Model(dsPathInfo).
		Where("outputhash = ?", outputHash).
		Relation("NarinfoSignatures").
		Limit(1).
		Scan(ctx)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("unable to get pathinfo: %v", err)
	}

	// convert to PathInfo
	pathInfo := &PathInfo{
		OutputHash: dsPathInfo.OutputHash,
		Name:       dsPathInfo.Name,

		NarHash: dsPathInfo.NarHash,

		Deriver: dsPathInfo.Deriver,
		System:  dsPathInfo.System,

		CA: dsPathInfo.CA,
	}

	for _, dsSig := range dsPathInfo.NarinfoSignatures {
		pathInfo.NarinfoSignatures = append(pathInfo.NarinfoSignatures, &narinfo.Signature{
			KeyName: dsSig.KeyName,
			Digest:  dsSig.Digest,
		})
	}

	return pathInfo, nil
}

// TODO: not nulls, verify foreign key constraints, on delete cascade

func (ds *DatabaseStore) PutPathInfo(ctx context.Context, pathInfo *PathInfo) error {
	err := pathInfo.Check()
	if err != nil {
		return err
	}

	dsPathInfo := DatabaseStorePathInfo{
		OutputHash: pathInfo.OutputHash,
		Name:       pathInfo.Name,

		NarHash: pathInfo.NarHash,
		Deriver: pathInfo.Deriver,
		System:  pathInfo.System,
		CA:      pathInfo.CA,
	}

	for _, niSig := range pathInfo.NarinfoSignatures {
		dsPathInfo.NarinfoSignatures = append(dsPathInfo.NarinfoSignatures, &DatabaseStoreNarinfoSignature{
			OutputHash: pathInfo.OutputHash,
			KeyName:    niSig.KeyName,
			Digest:     niSig.Digest,
		})
	}

	return ds.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err = tx.NewInsert().
			Model(dsPathInfo.NarinfoSignatures).
			Exec(ctx)
		if err != nil {
			return err
		}

		_, err = tx.NewInsert().
			Model(&dsPathInfo).
			Exec(ctx)
		return err

	})
}

func (ds *DatabaseStore) GetNarMeta(ctx context.Context, narHash []byte) (*NarMeta, error) {
	dsNarMeta := new(DatabaseStoreNarMeta)

	err := ds.db.NewSelect().
		Model(dsNarMeta).
		Where("narhash = ?", narHash).
		Relation("References").
		Limit(1).
		Scan(ctx)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("unable to get narmeta: %v", err)
	}

	//convert to NarMeta
	narMeta := &NarMeta{
		NarHash: dsNarMeta.NarHash,
		Size:    dsNarMeta.Size,
	}

	for _, ref := range dsNarMeta.References {
		narMeta.References = append(narMeta.References, ref.OutputHash)
		narMeta.ReferencesStr = append(narMeta.ReferencesStr, ref.StorePath.Name)
	}

	return narMeta, nil
}

func (ds *DatabaseStore) PutNarMeta(ctx context.Context, narMeta *NarMeta) error {
	err := narMeta.Check()
	if err != nil {
		return err
	}

	dsNarMeta := DatabaseStoreNarMeta{
		NarHash: narMeta.NarHash,
		Size:    narMeta.Size,
	}

	return ds.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().
			Model(&dsNarMeta).
			Exec(ctx)
		if err != nil {
			return err
		}

		// insert references
		dsNarReferences := []DatabaseStoreNarReference{}

		for _, refHash := range narMeta.References {
			dsNarReferences = append(dsNarReferences, DatabaseStoreNarReference{
				NarHash:    narMeta.NarHash,
				OutputHash: refHash,
			})
		}

		_, err = tx.NewInsert().
			Model(&dsNarReferences).
			Exec(ctx)

		if err != nil {
			return err
		}

		return nil
	})
}

func (ds *DatabaseStore) DropAll(ctx context.Context) error {
	_, err := ds.db.NewTruncateTable().
		Model(&DatabaseStoreNarReference{}).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to delete databaseStoreNarReference: %v", err)
	}

	_, err = ds.db.NewTruncateTable().
		Model(&DatabaseStoreNarinfoSignature{}).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to delete databaseStoreNarinfoSignature: %v", err)
	}

	_, err = ds.db.NewTruncateTable().
		Model(&DatabaseStorePathInfo{}).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to delete databaseStorePathInfo: %v", err)
	}

	_, err = ds.db.NewTruncateTable().
		Model(&DatabaseStoreNarMeta{}).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("unable to delete databaseStorNarMeta: %v", err)
	}
	return nil
}

func (ds *DatabaseStore) Close() error {
	return ds.db.Close()
}
