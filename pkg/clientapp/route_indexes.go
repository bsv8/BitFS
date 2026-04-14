package clientapp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procpublishedrouteindexes"
	oldproto "github.com/golang/protobuf/proto"
)

func buildRouteIndexManifest(ctx context.Context, store *clientDB, route string) ([]byte, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	normalizedRoute := normalizeResolveRoute(route)
	var manifest routeIndexManifest
	if err := clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		row, err := tx.ProcPublishedRouteIndexes.Query().
			Where(procpublishedrouteindexes.RouteEQ(normalizedRoute)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return sql.ErrNoRows
			}
			return err
		}
		seed, err := tx.BizSeeds.Query().
			Where(bizseeds.SeedHashEQ(row.SeedHash)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return sql.ErrNoRows
			}
			return err
		}
		manifest.Route = row.Route
		manifest.SeedHash = row.SeedHash
		manifest.RecommendedFileName = seed.RecommendedFileName
		manifest.MIMEHint = seed.MimeHint
		manifest.FileSize = seed.FileSize
		manifest.UpdatedAtUnix = row.UpdatedAtUnix
		return nil
	}); err != nil {
		return nil, err
	}
	return oldproto.Marshal(&manifest)
}
