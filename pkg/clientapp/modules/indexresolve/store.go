package indexresolve

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type dbStore struct {
	store moduleapi.Store
}

func BootstrapStore(ctx context.Context, store moduleapi.Store) (*dbStore, error) {
	if store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	return &dbStore{store: store}, nil
}

func (s *dbStore) ListIndexResolveRoutes(ctx context.Context) ([]RouteItem, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("index resolve store is disabled")
	}
	var out []RouteItem
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT route, seed_hash, updated_at_unix
			  FROM proc_index_resolve_routes
			 ORDER BY route ASC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item RouteItem
			if err := rows.Scan(&item.Route, &item.SeedHash, &item.UpdatedAtUnix); err != nil {
				return err
			}
			out = append(out, item)
		}
		return rows.Err()
	})
	return out, err
}

func (s *dbStore) ResolveIndexRoute(ctx context.Context, rawRoute string) (Manifest, error) {
	if s == nil || s.store == nil {
		return Manifest{}, fmt.Errorf("index resolve store is disabled")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return Manifest{}, err
	}
	var out Manifest
	err = s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		var row struct {
			Route         string
			SeedHash      string
			UpdatedAtUnix int64
		}
		rows, err := rc.QueryContext(ctx, `
			SELECT route, seed_hash, updated_at_unix
			  FROM proc_index_resolve_routes
			 WHERE route=?`,
			route,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return sql.ErrNoRows
		}
		if err := rows.Scan(&row.Route, &row.SeedHash, &row.UpdatedAtUnix); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		seed, err := loadSeedItem(ctx, rc, row.SeedHash)
		if err != nil {
			return err
		}
		out = Manifest{
			Route:               row.Route,
			SeedHash:            row.SeedHash,
			RecommendedFileName: seed.RecommendedFileName,
			MIMEHint:            seed.MIMEHint,
			FileSize:            seed.FileSize,
			UpdatedAtUnix:       row.UpdatedAtUnix,
		}
		return nil
	})
	return out, err
}

func (s *dbStore) UpsertIndexResolveRoute(ctx context.Context, rawRoute string, rawSeedHash string, updatedAtUnix int64) (RouteItem, error) {
	if s == nil || s.store == nil {
		return RouteItem{}, fmt.Errorf("index resolve store is disabled")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return RouteItem{}, err
	}
	seedHash, err := NormalizeSeedHashHex(rawSeedHash)
	if err != nil {
		return RouteItem{}, err
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	var out RouteItem
	err = s.store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		if _, err := loadSeedItem(ctx, wtx, seedHash); err != nil {
			return err
		}
		if _, err := wtx.ExecContext(ctx, `
			INSERT INTO proc_index_resolve_routes(route, seed_hash, updated_at_unix)
			VALUES(?, ?, ?)
			ON CONFLICT(route) DO UPDATE SET
				seed_hash=excluded.seed_hash,
				updated_at_unix=excluded.updated_at_unix`,
			route, seedHash, updatedAtUnix,
		); err != nil {
			return err
		}
		out = RouteItem{
			Route:         route,
			SeedHash:      seedHash,
			UpdatedAtUnix: updatedAtUnix,
		}
		return nil
	})
	return out, err
}

func (s *dbStore) DeleteIndexResolveRoute(ctx context.Context, rawRoute string) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("index resolve store is disabled")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return err
	}
	return s.store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		_, err := wtx.ExecContext(ctx, `DELETE FROM proc_index_resolve_routes WHERE route=?`, route)
		return err
	})
}

func (s *dbStore) GetIndexResolveSeed(ctx context.Context, rawSeedHash string) (SeedItem, error) {
	if s == nil || s.store == nil {
		return SeedItem{}, fmt.Errorf("index resolve store is disabled")
	}
	seedHash, err := NormalizeSeedHashHex(rawSeedHash)
	if err != nil {
		return SeedItem{}, err
	}
	var out SeedItem
	err = s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		seed, err := loadSeedItem(ctx, rc, seedHash)
		if err != nil {
			return err
		}
		out = seed
		return nil
	})
	return out, err
}

func loadSeedItem(ctx context.Context, rc moduleapi.ReadConn, seedHash string) (SeedItem, error) {
	var out SeedItem
	if rc == nil {
		return out, fmt.Errorf("read conn is nil")
	}
	if ctx == nil {
		return out, fmt.Errorf("ctx is required")
	}
	rows, err := rc.QueryContext(ctx, `
		SELECT seed_hash, recommended_file_name, mime_hint, file_size
		  FROM biz_seeds
		 WHERE seed_hash=?`,
		seedHash,
	)
	if err != nil {
		return SeedItem{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return SeedItem{}, err
		}
		return SeedItem{}, sql.ErrNoRows
	}
	if err := rows.Scan(&out.SeedHash, &out.RecommendedFileName, &out.MIMEHint, &out.FileSize); err != nil {
		return SeedItem{}, err
	}
	if err := rows.Err(); err != nil {
		return SeedItem{}, err
	}
	return out, nil
}
