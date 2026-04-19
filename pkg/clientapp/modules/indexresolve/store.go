package indexresolve

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// Conn 是模块内使用的最小数据库连接能力。
// 设计说明：查询和 Scan 必须在同一个闭包里完成，所以这里只暴露最小面。
type Conn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type dbCapability = Conn

// SerialExecutor 表示“把整段 DB 闭包串行执行”的能力。
// 没有这个能力时，模块会退回到内部 mutex 兜底。
type SerialExecutor interface {
	Do(context.Context, func(Conn) error) error
}

type dbStore struct {
	db     dbCapability
	serial SerialExecutor
	mu     sync.Mutex
}

func BootstrapStore(ctx context.Context, db dbCapability, serial SerialExecutor) (*dbStore, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if err := bootstrapSchema(ctx, db); err != nil {
		return nil, err
	}
	return &dbStore{
		db:     db,
		serial: serial,
	}, nil
}

func (s *dbStore) ListIndexResolveRoutes(ctx context.Context) ([]RouteItem, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("index resolve store is disabled")
	}
	var out []RouteItem
	err := s.withConn(ctx, func(conn Conn) error {
		rows, err := conn.QueryContext(ctx, `
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
	if s == nil || s.db == nil {
		return Manifest{}, fmt.Errorf("index resolve store is disabled")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return Manifest{}, err
	}
	var out Manifest
	err = s.withConn(ctx, func(conn Conn) error {
		var row struct {
			Route         string
			SeedHash      string
			UpdatedAtUnix int64
		}
		rows, err := conn.QueryContext(ctx, `
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
		seed, err := loadSeedItem(ctx, conn, row.SeedHash)
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
	if s == nil || s.db == nil {
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
	err = s.withConn(ctx, func(conn Conn) error {
		if _, err := loadSeedItem(ctx, conn, seedHash); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `
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
	if s == nil || s.db == nil {
		return fmt.Errorf("index resolve store is disabled")
	}
	route, err := NormalizeRoute(rawRoute)
	if err != nil {
		return err
	}
	return s.withConn(ctx, func(conn Conn) error {
		_, err := conn.ExecContext(ctx, `DELETE FROM proc_index_resolve_routes WHERE route=?`, route)
		return err
	})
}

func (s *dbStore) GetIndexResolveSeed(ctx context.Context, rawSeedHash string) (SeedItem, error) {
	if s == nil || s.db == nil {
		return SeedItem{}, fmt.Errorf("index resolve store is disabled")
	}
	seedHash, err := NormalizeSeedHashHex(rawSeedHash)
	if err != nil {
		return SeedItem{}, err
	}
	var out SeedItem
	err = s.withConn(ctx, func(conn Conn) error {
		seed, err := loadSeedItem(ctx, conn, seedHash)
		if err != nil {
			return err
		}
		out = seed
		return nil
	})
	return out, err
}

func (s *dbStore) withConn(ctx context.Context, fn func(Conn) error) error {
	if s == nil {
		return fmt.Errorf("index resolve store is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if fn == nil {
		return fmt.Errorf("store conn func is nil")
	}
	if s.serial != nil {
		return s.serial.Do(ctx, fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s.db)
}

func loadSeedItem(ctx context.Context, conn Conn, seedHash string) (SeedItem, error) {
	var out SeedItem
	if conn == nil {
		return out, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return out, fmt.Errorf("ctx is required")
	}
	rows, err := conn.QueryContext(ctx, `
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
