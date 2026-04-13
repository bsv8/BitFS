package clientapp

import (
	"fmt"
	"strings"
	"time"

	oldproto "github.com/golang/protobuf/proto"
)

func upsertPublishedRouteIndex(db sqlConn, route string, seedHash string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	normalizedRoute := normalizeResolveRoute(route)
	normalizedSeedHash := strings.ToLower(strings.TrimSpace(seedHash))
	if normalizedSeedHash == "" {
		return 0, fmt.Errorf("seed_hash is required")
	}
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO proc_published_route_indexes(route,seed_hash,updated_at_unix)
		 VALUES(?,?,?)
		 ON CONFLICT(route) DO UPDATE SET seed_hash=excluded.seed_hash,updated_at_unix=excluded.updated_at_unix`,
		normalizedRoute,
		normalizedSeedHash,
		now,
	); err != nil {
		return 0, err
	}
	return now, nil
}

func buildRouteIndexManifest(db sqlConn, route string) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	normalizedRoute := normalizeResolveRoute(route)
	var manifest routeIndexManifest
	if err := db.QueryRow(
		`SELECT p.route,p.seed_hash,COALESCE(s.recommended_file_name,''),COALESCE(s.mime_hint,''),COALESCE(s.file_size,0),p.updated_at_unix
		   FROM proc_published_route_indexes p
		   JOIN biz_seeds s ON s.seed_hash=p.seed_hash
		  WHERE p.route=?`,
		normalizedRoute,
	).Scan(
		&manifest.Route,
		&manifest.SeedHash,
		&manifest.RecommendedFileName,
		&manifest.MIMEHint,
		&manifest.FileSize,
		&manifest.UpdatedAtUnix,
	); err != nil {
		return nil, err
	}
	return oldproto.Marshal(&manifest)
}
