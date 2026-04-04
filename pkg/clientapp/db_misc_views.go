package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
)

type inboxMessageListItem struct {
	ID             int64  `json:"id"`
	MessageID      string `json:"message_id"`
	SenderPubKey   string `json:"sender_pubkey_hex"`
	TargetInput    string `json:"target_input"`
	Route          string `json:"route"`
	ContentType    string `json:"content_type"`
	BodySizeBytes  int64  `json:"body_size_bytes"`
	ReceivedAtUnix int64  `json:"received_at_unix"`
}

type inboxMessageDetailItem struct {
	ID             int64
	MessageID      string
	SenderPubKey   string
	TargetInput    string
	Route          string
	ContentType    string
	BodyBytes      []byte
	BodySizeBytes  int64
	ReceivedAtUnix int64
}

type routeIndexItem struct {
	Route         string `json:"route"`
	SeedHash      string `json:"seed_hash"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

func dbListInboxMessages(ctx context.Context, store *clientDB) ([]inboxMessageListItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]inboxMessageListItem, error) {
		rows, err := db.Query(
			`SELECT id,message_id,sender_pubkey_hex,target_input,route,content_type,body_size_bytes,received_at_unix
			   FROM proc_inbox_messages
			  ORDER BY received_at_unix DESC,id DESC`,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		items := make([]inboxMessageListItem, 0)
		for rows.Next() {
			var it inboxMessageListItem
			if err := rows.Scan(&it.ID, &it.MessageID, &it.SenderPubKey, &it.TargetInput, &it.Route, &it.ContentType, &it.BodySizeBytes, &it.ReceivedAtUnix); err != nil {
				return nil, err
			}
			items = append(items, it)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return items, nil
	})
}

func dbGetInboxMessageDetail(ctx context.Context, store *clientDB, id int64) (inboxMessageDetailItem, error) {
	if store == nil {
		return inboxMessageDetailItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (inboxMessageDetailItem, error) {
		var out inboxMessageDetailItem
		err := db.QueryRow(
			`SELECT message_id,sender_pubkey_hex,target_input,route,content_type,body_bytes,body_size_bytes,received_at_unix
			   FROM proc_inbox_messages WHERE id=?`,
			id,
		).Scan(&out.MessageID, &out.SenderPubKey, &out.TargetInput, &out.Route, &out.ContentType, &out.BodyBytes, &out.BodySizeBytes, &out.ReceivedAtUnix)
		if err != nil {
			return inboxMessageDetailItem{}, err
		}
		out.ID = id
		return out, nil
	})
}

func dbListRouteIndexes(ctx context.Context, store *clientDB) ([]routeIndexItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]routeIndexItem, error) {
		rows, err := db.Query(`SELECT route,seed_hash,updated_at_unix FROM proc_published_route_indexes ORDER BY route ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		items := make([]routeIndexItem, 0)
		for rows.Next() {
			var it routeIndexItem
			if err := rows.Scan(&it.Route, &it.SeedHash, &it.UpdatedAtUnix); err != nil {
				return nil, err
			}
			items = append(items, it)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return items, nil
	})
}

func dbDeleteRouteIndex(ctx context.Context, store *clientDB, route string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(`DELETE FROM proc_published_route_indexes WHERE route=?`, route)
		return err
	})
}

func dbUpsertRouteIndex(ctx context.Context, store *clientDB, route string, seedHash string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (int64, error) {
		return upsertPublishedRouteIndex(db, route, seedHash)
	})
}

func dbGetSeedChunkCount(ctx context.Context, store *clientDB, seedHash string) (uint32, bool) {
	if store == nil {
		return 0, false
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (uint32, error) {
		var chunkCount uint32
		if err := db.QueryRow(`SELECT chunk_count FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&chunkCount); err != nil {
			return 0, err
		}
		return chunkCount, nil
	})
	if err != nil {
		return 0, false
	}
	return out, true
}

func dbRecommendedFileNameBySeedHash(ctx context.Context, store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		seedHash = strings.ToLower(strings.TrimSpace(seedHash))
		if seedHash == "" {
			return "", nil
		}
		var stored string
		if err := db.QueryRow(`SELECT recommended_file_name FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&stored); err == nil {
			if normalized := sanitizeRecommendedFileName(stored); normalized != "" {
				return normalized, nil
			}
		}
		var workspacePath, filePath string
		if err := db.QueryRow(`SELECT workspace_path,file_path FROM biz_workspace_files WHERE seed_hash=? ORDER BY workspace_path ASC,file_path ASC LIMIT 1`, seedHash).Scan(&workspacePath, &filePath); err != nil {
			return "", nil
		}
		return sanitizeRecommendedFileName(filepath.Base(strings.TrimSpace(workspacePathJoin(workspacePath, filePath)))), nil
	})
	if err != nil {
		return ""
	}
	return out
}

func dbMimeHintBySeedHash(ctx context.Context, store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		seedHash = strings.ToLower(strings.TrimSpace(seedHash))
		if seedHash == "" {
			return "", nil
		}
		var stored string
		if err := db.QueryRow(`SELECT mime_hint FROM biz_seeds WHERE seed_hash=?`, seedHash).Scan(&stored); err != nil {
			return "", nil
		}
		return sanitizeMIMEHint(stored), nil
	})
	if err != nil {
		return ""
	}
	return out
}
