package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspacefiles"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procinboxmessages"
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

func dbListInboxMessages(ctx context.Context, store *clientDB) ([]inboxMessageListItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) ([]inboxMessageListItem, error) {
		rows, err := tx.ProcInboxMessages.Query().Order(procinboxmessages.ByReceivedAtUnix(entsql.OrderDesc()), procinboxmessages.ByID(entsql.OrderDesc())).All(ctx)
		if err != nil {
			return nil, err
		}
		items := make([]inboxMessageListItem, 0, len(rows))
		for _, row := range rows {
			items = append(items, inboxMessageListItem{
				ID:             int64(row.ID),
				MessageID:      row.MessageID,
				SenderPubKey:   row.SenderPubkeyHex,
				TargetInput:    row.TargetInput,
				Route:          row.Route,
				ContentType:    row.ContentType,
				BodySizeBytes:  row.BodySizeBytes,
				ReceivedAtUnix: row.ReceivedAtUnix,
			})
		}
		return items, nil
	})
}

func dbGetInboxMessageDetail(ctx context.Context, store *clientDB, id int64) (inboxMessageDetailItem, error) {
	if store == nil {
		return inboxMessageDetailItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (inboxMessageDetailItem, error) {
		row, err := tx.ProcInboxMessages.Query().Where(procinboxmessages.IDEQ(int(id))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return inboxMessageDetailItem{}, sql.ErrNoRows
			}
			return inboxMessageDetailItem{}, err
		}
		out := inboxMessageDetailItem{
			ID:             int64(row.ID),
			MessageID:      row.MessageID,
			SenderPubKey:   row.SenderPubkeyHex,
			TargetInput:    row.TargetInput,
			Route:          row.Route,
			ContentType:    row.ContentType,
			BodyBytes:      row.BodyBytes,
			BodySizeBytes:  row.BodySizeBytes,
			ReceivedAtUnix: row.ReceivedAtUnix,
		}
		return out, nil
	})
}

func dbGetSeedChunkCount(ctx context.Context, store *clientDB, seedHash string) (uint32, bool) {
	if store == nil {
		return 0, false
	}
	out, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (uint32, error) {
		row, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(normalizeSeedHashHex(seedHash))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, sql.ErrNoRows
			}
			return 0, err
		}
		return uint32(row.ChunkCount), nil
	})
	if err != nil {
		return 0, false
	}
	return out, true
}

// dbGetSeedChunkCountForPricing 返回种子 chunk_count，用于定价接口。
// 这是 biz_seeds 旧表查询的过渡封装，后续如需迁移到新的文件元数据表，只需修改此函数。
func dbGetSeedChunkCountForPricing(ctx context.Context, store *clientDB, seedHash string) (uint32, error) {
	if store == nil {
		return 0, fmt.Errorf("store is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return 0, fmt.Errorf("seed_hash is required")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (uint32, error) {
		row, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, sql.ErrNoRows
			}
			return 0, err
		}
		return uint32(row.ChunkCount), nil
	})
}

func dbRecommendedFileNameBySeedHash(ctx context.Context, store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	out, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			return "", nil
		}
		if row, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx); err == nil {
			if normalized := sanitizeRecommendedFileName(row.RecommendedFileName); normalized != "" {
				return normalized, nil
			}
		}
		row, err := tx.BizWorkspaceFiles.Query().
			Where(bizworkspacefiles.SeedHashEQ(seedHash)).
			Order(bizworkspacefiles.ByWorkspacePath(), bizworkspacefiles.ByFilePath()).
			First(ctx)
		if err != nil {
			return "", nil
		}
		return sanitizeRecommendedFileName(filepath.Base(strings.TrimSpace(workspacePathJoin(row.WorkspacePath, row.FilePath)))), nil
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
	out, err := clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (string, error) {
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			return "", nil
		}
		row, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
		if err != nil {
			return "", nil
		}
		return sanitizeMIMEHint(row.MimeHint), nil
	})
	if err != nil {
		return ""
	}
	return out
}
