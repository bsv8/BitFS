package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// 设计说明：
// - BSV21 只保留已提交这个主事实，状态机不再单独漂在旧表里；
// - 事件表只记真实发生过的业务片段，方便追踪历史，不参与状态判断。

type factBSV21CreateItem struct {
	TokenID         string
	CreateTxID      string
	WalletID        string
	Address         string
	TokenStandard   string
	Symbol          string
	MaxSupply       string
	Decimals        int
	Icon            string
	CreatedAtUnix   int64
	SubmittedAtUnix int64
	UpdatedAtUnix   int64
	PayloadJSON     string
}

type factBSV21EventItem struct {
	TokenID     string
	EventKind   string
	EventAtUnix int64
	TxID        string
	Note        string
	PayloadJSON string
}

func upsertFactBSV21Create(ctx context.Context, store *clientDB, item factBSV21CreateItem) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	item.TokenID = strings.ToLower(strings.TrimSpace(item.TokenID))
	item.CreateTxID = strings.ToLower(strings.TrimSpace(item.CreateTxID))
	item.WalletID = strings.TrimSpace(item.WalletID)
	item.Address = strings.TrimSpace(item.Address)
	item.TokenStandard = strings.ToLower(strings.TrimSpace(item.TokenStandard))
	item.Symbol = strings.TrimSpace(item.Symbol)
	item.MaxSupply = strings.TrimSpace(item.MaxSupply)
	item.Icon = strings.TrimSpace(item.Icon)
	item.PayloadJSON = strings.TrimSpace(item.PayloadJSON)
	if item.TokenID == "" || item.CreateTxID == "" {
		return fmt.Errorf("token_id and create_txid are required")
	}
	if item.TokenStandard == "" {
		item.TokenStandard = "bsv21"
	}
	if item.CreatedAtUnix <= 0 {
		item.CreatedAtUnix = time.Now().Unix()
	}
	if item.SubmittedAtUnix <= 0 {
		item.SubmittedAtUnix = item.CreatedAtUnix
	}
	if item.UpdatedAtUnix <= 0 {
		item.UpdatedAtUnix = time.Now().Unix()
	}
	if item.PayloadJSON == "" {
		item.PayloadJSON = "{}"
	}
	return store.Do(ctx, func(db sqlConn) error {
		_, err := ExecContext(ctx, db, `INSERT INTO fact_bsv21(
			token_id,create_txid,wallet_id,address,token_standard,symbol,max_supply,decimals,icon,created_at_unix,submitted_at_unix,updated_at_unix,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(token_id) DO UPDATE SET
			create_txid=excluded.create_txid,
			wallet_id=excluded.wallet_id,
			address=excluded.address,
			token_standard=excluded.token_standard,
			symbol=excluded.symbol,
			max_supply=excluded.max_supply,
			decimals=excluded.decimals,
			icon=excluded.icon,
			created_at_unix=excluded.created_at_unix,
			submitted_at_unix=excluded.submitted_at_unix,
			updated_at_unix=excluded.updated_at_unix,
			payload_json=excluded.payload_json`,
			item.TokenID,
			item.CreateTxID,
			item.WalletID,
			item.Address,
			item.TokenStandard,
			item.Symbol,
			item.MaxSupply,
			item.Decimals,
			item.Icon,
			item.CreatedAtUnix,
			item.SubmittedAtUnix,
			item.UpdatedAtUnix,
			item.PayloadJSON,
		)
		return err
	})
}

func appendFactBSV21Event(ctx context.Context, store *clientDB, item factBSV21EventItem) error {
	if store == nil {
		return fmt.Errorf("db is nil")
	}
	item.TokenID = strings.ToLower(strings.TrimSpace(item.TokenID))
	item.EventKind = strings.TrimSpace(item.EventKind)
	item.TxID = strings.ToLower(strings.TrimSpace(item.TxID))
	item.Note = strings.TrimSpace(item.Note)
	item.PayloadJSON = strings.TrimSpace(item.PayloadJSON)
	if item.TokenID == "" {
		return fmt.Errorf("token_id is required")
	}
	if item.EventKind == "" {
		return fmt.Errorf("event_kind is required")
	}
	if item.EventAtUnix <= 0 {
		item.EventAtUnix = time.Now().Unix()
	}
	if item.PayloadJSON == "" {
		item.PayloadJSON = "{}"
	}
	return store.Do(ctx, func(db sqlConn) error {
		_, err := ExecContext(ctx, db, `INSERT INTO fact_bsv21_events(
			token_id,event_kind,event_at_unix,txid,note,payload_json
		) VALUES(?,?,?,?,?,?)`,
			item.TokenID,
			item.EventKind,
			item.EventAtUnix,
			item.TxID,
			item.Note,
			item.PayloadJSON,
		)
		return err
	})
}

func marshalFactBSV21Payload(values map[string]any) string {
	if len(values) == 0 {
		return "{}"
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
