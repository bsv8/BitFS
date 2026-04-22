package inboxmessage

import (
	"context"
	"database/sql"
	"fmt"

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

func (s *dbStore) WriteInboxMessage(ctx context.Context, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte, receivedAtUnix int64) (InboxReceipt, error) {
	if s == nil || s.store == nil {
		return InboxReceipt{}, fmt.Errorf("inbox message store is disabled")
	}
	var receipt InboxReceipt
	err := s.store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		row, err := wtx.QueryContext(ctx, `
			SELECT id, received_at_unix
			  FROM proc_inbox_messages
			 WHERE sender_pubkey_hex=? AND message_id=?`,
			senderPubKeyHex, messageID,
		)
		if err != nil {
			return err
		}
		defer row.Close()
		if row.Next() {
			if err := row.Scan(&receipt.InboxMessageID, &receipt.ReceivedAtUnix); err != nil {
				return err
			}
			return nil
		}
		if err := row.Err(); err != nil {
			return err
		}
		_, err = wtx.ExecContext(ctx, `
			INSERT INTO proc_inbox_messages(message_id,sender_pubkey_hex,target_input,route,content_type,body_bytes,body_size_bytes,received_at_unix)
			VALUES(?,?,?,?,?,?,?,?)`,
			messageID, senderPubKeyHex, targetInput, route, contentType, body, int64(len(body)), receivedAtUnix,
		)
		if err != nil {
			return err
		}
		row2, err := wtx.QueryContext(ctx, `SELECT last_insert_rowid()`)
		if err != nil {
			return err
		}
		defer row2.Close()
		if row2.Next() {
			if err := row2.Scan(&receipt.InboxMessageID); err != nil {
				return err
			}
		}
		receipt.ReceivedAtUnix = receivedAtUnix
		return nil
	})
	return receipt, err
}

func (s *dbStore) ListInboxMessages(ctx context.Context) ([]InboxMessageListItem, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("inbox message store is disabled")
	}
	var out []InboxMessageListItem
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT id, message_id, sender_pubkey_hex, target_input, route, content_type, body_size_bytes, received_at_unix
			  FROM proc_inbox_messages
			 ORDER BY received_at_unix DESC, id DESC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item InboxMessageListItem
			if err := rows.Scan(&item.ID, &item.MessageID, &item.SenderPubKey, &item.TargetInput, &item.Route, &item.ContentType, &item.BodySizeBytes, &item.ReceivedAtUnix); err != nil {
				return err
			}
			out = append(out, item)
		}
		return rows.Err()
	})
	return out, err
}

func (s *dbStore) GetInboxMessageDetail(ctx context.Context, id int64) (InboxMessageDetailItem, error) {
	if s == nil || s.store == nil {
		return InboxMessageDetailItem{}, fmt.Errorf("inbox message store is disabled")
	}
	var out InboxMessageDetailItem
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT id, message_id, sender_pubkey_hex, target_input, route, content_type, body_bytes, body_size_bytes, received_at_unix
			  FROM proc_inbox_messages
			 WHERE id=?`, id,
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
		if err := rows.Scan(&out.ID, &out.MessageID, &out.SenderPubKey, &out.TargetInput, &out.Route, &out.ContentType, &out.BodyBytes, &out.BodySizeBytes, &out.ReceivedAtUnix); err != nil {
			return err
		}
		return rows.Err()
	})
	return out, err
}
