package inboxmessage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// Conn 是模块内使用的最小数据库连接能力。
type Conn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// SerialExecutor 表示把整段 DB 闭包串行执行的能力。
type SerialExecutor interface {
	Do(context.Context, func(Conn) error) error
}

type dbStore struct {
	db     Conn
	serial SerialExecutor
	mu     sync.Mutex
}

// BootstrapStore 创建并初始化 inbox message store。
func BootstrapStore(ctx context.Context, db Conn, serial SerialExecutor) (*dbStore, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if err := bootstrapSchema(ctx, db); err != nil {
		return nil, err
	}
	return &dbStore{db: db, serial: serial}, nil
}

func (s *dbStore) WriteInboxMessage(ctx context.Context, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte, receivedAtUnix int64) (InboxReceipt, error) {
	if s == nil || s.db == nil {
		return InboxReceipt{}, fmt.Errorf("inbox message store is disabled")
	}
	var receipt InboxReceipt
	err := s.withConn(ctx, func(conn Conn) error {
		row, err := conn.QueryContext(ctx, `
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
		_, err = conn.ExecContext(ctx, `
			INSERT INTO proc_inbox_messages(message_id,sender_pubkey_hex,target_input,route,content_type,body_bytes,body_size_bytes,received_at_unix)
			VALUES(?,?,?,?,?,?,?,?)`,
			messageID, senderPubKeyHex, targetInput, route, contentType, body, int64(len(body)), receivedAtUnix,
		)
		if err != nil {
			return err
		}
		row2, err := conn.QueryContext(ctx, `SELECT last_insert_rowid()`)
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
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("inbox message store is disabled")
	}
	var out []InboxMessageListItem
	err := s.withConn(ctx, func(conn Conn) error {
		rows, err := conn.QueryContext(ctx, `
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
	if s == nil || s.db == nil {
		return InboxMessageDetailItem{}, fmt.Errorf("inbox message store is disabled")
	}
	var out InboxMessageDetailItem
	err := s.withConn(ctx, func(conn Conn) error {
		rows, err := conn.QueryContext(ctx, `
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

func (s *dbStore) withConn(ctx context.Context, fn func(Conn) error) error {
	if s == nil {
		return fmt.Errorf("inbox message store is nil")
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
