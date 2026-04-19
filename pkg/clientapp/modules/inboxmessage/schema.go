package inboxmessage

import (
	"context"
	"fmt"
)

const (
	inboxMessagesTableSQL = `CREATE TABLE IF NOT EXISTS proc_inbox_messages(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		message_id TEXT NOT NULL,
		sender_pubkey_hex TEXT NOT NULL,
		target_input TEXT NOT NULL,
		route TEXT NOT NULL,
		content_type TEXT NOT NULL,
		body_bytes BLOB,
		body_size_bytes INTEGER NOT NULL,
		received_at_unix INTEGER NOT NULL,
		UNIQUE(sender_pubkey_hex, message_id)
	)`
	inboxMessagesSenderMessageIndexSQL = `CREATE INDEX IF NOT EXISTS idx_proc_inbox_messages_sender_message
		ON proc_inbox_messages(sender_pubkey_hex, message_id)`
	inboxMessagesReceivedAtIndexSQL = `CREATE INDEX IF NOT EXISTS idx_proc_inbox_messages_received_at
		ON proc_inbox_messages(received_at_unix DESC, id DESC)`
)

func bootstrapSchema(ctx context.Context, db Conn) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	stmts := []string{
		inboxMessagesTableSQL,
		inboxMessagesSenderMessageIndexSQL,
		inboxMessagesReceivedAtIndexSQL,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap inbox_message schema: %w", err)
		}
	}
	return nil
}
