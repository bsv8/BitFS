package inboxmessage

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type fakeInboxStore struct {
	messages map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	}
	nextID int64
}

func (f *fakeInboxStore) WriteInboxMessage(ctx context.Context, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte, receivedAtUnix int64) (InboxReceipt, error) {
	key := senderPubKeyHex + ":" + messageID
	if existing, ok := f.messages[key]; ok {
		return existing.receipt, nil
	}
	f.nextID++
	receipt := InboxReceipt{InboxMessageID: f.nextID, ReceivedAtUnix: receivedAtUnix}
	f.messages[key] = struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	}{id: f.nextID, messageID: messageID, sender: senderPubKeyHex, receipt: receipt}
	return receipt, nil
}

func (f *fakeInboxStore) ListInboxMessages(ctx context.Context) ([]InboxMessageListItem, error) {
	out := make([]InboxMessageListItem, 0, len(f.messages))
	for _, m := range f.messages {
		out = append(out, InboxMessageListItem{
			ID:             m.id,
			MessageID:      m.messageID,
			SenderPubKey:   m.sender,
			BodySizeBytes:  int64(len(m.messageID)),
			ReceivedAtUnix: m.receipt.ReceivedAtUnix,
		})
	}
	return out, nil
}

func (f *fakeInboxStore) GetInboxMessageDetail(ctx context.Context, id int64) (InboxMessageDetailItem, error) {
	for _, m := range f.messages {
		if m.id == id {
			return InboxMessageDetailItem{
				ID:             m.id,
				MessageID:      m.messageID,
				SenderPubKey:   m.sender,
				BodySizeBytes:  int64(len(m.messageID)),
				ReceivedAtUnix: m.receipt.ReceivedAtUnix,
			}, nil
		}
	}
	return InboxMessageDetailItem{}, sql.ErrNoRows
}

func TestBizReceiveIdempotent(t *testing.T) {
	t.Parallel()
	store := &fakeInboxStore{messages: make(map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	})}

	receipt1, err := BizReceive(context.Background(), store, ReceiveParams{
		MessageID:       "msg1",
		SenderPubKeyHex: "sender1",
		ContentType:     "application/json",
		Body:            []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("first receive failed: %v", err)
	}
	receipt2, err := BizReceive(context.Background(), store, ReceiveParams{
		MessageID:       "msg1",
		SenderPubKeyHex: "sender1",
		ContentType:     "application/json",
		Body:            []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("second receive (idempotent) failed: %v", err)
	}
	if receipt1.InboxMessageID != receipt2.InboxMessageID {
		t.Fatalf("idempotent receive should return same inbox_id: %d vs %d", receipt1.InboxMessageID, receipt2.InboxMessageID)
	}
}

func TestBizReceiveRejectsEmptyMessageID(t *testing.T) {
	t.Parallel()
	store := &fakeInboxStore{messages: make(map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	})}
	_, err := BizReceive(context.Background(), store, ReceiveParams{
		MessageID:       "",
		SenderPubKeyHex: "sender1",
		ContentType:     "application/json",
	})
	if err == nil || CodeOf(err) != "BAD_REQUEST" {
		t.Fatalf("expected BAD_REQUEST for empty message_id, got: %v", err)
	}
}

func TestBizReceiveRejectsEmptyContentType(t *testing.T) {
	t.Parallel()
	store := &fakeInboxStore{messages: make(map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	})}
	_, err := BizReceive(context.Background(), store, ReceiveParams{
		MessageID:       "msg1",
		SenderPubKeyHex: "sender1",
		ContentType:     "",
	})
	if err == nil || CodeOf(err) != "BAD_REQUEST" {
		t.Fatalf("expected BAD_REQUEST for empty content_type, got: %v", err)
	}
}

func TestBizListRejectsNilReader(t *testing.T) {
	t.Parallel()
	_, err := BizList(context.Background(), nil)
	if err == nil || CodeOf(err) != "MODULE_DISABLED" {
		t.Fatalf("expected MODULE_DISABLED, got: %v", err)
	}
}

func TestBizDetailRejectsNilReader(t *testing.T) {
	t.Parallel()
	_, err := BizDetail(context.Background(), nil, 1)
	if err == nil || CodeOf(err) != "MODULE_DISABLED" {
		t.Fatalf("expected MODULE_DISABLED, got: %v", err)
	}
}

func TestBizDetailRejectsInvalidID(t *testing.T) {
	t.Parallel()
	store := &fakeInboxStore{messages: make(map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	})}
	_, err := BizDetail(context.Background(), store, 0)
	if err == nil || CodeOf(err) != "BAD_REQUEST" {
		t.Fatalf("expected BAD_REQUEST for invalid id, got: %v", err)
	}
}

func TestBizDetailNotFound(t *testing.T) {
	t.Parallel()
	store := &fakeInboxStore{messages: make(map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	})}
	_, err := BizDetail(context.Background(), store, 999)
	if err == nil || !stringsContains(CodeOf(err), "NOT_FOUND") && !stringsContains(err.Error(), "NOT_FOUND") {
		t.Fatalf("expected NOT_FOUND, got: %v", err)
	}
}

func TestBizReceiveContextCanceled(t *testing.T) {
	t.Parallel()
	store := &fakeInboxStore{messages: make(map[string]struct {
		id        int64
		messageID string
		sender    string
		receipt   InboxReceipt
	})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := BizReceive(ctx, store, ReceiveParams{
		MessageID:       "msg1",
		SenderPubKeyHex: "sender1",
		ContentType:     "application/json",
	})
	if err == nil || CodeOf(err) != "REQUEST_CANCELED" {
		t.Fatalf("expected REQUEST_CANCELED, got: %v", err)
	}
}

func TestErrorHelpers(t *testing.T) {
	t.Parallel()
	err := NewError("BAD_REQUEST", "test error")
	var typed *Error
	if !errors.As(err, &typed) {
		t.Fatal("expected typed error")
	}
	if CodeOf(err) != "BAD_REQUEST" {
		t.Fatalf("unexpected code: %s", CodeOf(err))
	}
	if MessageOf(err) != "test error" {
		t.Fatalf("unexpected message: %s", MessageOf(err))
	}
}

func stringsContains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
