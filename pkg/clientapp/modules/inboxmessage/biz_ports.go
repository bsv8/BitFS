package inboxmessage

import "context"

// InboxWriter 只提供写入能力（libp2p call 专用）。
type InboxWriter interface {
	WriteInboxMessage(ctx context.Context, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte, receivedAtUnix int64) (InboxReceipt, error)
}

// InboxReader 只提供读取能力（HTTP settings 面专用）。
type InboxReader interface {
	ListInboxMessages(ctx context.Context) ([]InboxMessageListItem, error)
	GetInboxMessageDetail(ctx context.Context, id int64) (InboxMessageDetailItem, error)
}

// Store 是模块对外的完整存储能力。
type Store interface {
	InboxReader
	InboxWriter
}

// ReceiveParams 是 BizReceive 的输入参数。
type ReceiveParams struct {
	MessageID       string
	SenderPubKeyHex string
	TargetInput     string
	Route           string
	ContentType     string
	Body            []byte
	ReceivedAtUnix  int64
}
