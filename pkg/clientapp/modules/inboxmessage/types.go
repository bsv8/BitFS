package inboxmessage

import (
	"errors"
	"strings"

	oldproto "github.com/golang/protobuf/proto"
)

// Error 模块统一错误壳。
// 设计说明：错误码是流程判断依据，错误文本只给人看。
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

func NewError(code, message string) error {
	return &Error{Code: strings.TrimSpace(code), Message: strings.TrimSpace(message)}
}

func CodeOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.Code)
	}
	return ""
}

func MessageOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.Message)
	}
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

// InboxReceipt 是 libp2p call 写入成功后返回给调用方的回执。
type InboxReceipt struct {
	InboxMessageID int64 `protobuf:"varint,1,opt,name=inbox_message_id,json=inboxMessageId,proto3" json:"inbox_message_id"`
	ReceivedAtUnix int64 `protobuf:"varint,2,opt,name=received_at_unix,json=receivedAtUnix,proto3" json:"received_at_unix"`
}

func (m *InboxReceipt) Reset()         { *m = InboxReceipt{} }
func (m *InboxReceipt) String() string { return oldproto.CompactTextString(m) }
func (*InboxReceipt) ProtoMessage()    {}

// InboxMessageListItem 是列表接口返回的摘要。
type InboxMessageListItem struct {
	ID             int64  `json:"id"`
	MessageID      string `json:"message_id"`
	SenderPubKey   string `json:"sender_pubkey_hex"`
	TargetInput    string `json:"target_input"`
	Route          string `json:"route"`
	ContentType    string `json:"content_type"`
	BodySizeBytes  int64  `json:"body_size_bytes"`
	ReceivedAtUnix int64  `json:"received_at_unix"`
}

// InboxMessageDetailItem 是详情接口返回的完整内容。
type InboxMessageDetailItem struct {
	ID             int64  `json:"id"`
	MessageID      string `json:"message_id"`
	SenderPubKey   string `json:"sender_pubkey_hex"`
	TargetInput    string `json:"target_input"`
	Route          string `json:"route"`
	ContentType    string `json:"content_type"`
	BodyBytes      []byte `json:"-"`
	BodySizeBytes  int64  `json:"body_size_bytes"`
	ReceivedAtUnix int64  `json:"received_at_unix"`
}
