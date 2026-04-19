package inboxmessage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// BizReceive 是 libp2p call 写入 inbox 的业务入口。
// 幂等：同 sender + message_id 已存在时返回同一 inbox_id，不重复插入。
func BizReceive(ctx context.Context, writer InboxWriter, params ReceiveParams) (InboxReceipt, error) {
	if ctx == nil {
		return InboxReceipt{}, NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return InboxReceipt{}, NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if writer == nil {
		return InboxReceipt{}, moduleDisabledErr()
	}
	if strings.TrimSpace(params.MessageID) == "" {
		return InboxReceipt{}, NewError("BAD_REQUEST", "message_id is required")
	}
	if strings.TrimSpace(params.ContentType) == "" {
		return InboxReceipt{}, NewError("BAD_REQUEST", "content_type is required")
	}
	if strings.TrimSpace(params.SenderPubKeyHex) == "" {
		return InboxReceipt{}, NewError("BAD_REQUEST", "sender_pubkey_hex is required")
	}
	if params.Body == nil {
		params.Body = []byte{}
	}
	if params.ReceivedAtUnix <= 0 {
		params.ReceivedAtUnix = time.Now().Unix()
	}

	receipt, err := writer.WriteInboxMessage(
		ctx,
		strings.TrimSpace(params.MessageID),
		strings.TrimSpace(params.SenderPubKeyHex),
		strings.TrimSpace(params.TargetInput),
		strings.TrimSpace(params.Route),
		strings.TrimSpace(params.ContentType),
		params.Body,
		params.ReceivedAtUnix,
	)
	if err != nil {
		return InboxReceipt{}, mapBizStoreError(err)
	}
	return receipt, nil
}

// BizList 是 HTTP settings 列表的业务入口。
func BizList(ctx context.Context, reader InboxReader) ([]InboxMessageListItem, error) {
	if ctx == nil {
		return nil, NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return nil, NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if reader == nil {
		return nil, moduleDisabledErr()
	}
	items, err := reader.ListInboxMessages(ctx)
	if err != nil {
		return nil, mapBizStoreError(err)
	}
	return items, nil
}

// BizDetail 是 HTTP settings 详情的业务入口。
func BizDetail(ctx context.Context, reader InboxReader, id int64) (InboxMessageDetailItem, error) {
	if ctx == nil {
		return InboxMessageDetailItem{}, NewError("BAD_REQUEST", "ctx is required")
	}
	if ctx.Err() != nil {
		return InboxMessageDetailItem{}, NewError("REQUEST_CANCELED", ctx.Err().Error())
	}
	if reader == nil {
		return InboxMessageDetailItem{}, moduleDisabledErr()
	}
	if id <= 0 {
		return InboxMessageDetailItem{}, NewError("BAD_REQUEST", "id must be positive")
	}
	item, err := reader.GetInboxMessageDetail(ctx, id)
	if err != nil {
		return InboxMessageDetailItem{}, mapBizStoreError(err)
	}
	return item, nil
}

func moduleDisabledErr() error {
	return NewError("MODULE_DISABLED", "inbox message module is disabled")
}

func mapBizStoreError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return NewError("NOT_FOUND", "inbox message not found")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return NewError("REQUEST_CANCELED", err.Error())
	}
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}
	return NewError("INTERNAL_ERROR", err.Error())
}
