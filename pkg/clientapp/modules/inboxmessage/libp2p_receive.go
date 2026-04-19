package inboxmessage

import (
	"context"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	oldproto "github.com/golang/protobuf/proto"
)

func receiveInboxMessage(store Store) moduleapi.LibP2PHook {
	return func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		receipt, err := BizReceive(ctx, store, ReceiveParams{
			MessageID:       ev.MessageID,
			SenderPubKeyHex: ev.SenderPubkeyHex,
			TargetInput:     ev.Request.To,
			Route:           InboxMessageRoute,
			ContentType:     ev.Request.ContentType,
			Body:            ev.Request.Body,
			ReceivedAtUnix:  time.Now().Unix(),
		})
		if err != nil {
			return moduleapi.LibP2PResult{}, toModuleAPIError(err)
		}
		bodyBytes, _ := oldproto.Marshal(&InboxReceipt{
			InboxMessageID: receipt.InboxMessageID,
			ReceivedAtUnix: receipt.ReceivedAtUnix,
		})
		return moduleapi.LibP2PResult{
			CallResp: moduleapi.CallResponse{
				Ok:          true,
				Code:        "OK",
				ContentType: contractmessage.ContentTypeProto,
				Body:        bodyBytes,
			},
		}, nil
	}
}
