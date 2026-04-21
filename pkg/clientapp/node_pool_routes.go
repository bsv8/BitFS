package clientapp

import (
	"context"
	"strings"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func callNodePoolInfo(ctx context.Context, rt *Runtime, peerID peer.ID) (poolcore.InfoResp, error) {
	var out poolcore.InfoResp
	body, err := oldproto.Marshal(&poolcore.InfoReq{})
	if err != nil {
		return out, err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, contractprotoid.ProtoPoolV1Info, ncall.CallReq{
		To:          peerID.String(),
		ContentType: "application/x-protobuf",
		Body:        body,
	})
	if err != nil {
		return out, err
	}
	if err := oldproto.Unmarshal(resp.Body, &out); err != nil {
		return out, err
	}
	return out, nil
}

func callNodePoolCreate(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.CreateReq) (poolcore.CreateResp, error) {
	req.ClientID = ""
	var out poolcore.CreateResp
	body, err := oldproto.Marshal(&req)
	if err != nil {
		return out, err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, contractprotoid.ProtoPoolV1Create, ncall.CallReq{
		To:          peerID.String(),
		ContentType: "application/x-protobuf",
		Body:        body,
	})
	if err != nil {
		return out, err
	}
	if err := oldproto.Unmarshal(resp.Body, &out); err != nil {
		return out, err
	}
	return out, nil
}

func callNodePoolBaseTx(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.BaseTxReq) (poolcore.BaseTxResp, error) {
	req.ClientID = ""
	var out poolcore.BaseTxResp
	body, err := oldproto.Marshal(&req)
	if err != nil {
		return out, err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, contractprotoid.ProtoPoolV1BaseTx, ncall.CallReq{
		To:          peerID.String(),
		ContentType: "application/x-protobuf",
		Body:        body,
	})
	if err != nil {
		return out, err
	}
	if err := oldproto.Unmarshal(resp.Body, &out); err != nil {
		return out, err
	}
	return out, nil
}

func callNodePoolPayConfirm(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.PayConfirmReq) (poolcore.PayConfirmResp, error) {
	req.ClientID = ""
	var out poolcore.PayConfirmResp
	body, err := oldproto.Marshal(&req)
	if err != nil {
		return out, err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, contractprotoid.ProtoPoolV1PayConfirm, ncall.CallReq{
		To:          peerID.String(),
		ContentType: "application/x-protobuf",
		Body:        body,
	})
	if err != nil {
		return out, err
	}
	if err := oldproto.Unmarshal(resp.Body, &out); err != nil {
		return out, err
	}
	return out, nil
}

func callNodePoolClose(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.CloseReq) (poolcore.CloseResp, error) {
	req.ClientID = ""
	var out poolcore.CloseResp
	body, err := oldproto.Marshal(&req)
	if err != nil {
		return out, err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, contractprotoid.ProtoPoolV1Close, ncall.CallReq{
		To:          peerID.String(),
		ContentType: "application/x-protobuf",
		Body:        body,
	})
	if err != nil {
		return out, err
	}
	if err := oldproto.Unmarshal(resp.Body, &out); err != nil {
		return out, err
	}
	return out, nil
}

func callNodePoolSessionState(ctx context.Context, rt *Runtime, peerID peer.ID, spendTxID string) (poolcore.StateResp, error) {
	req := poolcore.StateReq{
		SpendTxID: strings.TrimSpace(spendTxID),
	}
	var out poolcore.StateResp
	body, err := oldproto.Marshal(&req)
	if err != nil {
		return out, err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, contractprotoid.ProtoPoolV1SessionState, ncall.CallReq{
		To:          peerID.String(),
		ContentType: "application/x-protobuf",
		Body:        body,
	})
	if err != nil {
		return out, err
	}
	if err := oldproto.Unmarshal(resp.Body, &out); err != nil {
		return out, err
	}
	return out, nil
}
