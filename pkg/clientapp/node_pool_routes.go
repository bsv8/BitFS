package clientapp

import (
	"context"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/libp2p/go-libp2p/core/peer"
)

func callNodePoolInfo(ctx context.Context, rt *Runtime, peerID peer.ID) (poolcore.InfoResp, error) {
	var out poolcore.InfoResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePoolV1Info, nil, &out)
	return out, err
}

func callNodeServiceQuote(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.ServiceQuoteReq) (poolcore.ServiceQuoteResp, error) {
	req.ClientID = ""
	var out poolcore.ServiceQuoteResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePaymentV1Quote, &req, &out)
	return out, err
}

func callNodePoolCreate(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.CreateReq) (poolcore.CreateResp, error) {
	req.ClientID = ""
	var out poolcore.CreateResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePoolV1Create, &req, &out)
	return out, err
}

func callNodePoolBaseTx(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.BaseTxReq) (poolcore.BaseTxResp, error) {
	req.ClientID = ""
	var out poolcore.BaseTxResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePoolV1BaseTx, &req, &out)
	return out, err
}

func callNodePoolPayConfirm(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.PayConfirmReq) (poolcore.PayConfirmResp, error) {
	req.ClientID = ""
	var out poolcore.PayConfirmResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePoolV1PayConfirm, &req, &out)
	return out, err
}

func callNodePoolClose(ctx context.Context, rt *Runtime, peerID peer.ID, req poolcore.CloseReq) (poolcore.CloseResp, error) {
	req.ClientID = ""
	var out poolcore.CloseResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePoolV1Close, &req, &out)
	return out, err
}

func callNodePoolSessionState(ctx context.Context, rt *Runtime, peerID peer.ID, spendTxID string) (poolcore.StateResp, error) {
	req := poolcore.StateReq{
		SpendTxID: strings.TrimSpace(spendTxID),
	}
	var out poolcore.StateResp
	err := callNodeRouteProto(ctx, rt, peerID, ncall.RoutePoolV1SessionState, &req, &out)
	return out, err
}
