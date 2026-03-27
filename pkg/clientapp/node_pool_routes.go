package clientapp

import (
	"context"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/libp2p/go-libp2p/core/peer"
)

func callNodePoolInfo(ctx context.Context, rt *Runtime, peerID peer.ID) (dual2of2.InfoResp, error) {
	var out dual2of2.InfoResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1Info, nil, &out)
	return out, err
}

func callNodePoolServiceQuote(ctx context.Context, rt *Runtime, peerID peer.ID, req dual2of2.ServiceQuoteReq) (dual2of2.ServiceQuoteResp, error) {
	req.ClientID = ""
	var out dual2of2.ServiceQuoteResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1ServiceQuote, &req, &out)
	return out, err
}

func callNodePoolCreate(ctx context.Context, rt *Runtime, peerID peer.ID, req dual2of2.CreateReq) (dual2of2.CreateResp, error) {
	req.ClientID = ""
	var out dual2of2.CreateResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1Create, &req, &out)
	return out, err
}

func callNodePoolBaseTx(ctx context.Context, rt *Runtime, peerID peer.ID, req dual2of2.BaseTxReq) (dual2of2.BaseTxResp, error) {
	req.ClientID = ""
	var out dual2of2.BaseTxResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1BaseTx, &req, &out)
	return out, err
}

func callNodePoolPayConfirm(ctx context.Context, rt *Runtime, peerID peer.ID, req dual2of2.PayConfirmReq) (dual2of2.PayConfirmResp, error) {
	req.ClientID = ""
	var out dual2of2.PayConfirmResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1PayConfirm, &req, &out)
	return out, err
}

func callNodePoolClose(ctx context.Context, rt *Runtime, peerID peer.ID, req dual2of2.CloseReq) (dual2of2.CloseResp, error) {
	req.ClientID = ""
	var out dual2of2.CloseResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1Close, &req, &out)
	return out, err
}

func callNodePoolState(ctx context.Context, rt *Runtime, peerID peer.ID, spendTxID string) (dual2of2.StateResp, error) {
	req := dual2of2.StateReq{
		SpendTxID: strings.TrimSpace(spendTxID),
	}
	var out dual2of2.StateResp
	err := callNodeRouteProto(ctx, rt, peerID, nodesvc.RoutePoolV1State, &req, &out)
	return out, err
}
