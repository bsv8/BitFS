package domainwire

import (
	"context"
	"strings"

	bfterrors "github.com/bsv8/BFTP-contract/pkg/v1/errors"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
)

type NodeRouteRuntime struct {
	Pricing        func(context.Context) (DomainPricingBody, error)
	Resolve        func(context.Context, ncall.CallContext, ncall.CallReq, NameRouteReq) (ncall.CallResp, error)
	Query          func(context.Context, ncall.CallContext, ncall.CallReq, NameRouteReq) (ncall.CallResp, error)
	Lock           func(context.Context, ncall.CallContext, ncall.CallReq, NameTargetRouteReq) (ncall.CallResp, error)
	ListOwned      func(context.Context, ncall.CallContext, ListOwnedReq) (ncall.CallResp, error)
	SetTarget      func(context.Context, ncall.CallContext, ncall.CallReq, NameTargetRouteReq) (ncall.CallResp, error)
	RegisterSubmit func(context.Context, ncall.CallContext, RegisterSubmitReq) (ncall.CallResp, error)
}

func HandleNodeCall(ctx context.Context, rt NodeRouteRuntime, meta ncall.CallContext, req ncall.CallReq) (bool, ncall.CallResp, error) {
	routeKey := strings.TrimSpace(req.Route)
	switch {
	case routeKey == string(contractroute.RouteDomainV1Pricing):
		if rt.Pricing == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.Pricing(ctx)
		if err != nil {
			return true, ncall.CallResp{}, err
		}
		out, err := ncall.MarshalProto(&resp)
		return true, out, err

	case routeKey == string(contractroute.RouteDomainV1Resolve):
		var body NameRouteReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.Resolve == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.Resolve(ctx, meta, req, body)
		return true, resp, err

	case routeKey == string(contractroute.RouteDomainV1Query):
		var body NameRouteReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.Query == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.Query(ctx, meta, req, body)
		return true, resp, err

	case routeKey == string(contractroute.RouteDomainV1Lock):
		var body NameTargetRouteReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.Lock == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.Lock(ctx, meta, req, body)
		return true, resp, err

	case routeKey == string(contractroute.RouteDomainV1ListOwned):
		var body ListOwnedReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, false); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if strings.TrimSpace(body.OwnerPubkeyHex) == "" {
			body.OwnerPubkeyHex = strings.TrimSpace(meta.SenderPubkeyHex)
		}
		if rt.ListOwned == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.ListOwned(ctx, meta, body)
		return true, resp, err

	case routeKey == string(contractroute.RouteDomainV1SetTarget):
		var body NameTargetRouteReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.SetTarget == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.SetTarget(ctx, meta, req, body)
		return true, resp, err

	case routeKey == string(contractroute.RouteDomainV1RegisterSubmit):
		var body RegisterSubmitReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if len(body.RegisterTx) == 0 {
			return true, badRequestResp("register_tx required"), nil
		}
		body.ClientID = strings.TrimSpace(meta.SenderPubkeyHex)
		body.RegisterTx = append([]byte(nil), body.RegisterTx...)
		if rt.RegisterSubmit == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.RegisterSubmit(ctx, meta, body)
		if err != nil {
			return true, ncall.CallResp{}, err
		}
		out, err := ncall.MarshalProto(&resp)
		return true, out, err

	default:
		return false, ncall.CallResp{}, nil
	}
}

func badRequestResp(message string) ncall.CallResp {
	return ncall.CallResp{Ok: false, Code: string(bfterrors.CodeBadRequest), Message: message}
}

func routeNotFoundResp() ncall.CallResp {
	return ncall.CallResp{Ok: false, Code: string(bfterrors.CodeRouteNotFound), Message: "route not found"}
}