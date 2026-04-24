package broadcast

import (
	"context"
	"strings"

	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
)

// NodeRouteRuntime 描述 broadcast.v1.* 公开 node.call 路由在角色侧需要注入的运行时。
type NodeRouteRuntime struct {
	ListenCycle              func(context.Context, ncall.CallContext, ncall.CallReq, ListenCycleReq) (ncall.CallResp, error)
	DemandPublish            func(context.Context, ncall.CallContext, ncall.CallReq, DemandPublishReq) (ncall.CallResp, error)
	DemandPublishBatch       func(context.Context, ncall.CallContext, ncall.CallReq, DemandPublishBatchReq) (ncall.CallResp, error)
	LiveDemandPublish        func(context.Context, ncall.CallContext, ncall.CallReq, LiveDemandPublishReq) (ncall.CallResp, error)
	NodeReachabilityAnnounce func(context.Context, ncall.CallContext, ncall.CallReq, NodeReachabilityAnnounceReq) (ncall.CallResp, error)
	NodeReachabilityQuery    func(context.Context, ncall.CallContext, ncall.CallReq, NodeReachabilityQueryReq) (ncall.CallResp, error)
}

// HandleNodeCall 统一处理 broadcast.v1.* 的 node.call 路由。
func HandleNodeCall(ctx context.Context, rt NodeRouteRuntime, meta ncall.CallContext, req ncall.CallReq) (bool, ncall.CallResp, error) {
	switch strings.TrimSpace(req.Route) {
	case RouteBroadcastV1ListenCycle:
		var body ListenCycleReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.ListenCycle == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.ListenCycle(ctx, meta, req, body)
		return true, resp, err
	case RouteBroadcastV1DemandPublish:
		var body DemandPublishReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.DemandPublish == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.DemandPublish(ctx, meta, req, body)
		return true, resp, err
	case RouteBroadcastV1DemandPublishBatch:
		var body DemandPublishBatchReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.DemandPublishBatch == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.DemandPublishBatch(ctx, meta, req, body)
		return true, resp, err
	case RouteBroadcastV1LiveDemandPublish:
		var body LiveDemandPublishReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.LiveDemandPublish == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.LiveDemandPublish(ctx, meta, req, body)
		return true, resp, err
	case RouteBroadcastV1NodeReachabilityAnnounce:
		var body NodeReachabilityAnnounceReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.NodeReachabilityAnnounce == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.NodeReachabilityAnnounce(ctx, meta, req, body)
		return true, resp, err
	case RouteBroadcastV1NodeReachabilityQuery:
		var body NodeReachabilityQueryReq
		if err := ncall.DecodeProto(req.Route, req.Body, &body, true); err != nil {
			return true, badRequestResp(err.Error()), nil
		}
		if rt.NodeReachabilityQuery == nil {
			return true, routeNotFoundResp(), nil
		}
		resp, err := rt.NodeReachabilityQuery(ctx, meta, req, body)
		return true, resp, err
	default:
		return false, ncall.CallResp{}, nil
	}
}

func badRequestResp(message string) ncall.CallResp {
	return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: message}
}

func routeNotFoundResp() ncall.CallResp {
	return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}
}
