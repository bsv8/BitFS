package clientapp

import (
	"context"
	"fmt"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type TriggerPeerCallParams struct {
	To                   string
	Route                string
	ContentType          string
	Body                 []byte
	Store                *clientDB
	PaymentMode          string
	PaymentScheme        string
	ServiceQuote         []byte
	RequireActiveFeePool bool
}

type TriggerPeerResolveParams struct {
	To    string
	Route string
	Store *clientDB
}

type peerCallPaymentDecision struct {
	Scheme string
	Option *ncall.PaymentOption
}

type routeIndexManifest struct {
	Route               string `protobuf:"bytes,1,opt,name=route,proto3" json:"route"`
	SeedHash            string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	RecommendedFileName string `protobuf:"bytes,3,opt,name=recommended_file_name,json=recommendedFileName,proto3" json:"recommended_file_name,omitempty"`
	MIMEHint            string `protobuf:"bytes,4,opt,name=mime_hint,json=mimeHint,proto3" json:"mime_hint,omitempty"`
	FileSize            int64  `protobuf:"varint,5,opt,name=file_size,json=fileSize,proto3" json:"file_size,omitempty"`
	UpdatedAtUnix       int64  `protobuf:"varint,6,opt,name=updated_at_unix,json=updatedAtUnix,proto3" json:"updated_at_unix"`
}

// registerNodeRouteHandlers 把节点级 route-call / route-resolve 能力挂到统一 libp2p 协议上。
// 设计说明：
// - bitfs.peer.call 未来会承接多支付协议，因此这里先统一“所有节点共用一个外壳”；
// - 壳只认协议路由，不认具体模块；模块细节只通过注册表分发；
// - 业务层 route 继续由各自服务挂载，壳不需要知道 domain/gateway 的细节。
func registerNodeRouteHandlers(rt *Runtime, store *clientDB) {
	if rt == nil || rt.Host == nil || store == nil {
		return
	}
	ncall.Register(rt.Host, nodeSecForRuntime(rt), func(ctx context.Context, meta ncall.CallContext, req ncall.CallReq) (ncall.CallResp, error) {
		route, bad := normalizeCallRoute(req.Route)
		if bad != "" {
			return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: bad}, nil
		}
		contentType, bad := normalizeContentType(req.ContentType)
		if bad != "" {
			return ncall.CallResp{Ok: false, Code: "BAD_REQUEST", Message: bad}, nil
		}
		switch route {
		case string(contractroute.RouteNodeV1CapabilitiesShow):
			body := clientCapabilitiesShowBody(rt)
			return marshalNodeCallProto(&body)
		default:
			if rt.modules == nil {
				return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
			}
			out, err := rt.modules.DispatchLibP2P(ctx, LibP2PEvent{
				Protocol:    LibP2PProtocolNodeCall,
				Route:       route,
				Meta:        meta,
				Req:         req,
				ContentType: contentType,
			})
			if err != nil {
				if code := moduleHookCode(err); code != "" {
					return ncall.CallResp{Ok: false, Code: code, Message: moduleHookMessage(err)}, nil
				}
				return ncall.CallResp{}, err
			}
			return out.CallResp, nil
		}
	}, func(ctx context.Context, req ncall.ResolveReq) (ncall.ResolveResp, error) {
		if rt == nil || rt.modules == nil {
			return ncall.ResolveResp{Ok: false, Code: "MODULE_DISABLED", Message: "module is disabled"}, nil
		}
		// resolve 的注册键固定留空，真正要查的路由还在 Req.Route 里。
		out, err := rt.modules.DispatchLibP2P(ctx, LibP2PEvent{
			Protocol:    LibP2PProtocolNodeResolve,
			Route:       "",
			Req:         ncall.CallReq{Route: req.Route},
			ContentType: contractmessage.ContentTypeProto,
		})
		if err != nil {
			if code := moduleHookCode(err); code != "" {
				return ncall.ResolveResp{Ok: false, Code: code, Message: moduleHookMessage(err)}, nil
			}
			return ncall.ResolveResp{}, err
		}
		body, err := oldproto.Marshal(&out.ResolveManifest)
		if err != nil {
			return ncall.ResolveResp{}, err
		}
		return ncall.ResolveResp{
			Ok:          true,
			Code:        "OK",
			ContentType: contractmessage.ContentTypeProto,
			Body:        body,
		}, nil
	})
}

func TriggerPeerCall(ctx context.Context, rt *Runtime, p TriggerPeerCallParams) (ncall.CallResp, error) {
	var out ncall.CallResp
	if rt == nil || rt.Host == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	to, peerID, err := resolveClientTarget(strings.TrimSpace(p.To))
	if err != nil {
		return out, err
	}
	if err := ensureTargetPeerReachable(ctx, p.Store, rt, to, peerID); err != nil {
		return out, err
	}
	req := ncall.CallReq{
		To:          to,
		Route:       strings.TrimSpace(p.Route),
		ContentType: strings.TrimSpace(p.ContentType),
		Body:        append([]byte(nil), p.Body...),
	}
	paymentMode := normalizePeerCallPaymentMode(p.PaymentMode)
	if paymentMode == "pay" && len(p.ServiceQuote) > 0 {
		return payPeerCallWithAcceptedQuote(ctx, rt, p.Store, peerID, req, p.ServiceQuote, p.PaymentScheme, p.RequireActiveFeePool)
	}
	out, err = callNodeRoute(ctx, rt, peerID, req)
	if err != nil {
		return out, err
	}
	if !isNodePaymentQuoted(out) {
		return out, nil
	}
	decision, err := decidePeerCallPayment(rt, p.PaymentScheme, out.PaymentSchemes)
	if err != nil {
		return ncall.CallResp{}, err
	}
	if paymentMode == "quote" {
		return quotePeerCallFromPaymentQuoted(ctx, rt, p.Store, peerID, req, decision.Option, p.RequireActiveFeePool)
	}
	paidOut, payErr := retryPeerCallWithAutoPayment(ctx, rt, p.Store, peerID, req, decision.Option, p.RequireActiveFeePool)
	if payErr != nil {
		return ncall.CallResp{}, payErr
	}
	return paidOut, nil
}

func TriggerPeerResolve(ctx context.Context, rt *Runtime, p TriggerPeerResolveParams) (ncall.ResolveResp, error) {
	var out ncall.ResolveResp
	if rt == nil || rt.Host == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	to, peerID, err := resolveClientTarget(strings.TrimSpace(p.To))
	if err != nil {
		return out, err
	}
	if err := ensureTargetPeerReachable(ctx, p.Store, rt, to, peerID); err != nil {
		return out, err
	}
	// 这里只发 resolve 查询，目标端如果要改 settings，必须走独立 HTTP 管理面。
	err = pproto.CallProto(ctx, rt.Host, peerID, contractprotoid.ProtoNodeResolve, nodeSecForRuntime(rt), contractmessage.ResolveReq{
		To:    to,
		Route: strings.TrimSpace(p.Route),
	}, &out)
	return out, err
}

func callNodeRoute(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq) (ncall.CallResp, error) {
	var out ncall.CallResp
	if err := pproto.CallProto(ctx, rt.Host, peerID, contractprotoid.ProtoNodeCall, nodeSecForRuntime(rt), req, &out); err != nil {
		return ncall.CallResp{}, err
	}
	return out, nil
}

func marshalNodeCallProto(msg oldproto.Message) (ncall.CallResp, error) {
	body, err := marshalNodeRouteProtoBody(msg)
	if err != nil {
		return ncall.CallResp{}, err
	}
	return ncall.CallResp{
		Ok:          true,
		Code:        "OK",
		ContentType: contractmessage.ContentTypeProto,
		Body:        body,
	}, nil
}

func isNodePaymentQuoted(resp ncall.CallResp) bool {
	return !resp.Ok && strings.EqualFold(strings.TrimSpace(resp.Code), "PAYMENT_QUOTED") && len(resp.PaymentSchemes) > 0
}

func normalizePeerCallPaymentMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "quote":
		return "quote"
	case "pay":
		return "pay"
	default:
		return "auto"
	}
}

// decidePeerCallPayment 是 payment_scheme 唯一决策入口。
// 设计说明：
// - quote/auto/pay 三种入口都调用这里；
// - 执行层只消费这里给出的决策结果，不再维护平行规则。
func decidePeerCallPayment(rt *Runtime, requestedScheme string, options []*ncall.PaymentOption) (peerCallPaymentDecision, error) {
	preferred := strings.TrimSpace(requestedScheme)
	if preferred == "" {
		preferred = preferredPaymentScheme(rt)
	}
	normalized, err := normalizePreferredPaymentScheme(preferred)
	if err != nil {
		return peerCallPaymentDecision{}, err
	}
	if len(options) == 0 {
		return peerCallPaymentDecision{Scheme: normalized}, nil
	}
	option, ok := choosePeerCallPaymentOption(options, normalized)
	if !ok || option == nil {
		if strings.TrimSpace(requestedScheme) != "" {
			return peerCallPaymentDecision{}, fmt.Errorf("payment scheme unavailable: %s", normalized)
		}
		return peerCallPaymentDecision{}, fmt.Errorf("no supported payment option")
	}
	if strings.TrimSpace(requestedScheme) != "" && !strings.EqualFold(strings.TrimSpace(option.Scheme), normalized) {
		return peerCallPaymentDecision{}, fmt.Errorf("payment scheme unavailable: %s", normalized)
	}
	return peerCallPaymentDecision{
		Scheme: strings.TrimSpace(option.Scheme),
		Option: option,
	}, nil
}

func normalizeCallRoute(raw string) (string, string) {
	route := strings.TrimSpace(raw)
	if route == "" {
		return "", "route is required"
	}
	return route, ""
}

func normalizeContentType(raw string) (string, string) {
	contentType := strings.TrimSpace(raw)
	if contentType == "" {
		return "", "content_type is required"
	}
	return contentType, ""
}

func resolveClientTarget(raw string) (string, peer.ID, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", "", fmt.Errorf("target is required")
	}
	if strings.HasSuffix(strings.ToLower(target), ".bsv") {
		return "", "", fmt.Errorf("shortname resolver not implemented yet")
	}
	pubKeyHex, err := normalizeCompressedPubKeyHex(target)
	if err != nil {
		return "", "", fmt.Errorf("target invalid: %w", err)
	}
	pid, err := poolcore.PeerIDFromClientID(pubKeyHex)
	if err != nil {
		return "", "", err
	}
	return pubKeyHex, pid, nil
}
