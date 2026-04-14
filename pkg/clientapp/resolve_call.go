package clientapp

import (
	"context"
	"database/sql"
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

const (
	defaultNodeResolveRoute = "index"
	routeInboxMessage       = "inbox.message"
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

type inboxReceipt struct {
	InboxMessageID int64 `protobuf:"varint,1,opt,name=inbox_message_id,json=inboxMessageId,proto3" json:"inbox_message_id"`
	ReceivedAtUnix int64 `protobuf:"varint,2,opt,name=received_at_unix,json=receivedAtUnix,proto3" json:"received_at_unix"`
}

type routeIndexManifest struct {
	Route               string `protobuf:"bytes,1,opt,name=route,proto3" json:"route"`
	SeedHash            string `protobuf:"bytes,2,opt,name=seed_hash,json=seedHash,proto3" json:"seed_hash"`
	RecommendedFileName string `protobuf:"bytes,3,opt,name=recommended_file_name,json=recommendedFileName,proto3" json:"recommended_file_name,omitempty"`
	MIMEHint            string `protobuf:"bytes,4,opt,name=mime_hint,json=mimeHint,proto3" json:"mime_hint,omitempty"`
	FileSize            int64  `protobuf:"varint,5,opt,name=file_size,json=fileSize,proto3" json:"file_size,omitempty"`
	UpdatedAtUnix       int64  `protobuf:"varint,6,opt,name=updated_at_unix,json=updatedAtUnix,proto3" json:"updated_at_unix"`
}

// registerNodeRouteHandlers 把节点级 route-call / route-resolve 能力挂到统一 node 协议上。
// 设计说明：
// - bitfs.peer.call 未来会承接多支付协议，因此这里先统一“所有节点共用一个外壳”；
// - client 自己目前只暴露 inbox.message、capabilities_show 与 route index resolve；
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
		case routeInboxMessage:
			return storeInboxMessage(store, meta.MessageID, meta.SenderPubkeyHex, strings.TrimSpace(req.To), route, contentType, req.Body)
		default:
			return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
	}, func(ctx context.Context, req ncall.ResolveReq) (ncall.ResolveResp, error) {
		route := normalizeResolveRoute(req.Route)
		body, err := buildRouteIndexManifest(ctx, store, route)
		if err != nil {
			if err == sql.ErrNoRows {
				return ncall.ResolveResp{Ok: false, Code: "NOT_FOUND", Message: "route not found"}, nil
			}
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
	if paymentMode == "quote" {
		return quotePeerCallFromPaymentQuoted(ctx, rt, p.Store, peerID, req, out.PaymentSchemes, p.RequireActiveFeePool)
	}
	paidOut, payErr := retryPeerCallWithAutoPayment(ctx, rt, p.Store, peerID, req, out.PaymentSchemes, p.RequireActiveFeePool)
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
	err = pproto.CallProto(ctx, rt.Host, peerID, contractprotoid.ProtoNodeResolve, nodeSecForRuntime(rt), contractmessage.ResolveReq{
		To:    to,
		Route: normalizeResolveRoute(p.Route),
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

func clientCapabilitiesShowBody(rt *Runtime) contractmessage.CapabilitiesShowBody {
	nodePubkeyHex := ""
	if rt != nil && rt.runIn.ClientID != "" {
		nodePubkeyHex = strings.ToLower(strings.TrimSpace(rt.runIn.ClientID))
	}
	return contractmessage.CapabilitiesShowBody{
		NodePubkeyHex: nodePubkeyHex,
		Capabilities: []*contractmessage.CapabilityItem{
			{
				ID:      "wallet",
				Version: 1,
			},
			{
				ID:      "bitfs",
				Version: 1,
			},
		},
	}
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

func normalizeCallRoute(raw string) (string, string) {
	route := strings.TrimSpace(raw)
	if route == "" {
		return "", "route is required"
	}
	return route, ""
}

func normalizeResolveRoute(raw string) string {
	route := strings.TrimSpace(raw)
	if route == "" {
		return defaultNodeResolveRoute
	}
	return route
}

func normalizeContentType(raw string) (string, string) {
	contentType := strings.TrimSpace(raw)
	if contentType == "" {
		return "", "content_type is required"
	}
	return contentType, ""
}

func storeInboxMessage(store *clientDB, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte) (ncall.CallResp, error) {
	return dbStoreInboxMessage(context.Background(), store, messageID, senderPubKeyHex, targetInput, route, contentType, body)
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
