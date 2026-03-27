package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	To           string
	Route        string
	ContentType  string
	Body         []byte
	PaymentMode  string
	PaymentQuote []byte
}

type TriggerPeerResolveParams struct {
	To    string
	Route string
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
func registerNodeRouteHandlers(rt *Runtime) {
	if rt == nil || rt.Host == nil || rt.DB == nil {
		return
	}
	db := rt.DB
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
		case ncall.RouteNodeV1CapabilitiesShow:
			body := clientCapabilitiesShowBody(rt)
			return marshalNodeCallProto(&body)
		case routeInboxMessage:
			return storeInboxMessage(db, meta.MessageID, meta.SenderPubkeyHex, strings.TrimSpace(req.To), route, contentType, req.Body)
		default:
			return ncall.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
	}, func(_ context.Context, req ncall.ResolveReq) (ncall.ResolveResp, error) {
		route := normalizeResolveRoute(req.Route)
		body, err := buildRouteIndexManifest(db, route)
		if err != nil {
			if err == sql.ErrNoRows {
				return ncall.ResolveResp{Ok: false, Code: "NOT_FOUND", Message: "route not found"}, nil
			}
			return ncall.ResolveResp{}, err
		}
		return ncall.ResolveResp{
			Ok:          true,
			Code:        "OK",
			ContentType: ncall.ContentTypeProto,
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
	if err := ensureTargetPeerReachable(ctx, rt, to, peerID); err != nil {
		return out, err
	}
	req := ncall.CallReq{
		To:          to,
		Route:       strings.TrimSpace(p.Route),
		ContentType: strings.TrimSpace(p.ContentType),
		Body:        append([]byte(nil), p.Body...),
	}
	paymentMode := normalizePeerCallPaymentMode(p.PaymentMode)
	if paymentMode == "pay" && len(p.PaymentQuote) > 0 {
		return payPeerCallWithAcceptedQuote(ctx, rt, peerID, req, p.PaymentQuote)
	}
	out, err = callNodeRoute(ctx, rt, peerID, req)
	if err != nil {
		return out, err
	}
	if !isNodePaymentRequired(out) {
		return out, nil
	}
	if paymentMode == "quote" {
		return quotePeerCallFromPaymentRequired(ctx, rt, peerID, req, out.PaymentOptions)
	}
	paidOut, payErr := retryPeerCallWithAutoPayment(ctx, rt, peerID, req, out.PaymentOptions)
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
	if err := ensureTargetPeerReachable(ctx, rt, to, peerID); err != nil {
		return out, err
	}
	err = pproto.CallProto(ctx, rt.Host, peerID, ncall.ProtoNodeResolve, nodeSecForRuntime(rt), ncall.ResolveReq{
		To:    to,
		Route: normalizeResolveRoute(p.Route),
	}, &out)
	return out, err
}

func callNodeRoute(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq) (ncall.CallResp, error) {
	var out ncall.CallResp
	if err := pproto.CallProto(ctx, rt.Host, peerID, ncall.ProtoNodeCall, nodeSecForRuntime(rt), req, &out); err != nil {
		return ncall.CallResp{}, err
	}
	return out, nil
}

func clientCapabilitiesShowBody(rt *Runtime) ncall.CapabilitiesShowBody {
	nodePubkeyHex := ""
	if rt != nil && rt.runIn.ClientID != "" {
		nodePubkeyHex = strings.ToLower(strings.TrimSpace(rt.runIn.ClientID))
	}
	return ncall.CapabilitiesShowBody{
		NodePubkeyHex: nodePubkeyHex,
		Capabilities: []*ncall.CapabilityItem{
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
		ContentType: ncall.ContentTypeProto,
		Body:        body,
	}, nil
}

func isNodePaymentRequired(resp ncall.CallResp) bool {
	return !resp.Ok && strings.EqualFold(strings.TrimSpace(resp.Code), "PAYMENT_REQUIRED") && len(resp.PaymentOptions) > 0
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

func storeInboxMessage(db *sql.DB, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte) (ncall.CallResp, error) {
	if db == nil {
		return ncall.CallResp{}, fmt.Errorf("db is nil")
	}
	now := time.Now().Unix()
	result, err := db.Exec(
		`INSERT INTO inbox_messages(message_id,sender_pubkey_hex,target_input,route,content_type,body_bytes,body_size_bytes,received_at_unix)
		 VALUES(?,?,?,?,?,?,?,?)`,
		strings.TrimSpace(messageID),
		strings.TrimSpace(senderPubKeyHex),
		strings.TrimSpace(targetInput),
		strings.TrimSpace(route),
		strings.TrimSpace(contentType),
		append([]byte(nil), body...),
		len(body),
		now,
	)
	var inboxID int64
	switch {
	case err == nil:
		inboxID, _ = result.LastInsertId()
	case strings.Contains(strings.ToLower(err.Error()), "unique constraint failed"):
		row := db.QueryRow(
			`SELECT id,received_at_unix FROM inbox_messages WHERE sender_pubkey_hex=? AND message_id=?`,
			strings.TrimSpace(senderPubKeyHex),
			strings.TrimSpace(messageID),
		)
		if scanErr := row.Scan(&inboxID, &now); scanErr != nil {
			return ncall.CallResp{}, scanErr
		}
	default:
		return ncall.CallResp{}, err
	}
	ack, err := oldproto.Marshal(&inboxReceipt{InboxMessageID: inboxID, ReceivedAtUnix: now})
	if err != nil {
		return ncall.CallResp{}, err
	}
	return ncall.CallResp{
		Ok:          true,
		Code:        "OK",
		ContentType: ncall.ContentTypeProto,
		Body:        ack,
	}, nil
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
