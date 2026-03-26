package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/nodesvc"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	defaultNodeResolveRoute = "index"
	routeInboxMessage       = "inbox.message"
)

type TriggerPeerCallParams struct {
	To          string
	Route       string
	ContentType string
	Body        []byte
}

type TriggerPeerResolveParams struct {
	To    string
	Route string
}

type inboxReceipt struct {
	InboxMessageID int64 `json:"inbox_message_id"`
	ReceivedAtUnix int64 `json:"received_at_unix"`
}

type routeIndexManifest struct {
	Route               string `json:"route"`
	SeedHash            string `json:"seed_hash"`
	RecommendedFileName string `json:"recommended_file_name,omitempty"`
	MIMEHint            string `json:"mime_hint,omitempty"`
	FileSize            int64  `json:"file_size,omitempty"`
	UpdatedAtUnix       int64  `json:"updated_at_unix"`
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
	nodesvc.Register(rt.Host, nodeSecForRuntime(rt), func(ctx context.Context, meta nodesvc.CallContext, req nodesvc.CallReq) (nodesvc.CallResp, error) {
		route, bad := normalizeCallRoute(req.Route)
		if bad != "" {
			return nodesvc.CallResp{Ok: false, Code: "BAD_REQUEST", Message: bad}, nil
		}
		contentType, bad := normalizeContentType(req.ContentType)
		if bad != "" {
			return nodesvc.CallResp{Ok: false, Code: "BAD_REQUEST", Message: bad}, nil
		}
		switch route {
		case nodesvc.RouteNodeV1CapabilitiesShow:
			return marshalNodeCallJSON(clientCapabilitiesShowBody(rt))
		case routeInboxMessage:
			return storeInboxMessage(db, meta.MessageID, meta.SenderPubkeyHex, strings.TrimSpace(req.To), route, contentType, req.Body)
		default:
			return nodesvc.CallResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
	}, func(_ context.Context, req nodesvc.ResolveReq) (nodesvc.ResolveResp, error) {
		route := normalizeResolveRoute(req.Route)
		body, err := buildRouteIndexManifest(db, route)
		if err != nil {
			if err == sql.ErrNoRows {
				return nodesvc.ResolveResp{Ok: false, Code: "NOT_FOUND", Message: "route not found"}, nil
			}
			return nodesvc.ResolveResp{}, err
		}
		return nodesvc.ResolveResp{
			Ok:          true,
			Code:        "OK",
			ContentType: "application/json",
			Body:        body,
		}, nil
	})
}

func TriggerPeerCall(ctx context.Context, rt *Runtime, p TriggerPeerCallParams) (nodesvc.CallResp, error) {
	var out nodesvc.CallResp
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
	req := nodesvc.CallReq{
		To:          to,
		Route:       strings.TrimSpace(p.Route),
		ContentType: strings.TrimSpace(p.ContentType),
		Body:        append([]byte(nil), p.Body...),
	}
	out, err = callNodeRoute(ctx, rt, peerID, req)
	if err != nil {
		return out, err
	}
	if !isNodePaymentRequired(out) {
		return out, nil
	}
	paidOut, payErr := retryPeerCallWithAutoPayment(ctx, rt, peerID, req, out.PaymentOptions)
	if payErr != nil {
		return nodesvc.CallResp{}, payErr
	}
	return paidOut, nil
}

func TriggerPeerResolve(ctx context.Context, rt *Runtime, p TriggerPeerResolveParams) (nodesvc.ResolveResp, error) {
	var out nodesvc.ResolveResp
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
	err = p2prpc.CallProto(ctx, rt.Host, peerID, nodesvc.ProtoNodeResolve, nodeSecForRuntime(rt), nodesvc.ResolveReq{
		To:    to,
		Route: normalizeResolveRoute(p.Route),
	}, &out)
	return out, err
}

func callNodeRoute(ctx context.Context, rt *Runtime, peerID peer.ID, req nodesvc.CallReq) (nodesvc.CallResp, error) {
	var out nodesvc.CallResp
	if err := p2prpc.CallProto(ctx, rt.Host, peerID, nodesvc.ProtoNodeCall, nodeSecForRuntime(rt), req, &out); err != nil {
		return nodesvc.CallResp{}, err
	}
	return out, nil
}

func clientCapabilitiesShowBody(rt *Runtime) nodesvc.CapabilitiesShowBody {
	nodePubkeyHex := ""
	if rt != nil && rt.runIn.ClientID != "" {
		nodePubkeyHex = strings.ToLower(strings.TrimSpace(rt.runIn.ClientID))
	}
	return nodesvc.CapabilitiesShowBody{
		NodePubkeyHex: nodePubkeyHex,
		Capabilities: []nodesvc.CapabilityItem{
			{
				ID:             "wallet",
				Version:        1,
				PaymentSchemes: []string{nodesvc.PaymentSchemePool2of2V1},
			},
			{
				ID:      "bitfs",
				Version: 1,
			},
		},
	}
}

func marshalNodeCallJSON(v any) (nodesvc.CallResp, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	return nodesvc.CallResp{
		Ok:          true,
		Code:        "OK",
		ContentType: "application/json",
		Body:        body,
	}, nil
}

func isNodePaymentRequired(resp nodesvc.CallResp) bool {
	return !resp.Ok && strings.EqualFold(strings.TrimSpace(resp.Code), "PAYMENT_REQUIRED") && len(resp.PaymentOptions) > 0
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

func storeInboxMessage(db *sql.DB, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte) (nodesvc.CallResp, error) {
	if db == nil {
		return nodesvc.CallResp{}, fmt.Errorf("db is nil")
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
			return nodesvc.CallResp{}, scanErr
		}
	default:
		return nodesvc.CallResp{}, err
	}
	ack, err := json.Marshal(inboxReceipt{InboxMessageID: inboxID, ReceivedAtUnix: now})
	if err != nil {
		return nodesvc.CallResp{}, err
	}
	return nodesvc.CallResp{
		Ok:          true,
		Code:        "OK",
		ContentType: "application/json",
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
	pid, err := dual2of2.PeerIDFromClientID(pubKeyHex)
	if err != nil {
		return "", "", err
	}
	return pubKeyHex, pid, nil
}
