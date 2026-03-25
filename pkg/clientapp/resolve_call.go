package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ProtoClientCall    protocol.ID = "/bsv-transfer/client/call/1.0.0"
	ProtoClientResolve protocol.ID = "/bsv-transfer/client/resolve/1.0.0"

	defaultClientResolveRoute = "index"
	routeInboxMessage         = "inbox.message"
)

type callReq struct {
	To          string `protobuf:"bytes,1,opt,name=to,proto3" json:"to"`
	Route       string `protobuf:"bytes,2,opt,name=route,proto3" json:"route"`
	ContentType string `protobuf:"bytes,3,opt,name=content_type,json=contentType,proto3" json:"content_type"`
	Body        []byte `protobuf:"bytes,4,opt,name=body,proto3" json:"body,omitempty"`
}

type callResp struct {
	Ok          bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok"`
	Code        string `protobuf:"bytes,2,opt,name=code,proto3" json:"code,omitempty"`
	Message     string `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	ContentType string `protobuf:"bytes,4,opt,name=content_type,json=contentType,proto3" json:"content_type,omitempty"`
	Body        []byte `protobuf:"bytes,5,opt,name=body,proto3" json:"body,omitempty"`
}

type resolveReq struct {
	To    string `protobuf:"bytes,1,opt,name=to,proto3" json:"to"`
	Route string `protobuf:"bytes,2,opt,name=route,proto3" json:"route,omitempty"`
}

type resolveResp struct {
	Ok          bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok"`
	Code        string `protobuf:"bytes,2,opt,name=code,proto3" json:"code,omitempty"`
	Message     string `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	ContentType string `protobuf:"bytes,4,opt,name=content_type,json=contentType,proto3" json:"content_type,omitempty"`
	Body        []byte `protobuf:"bytes,5,opt,name=body,proto3" json:"body,omitempty"`
}

type TriggerClientCallParams struct {
	To          string
	Route       string
	ContentType string
	Body        []byte
}

type TriggerClientResolveParams struct {
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

// registerResolveCallHandlers 把节点级 resolve/call 能力挂到现有 client p2prpc 上。
// 设计说明：
// - v1 只做“裸公钥可直连”的最小闭环，短名解析明确留空，避免伪解析；
// - resolve 只返回资源声明（hash/元信息），不返回文件内容；
// - call 先内建 inbox.message，其它 route 以后继续加，不先发明注册框架。
func registerResolveCallHandlers(rt *Runtime) {
	if rt == nil || rt.Host == nil || rt.DB == nil {
		return
	}
	h := rt.Host
	db := rt.DB
	trace := rt.rpcTrace
	p2prpc.HandleProto[callReq, callResp](h, ProtoClientCall, clientSec(trace), func(ctx context.Context, req callReq) (callResp, error) {
		route, bad := normalizeCallRoute(req.Route)
		if bad != "" {
			return callResp{Ok: false, Code: "BAD_REQUEST", Message: bad}, nil
		}
		contentType, bad := normalizeContentType(req.ContentType)
		if bad != "" {
			return callResp{Ok: false, Code: "BAD_REQUEST", Message: bad}, nil
		}
		senderPubKeyHex, ok := p2prpc.SenderPubkeyHexFromContext(ctx)
		if !ok {
			return callResp{Ok: false, Code: "UNAUTHORIZED", Message: "sender identity missing"}, nil
		}
		messageID, ok := p2prpc.MessageIDFromContext(ctx)
		if !ok {
			return callResp{Ok: false, Code: "BAD_REQUEST", Message: "message id missing"}, nil
		}
		switch route {
		case routeInboxMessage:
			return storeInboxMessage(db, messageID, senderPubKeyHex, strings.TrimSpace(req.To), route, contentType, req.Body)
		default:
			return callResp{Ok: false, Code: "ROUTE_NOT_FOUND", Message: "route not found"}, nil
		}
	})
	p2prpc.HandleProto[resolveReq, resolveResp](h, ProtoClientResolve, clientSec(trace), func(_ context.Context, req resolveReq) (resolveResp, error) {
		route := normalizeResolveRoute(req.Route)
		body, err := buildRouteIndexManifest(db, route)
		if err != nil {
			if err == sql.ErrNoRows {
				return resolveResp{Ok: false, Code: "NOT_FOUND", Message: "route not found"}, nil
			}
			return resolveResp{}, err
		}
		return resolveResp{
			Ok:          true,
			Code:        "OK",
			ContentType: "application/json",
			Body:        body,
		}, nil
	})
}

func TriggerClientCall(ctx context.Context, rt *Runtime, p TriggerClientCallParams) (callResp, error) {
	var out callResp
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
	err = p2prpc.CallProto(ctx, rt.Host, peerID, ProtoClientCall, clientSec(rt.rpcTrace), callReq{
		To:          to,
		Route:       strings.TrimSpace(p.Route),
		ContentType: strings.TrimSpace(p.ContentType),
		Body:        append([]byte(nil), p.Body...),
	}, &out)
	return out, err
}

func TriggerClientResolve(ctx context.Context, rt *Runtime, p TriggerClientResolveParams) (resolveResp, error) {
	var out resolveResp
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
	err = p2prpc.CallProto(ctx, rt.Host, peerID, ProtoClientResolve, clientSec(rt.rpcTrace), resolveReq{
		To:    to,
		Route: normalizeResolveRoute(p.Route),
	}, &out)
	return out, err
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
		return defaultClientResolveRoute
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

func storeInboxMessage(db *sql.DB, messageID, senderPubKeyHex, targetInput, route, contentType string, body []byte) (callResp, error) {
	if db == nil {
		return callResp{}, fmt.Errorf("db is nil")
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
			return callResp{}, scanErr
		}
	default:
		return callResp{}, err
	}
	ack, err := json.Marshal(inboxReceipt{InboxMessageID: inboxID, ReceivedAtUnix: now})
	if err != nil {
		return callResp{}, err
	}
	return callResp{
		Ok:          true,
		Code:        "OK",
		ContentType: "application/json",
		Body:        ack,
	}, nil
}

func buildRouteIndexManifest(db *sql.DB, route string) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	var item routeIndexManifest
	row := db.QueryRow(
		`SELECT pri.route,pri.seed_hash,pri.updated_at_unix,COALESCE(s.recommended_file_name,''),COALESCE(s.mime_hint,''),COALESCE(s.file_size,0)
		   FROM published_route_indexes pri
		   LEFT JOIN seeds s ON s.seed_hash=pri.seed_hash
		  WHERE pri.route=?`,
		strings.TrimSpace(route),
	)
	if err := row.Scan(&item.Route, &item.SeedHash, &item.UpdatedAtUnix, &item.RecommendedFileName, &item.MIMEHint, &item.FileSize); err != nil {
		return nil, err
	}
	return json.Marshal(item)
}

func upsertPublishedRouteIndex(db *sql.DB, route, seedHash string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	route = strings.TrimSpace(route)
	if route == "" {
		return 0, fmt.Errorf("route is required")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return 0, fmt.Errorf("seed_hash is required")
	}
	var exists int
	if err := db.QueryRow(`SELECT 1 FROM seeds WHERE seed_hash=? LIMIT 1`, seedHash).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("seed not found")
		}
		return 0, err
	}
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO published_route_indexes(route,seed_hash,updated_at_unix) VALUES(?,?,?)
		 ON CONFLICT(route) DO UPDATE SET seed_hash=excluded.seed_hash,updated_at_unix=excluded.updated_at_unix`,
		route,
		seedHash,
		now,
	); err != nil {
		return 0, err
	}
	return now, nil
}
