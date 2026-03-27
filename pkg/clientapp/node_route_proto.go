package clientapp

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/infra/ncall"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// marshalNodeRouteProtoBody 统一 node route 的 proto body 编码。
// 设计说明：
// - node.call 外层已经是 protobuf，业务 body 不再退回 JSON；
// - 空消息体允许直接传 nil，避免为“空请求”再造一层无意义结构。
func marshalNodeRouteProtoBody(msg oldproto.Message) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	return oldproto.Marshal(msg)
}

func unmarshalNodeRouteProtoBody(route string, raw []byte, out oldproto.Message, required bool) error {
	if out == nil {
		return fmt.Errorf("proto body target missing")
	}
	if len(raw) == 0 {
		if required {
			return fmt.Errorf("request body missing")
		}
		return nil
	}
	if err := oldproto.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode node route body failed: route=%s: %w", strings.TrimSpace(route), err)
	}
	return nil
}

func callNodeRouteProto(ctx context.Context, rt *Runtime, peerID peer.ID, route string, body oldproto.Message, out oldproto.Message) error {
	if rt == nil || rt.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	rawBody, err := marshalNodeRouteProtoBody(body)
	if err != nil {
		return err
	}
	resp, err := callNodeRoute(ctx, rt, peerID, nodesvc.CallReq{
		To:          peerID.String(),
		Route:       strings.TrimSpace(route),
		ContentType: nodesvc.ContentTypeProto,
		Body:        rawBody,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		code := strings.ToUpper(strings.TrimSpace(resp.Code))
		if code == "" {
			code = "ERROR"
		}
		msg := strings.TrimSpace(resp.Message)
		if msg == "" {
			msg = "node route failed"
		}
		return fmt.Errorf("node route failed: route=%s code=%s message=%s", strings.TrimSpace(route), code, msg)
	}
	return unmarshalNodeRouteProtoBody(route, resp.Body, out, false)
}
