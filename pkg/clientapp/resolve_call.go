package clientapp

import (
	"context"
	"fmt"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/pproto"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type TriggerPeerCallParams struct {
	To                   string
	ProtocolID           protocol.ID
	ContentType          string
	Body                 []byte
	Store                *clientDB
	PaymentMode          string
	PaymentScheme        string
	ServiceQuote         []byte
	RequireActiveFeePool bool
}

type peerCallPaymentDecision struct {
	Scheme string
	Option *ncall.PaymentOption
}

// TriggerPeerCall 按 protocol.ID 发起远端调用。
// 设计说明：
// - 硬切后不再使用 node.call + route 分发模型；
// - 调用方直接指定完整 protocol.ID，如 /bsv-transfer/index/resolve/1.0.0；
// - 每个能力模块独立注册自己的 protocol.ID handler。
// - 支付逻辑已迁移到 PaymentFacade，TriggerPeerCall 只负责业务调用和结果转换。
func TriggerPeerCall(ctx context.Context, rt *Runtime, p TriggerPeerCallParams) (ncall.CallResp, error) {
	var out ncall.CallResp
	if rt == nil || rt.Host == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	if p.ProtocolID == "" {
		return out, fmt.Errorf("protocol ID is required")
	}
	to, peerID, err := resolvePeerCallTarget(ctx, rt, strings.TrimSpace(p.To))
	if err != nil {
		return out, err
	}
	if err := ensureTargetPeerReachable(ctx, p.Store, rt, to, peerID); err != nil {
		return out, err
	}
	req := ncall.CallReq{
		To:          to,
		Route:       string(p.ProtocolID),
		ContentType: strings.TrimSpace(p.ContentType),
		Body:        append([]byte(nil), p.Body...),
	}
	paymentMode := normalizePeerCallPaymentMode(p.PaymentMode)
	if paymentMode == "pay" && len(p.ServiceQuote) > 0 {
		return payViaPaymentFacade(ctx, rt, peerID, req, p.ServiceQuote, p.PaymentScheme, p.ProtocolID)
	}
	out, err = callProto(ctx, rt, peerID, p.ProtocolID, req)
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
		return out, nil
	}
	paidOut, payErr := payViaPaymentFacade(ctx, rt, peerID, req, out.ServiceQuote, decision.Scheme, p.ProtocolID)
	if payErr != nil {
		return ncall.CallResp{}, payErr
	}
	return paidOut, nil
}

func payViaPaymentFacade(ctx context.Context, rt *Runtime, peerID peer.ID, req ncall.CallReq, rawQuote []byte, scheme string, protoID protocol.ID) (ncall.CallResp, error) {
	facade := ensurePaymentFacade(rt)
	if facade == nil {
		return ncall.CallResp{}, fmt.Errorf("payment facade not available")
	}
	targetPeer := peerID.String()
	scReq := moduleapi.ServiceCallRequest{
		Route:       string(protoID),
		Body:        req.Body,
		ContentType: req.ContentType,
	}
	scResp, receipt, err := facade.PayQuotedService(ctx, scheme, rawQuote, scReq, targetPeer)
	if err != nil {
		return ncall.CallResp{}, err
	}
	receiptScheme := ""
	if len(receipt) > 0 {
		receiptScheme = scheme
	}
	return ncall.CallResp{
		Ok:                   scResp.OK,
		Code:                 scResp.Code,
		Message:              scResp.Message,
		ContentType:          scResp.ContentType,
		Body:                 scResp.Body,
		PaymentReceiptScheme: receiptScheme,
		PaymentReceipt:       receipt,
	}, nil
}

func callProto(ctx context.Context, rt *Runtime, peerID peer.ID, protoID protocol.ID, req ncall.CallReq) (ncall.CallResp, error) {
	var out ncall.CallResp
	if err := pproto.CallProto(ctx, rt.Host, peerID, protoID, nodeSecForRuntime(rt), req, &out); err != nil {
		return ncall.CallResp{}, err
	}
	return out, nil
}

func callNodeRoute(ctx context.Context, rt *Runtime, peerID peer.ID, protoID protocol.ID, req ncall.CallReq) (ncall.CallResp, error) {
	var out ncall.CallResp
	if err := pproto.CallProto(ctx, rt.Host, peerID, protoID, nodeSecForRuntime(rt), req, &out); err != nil {
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

func resolvePeerCallTarget(ctx context.Context, rt *Runtime, raw string) (string, peer.ID, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", "", fmt.Errorf("target is required")
	}
	if pubkeyHex, peerID, err := resolveClientTarget(target); err == nil {
		return pubkeyHex, peerID, nil
	}
	pubkeyHex, err := ResolveDomainToPubkey(ctx, rt, target)
	if err != nil {
		return "", "", err
	}
	pid, err := poolcore.PeerIDFromClientID(pubkeyHex)
	if err != nil {
		return "", "", err
	}
	return pubkeyHex, pid, nil
}

func choosePeerCallPaymentOption(options []*ncall.PaymentOption, preferredScheme string) (*ncall.PaymentOption, bool) {
	preferredScheme, err := normalizePreferredPaymentScheme(preferredScheme)
	if err != nil {
		preferredScheme = defaultPreferredPaymentScheme
	}
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == preferredScheme {
			return item, true
		}
	}
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == ncall.PaymentSchemePool2of2V1 {
			return item, true
		}
	}
	for _, item := range options {
		if item == nil {
			continue
		}
		if strings.TrimSpace(item.Scheme) == ncall.PaymentSchemeChainTxV1 {
			return item, true
		}
	}
	return nil, false
}
