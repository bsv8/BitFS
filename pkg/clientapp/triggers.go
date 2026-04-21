package clientapp

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/obs"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
	te "github.com/bsv8/MultisigPool/pkg/triple_endpoint"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// 说明：
// - 这里的函数是明确的“测试触发入口”，e2e 只应通过这些入口推进流程。
// - 触发函数尽量保持同步语义：返回时表示该动作已完成或已明确失败。
// - 所有业务消息必须走 libp2p；HTTP 只允许调用这些触发入口，不应内嵌业务协议实现。

type WorkspaceSeed struct {
	SeedHash   string `json:"seed_hash"`
	ChunkCount uint32 `json:"chunk_count"`
	SeedPrice  uint64 `json:"seed_price"`
	ChunkPrice uint64 `json:"chunk_price"`
}

// TriggerBizOrderPayBSV 新的 BSV 业务下单入口。
// 设计说明：
// - 先写 biz_ 订单、事件和 pending settle 单；
// - 再交给 settle 层做真实链上支付；
// - e2e 只调用这个入口，不再碰钱包直发函数。
func TriggerBizOrderPayBSV(ctx context.Context, store *clientDB, rt *Runtime, req BizOrderPayBSVRequest) (BizOrderPayBSVResponse, error) {
	orderID := strings.ToLower(strings.TrimSpace(req.OrderID))
	toAddress := strings.TrimSpace(req.ToAddress)
	if orderID == "" {
		orderID = "order_biz_pay_bsv_" + fmt.Sprintf("%d_%s", time.Now().UnixNano(), randHex(4))
	}
	if err := validatePreviewAddress(toAddress); err != nil {
		return BizOrderPayBSVResponse{}, err
	}
	if req.AmountSatoshi == 0 {
		return BizOrderPayBSVResponse{}, fmt.Errorf("amount_satoshi must be greater than zero")
	}
	if store == nil {
		return BizOrderPayBSVResponse{}, fmt.Errorf("client db is nil")
	}
	if rt == nil || rt.ActionChain == nil {
		return BizOrderPayBSVResponse{}, fmt.Errorf("runtime not initialized")
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return BizOrderPayBSVResponse{}, err
	}
	actor := identity.Actor
	if actor == nil {
		return BizOrderPayBSVResponse{}, fmt.Errorf("runtime not initialized")
	}
	fromPubHex := strings.ToLower(strings.TrimSpace(actor.PubHex))
	fromAddress := strings.TrimSpace(actor.Addr)
	toAddress = strings.TrimSpace(toAddress)
	frontOrderID, businessID, settlementID, triggerID := bizOrderPayBSVIdentity(orderID)
	nowNote := fmt.Sprintf("BSV payment order %s", orderID)
	payload := map[string]any{
		"order_id":        orderID,
		"to_address":      toAddress,
		"amount_satoshi":  req.AmountSatoshi,
		"from_pubkey_hex": fromPubHex,
		"from_address":    fromAddress,
	}

	if _, err := dbGetBusinessSettlementByBusinessID(ctx, store, businessID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return BizOrderPayBSVResponse{}, err
		}
		if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
			FrontOrderID:      frontOrderID,
			FrontType:         "biz_order",
			FrontSubtype:      "pay_bsv",
			OwnerPubkeyHex:    fromPubHex,
			TargetObjectType:  "bsv_address",
			TargetObjectID:    toAddress,
			FrontOrderNote:    nowNote,
			FrontOrderPayload: payload,

			BusinessID:        businessID,
			BusinessRole:      "formal",
			SourceType:        "front_order",
			SourceID:          frontOrderID,
			AccountingScene:   "wallet_transfer",
			AccountingSubType: "pay_bsv",
			FromPartyID:       fromPubHex,
			ToPartyID:         toAddress,
			BusinessNote:      nowNote,
			BusinessPayload:   payload,

			TriggerID:      triggerID,
			TriggerType:    "front_order",
			TriggerIDValue: frontOrderID,
			TriggerRole:    "primary",
			TriggerNote:    "front order pay bsv",
			TriggerPayload: payload,

			SettlementID:         settlementID,
			SettlementMethod:     SettlementMethodChain,
			SettlementTargetType: "chain_quote_pay",
			SettlementTargetID:   "",
			SettlementPayload:    payload,
		}); err != nil {
			return BizOrderPayBSVResponse{}, err
		}
	}

	resp, execErr := executeBizOrderPayBSVSettlement(ctx, store, rt, frontOrderID, businessID, settlementID, toAddress, req.AmountSatoshi, fromPubHex, toAddress)
	if execErr != nil {
		return resp, execErr
	}
	return resp, nil
}

type PublishDemandParams struct {
	SeedHash      string `json:"seed_hash"`
	ChunkCount    uint32 `json:"chunk_count"`
	GatewayPeerID string `json:"gateway_pubkey_hex,omitempty"`
}

type PublishDemandBatchItem struct {
	SeedHash   string `json:"seed_hash"`
	ChunkCount uint32 `json:"chunk_count"`
}

type PublishDemandBatchParams struct {
	Items         []PublishDemandBatchItem `json:"items,omitempty"`
	GatewayPeerID string                   `json:"gateway_pubkey_hex,omitempty"`
}

type PublishLiveDemandParams struct {
	StreamID         string `json:"stream_id"`
	HaveSegmentIndex int64  `json:"have_segment_index"`
	Window           uint32 `json:"window"`
	GatewayPeerID    string `json:"gateway_pubkey_hex,omitempty"`
}

type LivePlanParams struct {
	StreamID         string `json:"stream_id"`
	HaveSegmentIndex int64  `json:"have_segment_index"`
}

type LivePlanResult struct {
	StreamID         string               `json:"stream_id"`
	HaveSegmentIndex int64                `json:"have_segment_index"`
	Decision         LivePurchaseDecision `json:"decision"`
}

// TriggerLivePlan 触发直播价速策略命令，统一走客户端业务内核。
func TriggerLivePlan(ctx context.Context, rt *Runtime, p LivePlanParams) (LivePlanResult, error) {
	if rt == nil || rt.Host == nil {
		return LivePlanResult{}, fmt.Errorf("runtime not initialized")
	}
	kernel := rt.kernel
	if kernel == nil {
		return LivePlanResult{}, fmt.Errorf("client kernel not initialized")
	}
	streamID := strings.ToLower(strings.TrimSpace(p.StreamID))
	res := kernel.dispatch(ctx, prepareClientKernelCommand(clientKernelCommand{
		CommandType: clientKernelCommandLivePlanPurchase,
		RequestedBy: "trigger_live_plan",
		Payload: map[string]any{
			"stream_id":          streamID,
			"have_segment_index": p.HaveSegmentIndex,
		},
	}))
	if !res.Accepted || strings.TrimSpace(res.Status) != "applied" {
		msg := strings.TrimSpace(res.ErrorMessage)
		if msg == "" {
			msg = strings.TrimSpace(res.ErrorCode)
		}
		if msg == "" {
			msg = "live plan failed"
		}
		return LivePlanResult{}, fmt.Errorf("%s", msg)
	}
	rawDecision, ok := res.Data["decision"]
	if !ok {
		return LivePlanResult{}, fmt.Errorf("live plan decision missing")
	}
	dec, ok := rawDecision.(LivePurchaseDecision)
	if !ok {
		return LivePlanResult{}, fmt.Errorf("live plan decision invalid")
	}
	return LivePlanResult{
		StreamID:         streamID,
		HaveSegmentIndex: p.HaveSegmentIndex,
		Decision:         dec,
	}, nil
}

func TriggerGatewayPublishDemand(ctx context.Context, store *clientDB, rt *Runtime, p PublishDemandParams) (contractmessage.DemandPublishPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return contractmessage.DemandPublishPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" || p.ChunkCount == 0 {
		return contractmessage.DemandPublishPaidResp{}, fmt.Errorf("invalid params")
	}

	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return contractmessage.DemandPublishPaidResp{}, err
	}
	if len(rt.HealthyGWs) == 0 {
		return contractmessage.DemandPublishPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	buyerAddrs := localAdvertiseAddrs(rt)
	body := &contractmessage.DemandPublishReq{
		SeedHash:   seedHash,
		ChunkCount: p.ChunkCount,
		BuyerAddrs: buyerAddrs,
	}
	obs.Business(ServiceName, "evt_trigger_gateway_demand_publish_begin", map[string]any{"seed_hash": seedHash})
	resp, _, err := triggerTypedPeerCall(ctx, store, rt, gatewayBusinessID(rt, gw.ID), ncall.ProtoBroadcastDemandPublish, body, decodeDemandPublishRouteResp)
	if err != nil {
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_failed", map[string]any{"error": err.Error()})
		return contractmessage.DemandPublishPaidResp{}, err
	}
	if err := validateDemandPublishPaidResp(resp); err != nil {
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_failed", map[string]any{"error": err.Error()})
		return contractmessage.DemandPublishPaidResp{}, err
	}
	obs.Business(ServiceName, "evt_trigger_gateway_demand_publish_end", map[string]any{
		"demand_id": resp.DemandID,
		"status":    resp.Status,
		"success":   resp.Success,
		"error":     strings.TrimSpace(resp.Error),
	})
	if err := dbRecordDemand(ctx, store, resp.DemandID, seedHash); err != nil {
		return contractmessage.DemandPublishPaidResp{}, err
	}
	return resp, nil
}

func TriggerGatewayPublishDemandBatch(ctx context.Context, store *clientDB, rt *Runtime, p PublishDemandBatchParams) (contractmessage.DemandPublishBatchPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return contractmessage.DemandPublishBatchPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	items, err := normalizeDemandBatchItems(p.Items)
	if err != nil {
		return contractmessage.DemandPublishBatchPaidResp{}, err
	}

	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return contractmessage.DemandPublishBatchPaidResp{}, err
	}
	if len(rt.HealthyGWs) == 0 {
		return contractmessage.DemandPublishBatchPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	reqItems := make([]*contractmessage.DemandPublishBatchPaidItem, 0, len(items))
	for _, item := range items {
		reqItems = append(reqItems, &contractmessage.DemandPublishBatchPaidItem{
			SeedHash:   item.SeedHash,
			ChunkCount: item.ChunkCount,
		})
	}
	body := &contractmessage.DemandPublishBatchReq{
		Items:      reqItems,
		BuyerAddrs: localAdvertiseAddrs(rt),
	}
	obs.Business(ServiceName, "evt_trigger_gateway_demand_publish_batch_begin", map[string]any{"item_count": len(items)})
	resp, _, err := triggerTypedPeerCall(ctx, store, rt, gatewayBusinessID(rt, gw.ID), ncall.ProtoBroadcastDemandPublishBatch, body, decodeDemandPublishBatchRouteResp)
	if err != nil {
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_batch_failed", map[string]any{"error": err.Error()})
		return contractmessage.DemandPublishBatchPaidResp{}, err
	}
	if err := validateDemandPublishBatchPaidResp(resp); err != nil {
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_batch_failed", map[string]any{"error": err.Error()})
		return contractmessage.DemandPublishBatchPaidResp{}, err
	}
	obs.Business(ServiceName, "evt_trigger_gateway_demand_publish_batch_end", map[string]any{
		"published_count": resp.PublishedCount,
		"status":          resp.Status,
		"success":         resp.Success,
		"error":           strings.TrimSpace(resp.Error),
	})
	for _, item := range resp.Items {
		if item == nil {
			continue
		}
		if err := dbRecordDemand(ctx, store, item.DemandID, item.SeedHash); err != nil {
			return contractmessage.DemandPublishBatchPaidResp{}, err
		}
	}
	return resp, nil
}

func TriggerGatewayPublishLiveDemand(ctx context.Context, store *clientDB, rt *Runtime, p PublishLiveDemandParams) (contractmessage.LiveDemandPublishPaidResp, error) {
	if rt == nil || rt.Host == nil {
		return contractmessage.LiveDemandPublishPaidResp{}, fmt.Errorf("runtime not initialized")
	}
	streamID := strings.ToLower(strings.TrimSpace(p.StreamID))
	if !isSeedHashHex(streamID) || p.Window == 0 {
		return contractmessage.LiveDemandPublishPaidResp{}, fmt.Errorf("invalid params")
	}
	gw, err := pickGatewayForBusiness(rt, p.GatewayPeerID)
	if err != nil {
		return contractmessage.LiveDemandPublishPaidResp{}, err
	}
	if len(rt.HealthyGWs) == 0 {
		return contractmessage.LiveDemandPublishPaidResp{}, fmt.Errorf("no healthy gateway")
	}
	body := &contractmessage.LiveDemandPublishReq{
		StreamID:         streamID,
		HaveSegmentIndex: p.HaveSegmentIndex,
		Window:           p.Window,
		BuyerAddrs:       localAdvertiseAddrs(rt),
	}
	resp, _, err := triggerTypedPeerCall(ctx, store, rt, gatewayBusinessID(rt, gw.ID), ncall.ProtoBroadcastLiveDemandPublish, body, decodeLiveDemandPublishRouteResp)
	if err != nil {
		return contractmessage.LiveDemandPublishPaidResp{}, err
	}
	if err := validateLiveDemandPublishPaidResp(resp); err != nil {
		return contractmessage.LiveDemandPublishPaidResp{}, err
	}
	return resp, nil
}

// applyFeePoolChargeToSession 在扣费成功后推进本地费用池会话。
// 设计约束：以“池总额 - 服务器额 - fee”回算 client_amount，避免累计误差。
func applyFeePoolChargeToSession(session *feePoolSession, nextSeq uint32, nextServerAmount uint64, updatedTxHex string) {
	if session == nil {
		return
	}
	session.Sequence = nextSeq
	session.ServerAmount = nextServerAmount
	session.CurrentTxHex = strings.TrimSpace(updatedTxHex)
	if session.PoolAmountSat >= session.ServerAmount+session.SpendTxFeeSat {
		session.ClientAmount = session.PoolAmountSat - session.ServerAmount - session.SpendTxFeeSat
		return
	}
	session.ClientAmount = 0
}

// validateDemandPublishPaidResp 统一校验网关 publish_demand 业务响应。
// 设计约束：
// - RPC 成功 != 业务成功，必须检查 success 位；
// - success=true 时 demand_id 必须存在，否则后续 list quotes 必然失败。
func validateDemandPublishPaidResp(resp contractmessage.DemandPublishPaidResp) error {
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway publish demand failed"
		}
		return fmt.Errorf("gateway demand publish rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if strings.TrimSpace(resp.DemandID) == "" {
		return fmt.Errorf("gateway demand publish returned empty demand_id")
	}
	return nil
}

func validateDemandPublishBatchPaidResp(resp contractmessage.DemandPublishBatchPaidResp) error {
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway publish demand batch failed"
		}
		return fmt.Errorf("gateway demand batch publish rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if len(resp.Items) == 0 {
		return fmt.Errorf("gateway demand batch publish returned empty items")
	}
	for _, item := range resp.Items {
		if item == nil {
			return fmt.Errorf("gateway demand batch publish returned nil item")
		}
		if strings.TrimSpace(item.DemandID) == "" {
			return fmt.Errorf("gateway demand batch publish returned empty demand_id")
		}
	}
	return nil
}

// validateLiveDemandPublishPaidResp 统一校验网关 publish_live_demand 业务响应。
func validateLiveDemandPublishPaidResp(resp contractmessage.LiveDemandPublishPaidResp) error {
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "gateway publish live demand failed"
		}
		return fmt.Errorf("gateway live demand publish rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	if strings.TrimSpace(resp.DemandID) == "" {
		return fmt.Errorf("gateway live demand publish returned empty demand_id")
	}
	return nil
}

func normalizeDemandBatchItems(items []PublishDemandBatchItem) ([]PublishDemandBatchItem, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("invalid params")
	}
	out := make([]PublishDemandBatchItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		seedHash := strings.ToLower(strings.TrimSpace(item.SeedHash))
		if seedHash == "" || item.ChunkCount == 0 {
			return nil, fmt.Errorf("invalid params")
		}
		if _, ok := seen[seedHash]; ok {
			continue
		}
		seen[seedHash] = struct{}{}
		out = append(out, PublishDemandBatchItem{
			SeedHash:   seedHash,
			ChunkCount: item.ChunkCount,
		})
	}
	return out, nil
}

func pickGatewayForBusiness(rt *Runtime, gatewayPeerID string) (peer.AddrInfo, error) {
	if rt == nil {
		return peer.AddrInfo{}, fmt.Errorf("runtime not initialized")
	}
	override := strings.TrimSpace(gatewayPeerID)
	if override == "" {
		return peer.AddrInfo{}, fmt.Errorf("gateway_pubkey_hex is required")
	}
	if len(rt.HealthyGWs) == 0 {
		return peer.AddrInfo{}, fmt.Errorf("no healthy gateway")
	}
	for _, gw := range rt.HealthyGWs {
		if strings.EqualFold(gatewayBusinessID(rt, gw.ID), override) {
			return gw, nil
		}
	}
	return peer.AddrInfo{}, fmt.Errorf("gateway_pubkey_hex not connected: %s", override)
}

// gatewayBusinessID 返回业务层统一网关 ID（优先使用配置中的网关公钥）。
// 说明：内部连接仍以 libp2p peer.ID 路由；对外观测/触发参数统一为公钥 ID。
func gatewayBusinessID(rt *Runtime, pid peer.ID) string {
	peerID := strings.TrimSpace(pid.String())
	if peerID == "" || rt == nil {
		return peerID
	}
	for _, g := range rt.ConfigSnapshot().Network.Gateways {
		ai, err := parseAddr(g.Addr)
		if err != nil || ai == nil || ai.ID != pid {
			continue
		}
		pubkey := strings.ToLower(strings.TrimSpace(g.Pubkey))
		if pubkey != "" {
			return pubkey
		}
		break
	}
	return peerID
}

type DirectQuoteParams struct {
	DemandID                string   `json:"demand_id"`
	BuyerPubHex             string   `json:"buyer_pubkey_hex"`
	BuyerAddrs              []string `json:"buyer_addrs"`
	SeedPrice               uint64   `json:"seed_price"`
	ChunkPrice              uint64   `json:"chunk_price"`
	ChunkCount              uint32   `json:"chunk_count"`
	FileSizeBytes           uint64   `json:"file_size"`
	ExpiresAtUnix           int64    `json:"expires_at_unix"`
	RecommendedFileName     string   `json:"recommended_file_name,omitempty"`
	MimeType                string   `json:"mime_hint,omitempty"`
	ArbiterPubHexes         []string `json:"arbiter_pubkey_hexes,omitempty"`
	AvailableChunkBitmapHex string   `json:"available_chunk_bitmap_hex,omitempty"`
}

func TriggerClientSubmitDirectQuote(ctx context.Context, seller *Runtime, p DirectQuoteParams) error {
	if seller == nil || seller.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	return submitDirectQuote(ctx, seller.Host, seller.rpcTrace, p)
}

type DirectQuoteItem struct {
	DemandID                string   `json:"demand_id"`
	SellerPubHex            string   `json:"seller_pubkey_hex"`
	SeedPrice               uint64   `json:"seed_price"`
	ChunkPrice              uint64   `json:"chunk_price"`
	ChunkCount              uint32   `json:"chunk_count"`
	FileSizeBytes           uint64   `json:"file_size"`
	ExpiresAtUnix           int64    `json:"expires_at_unix"`
	RecommendedFileName     string   `json:"recommended_file_name,omitempty"`
	MimeType                string   `json:"mime_hint,omitempty"`
	SellerArbiterPubHexes   []string `json:"seller_arbiter_pubkey_hexes,omitempty"`
	AvailableChunkBitmapHex string   `json:"available_chunk_bitmap_hex,omitempty"`
	AvailableChunkIndexes   []uint32 `json:"available_chunk_indexes,omitempty"`
}

type LiveQuoteSegment struct {
	SegmentIndex uint64 `json:"segment_index"`
	SeedHash     string `json:"seed_hash"`
}

type LiveQuoteParams struct {
	DemandID           string             `json:"demand_id"`
	BuyerPeerID        string             `json:"buyer_pubkey_hex"`
	BuyerAddrs         []string           `json:"buyer_addrs"`
	StreamID           string             `json:"stream_id"`
	LatestSegmentIndex uint64             `json:"latest_segment_index"`
	RecentSegments     []LiveQuoteSegment `json:"recent_segments"`
	ExpiresAtUnix      int64              `json:"expires_at_unix"`
}

type LiveQuoteItem struct {
	DemandID           string             `json:"demand_id"`
	SellerPubHex       string             `json:"seller_pubkey_hex"`
	StreamID           string             `json:"stream_id"`
	LatestSegmentIndex uint64             `json:"latest_segment_index"`
	RecentSegments     []LiveQuoteSegment `json:"recent_segments"`
	ExpiresAtUnix      int64              `json:"expires_at_unix"`
}

func TriggerClientListDirectQuotes(ctx context.Context, store *clientDB, demandID string) ([]DirectQuoteItem, error) {
	page, err := dbListDemandQuotes(ctx, store, demandQuoteFilter{
		Limit:    1000,
		Offset:   0,
		DemandID: strings.TrimSpace(demandID),
	})
	if err != nil {
		return nil, err
	}
	out := make([]DirectQuoteItem, 0, 4)
	now := time.Now().Unix()
	for _, raw := range page.Items {
		it := DirectQuoteItem{
			DemandID:                raw.DemandID,
			SellerPubHex:            raw.SellerPubHex,
			SeedPrice:               raw.SeedPriceSatoshi,
			ChunkPrice:              raw.ChunkPriceSatoshi,
			ChunkCount:              raw.ChunkCount,
			FileSizeBytes:           raw.FileSizeBytes,
			ExpiresAtUnix:           raw.ExpiresAtUnix,
			RecommendedFileName:     raw.RecommendedFileName,
			MimeType:                raw.MimeType,
			SellerArbiterPubHexes:   append([]string(nil), raw.SellerArbiterPubHexes...),
			AvailableChunkBitmapHex: raw.AvailableChunkBitmapHex,
		}
		if strings.TrimSpace(it.AvailableChunkBitmapHex) != "" {
			norm, err := normalizeChunkBitmapHex(it.AvailableChunkBitmapHex)
			if err != nil {
				continue
			}
			it.AvailableChunkBitmapHex = norm
			indexes, err := chunkIndexesFromBitmapHex(norm, 0)
			if err != nil {
				continue
			}
			it.AvailableChunkIndexes = indexes
		}
		if it.ExpiresAtUnix > 0 && it.ExpiresAtUnix < now {
			continue
		}
		out = append(out, it)
	}
	return out, nil
}

func TriggerClientSubmitLiveQuote(ctx context.Context, seller *Runtime, p LiveQuoteParams) error {
	if seller == nil || seller.Host == nil {
		return fmt.Errorf("runtime not initialized")
	}
	return submitLiveQuote(ctx, seller.Host, seller.rpcTrace, p)
}

func TriggerClientListLiveQuotes(ctx context.Context, store *clientDB, demandID string) ([]LiveQuoteItem, error) {
	rows, err := dbListLiveQuotes(ctx, store, demandID)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	out := make([]LiveQuoteItem, 0, 4)
	for _, it := range rows {
		if it.ExpiresAtUnix > 0 && it.ExpiresAtUnix < now {
			continue
		}
		out = append(out, it)
	}
	return out, nil
}

type SeedGetParams struct {
	SellerPeerID string `json:"seller_pubkey_hex"`
	SessionID    string `json:"session_id"`
	SeedHash     string `json:"seed_hash"`
}

type SeedGetResult struct {
	Seed []byte `json:"seed"`
}

func TriggerClientSeedGet(ctx context.Context, rt *Runtime, p SeedGetParams) (SeedGetResult, error) {
	if rt == nil || rt.Host == nil {
		return SeedGetResult{}, fmt.Errorf("runtime not initialized")
	}
	sellerPeerID := strings.TrimSpace(p.SellerPeerID)
	sessionID := strings.TrimSpace(p.SessionID)
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if sellerPeerID == "" || sessionID == "" || seedHash == "" {
		return SeedGetResult{}, fmt.Errorf("invalid params")
	}
	seller, err := peerIDFromClientID(sellerPeerID)
	if err != nil {
		return SeedGetResult{}, err
	}
	var resp seedGetResp
	err = pproto.CallProto(ctx, rt.Host, seller, ProtoSeedGet, clientSec(rt.rpcTrace), seedGetReq{
		SessionId: sessionID,
		SeedHash:  seedHash,
	}, &resp)
	if err != nil {
		return SeedGetResult{}, err
	}
	return SeedGetResult{Seed: append([]byte(nil), resp.Seed...)}, nil
}

type directTransferPoolOpenParams struct {
	SellerPubHex  string
	ArbiterPubHex string
	FrontOrderID  string // 本次下载发起唯一，显式参数
	DemandID      string
	SeedHash      string
	SeedPrice     uint64
	ChunkPrice    uint64
	ExpiresAtUnix int64
	DealID        string
	SessionID     string
	PoolAmount    uint64
}

type directTransferPoolOpenResult struct {
	DealID     string
	SessionID  string
	BaseTxID   string
	Sequence   uint32
	PoolAmount uint64
}

type directTransferPoolPayParams struct {
	SellerPubHex string
	SessionID    string
	Amount       uint64
	SeedHash     string
	ChunkHash    string
	ChunkIndex   uint32
}

type directTransferPoolPayResult struct {
	Sequence uint32
}

type directTransferChunkGetParams struct {
	SellerPubHex string
	SessionID    string
	SeedHash     string
	ChunkHash    string
	ChunkIndex   uint32
	Sequence     uint32
}

type directTransferChunkGetResult struct {
	Chunk []byte
}

type directTransferArbitrateParams struct {
	SellerPubHex  string
	SessionID     string
	DemandID      string
	SeedHash      string
	ChunkHash     string
	ChunkIndex    uint32
	Sequence      uint32
	ArbiterFeeSat uint64
	EvidenceJSON  []byte
}

type directTransferArbitrateResult struct {
	Status    string
	AwardTxID string
	AwardTx   []byte
}

type directTransferPoolCloseParams struct {
	SellerPubHex string
	SessionID    string
}

type directTransferPoolCloseResult struct {
	FinalTxID string
}

// triggerDirectTransferPoolOpen 正式下载开池入口
// 设计约束（第五步）：
// - FrontOrderID 是正式下载主流程的必填字段，用于关联 front_order -> business -> settlement 主线
// - 缺失 FrontOrderID 表示调用方试图绕过新主线，直接拒绝
// - 如需临时调试老路径，应使用专门的 debug 包装函数，不能共用此正式入口
func triggerDirectTransferPoolOpen(ctx context.Context, store *clientDB, buyer *Runtime, p directTransferPoolOpenParams) (directTransferPoolOpenResult, error) {
	if buyer == nil || buyer.Host == nil || buyer.ActionChain == nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("runtime not initialized")
	}

	// 第五步硬约束：正式下载入口必须带 FrontOrderID
	frontOrderID := strings.TrimSpace(p.FrontOrderID)
	if frontOrderID == "" {
		return directTransferPoolOpenResult{}, fmt.Errorf("front_order_id is required for mainflow download: cannot bypass front_order business chain")
	}

	lock := buyer.transferPoolOpenMutex()
	lock.Lock()
	defer lock.Unlock()

	dealID := strings.TrimSpace(p.DealID)
	if dealID == "" {
		deal, err := TriggerClientAcceptDirectDeal(ctx, buyer, DirectDealAcceptParams{
			SellerPubHex:  p.SellerPubHex,
			DemandID:      p.DemandID,
			SeedHash:      p.SeedHash,
			SeedPrice:     p.SeedPrice,
			ChunkPrice:    p.ChunkPrice,
			ExpiresAtUnix: p.ExpiresAtUnix,
			ArbiterPubHex: p.ArbiterPubHex,
		})
		if err != nil {
			return directTransferPoolOpenResult{}, err
		}
		dealID = strings.TrimSpace(deal.DealId)
	}
	sessionID := strings.TrimSpace(p.SessionID)
	if sessionID == "" {
		sessionID = "tp_" + randHex(8)
	}

	// 第三步：挂新主线骨架
	// 一个 seller 的这次下载收费对应一条 business 和一条 settlement
	// Open 只是开池准备，settlement 保持 pending
	// 注：frontOrderID 已在顶部强制校验，此处必然有效
	uniqueSuffix := fmt.Sprintf("%d_%04x", time.Now().UnixNano(), time.Now().UnixNano()&0xFFFF)
	businessID := "biz_download_pool_" + uniqueSuffix
	settlementID := "set_download_pool_" + uniqueSuffix
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		// 前台订单（已存在，这里只关联）
		FrontOrderID:     frontOrderID,
		FrontType:        "download",
		FrontSubtype:     "direct_transfer",
		OwnerPubkeyHex:   strings.ToLower(strings.TrimSpace(buyer.ClientID())),
		TargetObjectType: "demand",
		TargetObjectID:   strings.TrimSpace(p.DemandID),
		FrontOrderNote:   "下载: " + strings.TrimSpace(p.DemandID),
		FrontOrderPayload: map[string]any{
			"demand_id":         strings.TrimSpace(p.DemandID),
			"seed_hash":         strings.ToLower(strings.TrimSpace(p.SeedHash)),
			"seller_pubkey_hex": strings.TrimSpace(p.SellerPubHex),
		},

		// 业务（第七阶段整改：调用方显式传 business_role）
		BusinessID:        businessID,
		BusinessRole:      "formal", // 下载池支付是正式收费对象
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:" + strings.TrimSpace(p.SellerPubHex),
		BusinessNote:      "下载池支付: " + strings.TrimSpace(p.DemandID),
		BusinessPayload: map[string]any{
			"demand_id":         strings.TrimSpace(p.DemandID),
			"seed_hash":         strings.ToLower(strings.TrimSpace(p.SeedHash)),
			"seller_pubkey_hex": strings.TrimSpace(p.SellerPubHex),
		},

		// 触发器
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
		TriggerNote:    "前台订单触发池支付",
		TriggerPayload: map[string]any{
			"trigger_reason": "download_initiated",
		},

		// 结算
		SettlementID:         settlementID,
		SettlementMethod:     SettlementMethodPool,
		SettlementTargetType: "", // Open 阶段未确定 allocation，保持空
		SettlementTargetID:   "",
		SettlementPayload: map[string]any{
			"demand_id":         strings.TrimSpace(p.DemandID),
			"seller_pubkey_hex": strings.TrimSpace(p.SellerPubHex),
			"session_id":        sessionID,
			"status":            "pending",
		},
	}); err != nil {
		obs.Error(ServiceName, "download_pool_chain_init_failed", map[string]any{"error": err.Error(), "demand_id": p.DemandID, "seller_pubkey_hex": p.SellerPubHex})
		return directTransferPoolOpenResult{}, fmt.Errorf("create download pool business chain: %w", err)
	}

	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	sellerPubHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	sellerPub, err := ec.PublicKeyFromString(sellerPubHex)
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	arbiterPubHex := strings.ToLower(strings.TrimSpace(p.ArbiterPubHex))
	arbiterPID, err := peerIDFromSecp256k1PubHex(arbiterPubHex)
	if err != nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("invalid arbiter pubkey hex: %w", err)
	}
	arbiterPub := buyer.Host.Peerstore().PubKey(arbiterPID)
	if arbiterPub == nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("missing arbiter pubkey in peerstore")
	}
	arbiterRaw, err := arbiterPub.Raw()
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	arbiterPubHex = strings.ToLower(hex.EncodeToString(arbiterRaw))
	arbiterPubKey, err := ec.PublicKeyFromString(arbiterPubHex)
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}

	identity, err := buyer.runtimeIdentity()
	if err != nil {
		return directTransferPoolOpenResult{}, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return directTransferPoolOpenResult{}, fmt.Errorf("runtime not initialized")
	}
	clientLockScript := ""
	if addr, addrErr := kmlibs.GetAddressFromPubKey(clientActor.PubKey, identity.IsMainnet); addrErr == nil {
		if lock, lockErr := p2pkh.Lock(addr); lockErr == nil {
			clientLockScript = strings.TrimSpace(lock.String())
		}
	}

	target := p.PoolAmount
	if target == 0 {
		target = 200
	}

	const maxOpenAttempt = 4
	sessionPinned := strings.TrimSpace(p.SessionID) != ""
	var lastErr error
	for attempt := 1; attempt <= maxOpenAttempt; attempt++ {
		curSessionID := sessionID
		if attempt > 1 && !sessionPinned {
			curSessionID = "tp_" + randHex(8)
		}

		poolInputs, splitTxID, err := func() ([]poolcore.UTXO, string, error) {
			// 钱包 UTXO 分配必须单步串行：查询/选取/拆分在同一临界区完成。
			allocMu := buyer.walletAllocMutex()
			allocMu.Lock()
			defer allocMu.Unlock()

			selected, err := allocatePlainBSVWalletUTXOs(ctx, store, buyer, "direct_transfer_pool_open", target)
			if err != nil {
				return nil, "", err
			}
			return splitUTXOsToTarget(ctx, store, buyer, "direct_pool:"+curSessionID, clientActor, selected, target, 0.5)
		}()
		if err != nil {
			if isRetryableTransferPoolSplitErr(err) && attempt < maxOpenAttempt {
				lastErr = err
				wait := time.Duration(attempt) * 800 * time.Millisecond
				obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_open_retry", map[string]any{
					"session_id": reqSessionIDOrFallback(curSessionID, sessionID),
					"deal_id":    dealID,
					"attempt":    attempt,
					"wait_ms":    wait.Milliseconds(),
					"error":      err.Error(),
					"stage":      "split_fund",
				})
				select {
				case <-ctx.Done():
					return directTransferPoolOpenResult{}, ctx.Err()
				case <-time.After(wait):
				}
				continue
			}
			return directTransferPoolOpenResult{}, fmt.Errorf("prepare exact pool utxo failed: %w", err)
		}
		obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_open_fund_prepared", map[string]any{
			"session_id":     curSessionID,
			"deal_id":        dealID,
			"attempt":        attempt,
			"target_amount":  target,
			"input_count":    len(poolInputs),
			"split_txid":     splitTxID,
			"split_executed": strings.TrimSpace(splitTxID) != "",
		})

		kmUTXOs := make([]kmlibs.UTXO, 0, len(poolInputs))
		for _, u := range poolInputs {
			kmUTXOs = append(kmUTXOs, kmlibs.UTXO{TxID: u.TxID, Vout: u.Vout, Value: u.Value})
		}

		baseResp, err := te.BuildTripleFeePoolBaseTx(&kmUTXOs, arbiterPubKey, clientActor.PrivKey, sellerPub, false, 0.5)
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("build transfer pool base tx failed: %w", err)
		}
		tip, err := getTipHeightFromDB(ctx, store)
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("load tip height from snapshot failed: %w", err)
		}
		lockBlocks := uint32(6)
		spendTx, buyerOpenSig, buyerAmount, err := te.BuildTripleFeePoolSpendTX(baseResp.Tx, baseResp.Amount, tip+lockBlocks, arbiterPubKey, clientActor.PrivKey, sellerPub, false, 0.5)
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("build transfer pool spend tx failed: %w", err)
		}
		spendTxBytes, err := hex.DecodeString(spendTx.Hex())
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("encode spend tx bytes failed: %w", err)
		}
		baseTxBytes, err := hex.DecodeString(baseResp.Tx.Hex())
		if err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("encode base tx bytes failed: %w", err)
		}
		spendFee := poolcore.CalcFeeWithInputAmount(spendTx, baseResp.Amount)
		if spendFee == 0 {
			spendFee = 1
		}
		req := directTransferPoolOpenReq{
			SessionId:        curSessionID,
			DealId:           dealID,
			BuyerPubkeyHex:   strings.ToLower(strings.TrimSpace(buyer.ClientID())),
			ArbiterPubkeyHex: strings.TrimSpace(p.ArbiterPubHex),
			ArbiterPubkey:    arbiterPubHex,
			PoolAmount:       baseResp.Amount,
			SpendTxFee:       spendFee,
			Sequence:         1,
			SellerAmount:     0,
			BuyerAmount:      buyerAmount,
			CurrentTx:        spendTxBytes,
			BuyerSig:         append([]byte(nil), (*buyerOpenSig)...),
			BaseTx:           baseTxBytes,
			BaseTxid:         baseResp.Tx.TxID().String(),
			FeeRateSatByte:   0.5,
			LockBlocks:       lockBlocks,
		}
		obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_open_begin", map[string]any{
			"session_id": req.SessionId,
			"deal_id":    req.DealId,
			"attempt":    attempt,
		})
		var openResp directTransferPoolOpenResp
		if err := pproto.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferPoolOpen, clientSec(buyer.rpcTrace), req, &openResp); err != nil {
			obs.Error(ServiceName, "evt_trigger_direct_transfer_pool_open_failed", map[string]any{"error": err.Error()})
			return directTransferPoolOpenResult{}, err
		}
		if strings.TrimSpace(openResp.Status) != "active" || len(openResp.SellerSig) == 0 {
			return directTransferPoolOpenResult{}, fmt.Errorf("transfer pool open rejected: %s", strings.TrimSpace(openResp.Error))
		}
		sellerSig := append([]byte(nil), openResp.SellerSig...)
		merged, err := te.MergeTripleFeePoolSigForSpendTx(spendTx.Hex(), buyerOpenSig, &sellerSig)
		if err != nil {
			return directTransferPoolOpenResult{}, err
		}
		baseTxID, err := buyer.ActionChain.Broadcast(baseResp.Tx.Hex())
		if err != nil {
			if isRetryableTransferPoolBaseTxBroadcastErr(err) && attempt < maxOpenAttempt {
				lastErr = err
				wait := time.Duration(attempt) * 800 * time.Millisecond
				obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_open_retry", map[string]any{
					"session_id": req.SessionId,
					"deal_id":    req.DealId,
					"attempt":    attempt,
					"wait_ms":    wait.Milliseconds(),
					"error":      err.Error(),
				})
				select {
				case <-ctx.Done():
					return directTransferPoolOpenResult{}, ctx.Err()
				case <-time.After(wait):
				}
				continue
			}
			return directTransferPoolOpenResult{}, fmt.Errorf("broadcast transfer pool base tx failed: %w", err)
		}
		if err := applyLocalBroadcastWalletTx(ctx, store, buyer, baseResp.Tx.Hex(), "direct_transfer_pool_open_base"); err != nil {
			return directTransferPoolOpenResult{}, fmt.Errorf("project transfer pool base tx failed: %w", err)
		}
		buyer.setTriplePool(&triplePoolSession{
			DemandID:         strings.TrimSpace(p.DemandID),
			SessionID:        curSessionID,
			DealID:           dealID,
			BusinessID:       businessID, // 正式下载 business_id，供 pay 挂 tx_breakdown
			SettlementID:     settlementID,
			SellerPubHex:     strings.TrimSpace(p.SellerPubHex),
			ArbiterPubHex:    req.ArbiterPubkeyHex,
			PoolAmountSat:    req.PoolAmount,
			SpendTxFeeSat:    req.SpendTxFee,
			OpenSequence:     req.Sequence,
			Sequence:         req.Sequence,
			SellerAmount:     req.SellerAmount,
			BuyerAmount:      req.BuyerAmount,
			CurrentTxHex:     merged.Hex(),
			BaseTxHex:        baseResp.Tx.Hex(),
			BaseTxID:         baseTxID,
			FeeRateSatByte:   req.FeeRateSatByte,
			LockBlocks:       req.LockBlocks,
			SellerPubKeyHex:  sellerPubHex,
			BuyerPubKeyHex:   strings.ToLower(clientActor.PubHex),
			ArbiterPubKeyHex: arbiterPubHex,
			PayCount:         0,
			LastPaySequence:  0,
		})
		emitDirectTransferEvent(buyer, "direct_transfer_context_opened", map[string]any{
			"event_id":                fmt.Sprintf("%s:open", curSessionID),
			"demand_id":               strings.TrimSpace(p.DemandID),
			"deal_id":                 dealID,
			"session_id":              curSessionID,
			"seller_pubkey_hex":       strings.TrimSpace(p.SellerPubHex),
			"arbiter_pubkey_hex":      req.ArbiterPubkeyHex,
			"open_sequence":           req.Sequence,
			"open_base_txid":          baseTxID,
			"pool_amount_satoshi":     req.PoolAmount,
			"spend_tx_fee_satoshi":    req.SpendTxFee,
			"fee_rate_sat_per_byte":   req.FeeRateSatByte,
			"lock_blocks":             req.LockBlocks,
			"recommended_file_source": "direct_transfer_pool_open",
		})
		// 资金流水已迁移到 fact_* 事实表组装
		if err := dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
			SessionID:         curSessionID,
			DealID:            dealID,
			BaseTxID:          baseTxID,
			BaseTxHex:         baseResp.Tx.Hex(),
			ClientLockScript:  clientLockScript,
			PoolAmountSatoshi: req.PoolAmount,
			SellerPubHex:      strings.TrimSpace(p.SellerPubHex),
		}); err != nil {
			obs.Error(ServiceName, "evt_trigger_direct_transfer_pool_open_accounting_failed", map[string]any{"error": err.Error(), "session_id": curSessionID})
			return directTransferPoolOpenResult{}, err
		}
		obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_open_end", map[string]any{
			"session_id": req.SessionId,
			"base_txid":  baseTxID,
			"status":     openResp.Status,
			"attempt":    attempt,
		})
		return directTransferPoolOpenResult{DealID: dealID, SessionID: curSessionID, BaseTxID: baseTxID, Sequence: 1, PoolAmount: req.PoolAmount}, nil
	}
	return directTransferPoolOpenResult{}, fmt.Errorf("broadcast transfer pool base tx failed after retries: %w", lastErr)
}

func splitUTXOsToTarget(ctx context.Context, store *clientDB, rt *Runtime, flowID string, actor *poolcore.Actor, selected []poolcore.UTXO, target uint64, feeRateSatPerKB float64) ([]poolcore.UTXO, string, error) {
	if rt == nil || rt.ActionChain == nil {
		return nil, "", fmt.Errorf("runtime chain not initialized")
	}
	if actor == nil {
		return nil, "", fmt.Errorf("actor is nil")
	}
	if len(selected) == 0 {
		return nil, "", fmt.Errorf("selected utxos is empty")
	}
	if target == 0 {
		return nil, "", fmt.Errorf("target pool amount must be > 0")
	}
	total := sumUTXOValue(selected)
	if total < target {
		return nil, "", fmt.Errorf("insufficient selected utxos: have=%d target=%d", total, target)
	}
	if total == target {
		return selected, "", nil
	}

	identity, err := rt.runtimeIdentity()
	if err != nil {
		return nil, "", err
	}
	isMainnet := identity.IsMainnet
	clientAddr, err := kmlibs.GetAddressFromPubKey(actor.PubKey, isMainnet)
	if err != nil {
		return nil, "", fmt.Errorf("derive client address failed: %w", err)
	}
	lockScript, err := p2pkh.Lock(clientAddr)
	if err != nil {
		return nil, "", fmt.Errorf("build p2pkh lock script failed: %w", err)
	}
	prevLockHex := hex.EncodeToString(lockScript.Bytes())
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return nil, "", fmt.Errorf("build p2pkh unlock template failed: %w", err)
	}

	splitTx := tx.NewTransaction()
	for _, u := range selected {
		if err := splitTx.AddInputFrom(u.TxID, u.Vout, prevLockHex, u.Value, unlockTpl); err != nil {
			return nil, "", fmt.Errorf("add split input failed: %w", err)
		}
	}
	splitTx.AddOutput(&tx.TransactionOutput{
		Satoshis:      target,
		LockingScript: lockScript,
	})
	// 先放一个临时找零，签名后按真实大小估算手续费再回填。
	splitTx.AddOutput(&tx.TransactionOutput{
		Satoshis:      total - target,
		LockingScript: lockScript,
	})
	if err := signP2PKHAllInputs(splitTx, unlockTpl); err != nil {
		return nil, "", fmt.Errorf("pre-sign split tx failed: %w", err)
	}
	// direct transfer 这里也统一按 sat/KB 估算，和 domain 注册交易保持同一口径。
	fee := estimateMinerFeeSatPerKB(splitTx.Size(), feeRateSatPerKB)
	if total <= target+fee {
		return nil, "", fmt.Errorf("insufficient selected utxos for split fee: have=%d target=%d fee=%d", total, target, fee)
	}
	splitTx.Outputs[1].Satoshis = total - target - fee
	if err := signP2PKHAllInputs(splitTx, unlockTpl); err != nil {
		return nil, "", fmt.Errorf("final-sign split tx failed: %w", err)
	}

	localTxID := strings.ToLower(strings.TrimSpace(splitTx.TxID().String()))
	broadcastTxID, err := rt.ActionChain.Broadcast(splitTx.Hex())
	if err != nil {
		return nil, "", fmt.Errorf("broadcast split tx failed: %w", err)
	}
	splitTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if splitTxID == "" {
		splitTxID = localTxID
	}
	if err := applyLocalBroadcastWalletTx(ctx, store, rt, splitTx.Hex(), "direct_transfer_pool_split"); err != nil {
		return nil, "", fmt.Errorf("project split tx failed: %w", err)
	}
	_ = int64(total - target - fee) // change，资金流水已迁移到 fact_* 事实表组装

	deadline := time.Now().Add(20 * time.Second)
	for {
		utxos, err := getWalletUTXOsFromDB(ctx, store, rt)
		if err == nil {
			for _, u := range utxos {
				if strings.EqualFold(strings.TrimSpace(u.TxID), splitTxID) && u.Vout == 0 && u.Value == target {
					return []poolcore.UTXO{u}, splitTxID, nil
				}
			}
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return nil, "", fmt.Errorf("split tx output not visible in utxo set: txid=%s target=%d", splitTxID, target)
}

func signP2PKHAllInputs(t *tx.Transaction, unlockTpl *p2pkh.P2PKH) error {
	for i := range t.Inputs {
		unlockScript, err := unlockTpl.Sign(t, uint32(i))
		if err != nil {
			return err
		}
		t.Inputs[i].UnlockingScript = unlockScript
	}
	return nil
}

func sumUTXOValue(utxos []poolcore.UTXO) uint64 {
	var sum uint64
	for _, u := range utxos {
		sum += u.Value
	}
	return sum
}

func triggerDirectTransferChunkGet(ctx context.Context, _ *clientDB, buyer *Runtime, p directTransferChunkGetParams) (directTransferChunkGetResult, error) {
	if buyer == nil || buyer.Host == nil {
		return directTransferChunkGetResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	chunkHash := strings.ToLower(strings.TrimSpace(p.ChunkHash))
	sessionID := strings.TrimSpace(p.SessionID)
	if seedHash == "" || chunkHash == "" || sessionID == "" {
		return directTransferChunkGetResult{}, fmt.Errorf("session_id/seed_hash/chunk_hash are required")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directTransferChunkGetResult{}, err
	}
	req := directTransferChunkGetReq{
		SessionId:  sessionID,
		SeedHash:   seedHash,
		ChunkHash:  chunkHash,
		ChunkIndex: p.ChunkIndex,
		Sequence:   p.Sequence,
	}
	var resp directTransferChunkGetResp
	if err := pproto.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferChunkGet, clientSec(buyer.rpcTrace), req, &resp); err != nil {
		return directTransferChunkGetResult{}, err
	}
	if strings.TrimSpace(resp.Status) != "delivering" && strings.TrimSpace(resp.Status) != "active" {
		return directTransferChunkGetResult{}, fmt.Errorf("transfer chunk_get rejected: %s", strings.TrimSpace(resp.Error))
	}
	chunk := append([]byte(nil), resp.Chunk...)
	if len(chunk) == 0 {
		return directTransferChunkGetResult{}, fmt.Errorf("transfer chunk_get rejected: chunk missing")
	}
	return directTransferChunkGetResult{Chunk: chunk}, nil
}

func triggerDirectTransferPoolPay(ctx context.Context, store *clientDB, buyer *Runtime, p directTransferPoolPayParams) (directTransferPoolPayResult, error) {
	if buyer == nil || buyer.Host == nil {
		return directTransferPoolPayResult{}, fmt.Errorf("runtime not initialized")
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	chunkHash := strings.ToLower(strings.TrimSpace(p.ChunkHash))
	if seedHash == "" || chunkHash == "" {
		return directTransferPoolPayResult{}, fmt.Errorf("seed_hash and chunk_hash are required")
	}
	lock := buyer.transferPoolSessionMutex(p.SessionID)
	lock.Lock()
	defer lock.Unlock()

	session, ok := buyer.getTriplePool(strings.TrimSpace(p.SessionID))
	if !ok || session == nil {
		return directTransferPoolPayResult{}, fmt.Errorf("transfer pool session missing")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	identity, err := buyer.runtimeIdentity()
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return directTransferPoolPayResult{}, fmt.Errorf("runtime not initialized")
	}
	sellerPub, err := ec.PublicKeyFromString(session.SellerPubKeyHex)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	arbiterPub, err := ec.PublicKeyFromString(session.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	nextSeq := session.Sequence + 1
	nextSellerAmount := session.SellerAmount + p.Amount
	updatedTx, err := te.TripleFeePoolLoadTx(
		session.CurrentTxHex,
		nil,
		nextSeq,
		nextSellerAmount,
		arbiterPub,
		clientActor.PubKey,
		sellerPub,
		session.PoolAmountSat,
	)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	buyerSig, err := te.ClientATripleFeePoolSpendTXUpdateSign(updatedTx, arbiterPub, clientActor.PrivKey, sellerPub)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	updatedTxBytes, err := hex.DecodeString(updatedTx.Hex())
	if err != nil {
		return directTransferPoolPayResult{}, fmt.Errorf("encode updated tx bytes failed: %w", err)
	}
	req := directTransferPoolPayReq{
		SessionId:    session.SessionID,
		SeedHash:     seedHash,
		ChunkHash:    chunkHash,
		ChunkIndex:   p.ChunkIndex,
		Sequence:     nextSeq,
		SellerAmount: nextSellerAmount,
		BuyerAmount:  session.PoolAmountSat - nextSellerAmount - session.SpendTxFeeSat,
		CurrentTx:    updatedTxBytes,
		BuyerSig:     append([]byte(nil), (*buyerSig)...),
	}
	obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_pay_begin", map[string]any{
		"session_id":    req.SessionId,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
		"chunk_hash":    chunkHash,
		"chunk_index":   p.ChunkIndex,
	})
	var payResp directTransferPoolPayResp
	if err := pproto.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferPoolPay, clientSec(buyer.rpcTrace), req, &payResp); err != nil {
		obs.Error(ServiceName, "evt_trigger_direct_transfer_pool_pay_failed", map[string]any{"error": err.Error()})
		return directTransferPoolPayResult{}, err
	}
	if strings.TrimSpace(payResp.Status) != "active" || len(payResp.SellerSig) == 0 {
		return directTransferPoolPayResult{}, fmt.Errorf("transfer pool pay rejected: %s", strings.TrimSpace(payResp.Error))
	}
	sellerSig := append([]byte(nil), payResp.SellerSig...)
	merged, err := te.MergeTripleFeePoolSigForSpendTx(updatedTx.Hex(), buyerSig, &sellerSig)
	if err != nil {
		return directTransferPoolPayResult{}, err
	}
	session.Sequence = req.Sequence
	session.SellerAmount = req.SellerAmount
	session.BuyerAmount = req.BuyerAmount
	session.PayCount++
	session.LastPaySequence = req.Sequence
	session.CurrentTxHex = merged.Hex()
	buyer.setTriplePool(session)
	emitDirectTransferEvent(buyer, "direct_transfer_chunk_paid", map[string]any{
		"event_id":            fmt.Sprintf("%s:%d:%d", session.SessionID, req.Sequence, p.ChunkIndex),
		"demand_id":           strings.TrimSpace(session.DemandID),
		"deal_id":             strings.TrimSpace(session.DealID),
		"session_id":          session.SessionID,
		"seller_pubkey_hex":   strings.TrimSpace(session.SellerPubHex),
		"arbiter_pubkey_hex":  strings.TrimSpace(session.ArbiterPubHex),
		"seed_hash":           seedHash,
		"chunk_hash":          chunkHash,
		"chunk_index":         p.ChunkIndex,
		"sequence":            req.Sequence,
		"pay_count":           session.PayCount,
		"last_pay_sequence":   session.LastPaySequence,
		"seller_amount_sat":   session.SellerAmount,
		"buyer_amount_sat":    session.BuyerAmount,
		"pool_amount_satoshi": session.PoolAmountSat,
		"chunk_price_satoshi": p.Amount,
	})
	// 资金流水已迁移到 fact_* 事实表组装
	if err := dbRecordDirectPoolPayAccounting(
		ctx,
		store,
		session.BusinessID, // 正式下载 business_id
		session.SessionID,
		req.Sequence,
		p.Amount,
		strings.TrimSpace(merged.TxID().String()),
	); err != nil {
		obs.Error(ServiceName, "evt_trigger_direct_transfer_pool_pay_accounting_failed", map[string]any{"error": err.Error(), "session_id": session.SessionID})
		return directTransferPoolPayResult{}, err
	}

	// 第三步：第一次成功的真实业务 pay allocation 时回写 settlement
	// Open 只是开池准备，第一条真实 pay allocation 才代表收费事实
	// 判定条件：这是第一次成功的 pay（PayCount 在 update session 之前还是 0，++后变成 1）
	settlementID := strings.TrimSpace(session.SettlementID)
	if settlementID != "" && session.PayCount == 1 {
		// 通过 allocation_id 拿到 pool_allocation.id
		_, allocID := directTransferPoolAccountingSource(session.SessionID, "pay", req.Sequence)
		poolAllocationID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocID)
		if err != nil {
			obs.Error(ServiceName, "download_pool_settlement_pay_failed", map[string]any{"error": err.Error(), "settlement_id": settlementID})
			return directTransferPoolPayResult{}, fmt.Errorf("resolve pool_allocation id for settlement: %w", err)
		}
		if err := dbUpdateBusinessSettlementStatus(ctx, store, settlementID, "settled", ""); err != nil {
			obs.Error(ServiceName, "download_pool_settlement_update_failed", map[string]any{"error": err.Error(), "settlement_id": settlementID})
			return directTransferPoolPayResult{}, fmt.Errorf("update settlement status: %w", err)
		}
		// 回写 target_type='pool_allocation', target_id=<pool_allocation.id>
		if err := dbUpdateBusinessSettlementTarget(ctx, store, settlementID, "pool_allocation", fmt.Sprintf("%d", poolAllocationID)); err != nil {
			obs.Error(ServiceName, "download_pool_settlement_target_failed", map[string]any{"error": err.Error(), "settlement_id": settlementID})
			return directTransferPoolPayResult{}, fmt.Errorf("update settlement target: %w", err)
		}
	}

	obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_pay_end", map[string]any{
		"session_id":    req.SessionId,
		"sequence":      req.Sequence,
		"seller_amount": req.SellerAmount,
		"status":        payResp.Status,
		"chunk_hash":    chunkHash,
		"chunk_index":   p.ChunkIndex,
	})
	return directTransferPoolPayResult{Sequence: req.Sequence}, nil
}

func triggerDirectTransferArbitrate(ctx context.Context, _ *clientDB, buyer *Runtime, p directTransferArbitrateParams) (directTransferArbitrateResult, error) {
	if buyer == nil || buyer.Host == nil {
		return directTransferArbitrateResult{}, fmt.Errorf("runtime not initialized")
	}
	lock := buyer.transferPoolSessionMutex(p.SessionID)
	lock.Lock()
	defer lock.Unlock()

	session, ok := buyer.getTriplePool(strings.TrimSpace(p.SessionID))
	if !ok || session == nil {
		return directTransferArbitrateResult{}, fmt.Errorf("transfer pool session missing")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directTransferArbitrateResult{}, err
	}
	currentTxBytes, err := hex.DecodeString(strings.TrimSpace(session.CurrentTxHex))
	if err != nil {
		return directTransferArbitrateResult{}, fmt.Errorf("decode session current_tx failed: %w", err)
	}
	req := directTransferArbitrateReq{
		DemandId:         strings.TrimSpace(p.DemandID),
		SessionId:        strings.TrimSpace(session.SessionID),
		SeedHash:         strings.ToLower(strings.TrimSpace(p.SeedHash)),
		ChunkIndex:       p.ChunkIndex,
		ChunkHash:        strings.ToLower(strings.TrimSpace(p.ChunkHash)),
		Sequence:         p.Sequence,
		SellerPubkeyHex:  strings.TrimSpace(session.SellerPubHex),
		BuyerPubkeyHex:   strings.ToLower(strings.TrimSpace(buyer.ClientID())),
		ArbiterPubkeyHex: strings.TrimSpace(session.ArbiterPubHex),
		SellerAmount:     session.SellerAmount,
		BuyerAmount:      session.BuyerAmount,
		ArbiterFee:       p.ArbiterFeeSat,
		SpendTxFee:       session.SpendTxFeeSat,
		CurrentTx:        currentTxBytes,
		EvidencePayload:  append([]byte(nil), p.EvidenceJSON...),
	}
	var resp directTransferArbitrateResp
	if err := pproto.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferArbitrate, clientSec(buyer.rpcTrace), req, &resp); err != nil {
		return directTransferArbitrateResult{}, err
	}
	if strings.TrimSpace(resp.Status) == "" || strings.TrimSpace(resp.Status) == "rejected" {
		return directTransferArbitrateResult{}, fmt.Errorf("transfer arbitrate rejected: %s", strings.TrimSpace(resp.Error))
	}
	return directTransferArbitrateResult{
		Status:    strings.TrimSpace(resp.Status),
		AwardTxID: strings.TrimSpace(resp.AwardTxid),
		AwardTx:   append([]byte(nil), resp.AwardTx...),
	}, nil
}

func triggerDirectTransferPoolClose(ctx context.Context, store *clientDB, buyer *Runtime, p directTransferPoolCloseParams) (directTransferPoolCloseResult, error) {
	if buyer == nil || buyer.Host == nil || buyer.ActionChain == nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	lock := buyer.transferPoolSessionMutex(p.SessionID)
	lock.Lock()
	defer lock.Unlock()

	session, ok := buyer.getTriplePool(strings.TrimSpace(p.SessionID))
	if !ok || session == nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("transfer pool session missing")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	identity, err := buyer.runtimeIdentity()
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("runtime not initialized")
	}
	sellerPub, err := ec.PublicKeyFromString(session.SellerPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	arbiterPub, err := ec.PublicKeyFromString(session.ArbiterPubKeyHex)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	locktime := uint32(0xffffffff)
	finalTx, err := te.TripleFeePoolLoadTx(
		session.CurrentTxHex,
		&locktime,
		0xffffffff,
		session.SellerAmount,
		arbiterPub,
		clientActor.PubKey,
		sellerPub,
		session.PoolAmountSat,
	)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	buyerSig, err := te.ClientATripleFeePoolSpendTXUpdateSign(finalTx, arbiterPub, clientActor.PrivKey, sellerPub)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	finalTxBytes, err := hex.DecodeString(finalTx.Hex())
	if err != nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("encode final tx bytes failed: %w", err)
	}
	req := directTransferPoolCloseReq{
		SessionId:    session.SessionID,
		Sequence:     session.Sequence,
		SellerAmount: session.SellerAmount,
		BuyerAmount:  session.BuyerAmount,
		CurrentTx:    finalTxBytes,
		BuyerSig:     append([]byte(nil), (*buyerSig)...),
	}
	obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_close_begin", map[string]any{
		"session_id": req.SessionId,
		"sequence":   req.Sequence,
	})
	var closeResp directTransferPoolCloseResp
	if err := pproto.CallProto(ctx, buyer.Host, sellerPID, ProtoTransferPoolClose, clientSec(buyer.rpcTrace), req, &closeResp); err != nil {
		obs.Error(ServiceName, "evt_trigger_direct_transfer_pool_close_failed", map[string]any{"error": err.Error()})
		return directTransferPoolCloseResult{}, err
	}
	if len(closeResp.SellerSig) == 0 {
		return directTransferPoolCloseResult{}, fmt.Errorf("transfer pool close rejected: %s", strings.TrimSpace(closeResp.Error))
	}
	sellerSig := append([]byte(nil), closeResp.SellerSig...)
	merged, err := te.MergeTripleFeePoolSigForSpendTx(finalTx.Hex(), buyerSig, &sellerSig)
	if err != nil {
		return directTransferPoolCloseResult{}, err
	}
	finalTxID, err := buyer.ActionChain.Broadcast(merged.Hex())
	if err != nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("broadcast transfer pool final tx failed: %w", err)
	}
	if err := applyLocalBroadcastWalletTx(ctx, store, buyer, merged.Hex(), "direct_transfer_pool_close_final"); err != nil {
		return directTransferPoolCloseResult{}, fmt.Errorf("project transfer pool final tx failed: %w", err)
	}
	session.FinalTxID = finalTxID
	session.CurrentTxHex = merged.Hex()
	buyer.setTriplePool(session)
	emitDirectTransferEvent(buyer, "direct_transfer_context_closed", map[string]any{
		"event_id":              fmt.Sprintf("%s:close:%d", session.SessionID, session.Sequence),
		"demand_id":             strings.TrimSpace(session.DemandID),
		"deal_id":               strings.TrimSpace(session.DealID),
		"session_id":            session.SessionID,
		"seller_pubkey_hex":     strings.TrimSpace(session.SellerPubHex),
		"arbiter_pubkey_hex":    strings.TrimSpace(session.ArbiterPubHex),
		"open_sequence":         session.OpenSequence,
		"open_base_txid":        strings.TrimSpace(session.BaseTxID),
		"pool_amount_satoshi":   session.PoolAmountSat,
		"pay_count":             session.PayCount,
		"last_pay_sequence":     session.LastPaySequence,
		"close_final_txid":      finalTxID,
		"final_seller_amount":   session.SellerAmount,
		"final_buyer_amount":    session.BuyerAmount,
		"spend_tx_fee_satoshi":  session.SpendTxFeeSat,
		"close_reason":          "transfer_completed",
		"transfer_state":        "closed",
		"transfer_entry_source": "direct_transfer_pool_close",
	})
	// 资金流水已迁移到 fact_* 事实表组装
	if err := dbRecordDirectPoolCloseAccounting(
		ctx,
		store,
		session.SessionID,
		session.Sequence,
		finalTxID,
		merged.Hex(),
		session.SellerAmount,
		session.BuyerAmount,
		strings.TrimSpace(session.SellerPubHex),
	); err != nil {
		obs.Error(ServiceName, "evt_trigger_direct_transfer_pool_close_accounting_failed", map[string]any{"error": err.Error(), "session_id": session.SessionID})
		return directTransferPoolCloseResult{}, err
	}
	buyer.deleteTriplePool(session.SessionID)
	buyer.releaseTransferPoolSessionMutex(session.SessionID)
	obs.Business(ServiceName, "evt_trigger_direct_transfer_pool_close_end", map[string]any{
		"session_id": session.SessionID,
		"final_txid": finalTxID,
		"status":     "closed",
	})
	return directTransferPoolCloseResult{FinalTxID: finalTxID}, nil
}

func isRetryableTransferPoolBaseTxBroadcastErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "txn-mempool-conflict") ||
		strings.Contains(msg, "missing inputs") ||
		strings.Contains(msg, "bad-txns-inputs-spent")
}

func emitDirectTransferEvent(rt *Runtime, name string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	if rt != nil {
		clientID := strings.ToLower(strings.TrimSpace(rt.ClientID()))
		if clientID != "" {
			if _, ok := fields["client_pubkey_hex"]; !ok {
				fields["client_pubkey_hex"] = clientID
			}
		}
		if rt.Host != nil {
			if _, ok := fields["client_transport_peer_id"]; !ok {
				fields["client_transport_peer_id"] = rt.Host.ID().String()
			}
		}
	}
	obs.Business(ServiceName, name, fields)
}

func isRetryableTransferPoolSplitErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "txn-mempool-conflict") ||
		strings.Contains(msg, "missing inputs") ||
		strings.Contains(msg, "bad-txns-inputs-spent")
}

func reqSessionIDOrFallback(current string, fallback string) string {
	current = strings.TrimSpace(current)
	if current != "" {
		return current
	}
	return strings.TrimSpace(fallback)
}

type seedV1Meta struct {
	FileSize    uint64
	ChunkCount  uint32
	ChunkHashes []string
	SeedHashHex string
}

func parseSeedV1(seed []byte) (seedV1Meta, error) {
	if len(seed) < 22 {
		return seedV1Meta{}, fmt.Errorf("invalid seed length")
	}
	if string(seed[:4]) != "BSE1" {
		return seedV1Meta{}, fmt.Errorf("invalid seed magic")
	}
	chunkSize := binary.BigEndian.Uint32(seed[6:10])
	if chunkSize != seedBlockSize {
		return seedV1Meta{}, fmt.Errorf("unsupported chunk size")
	}
	fileSize := binary.BigEndian.Uint64(seed[10:18])
	chunkCount := binary.BigEndian.Uint32(seed[18:22])
	expect := 22 + int(chunkCount)*32
	if len(seed) != expect {
		return seedV1Meta{}, fmt.Errorf("invalid seed body length")
	}
	hashes := make([]string, 0, chunkCount)
	offset := 22
	for i := uint32(0); i < chunkCount; i++ {
		hashes = append(hashes, hex.EncodeToString(seed[offset:offset+32]))
		offset += 32
	}
	h := sha256.Sum256(seed)
	return seedV1Meta{
		FileSize:    fileSize,
		ChunkCount:  chunkCount,
		ChunkHashes: hashes,
		SeedHashHex: hex.EncodeToString(h[:]),
	}, nil
}

func resolveDealArbiter(buyer *Runtime, sellerArbiters []string, override string) (string, error) {
	if buyer == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	own := ownArbiterPubHexes(buyer)
	if len(own) == 0 {
		return "", fmt.Errorf("buyer has no configured arbiter")
	}
	sellerSet := map[string]struct{}{}
	normalizedSellerArbiters, err := normalizePubHexList(sellerArbiters)
	if err != nil {
		return "", err
	}
	for _, id := range normalizedSellerArbiters {
		sellerSet[id] = struct{}{}
	}
	pin := strings.TrimSpace(override)
	if pin != "" {
		pin = strings.ToLower(pin)
		if _, err := normalizeCompressedPubKeyHex(pin); err != nil {
			return "", fmt.Errorf("invalid arbiter_pubkey_hex override: %w", err)
		}
		if _, ok := sellerSet[pin]; !ok {
			return "", fmt.Errorf("arbiter override not supported by seller: %s", pin)
		}
		found := false
		for _, id := range own {
			if id == pin {
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("arbiter override not configured by buyer: %s", pin)
		}
		return pin, nil
	}
	for _, id := range own {
		if _, ok := sellerSet[id]; ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("no common arbiter between buyer and seller")
}

func ownArbiterPubHexes(rt *Runtime) []string {
	if rt == nil {
		return nil
	}
	snapshot := rt.ConfigSnapshot()
	ids := make([]string, 0, len(snapshot.Network.Arbiters))
	for _, a := range snapshot.Network.Arbiters {
		if !a.Enabled {
			continue
		}
		pubHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(a.Pubkey))
		if err != nil {
			continue
		}
		ids = append(ids, pubHex)
	}
	ids, _ = normalizePubHexList(ids)
	if len(ids) > 0 {
		return ids
	}
	// 回退：用已连接仲裁列表（保持连接顺序），再转成 pubkey hex。
	fallback := make([]string, 0, len(rt.HealthyArbiters))
	for _, ai := range rt.HealthyArbiters {
		if rt.Host == nil {
			continue
		}
		pub := rt.Host.Peerstore().PubKey(ai.ID)
		if pub == nil {
			continue
		}
		raw, err := pub.Raw()
		if err != nil {
			continue
		}
		if hexPub, err := normalizeCompressedPubKeyHex(hex.EncodeToString(raw)); err == nil {
			fallback = append(fallback, hexPub)
		}
	}
	out, _ := normalizePubHexList(fallback)
	return out
}

// defaultArbiterPubHex 统一给下载链路挑一个默认仲裁者。
// 设计说明：
// - 只返回 pub hex，不再把 peer ID 当成系统内 ID 继续传下去；
// - 优先用配置里显式启用的仲裁者，再退回到已连通仲裁者的 pub hex。

type DirectDealAcceptParams struct {
	SellerPubHex  string `json:"seller_pubkey_hex"`
	DemandID      string `json:"demand_id"`
	SeedHash      string `json:"seed_hash"`
	SeedPrice     uint64 `json:"seed_price"`
	ChunkPrice    uint64 `json:"chunk_price"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
	ArbiterPubHex string `json:"arbiter_pubkey_hex,omitempty"`
}

func TriggerClientAcceptDirectDeal(ctx context.Context, buyer *Runtime, p DirectDealAcceptParams) (directDealAcceptResp, error) {
	if buyer == nil || buyer.Host == nil {
		return directDealAcceptResp{}, fmt.Errorf("runtime not initialized")
	}
	sellerPID, err := peerIDFromClientID(strings.TrimSpace(p.SellerPubHex))
	if err != nil {
		return directDealAcceptResp{}, err
	}
	var resp directDealAcceptResp
	err = pproto.CallProto(ctx, buyer.Host, sellerPID, ProtoDirectDealAccept, clientSec(buyer.rpcTrace), directDealAcceptReq{
		DemandId:         strings.TrimSpace(p.DemandID),
		BuyerPubkeyHex:   strings.ToLower(strings.TrimSpace(buyer.ClientID())),
		SeedHash:         strings.ToLower(strings.TrimSpace(p.SeedHash)),
		SeedPrice:        p.SeedPrice,
		ChunkPrice:       p.ChunkPrice,
		ExpiresAtUnix:    p.ExpiresAtUnix,
		ArbiterPubkeyHex: strings.TrimSpace(p.ArbiterPubHex),
	}, &resp)
	if err != nil {
		return directDealAcceptResp{}, err
	}
	return resp, nil
}

func localAdvertiseAddrs(rt *Runtime) []string {
	if rt == nil || rt.Host == nil {
		return nil
	}
	out := make([]string, 0, len(rt.Host.Addrs()))
	for _, a := range rt.Host.Addrs() {
		out = append(out, fmt.Sprintf("%s/p2p/%s", a.String(), rt.Host.ID().String()))
	}
	return out
}

func peerIDFromClientID(clientID string) (peer.ID, error) {
	pubHex, err := normalizeCompressedPubKeyHex(clientID)
	if err != nil {
		return "", fmt.Errorf("client_pubkey_hex invalid: %w", err)
	}
	b, err := hex.DecodeString(pubHex)
	if err != nil {
		return "", fmt.Errorf("decode client_pubkey_hex: %w", err)
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(b)
	if err != nil {
		return "", fmt.Errorf("unmarshal client pubkey: %w", err)
	}
	pid, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("derive peer id: %w", err)
	}
	return pid, nil
}
