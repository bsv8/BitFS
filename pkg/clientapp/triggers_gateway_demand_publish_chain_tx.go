package clientapp

import (
	"context"
	"fmt"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/obs"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// gatewayDemandPublishChainTxEnv 是这个触发链真正需要的能力。
// 设计说明：
// - 不再把整个 Runtime 塞进来，只拿调用这条链所需的显式能力；
// - 这样入口更清楚，也避免把运行时总对象继续扩散到业务链里。
type gatewayDemandPublishChainTxEnv interface {
	Store() ClientStore
	ClientID() string
	HealthyGatewayInfos() []peer.AddrInfo
	PickGatewayForBusiness(gatewayPeerID string) (peer.AddrInfo, error)
	GatewayBusinessID(pid peer.ID) string
	LocalAdvertiseAddrs() []string
	CallNodeRoute(ctx context.Context, peerID peer.ID, req ncall.CallReq, protoID protocol.ID) (ncall.CallResp, error)
	RequestPeerCallChainTxQuote(ctx context.Context, store ClientStore, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, protoID protocol.ID) (peerCallChainTxQuoteBuilt, error)
	PayPeerCallWithChainTxQuote(ctx context.Context, store ClientStore, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, quoted peerCallChainTxQuoteBuilt, protoID protocol.ID) (ncall.CallResp, error)
}

// TriggerGatewayDemandPublishChainTxQuotePayResult 是 gateway demand publish chain_tx_v1 业务触发结果。
// 说明：
// - 这里只返回业务本身真正关心的成交要素；
// - gateway 侧的投影比对由 e2e case 在外层单独做，不把多表读写塞进这里。
type TriggerGatewayDemandPublishChainTxQuotePayResult struct {
	ClientPubkeyHex      string `json:"client_pubkey_hex"`
	GatewayPubkeyHex     string `json:"gateway_pubkey_hex"`
	DemandSeedHash       string `json:"demand_seed_hash"`
	ChunkCount           uint32 `json:"chunk_count"`
	PreflightCode        string `json:"preflight_code"`
	QuoteOfferHash       string `json:"quote_offer_hash"`
	QuoteChargeSatoshi   uint64 `json:"quote_charge_satoshi"`
	QuoteExpiresAtUnix   int64  `json:"quote_expires_at_unix"`
	QuoteServiceType     string `json:"quote_service_type"`
	ServiceQuoteHash     string `json:"service_quote_hash"`
	PaymentReceiptScheme string `json:"payment_receipt_scheme"`
	PaymentTxID          string `json:"payment_txid"`
	DemandID             string `json:"demand_id"`
	DemandPublished      bool   `json:"demand_published"`
	Status               string `json:"status"`
	Error                string `json:"error,omitempty"`
}

// TriggerGatewayDemandPublishChainTxQuotePay 统一触发 gateway demand publish 的 chain_tx_v1 直付链路。
// 设计说明：
// - 先做 route preflight，硬要求 payment_required 且广告 chain_tx_v1；
// - 再取正式 quote，硬要求 quote 有效；
// - 最后显式用 chain_tx_v1 支付，不走 e2e 手拼协议。
func TriggerGatewayDemandPublishChainTxQuotePay(ctx context.Context, store ClientStore, env gatewayDemandPublishChainTxEnv, p PublishDemandParams) (TriggerGatewayDemandPublishChainTxQuotePayResult, error) {
	out := TriggerGatewayDemandPublishChainTxQuotePayResult{}
	if env == nil {
		err := fmt.Errorf("trigger env not initialized")
		out.Error = err.Error()
		return out, err
	}
	if store == nil {
		store = env.Store()
	}
	if store == nil {
		err := fmt.Errorf("client db is nil")
		out.Error = err.Error()
		return out, err
	}
	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" || p.ChunkCount == 0 {
		err := fmt.Errorf("invalid params")
		out.Error = err.Error()
		return out, err
	}
	out.DemandSeedHash = seedHash
	out.ChunkCount = p.ChunkCount

	gw, err := env.PickGatewayForBusiness(p.GatewayPeerID)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	if len(env.HealthyGatewayInfos()) == 0 {
		err := fmt.Errorf("no healthy gateway")
		out.Error = err.Error()
		return out, err
	}
	out.GatewayPubkeyHex = env.GatewayBusinessID(gw.ID)
	out.ClientPubkeyHex = strings.TrimSpace(env.ClientID())

	buyerAddrs := env.LocalAdvertiseAddrs()
	body := &contractmessage.DemandPublishReq{
		SeedHash:   seedHash,
		ChunkCount: p.ChunkCount,
		BuyerAddrs: buyerAddrs,
	}
	bodyRaw, err := oldproto.Marshal(body)
	if err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}

	obs.Business(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_begin", map[string]any{
		"seed_hash":   seedHash,
		"chunk_count": p.ChunkCount,
		"gateway":     out.GatewayPubkeyHex,
	})

	preflightResp, err := env.CallNodeRoute(ctx, gw.ID, ncall.CallReq{
		To:          out.GatewayPubkeyHex,
		Route:       string(contractroute.RouteBroadcastV1DemandPublish),
		ContentType: contractmessage.ContentTypeProto,
		Body:        bodyRaw,
	}, contractprotoid.ProtoBroadcastV1DemandPublish)
	if err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}
	out.PreflightCode = strings.TrimSpace(preflightResp.Code)
	if err := validateGatewayDemandPublishChainTxPreflight(preflightResp); err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}

	quoted, err := env.RequestPeerCallChainTxQuote(ctx, store, gw.ID, ncall.CallReq{
		To:          out.GatewayPubkeyHex,
		Route:       string(contractroute.RouteBroadcastV1DemandPublish),
		ContentType: contractmessage.ContentTypeProto,
		Body:        bodyRaw,
	}, &ncall.PaymentOption{
		Scheme:          ncall.PaymentSchemeChainTxV1,
		PaymentDomain:   "",
		AmountSatoshi:   0,
		Description:     string(contractroute.RouteBroadcastV1DemandPublish),
		PricingMode:     "",
		ServiceQuantity: 1,
	}, contractprotoid.ProtoBroadcastV1DemandPublish)
	if err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}
	if err := validateGatewayDemandPublishChainTxQuoteBuilt(quoted); err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}
	out.QuoteOfferHash = strings.TrimSpace(quoted.ServiceQuote.OfferHash)
	out.QuoteChargeSatoshi = quoted.ServiceQuote.ChargeAmountSatoshi
	out.QuoteExpiresAtUnix = quoted.ServiceQuote.ExpiresAtUnix
	out.QuoteServiceType = quoteServiceTypeDemandPublish
	out.ServiceQuoteHash = strings.TrimSpace(quoted.ServiceQuoteHash)

	paidResp, payErr := env.PayPeerCallWithChainTxQuote(ctx, store, gw.ID, ncall.CallReq{
		To:          out.GatewayPubkeyHex,
		Route:       string(contractroute.RouteBroadcastV1DemandPublish),
		ContentType: contractmessage.ContentTypeProto,
		Body:        bodyRaw,
	}, &ncall.PaymentOption{
		Scheme:          ncall.PaymentSchemeChainTxV1,
		PaymentDomain:   "",
		AmountSatoshi:   quoted.ServiceQuote.ChargeAmountSatoshi,
		Description:     string(contractroute.RouteBroadcastV1DemandPublish),
		PricingMode:     "",
		ServiceQuantity: 1,
	}, quoted, contractprotoid.ProtoBroadcastV1DemandPublish)
	if payErr != nil {
		if strings.Contains(strings.ToLower(payErr.Error()), "submitted_unknown_projection") {
			out.Status = "submitted_unknown_projection"
		}
		out.Error = payErr.Error()
		if len(paidResp.PaymentReceipt) > 0 {
			out.PaymentReceiptScheme = strings.TrimSpace(paidResp.PaymentReceiptScheme)
			if demandResp, txid, err := parseDemandPublishPaidResp(paidResp); err == nil {
				out.DemandID = strings.TrimSpace(demandResp.DemandID)
				out.DemandPublished = demandResp.Published
				out.PaymentTxID = txid
			}
		}
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": payErr.Error()})
		return out, payErr
	}

	demandResp, txid, err := parseDemandPublishPaidResp(paidResp)
	if err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}
	if err := validateDemandPublishPaidResp(demandResp); err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}

	out.PaymentReceiptScheme = strings.TrimSpace(paidResp.PaymentReceiptScheme)
	out.PaymentTxID = txid
	out.DemandID = strings.TrimSpace(demandResp.DemandID)
	out.DemandPublished = demandResp.Published
	out.Status = "submitted"

	if err := dbRecordDemand(ctx, store, out.DemandID, seedHash); err != nil {
		out.Error = err.Error()
		obs.Error(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_failed", map[string]any{"error": err.Error()})
		return out, err
	}

	obs.Business(ServiceName, "evt_trigger_gateway_demand_publish_chain_tx_end", map[string]any{
		"demand_id":          out.DemandID,
		"payment_txid":       out.PaymentTxID,
		"quote_offer_hash":   out.QuoteOfferHash,
		"service_quote_hash": out.ServiceQuoteHash,
		"gateway":            out.GatewayPubkeyHex,
		"status":             out.Status,
	})
	return out, nil
}

// validateGatewayDemandPublishChainTxPreflight 硬要求：必须拿到 PAYMENT_QUOTED 且 chain_tx_v1 在广告里。
func validateGatewayDemandPublishChainTxPreflight(resp ncall.CallResp) error {
	if !strings.EqualFold(strings.TrimSpace(resp.Code), "PAYMENT_QUOTED") {
		return fmt.Errorf("chain_tx_v1 not offered")
	}
	for _, opt := range resp.PaymentSchemes {
		if opt == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(opt.Scheme), ncall.PaymentSchemeChainTxV1) {
			return nil
		}
	}
	return fmt.Errorf("chain_tx_v1 not offered")
}

// validateGatewayDemandPublishChainTxQuoteBuilt 硬要求：quote 不能为空，且必须带上可支付的正式报价。
func validateGatewayDemandPublishChainTxQuoteBuilt(quoted peerCallChainTxQuoteBuilt) error {
	if len(quoted.ServiceQuoteRaw) == 0 {
		return fmt.Errorf("service quote empty")
	}
	if strings.TrimSpace(quoted.ServiceQuoteHash) == "" {
		return fmt.Errorf("service quote hash missing")
	}
	return nil
}

// parseDemandPublishPaidResp 统一把 chain_tx_v1 的支付回执和业务体拆开。
func parseDemandPublishPaidResp(callResp ncall.CallResp) (contractmessage.DemandPublishPaidResp, string, error) {
	if strings.TrimSpace(callResp.PaymentReceiptScheme) != ncall.PaymentSchemeChainTxV1 || len(callResp.PaymentReceipt) == 0 {
		return contractmessage.DemandPublishPaidResp{}, "", fmt.Errorf("payment receipt missing")
	}
	var receipt ncall.ChainTxV1Receipt
	if err := oldproto.Unmarshal(callResp.PaymentReceipt, &receipt); err != nil {
		return contractmessage.DemandPublishPaidResp{}, "", err
	}
	txID := strings.TrimSpace(receipt.PaymentTxID)
	if txID == "" {
		return contractmessage.DemandPublishPaidResp{}, "", fmt.Errorf("payment txid missing")
	}
	var demandResp contractmessage.DemandPublishPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &demandResp); err != nil {
		return contractmessage.DemandPublishPaidResp{}, "", err
	}
	return demandResp, txID, nil
}
