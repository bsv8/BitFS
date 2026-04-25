package gatewayclient

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	oldproto "github.com/golang/protobuf/proto"
)

type DemandPublishParams struct {
	SeedHash      string
	ChunkCount    uint32
	GatewayPeerID string
}

// DemandPublishResult 是 demand publish 的结果。
type DemandPublishResult struct {
	ClientPubkeyHex      string
	GatewayPubkeyHex     string
	DemandSeedHash       string
	ChunkCount           uint32
	PreflightCode        string
	QuoteOfferHash       string
	QuoteChargeSatoshi   uint64
	QuoteExpiresAtUnix   int64
	QuoteServiceType     string
	ServiceQuoteHash     string
	PaymentReceiptScheme string
	PaymentTxID          string
	DemandID             string
	DemandPublished      bool
	Status               string
	Error                string
}

// PublishDemand 发布需求到 gateway。
func (s *service) PublishDemand(ctx context.Context, p DemandPublishParams) (DemandPublishResult, error) {
	if err := s.requireHost(); err != nil {
		return DemandPublishResult{}, err
	}
	out := DemandPublishResult{}

	seedHash := strings.ToLower(strings.TrimSpace(p.SeedHash))
	if seedHash == "" || p.ChunkCount == 0 {
		return out, fmt.Errorf("seed_hash and chunk_count are required")
	}
	out.DemandSeedHash = seedHash
	out.ChunkCount = p.ChunkCount
	out.ClientPubkeyHex = s.host.NodePubkeyHex()

	gateway, err := s.pickGateway(p.GatewayPeerID)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	out.GatewayPubkeyHex = strings.ToLower(strings.TrimSpace(gateway.Pubkey))

	_, gatewayPub, err := gatewayPeerFromHost(s.host, gateway.Pubkey)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}

	buyerAddrs := s.host.LocalAdvertiseAddrs()
	body := &contractmessage.DemandPublishReq{
		SeedHash:   seedHash,
		ChunkCount: p.ChunkCount,
		BuyerAddrs: buyerAddrs,
	}
	bodyRaw, err := marshalProto(body)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}

	preflightResp, err := s.quotePeer(ctx, gateway.Pubkey, ProtoBroadcastV1DemandPublish, bodyRaw)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	preflight, err := callRespToResult(preflightResp)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	if preflight == nil {
		out.Error = "gateway call failed"
		return out, fmt.Errorf("gateway call failed")
	}
	out.PreflightCode = strings.TrimSpace(preflight.Code)
	if err := validateDemandPublishPreflight(preflight); err != nil {
		out.Error = err.Error()
		return out, err
	}

	quoteResp, err := s.quotePeer(ctx, gateway.Pubkey, ProtoBroadcastV1DemandPublish, bodyRaw)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	quoteCall, err := callRespToResult(quoteResp)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	if quoteCall == nil {
		out.Error = "gateway quote missing"
		return out, fmt.Errorf("gateway quote missing")
	}
	quote, quoteHash, err := parseDemandPublishQuote(out.ClientPubkeyHex, gatewayPub, bodyRaw, quoteCall)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	out.QuoteOfferHash = quote.OfferHash
	out.QuoteChargeSatoshi = quote.ChargeAmountSatoshi
	out.QuoteExpiresAtUnix = quote.ExpiresAtUnix
	out.QuoteServiceType = QuoteServiceTypeDemandPublish
	out.ServiceQuoteHash = quoteHash

	paidResp, err := s.payQuotedCall(ctx, gateway.Pubkey, ProtoBroadcastV1DemandPublish, bodyRaw, ncall.PaymentSchemeChainTxV1, quoteResp.ServiceQuote)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	paidCall, err := callRespToResult(paidResp)
	if err != nil {
		out.Error = err.Error()
		return out, err
	}
	if paidCall == nil {
		out.Error = "gateway payment response missing"
		return out, fmt.Errorf("gateway payment response missing")
	}

	if strings.TrimSpace(paidCall.PaymentReceiptScheme) != ncall.PaymentSchemeChainTxV1 || len(paidCall.PaymentReceipt) == 0 {
		out.Error = "payment receipt missing"
		return out, fmt.Errorf("payment receipt missing")
	}
	var receipt ncall.ChainTxV1Receipt
	if err := oldproto.Unmarshal(paidCall.PaymentReceipt, &receipt); err != nil {
		out.Error = err.Error()
		return out, err
	}
	out.PaymentReceiptScheme = strings.TrimSpace(paidCall.PaymentReceiptScheme)
	out.PaymentTxID = strings.TrimSpace(receipt.PaymentTxID)
	if out.PaymentTxID == "" {
		out.Error = "payment txid missing"
		return out, fmt.Errorf("payment txid missing")
	}

	var publishResp contractmessage.DemandPublishPaidResp
	if err := oldproto.Unmarshal(paidCall.Body, &publishResp); err != nil {
		out.Error = err.Error()
		return out, err
	}
	if !publishResp.Success {
		msg := strings.TrimSpace(publishResp.Error)
		if msg == "" {
			msg = "gateway demand publish rejected"
		}
		out.Status = strings.TrimSpace(publishResp.Status)
		out.Error = msg
		return out, fmt.Errorf("gateway demand publish rejected: status=%s error=%s", out.Status, msg)
	}

	out.DemandID = strings.TrimSpace(publishResp.DemandID)
	out.DemandPublished = publishResp.Published
	out.Status = strings.TrimSpace(publishResp.Status)
	return out, nil
}

func validateDemandPublishPreflight(resp *ncall.CallResp) error {
	if resp == nil {
		return fmt.Errorf("preflight response missing")
	}
	if !strings.EqualFold(strings.TrimSpace(resp.Code), "PAYMENT_QUOTED") {
		return fmt.Errorf("payment quote unavailable")
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

func parseDemandPublishQuote(clientPubkeyHex string, gatewayPub *ec.PublicKey, bodyRaw []byte, resp *ncall.CallResp) (payflow.ServiceQuote, string, error) {
	if resp == nil {
		return payflow.ServiceQuote{}, "", fmt.Errorf("quote response missing")
	}
	if len(resp.ServiceQuote) == 0 {
		return payflow.ServiceQuote{}, "", fmt.Errorf("service quote empty")
	}
	quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(resp.ServiceQuote, gatewayPub)
	if err != nil {
		return payflow.ServiceQuote{}, "", err
	}
	serviceNodePubkeyHex := ""
	if gatewayPub != nil {
		serviceNodePubkeyHex = strings.ToLower(hex.EncodeToString(gatewayPub.Compressed()))
	}
	if err := poolcore.ValidateServiceQuoteBinding(quote, payflow.ServiceOffer{
		ServiceType:          QuoteServiceTypeDemandPublish,
		ServiceNodePubkeyHex: serviceNodePubkeyHex,
		ClientPubkeyHex:      strings.ToLower(strings.TrimSpace(clientPubkeyHex)),
		RequestParams:        append([]byte(nil), bodyRaw...),
		CreatedAtUnix:        time.Now().Unix(),
	}, serviceNodePubkeyHex, strings.ToLower(strings.TrimSpace(clientPubkeyHex)), QuoteServiceTypeDemandPublish, bodyRaw, time.Now().Unix()); err != nil {
		return payflow.ServiceQuote{}, "", err
	}
	return quote, quoteHash, nil
}
