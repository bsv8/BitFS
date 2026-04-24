package poolcore

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
)

const (
	QuoteServiceTypeListenCycle = "listen_cycle_fee"

	ServiceOfferPricingModeFixedPrice       = "fixed_price"
	ServiceOfferPricingModeBudgetForService = "budget_for_service"

	defaultServiceQuoteTTLSeconds = uint32(30)
)

type ServiceQuoteBuildInput struct {
	Offer               payflow.ServiceOffer
	ChargeAmountSatoshi uint64
	QuoteTTLSeconds     uint32
}

func ParseAndVerifyServiceQuote(raw []byte, serviceNodePub *ec.PublicKey) (payflow.ServiceQuote, string, error) {
	quote, err := payflow.UnmarshalServiceQuote(raw)
	if err != nil {
		return payflow.ServiceQuote{}, "", err
	}
	if err := payflow.VerifyServiceQuoteSignature(quote, serviceNodePub); err != nil {
		return payflow.ServiceQuote{}, "", err
	}
	hash, err := payflow.HashServiceQuote(quote)
	if err != nil {
		return payflow.ServiceQuote{}, "", err
	}
	return quote, hash, nil
}

func HashServiceParamsPayload(raw []byte) string {
	return payflow.HashPayloadBytes(raw)
}

func NormalizeServiceOfferPricingMode(raw string) string {
	return strings.TrimSpace(raw)
}

func ServiceOfferFromRow(row ServiceOfferRow) payflow.ServiceOffer {
	return payflow.ServiceOffer{
		ServiceType:          strings.TrimSpace(row.ServiceType),
		ServiceNodePubkeyHex: strings.ToLower(strings.TrimSpace(row.ServiceNodePubkeyHex)),
		ClientPubkeyHex:      strings.ToLower(strings.TrimSpace(row.ClientPubkeyHex)),
		RequestParams:        append([]byte(nil), row.RequestParams...),
		CreatedAtUnix:        row.CreatedAtUnix,
	}.Normalize()
}

func ValidateServiceQuoteBinding(quote payflow.ServiceQuote, offer payflow.ServiceOffer, expectedServiceNodePubkeyHex string, expectedClientID string, expectedServiceType string, requestParamsPayload []byte, nowUnix int64) error {
	offer = offer.Normalize()
	if err := offer.Validate(); err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(quote.OfferHash), mustHashServiceOffer(offer)) {
		return fmt.Errorf("service quote offer_hash mismatch")
	}
	if expectedServiceNodePubkeyHex != "" && !strings.EqualFold(strings.TrimSpace(offer.ServiceNodePubkeyHex), strings.TrimSpace(expectedServiceNodePubkeyHex)) {
		return fmt.Errorf("service offer service_node_pubkey_hex mismatch")
	}
	if expectedClientID != "" && !strings.EqualFold(strings.TrimSpace(offer.ClientPubkeyHex), NormalizeClientIDLoose(expectedClientID)) {
		return fmt.Errorf("service offer client_pubkey_hex mismatch")
	}
	if expectedServiceType != "" && !strings.EqualFold(strings.TrimSpace(offer.ServiceType), strings.TrimSpace(expectedServiceType)) {
		return fmt.Errorf("service offer service_type mismatch")
	}
	if requestParamsPayload != nil && string(offer.RequestParams) != string(requestParamsPayload) {
		return fmt.Errorf("service offer request_params mismatch")
	}
	if nowUnix > 0 && quote.ExpiresAtUnix > 0 && nowUnix > quote.ExpiresAtUnix {
		return fmt.Errorf("service quote expired")
	}
	return nil
}

func mustHashServiceOffer(offer payflow.ServiceOffer) string {
	hash, err := payflow.HashServiceOffer(offer)
	if err != nil {
		return ""
	}
	return hash
}

func (s *GatewayService) BuildServiceQuote(input ServiceQuoteBuildInput) (payflow.ServiceQuote, []byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.DB == nil {
		return payflow.ServiceQuote{}, nil, "", fmt.Errorf("db not initialized")
	}
	offer := input.Offer.Normalize()
	if err := offer.Validate(); err != nil {
		return payflow.ServiceQuote{}, nil, "", err
	}
	if input.ChargeAmountSatoshi == 0 {
		return payflow.ServiceQuote{}, nil, "", fmt.Errorf("charge_amount_satoshi required")
	}
	serverActor, err := BuildActor("service_quote", strings.TrimSpace(s.ServerPrivHex), s.IsMainnet)
	if err != nil {
		return payflow.ServiceQuote{}, nil, "", err
	}
	if !strings.EqualFold(strings.TrimSpace(offer.ServiceNodePubkeyHex), strings.TrimSpace(serverActor.PubHex)) {
		return payflow.ServiceQuote{}, nil, "", fmt.Errorf("service offer service_node_pubkey_hex mismatch")
	}
	offerHash, err := payflow.HashServiceOffer(offer)
	if err != nil {
		return payflow.ServiceQuote{}, nil, "", err
	}
	nowUnix := time.Now().Unix()
	ttlSeconds := input.QuoteTTLSeconds
	if ttlSeconds == 0 {
		ttlSeconds = defaultServiceQuoteTTLSeconds
	}
	signed, err := payflow.SignServiceQuote(payflow.ServiceQuote{
		OfferHash:           offerHash,
		ChargeAmountSatoshi: input.ChargeAmountSatoshi,
		ExpiresAtUnix:       nowUnix + int64(ttlSeconds),
	}, serverActor.PrivKey)
	if err != nil {
		return payflow.ServiceQuote{}, nil, "", err
	}
	if _, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, UpsertServiceOffer(db, ServiceOfferRow{
			OfferHash:              offerHash,
			ServiceType:            offer.ServiceType,
			ServiceNodePubkeyHex:   offer.ServiceNodePubkeyHex,
			ClientPubkeyHex:        offer.ClientPubkeyHex,
			RequestParams:          offer.RequestParams,
			CreatedAtUnix:          offer.CreatedAtUnix,
			LastQuotedAtUnix:       nowUnix,
			LastQuotedAmountSat:    input.ChargeAmountSatoshi,
			LastQuoteExpiresAtUnix: signed.ExpiresAtUnix,
		})
	}); err != nil {
		return payflow.ServiceQuote{}, nil, "", err
	}
	raw, err := payflow.MarshalServiceQuote(signed)
	if err != nil {
		return payflow.ServiceQuote{}, nil, "", err
	}
	return signed, raw, "accepted", nil
}

func LoadOfferByHash(db *sql.DB, offerHash string) (payflow.ServiceOffer, bool, error) {
	row, found, err := LoadServiceOfferByHash(db, offerHash)
	if err != nil {
		return payflow.ServiceOffer{}, false, err
	}
	if !found {
		return payflow.ServiceOffer{}, false, nil
	}
	offer := ServiceOfferFromRow(row)
	return offer, true, offer.Validate()
}

func ListenOfferBudgetToDurationSeconds(proposedPayment uint64, minimumPayment uint64, minimumDurationSeconds uint32) (uint64, error) {
	const maxGrantedDurationSeconds = uint64(^uint32(0))
	if minimumPayment == 0 {
		return 0, fmt.Errorf("minimum payment required")
	}
	if minimumDurationSeconds == 0 {
		return 0, fmt.Errorf("minimum duration required")
	}
	if proposedPayment == 0 {
		return 0, fmt.Errorf("proposed payment required")
	}
	product := proposedPayment * uint64(minimumDurationSeconds)
	if proposedPayment != 0 && product/proposedPayment != uint64(minimumDurationSeconds) {
		return maxGrantedDurationSeconds, nil
	}
	duration := product / minimumPayment
	if duration == 0 {
		duration = 1
	}
	if duration > maxGrantedDurationSeconds {
		return maxGrantedDurationSeconds, nil
	}
	return duration, nil
}
