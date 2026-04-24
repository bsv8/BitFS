package poolcore

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
)

func buildPayConfirmProofPayload(db *sql.DB, row GatewaySessionRow, req PayConfirmReq, updatedTxHex string) ([]byte, []byte, error) {
	if len(req.ProofIntent) == 0 && len(req.SignedProofCommit) == 0 {
		return nil, nil, nil
	}
	if len(req.ProofIntent) == 0 || len(req.SignedProofCommit) == 0 {
		return nil, nil, fmt.Errorf("proof intent and signed_proof_commit must be provided together")
	}
	intent, err := payflow.UnmarshalIntent(req.ProofIntent)
	if err != nil {
		return nil, nil, err
	}
	_, _, quoteHash, err := validateServiceQuoteAgainstPay(db, row, req)
	if err != nil {
		return nil, nil, err
	}
	if err := validateProofIntentAgainstPay(row, req, intent, quoteHash); err != nil {
		return nil, nil, err
	}
	clientPub, err := ec.PublicKeyFromString(strings.TrimSpace(row.ClientBSVCompressedPubHex))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid stored client pubkey: %w", err)
	}
	commit, err := payflow.VerifySignedClientCommit(req.SignedProofCommit, clientPub)
	if err != nil {
		return nil, nil, err
	}
	if err := validateProofCommitAgainstPay(row, req, updatedTxHex, intent, commit); err != nil {
		return nil, nil, err
	}
	prevState, found, err := payflow.ExtractProofStateFromTxHex(row.CurrentTxHex)
	if err != nil {
		return nil, nil, err
	}
	if found && !strings.EqualFold(prevState.SpendTxID, row.SpendTxID) {
		return nil, nil, fmt.Errorf("previous proof state spend_txid mismatch")
	}
	intentHash, err := payflow.HashIntent(intent)
	if err != nil {
		return nil, nil, err
	}
	commitHash, err := payflow.HashClientCommit(commit)
	if err != nil {
		return nil, nil, err
	}
	accepted := payflow.AcceptedCharge{
		IntentHash:          intentHash,
		ClientCommitHash:    commitHash,
		SpendTxID:           row.SpendTxID,
		SequenceNumber:      req.SequenceNumber,
		ServerAmountBefore:  row.ServerAmountSat,
		ChargeAmountSatoshi: req.ChargeAmountSatoshi,
		ServerAmountAfter:   req.ServerAmount,
		ServiceDeadlineUnix: intent.ServiceDeadlineUnix,
		PrevAcceptedHash:    prevState.AcceptedTipHash,
	}
	state, err := payflow.BuildNextProofState(prevState, accepted)
	if err != nil {
		return nil, nil, err
	}
	payload, err := payflow.MarshalProofState(state)
	if err != nil {
		return nil, nil, err
	}
	acceptedRaw, err := payflow.MarshalAcceptedCharge(accepted)
	if err != nil {
		return nil, nil, err
	}
	return payload, acceptedRaw, nil
}

func validateServiceQuoteAgainstPay(db *sql.DB, row GatewaySessionRow, req PayConfirmReq) (payflow.ServiceOffer, payflow.ServiceQuote, string, error) {
	if len(req.ServiceQuote) == 0 {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", nil
	}
	gatewayPub, err := ec.PublicKeyFromString(strings.TrimSpace(row.ServerBSVCompressedPubHex))
	if err != nil {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", fmt.Errorf("invalid stored gateway pubkey: %w", err)
	}
	quote, quoteHash, err := ParseAndVerifyServiceQuote(req.ServiceQuote, gatewayPub)
	if err != nil {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", err
	}
	offer, found, err := LoadOfferByHash(db, quote.OfferHash)
	if err != nil {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", err
	}
	if !found {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", fmt.Errorf("service offer not found")
	}
	if err := ValidateServiceQuoteBinding(quote, offer, row.ServerBSVCompressedPubHex, row.ClientBSVCompressedPubHex, "", nil, time.Now().Unix()); err != nil {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", err
	}
	if quote.ChargeAmountSatoshi != req.ChargeAmountSatoshi {
		return payflow.ServiceOffer{}, payflow.ServiceQuote{}, "", fmt.Errorf("service quote charge_amount mismatch")
	}
	return offer, quote, quoteHash, nil
}

func validateProofIntentAgainstPay(row GatewaySessionRow, req PayConfirmReq, intent payflow.ChargeIntent, quoteHash string) error {
	if !strings.EqualFold(strings.TrimSpace(intent.SpendTxID), row.SpendTxID) {
		return fmt.Errorf("proof intent spend_txid mismatch")
	}
	if strings.TrimSpace(intent.GatewayPubkeyHex) != "" && !strings.EqualFold(strings.TrimSpace(intent.GatewayPubkeyHex), row.ServerBSVCompressedPubHex) {
		return fmt.Errorf("proof intent gateway_pubkey_hex mismatch")
	}
	if strings.TrimSpace(intent.ClientPubkeyHex) != "" && !strings.EqualFold(strings.TrimSpace(intent.ClientPubkeyHex), row.ClientBSVCompressedPubHex) {
		return fmt.Errorf("proof intent client_pubkey_hex mismatch")
	}
	if quoteHash != "" && !strings.EqualFold(strings.TrimSpace(intent.GatewayQuoteHash), strings.TrimSpace(quoteHash)) {
		return fmt.Errorf("proof intent gateway_quote_hash mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(intent.ChargeReason), strings.TrimSpace(req.ChargeReason)) {
		return fmt.Errorf("proof intent charge_reason mismatch")
	}
	if intent.ChargeAmountSatoshi != req.ChargeAmountSatoshi {
		return fmt.Errorf("proof intent charge_amount mismatch")
	}
	if intent.SequenceNumber != req.SequenceNumber {
		return fmt.Errorf("proof intent sequence_number mismatch")
	}
	if intent.ServerAmountBefore != row.ServerAmountSat {
		return fmt.Errorf("proof intent server_amount_before mismatch")
	}
	if intent.ServerAmountAfter != req.ServerAmount {
		return fmt.Errorf("proof intent server_amount_after mismatch")
	}
	return nil
}

func validateProofCommitAgainstPay(row GatewaySessionRow, req PayConfirmReq, updatedTxHex string, intent payflow.ChargeIntent, commit payflow.ClientCommit) error {
	intentHash, err := payflow.HashIntent(intent)
	if err != nil {
		return err
	}
	if !strings.EqualFold(commit.IntentHash, intentHash) {
		return fmt.Errorf("proof commit intent_hash mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(commit.ClientPubkeyHex), row.ClientBSVCompressedPubHex) {
		return fmt.Errorf("proof commit client_pubkey_hex mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(commit.SpendTxID), row.SpendTxID) {
		return fmt.Errorf("proof commit spend_txid mismatch")
	}
	if commit.SequenceNumber != req.SequenceNumber {
		return fmt.Errorf("proof commit sequence_number mismatch")
	}
	if commit.ServerAmountBefore != row.ServerAmountSat {
		return fmt.Errorf("proof commit server_amount_before mismatch")
	}
	if commit.ChargeAmountSatoshi != req.ChargeAmountSatoshi {
		return fmt.Errorf("proof commit charge_amount mismatch")
	}
	if commit.ServerAmountAfter != req.ServerAmount {
		return fmt.Errorf("proof commit server_amount_after mismatch")
	}
	templateHash, err := payflow.UpdateTemplateHashFromTxHex(updatedTxHex)
	if err != nil {
		return err
	}
	if !strings.EqualFold(commit.UpdateTemplateHash, templateHash) {
		return fmt.Errorf("proof commit update_template_hash mismatch")
	}
	return nil
}

func publicKeyHexFromEC(pub *ec.PublicKey) string {
	if pub == nil {
		return ""
	}
	return strings.ToLower(hex.EncodeToString(pub.Compressed()))
}
