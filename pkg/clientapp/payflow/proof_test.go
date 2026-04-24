package payflow

import (
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
)

func TestProofRoundTripAndSignature(t *testing.T) {
	offer := ServiceOffer{
		ServiceType:          "domain.v1.query",
		ServiceNodePubkeyHex: "03aa",
		ClientPubkeyHex:      "02bb",
		RequestParams:        []byte(`["alice.bit"]`),
		CreatedAtUnix:        99,
	}
	rawOffer, err := MarshalServiceOffer(offer)
	if err != nil {
		t.Fatalf("MarshalServiceOffer() error = %v", err)
	}
	gotOffer, err := UnmarshalServiceOffer(rawOffer)
	if err != nil {
		t.Fatalf("UnmarshalServiceOffer() error = %v", err)
	}
	offerHash, err := HashServiceOffer(gotOffer)
	if err != nil {
		t.Fatalf("HashServiceOffer() error = %v", err)
	}
	if offerHash == "" {
		t.Fatalf("offer hash missing")
	}

	gatewayPriv, err := ec.PrivateKeyFromHex("6666666666666666666666666666666666666666666666666666666666666666")
	if err != nil {
		t.Fatalf("gateway PrivateKeyFromHex() error = %v", err)
	}
	signedQuote, err := SignServiceQuote(ServiceQuote{
		OfferHash:           offerHash,
		ChargeAmountSatoshi: 1,
		ExpiresAtUnix:       130,
	}, gatewayPriv)
	if err != nil {
		t.Fatalf("SignServiceQuote() error = %v", err)
	}
	rawQuote, err := MarshalServiceQuote(signedQuote)
	if err != nil {
		t.Fatalf("MarshalServiceQuote() error = %v", err)
	}
	gotQuote, err := UnmarshalServiceQuote(rawQuote)
	if err != nil {
		t.Fatalf("UnmarshalServiceQuote() error = %v", err)
	}
	if err := VerifyServiceQuoteSignature(gotQuote, gatewayPriv.PubKey()); err != nil {
		t.Fatalf("VerifyServiceQuoteSignature() error = %v", err)
	}
	quoteHash, err := HashServiceQuote(gotQuote)
	if err != nil {
		t.Fatalf("HashServiceQuote() error = %v", err)
	}
	if quoteHash == "" {
		t.Fatalf("quote hash missing")
	}

	intent := ChargeIntent{
		Domain:              "bitcast-gateway",
		Target:              "domain/query",
		GatewayPubkeyHex:    "03aa",
		ClientPubkeyHex:     "02bb",
		SpendTxID:           "spend_1",
		GatewayQuoteHash:    quoteHash,
		ChargeReason:        "domain_query_fee",
		ChargeAmountSatoshi: 1,
		SequenceNumber:      2,
		ServerAmountBefore:  5,
		ServerAmountAfter:   6,
		ServiceDeadlineUnix: 123,
	}
	rawIntent, err := MarshalIntent(intent)
	if err != nil {
		t.Fatalf("MarshalIntent() error = %v", err)
	}
	gotIntent, err := UnmarshalIntent(rawIntent)
	if err != nil {
		t.Fatalf("UnmarshalIntent() error = %v", err)
	}
	intentHash, err := HashIntent(gotIntent)
	if err != nil {
		t.Fatalf("HashIntent() error = %v", err)
	}
	if intentHash == "" {
		t.Fatalf("intent hash missing")
	}

	priv, err := ec.PrivateKeyFromHex("7777777777777777777777777777777777777777777777777777777777777777")
	if err != nil {
		t.Fatalf("PrivateKeyFromHex() error = %v", err)
	}
	updateHash, err := UpdateTemplateHashFromTxHex("01000000000000000000")
	if err != nil {
		t.Fatalf("UpdateTemplateHashFromTxHex() error = %v", err)
	}
	commit := ClientCommit{
		IntentHash:          intentHash,
		ClientPubkeyHex:     "03bc71c8ca6ba915e4b5170c5bd9d65e6c341a0dc1a4c72cb3ec38e68e6c10f14d",
		SpendTxID:           "spend_1",
		SequenceNumber:      2,
		ServerAmountBefore:  5,
		ChargeAmountSatoshi: 1,
		ServerAmountAfter:   6,
		UpdateTemplateHash:  updateHash,
		CreatedAtUnix:       100,
	}
	sig, err := SignClientCommit(commit, priv)
	if err != nil {
		t.Fatalf("SignClientCommit() error = %v", err)
	}
	if err := VerifyClientCommitSignature(commit, sig, priv.PubKey()); err != nil {
		t.Fatalf("VerifyClientCommitSignature() error = %v", err)
	}
	signedCommit, err := SignClientCommitEnvelope(commit, priv)
	if err != nil {
		t.Fatalf("SignClientCommitEnvelope() error = %v", err)
	}
	parsedCommit, err := VerifySignedClientCommit(signedCommit, priv.PubKey())
	if err != nil {
		t.Fatalf("VerifySignedClientCommit() error = %v", err)
	}
	if parsedCommit.IntentHash != commit.IntentHash || parsedCommit.UpdateTemplateHash != commit.UpdateTemplateHash {
		t.Fatalf("unexpected parsed commit: %+v", parsedCommit)
	}
	commitHash, err := HashClientCommit(commit)
	if err != nil {
		t.Fatalf("HashClientCommit() error = %v", err)
	}

	accepted := AcceptedCharge{
		IntentHash:          intentHash,
		ClientCommitHash:    commitHash,
		SpendTxID:           "spend_1",
		SequenceNumber:      2,
		ServerAmountBefore:  5,
		ChargeAmountSatoshi: 1,
		ServerAmountAfter:   6,
		ServiceDeadlineUnix: 123,
	}
	state, err := BuildNextProofState(ProofState{}, accepted)
	if err != nil {
		t.Fatalf("BuildNextProofState() error = %v", err)
	}
	rawState, err := MarshalProofState(state)
	if err != nil {
		t.Fatalf("MarshalProofState() error = %v", err)
	}
	parsed, err := UnmarshalProofState(rawState)
	if err != nil {
		t.Fatalf("UnmarshalProofState() error = %v", err)
	}
	if parsed.AcceptedTipHash == "" || parsed.AcceptedTipHash != parsed.LastAcceptedChargeHash {
		t.Fatalf("unexpected parsed state: %+v", parsed)
	}
}
