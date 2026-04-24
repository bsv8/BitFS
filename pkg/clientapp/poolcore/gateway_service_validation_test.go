package poolcore

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
	_ "modernc.org/sqlite"
)

type stubChainForValidation struct {
	tip uint32
}

func (s stubChainForValidation) GetUTXOs(address string) ([]UTXO, error) { return nil, nil }
func (s stubChainForValidation) GetTipHeight() (uint32, error)           { return s.tip, nil }
func (s stubChainForValidation) Broadcast(txHex string) (string, error)  { return "txid_stub", nil }

func TestValidateBaseTxMatchesSessionSpend(t *testing.T) {
	lock, err := script.NewFromHex("51")
	if err != nil {
		t.Fatalf("build lock script failed: %v", err)
	}
	baseTx := tx.NewTransaction()
	baseTx.Outputs = append(baseTx.Outputs, &tx.TransactionOutput{
		Satoshis:      1000,
		LockingScript: lock,
	})
	baseID := baseTx.TxID().String()
	h, err := chainhash.NewHashFromHex(baseID)
	if err != nil {
		t.Fatalf("hash base txid failed: %v", err)
	}
	spendTx := tx.NewTransaction()
	spendTx.Inputs = append(spendTx.Inputs, &tx.TransactionInput{
		SourceTXID:       h,
		SourceTxOutIndex: 0,
		SequenceNumber:   1,
	})
	spendTx.Outputs = append(spendTx.Outputs, &tx.TransactionOutput{
		Satoshis:      900,
		LockingScript: lock,
	})

	if err := validateBaseTxMatchesSessionSpend(baseTx, spendTx.Hex()); err != nil {
		t.Fatalf("validate match failed: %v", err)
	}

	spendTx.Inputs[0].SourceTxOutIndex = 1
	if err := validateBaseTxMatchesSessionSpend(baseTx, spendTx.Hex()); err == nil {
		t.Fatalf("expect vout mismatch error")
	}
}

func TestGatewayServicePayConfirmRejectListenQuoteAmountMismatch(t *testing.T) {
	db := openTimelineTestDB(t)
	serverPriv, err := ec.PrivateKeyFromHex("6666666666666666666666666666666666666666666666666666666666666666")
	if err != nil {
		t.Fatalf("server PrivateKeyFromHex failed: %v", err)
	}
	clientPriv, err := ec.PrivateKeyFromHex("7777777777777777777777777777777777777777777777777777777777777777")
	if err != nil {
		t.Fatalf("client PrivateKeyFromHex failed: %v", err)
	}
	row := GatewaySessionRow{
		SpendTxID:                 "tx_listen_fee_mismatch",
		ClientID:                  "client_a",
		ClientBSVCompressedPubHex: hex.EncodeToString(clientPriv.PubKey().Compressed()),
		ServerBSVCompressedPubHex: hex.EncodeToString(serverPriv.PubKey().Compressed()),
		InputAmountSat:            1000,
		PoolAmountSat:             1000,
		SpendTxFeeSat:             1,
		Sequence:                  1,
		ServerAmountSat:           100,
		ClientAmountSat:           899,
		BaseTxID:                  "base_a",
		FinalTxID:                 "",
		BaseTxHex:                 "00",
		CurrentTxHex:              "01000000000064000000",
		LifecycleState:            "active",
	}
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB: db,
		Chain: stubChainForValidation{
			tip: 10,
		},
		ServerPrivHex: strings.Repeat("66", 32),
		Params: GatewayParams{
			BillingCycleSeconds:         60,
			SingleCycleFeeSatoshi:       50,
			PayRejectBeforeExpiryBlocks: 1,
		},
	}
	nowUnix := time.Now().Unix()
	offer := payflow.ServiceOffer{
		ServiceType:          QuoteServiceTypeListenCycle,
		ServiceNodePubkeyHex: row.ServerBSVCompressedPubHex,
		ClientPubkeyHex:      row.ClientBSVCompressedPubHex,
		RequestParams:        []byte("params"),
		CreatedAtUnix:        nowUnix,
	}
	offerHash, err := payflow.HashServiceOffer(offer)
	if err != nil {
		t.Fatalf("hash service offer failed: %v", err)
	}
	if err := UpsertServiceOffer(db, ServiceOfferRow{
		OfferHash:            offerHash,
		ServiceType:          offer.ServiceType,
		ServiceNodePubkeyHex: offer.ServiceNodePubkeyHex,
		ClientPubkeyHex:      offer.ClientPubkeyHex,
		RequestParams:        offer.RequestParams,
		CreatedAtUnix:        offer.CreatedAtUnix,
	}); err != nil {
		t.Fatalf("upsert service offer failed: %v", err)
	}
	signedQuote, err := payflow.SignServiceQuote(payflow.ServiceQuote{
		OfferHash:           offerHash,
		ChargeAmountSatoshi: 50,
		ExpiresAtUnix:       nowUnix + 60,
	}, serverPriv)
	if err != nil {
		t.Fatalf("sign service quote failed: %v", err)
	}
	rawQuote, err := payflow.MarshalServiceQuote(signedQuote)
	if err != nil {
		t.Fatalf("marshal service quote failed: %v", err)
	}
	resp, err := svc.PayConfirm(PayConfirmReq{
		SpendTxID:           row.SpendTxID,
		SequenceNumber:      2,
		ServerAmount:        row.ServerAmountSat + 1,
		Fee:                 row.SpendTxFeeSat,
		ChargeReason:        "listen_cycle_fee",
		ChargeAmountSatoshi: 1,
		ServiceQuote:        rawQuote,
	})
	if err != nil {
		t.Fatalf("pay confirm returned unexpected error: %v", err)
	}
	if resp.Success {
		t.Fatalf("pay confirm should be rejected")
	}
	if !strings.Contains(strings.ToLower(resp.Error), "service quote charge_amount mismatch") {
		t.Fatalf("unexpected reject error: %s", resp.Error)
	}

	after, found, err := LoadSessionBySpendTxID(db, row.SpendTxID)
	if err != nil {
		t.Fatalf("reload session failed: %v", err)
	}
	if !found {
		t.Fatalf("session should exist")
	}
	if after.Sequence != row.Sequence || after.ServerAmountSat != row.ServerAmountSat {
		t.Fatalf("session should remain unchanged: seq=%d/%d server=%d/%d", after.Sequence, row.Sequence, after.ServerAmountSat, row.ServerAmountSat)
	}
}

func TestGatewayServicePayConfirmRejectPoolInsufficient(t *testing.T) {
	db := openTimelineTestDB(t)
	row := GatewaySessionRow{
		SpendTxID:                 "tx_pool_insufficient",
		ClientID:                  "client_pool_insufficient",
		ClientBSVCompressedPubHex: "02aa",
		ServerBSVCompressedPubHex: "03bb",
		InputAmountSat:            5,
		PoolAmountSat:             5,
		SpendTxFeeSat:             1,
		Sequence:                  4,
		ServerAmountSat:           4,
		ClientAmountSat:           0,
		BaseTxID:                  "base_pool_insufficient",
		FinalTxID:                 "",
		BaseTxHex:                 "00",
		CurrentTxHex:              "01000000000064000000",
		LifecycleState:            "active",
	}
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB: db,
		Chain: stubChainForValidation{
			tip: 10,
		},
	}
	resp, err := svc.PayConfirm(PayConfirmReq{
		SpendTxID:           row.SpendTxID,
		SequenceNumber:      5,
		ServerAmount:        5,
		Fee:                 row.SpendTxFeeSat,
		ChargeReason:        "domain_query_fee",
		ChargeAmountSatoshi: 1,
	})
	if err != nil {
		t.Fatalf("pay confirm returned unexpected error: %v", err)
	}
	if resp.Success {
		t.Fatalf("pay confirm should be rejected")
	}
	if !strings.Contains(strings.ToLower(resp.Error), "pool cannot cover charge") {
		t.Fatalf("unexpected reject error: %s", resp.Error)
	}
}

func TestLoadPreferredSessionByClientIDPrefersActive(t *testing.T) {
	db := openTimelineTestDB(t)
	if err := InsertSession(db, GatewaySessionRow{
		SpendTxID:                 "tx_closed",
		ClientID:                  "client_pref",
		ClientBSVCompressedPubHex: "02aa",
		ServerBSVCompressedPubHex: "03bb",
		InputAmountSat:            1000,
		PoolAmountSat:             1000,
		SpendTxFeeSat:             1,
		Sequence:                  1,
		ServerAmountSat:           1,
		ClientAmountSat:           998,
		BaseTxID:                  "base_closed",
		FinalTxID:                 "final_closed",
		BaseTxHex:                 "00",
		CurrentTxHex:              "00",
		LifecycleState:            "closed",
	}); err != nil {
		t.Fatalf("insert closed session failed: %v", err)
	}
	if err := InsertSession(db, GatewaySessionRow{
		SpendTxID:                 "tx_active",
		ClientID:                  "client_pref",
		ClientBSVCompressedPubHex: "02aa",
		ServerBSVCompressedPubHex: "03bb",
		InputAmountSat:            1000,
		PoolAmountSat:             1000,
		SpendTxFeeSat:             1,
		Sequence:                  2,
		ServerAmountSat:           2,
		ClientAmountSat:           997,
		BaseTxID:                  "base_active",
		FinalTxID:                 "",
		BaseTxHex:                 "00",
		CurrentTxHex:              "00",
		LifecycleState:            "active",
	}); err != nil {
		t.Fatalf("insert active session failed: %v", err)
	}

	// 模拟旧池在 rotate 后被晚一步 close，更新时间更晚。
	if _, err := db.Exec(`UPDATE fee_pool_sessions SET updated_at_unix=updated_at_unix+100 WHERE spend_txid='tx_closed'`); err != nil {
		t.Fatalf("bump closed updated_at failed: %v", err)
	}

	row, found, err := LoadPreferredSessionByClientID(db, "client_pref")
	if err != nil {
		t.Fatalf("load preferred failed: %v", err)
	}
	if !found {
		t.Fatalf("preferred session should exist")
	}
	if row.SpendTxID != "tx_active" {
		t.Fatalf("preferred session mismatch: got=%s want=tx_active", row.SpendTxID)
	}
}

func TestGatewayServiceCreateRejectsDuplicateFrozenSession(t *testing.T) {
	db := openTimelineTestDB(t)
	client := mustTestActor(t, "client", "7777777777777777777777777777777777777777777777777777777777777777")
	server := mustTestActor(t, "server", "8888888888888888888888888888888888888888888888888888888888888888")
	tip := uint32(100)
	lockBlocks := uint32(12)
	req, row := mustBuildCreateReqForGatewayTest(t, client, server, tip, lockBlocks)
	row.LifecycleState = lifecycleFrozen
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB:            db,
		Chain:         stubChainForValidation{tip: tip},
		ServerPrivHex: strings.Repeat("88", 32),
		Params: GatewayParams{
			LockBlocks: lockBlocks,
		},
	}
	if _, err := svc.Create(req); err == nil {
		t.Fatalf("create should reject duplicate frozen session")
	} else if !strings.Contains(strings.ToLower(err.Error()), "lifecycle frozen") {
		t.Fatalf("unexpected create error: %v", err)
	}
}

func TestGatewayServiceCreateAllowsDuplicatePendingBaseTx(t *testing.T) {
	db := openTimelineTestDB(t)
	client := mustTestActor(t, "client", "9999999999999999999999999999999999999999999999999999999999999999")
	server := mustTestActor(t, "server", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	tip := uint32(100)
	lockBlocks := uint32(12)
	req, row := mustBuildCreateReqForGatewayTest(t, client, server, tip, lockBlocks)
	row.LifecycleState = lifecyclePendingBaseTx
	if err := InsertSession(db, row); err != nil {
		t.Fatalf("insert session failed: %v", err)
	}

	svc := &GatewayService{
		DB:            db,
		Chain:         stubChainForValidation{tip: tip},
		ServerPrivHex: strings.Repeat("aa", 32),
		Params: GatewayParams{
			LockBlocks: lockBlocks,
		},
	}
	resp, err := svc.Create(req)
	if err != nil {
		t.Fatalf("create returned unexpected error: %v", err)
	}
	if resp.SpendTxID != row.SpendTxID {
		t.Fatalf("spend_txid mismatch: got=%s want=%s", resp.SpendTxID, row.SpendTxID)
	}
}

func mustBuildCreateReqForGatewayTest(t *testing.T, client *Actor, server *Actor, tip uint32, lockBlocks uint32) (CreateReq, GatewaySessionRow) {
	t.Helper()
	utxos := []kmlibs.UTXO{{
		TxID:  strings.Repeat("11", 32),
		Vout:  0,
		Value: 2000,
	}}
	baseResp, err := ce.BuildDualFeePoolBaseTx(&utxos, 1000, client.PrivKey, server.PubKey, false, 0.5)
	if err != nil {
		t.Fatalf("build base tx failed: %v", err)
	}
	spendTx, clientSig, clientAmount, err := ce.BuildDualFeePoolSpendTX(baseResp.Tx, 1000, 100, tip+lockBlocks, client.PrivKey, server.PubKey, false, 0.5)
	if err != nil {
		t.Fatalf("build spend tx failed: %v", err)
	}
	spendTxBytes, err := hex.DecodeString(spendTx.Hex())
	if err != nil {
		t.Fatalf("decode spend tx failed: %v", err)
	}
	serverSig, err := ce.SpendTXServerSign(spendTx, baseResp.Amount, server.PrivKey, client.PubKey)
	if err != nil {
		t.Fatalf("server sign spend tx failed: %v", err)
	}
	clientSigCopy := append([]byte(nil), (*clientSig)...)
	mergedTx, err := ce.MergeDualPoolSigForSpendTx(spendTx.Hex(), serverSig, &clientSigCopy)
	if err != nil {
		t.Fatalf("merge spend tx signatures failed: %v", err)
	}
	spendTxID := spendTx.TxID().String()
	spendTxFee := CalcFeeWithInputAmount(spendTx, baseResp.Amount)
	return CreateReq{
			ClientID:       client.PubHex,
			SpendTx:        spendTxBytes,
			InputAmount:    baseResp.Amount,
			SequenceNumber: 1,
			ServerAmount:   100,
			ClientSig:      append([]byte(nil), (*clientSig)...),
		}, GatewaySessionRow{
			SpendTxID:                 spendTxID,
			ClientID:                  client.PubHex,
			ClientBSVCompressedPubHex: client.PubHex,
			ServerBSVCompressedPubHex: server.PubHex,
			InputAmountSat:            baseResp.Amount,
			PoolAmountSat:             baseResp.Amount,
			SpendTxFeeSat:             spendTxFee,
			Sequence:                  1,
			ServerAmountSat:           100,
			ClientAmountSat:           clientAmount,
			BaseTxID:                  "",
			FinalTxID:                 "",
			BaseTxHex:                 "",
			CurrentTxHex:              mergedTx.Hex(),
		}
}
