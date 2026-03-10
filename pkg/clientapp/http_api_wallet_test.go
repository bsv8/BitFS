package clientapp

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/woc"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
)

func newWalletAPITestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	return db
}

func TestHandleWalletFundFlows_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "direct_pool:s1",
		FlowType:        "direct_transfer_pool",
		RefID:           "s1",
		Stage:           "open_lock",
		Direction:       "lock",
		Purpose:         "direct_transfer_pool_open",
		AmountSatoshi:   1000,
		UsedSatoshi:     0,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx_open_1",
		Note:            "deal_id=d1",
		Payload:         map[string]any{"sequence": 1},
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "direct_pool:s1",
		FlowType:        "direct_transfer_pool",
		RefID:           "s1",
		Stage:           "use_pay",
		Direction:       "out",
		Purpose:         "direct_transfer_chunk_pay",
		AmountSatoshi:   -120,
		UsedSatoshi:     120,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx_pay_1",
		Note:            "chunk_index=0",
		Payload:         map[string]any{"chunk_index": 0},
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "fee_pool:fp1",
		FlowType:        "fee_pool",
		RefID:           "fp1",
		Stage:           "close_settle",
		Direction:       "settle",
		Purpose:         "fee_pool_close",
		AmountSatoshi:   0,
		UsedSatoshi:     200,
		ReturnedSatoshi: 50,
		RelatedTxID:     "tx_close_1",
		Note:            "gateway=g1",
		Payload:         map[string]any{"status": "closed"},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?flow_type=direct_transfer_pool&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		Total int `json:"total"`
		Items []struct {
			ID       int64           `json:"id"`
			FlowType string          `json:"flow_type"`
			Payload  json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if list.Total != 2 {
		t.Fatalf("list total mismatch: got=%d want=2", list.Total)
	}
	if len(list.Items) != 2 {
		t.Fatalf("list item count mismatch: got=%d want=2", len(list.Items))
	}
	if list.Items[0].FlowType != "direct_transfer_pool" || len(list.Items[0].Payload) == 0 {
		t.Fatalf("unexpected list item: %+v", list.Items[0])
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id=999999", nil)
	detailRec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusNotFound {
		t.Fatalf("detail not found status mismatch: got=%d want=%d", detailRec.Code, http.StatusNotFound)
	}

	detailID := list.Items[0].ID
	detailReqOK := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id="+itoa64(detailID), nil)
	detailRecOK := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(detailRecOK, detailReqOK)
	if detailRecOK.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRecOK.Code, http.StatusOK, detailRecOK.Body.String())
	}
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func TestHandleWalletSummary(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "f1",
		FlowType:        "direct_transfer_pool",
		RefID:           "r1",
		Stage:           "open_lock",
		Direction:       "lock",
		Purpose:         "open",
		AmountSatoshi:   1000,
		UsedSatoshi:     0,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx1",
		Note:            "n1",
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "f1",
		FlowType:        "direct_transfer_pool",
		RefID:           "r1",
		Stage:           "use_pay",
		Direction:       "out",
		Purpose:         "pay",
		AmountSatoshi:   -400,
		UsedSatoshi:     400,
		ReturnedSatoshi: 0,
		RelatedTxID:     "tx2",
		Note:            "n2",
	})
	appendWalletFundFlow(db, walletFundFlowEntry{
		FlowID:          "f1",
		FlowType:        "direct_transfer_pool",
		RefID:           "r1",
		Stage:           "close_settle",
		Direction:       "settle",
		Purpose:         "close",
		AmountSatoshi:   0,
		UsedSatoshi:     0,
		ReturnedSatoshi: 600,
		RelatedTxID:     "tx3",
		Note:            "n3",
	})
	appendTxHistory(db, txHistoryEntry{GatewayPeerID: "g1", EventType: "evt_a", Direction: "out", AmountSatoshi: 1, Purpose: "p", Note: "n"})
	appendTxHistory(db, txHistoryEntry{GatewayPeerID: "g1", EventType: "evt_b", Direction: "in", AmountSatoshi: 2, Purpose: "p", Note: "n"})
	appendGatewayEvent(db, gatewayEventEntry{GatewayPeerID: "g1", Action: "a1", AmountSatoshi: 1, Payload: map[string]any{"x": 1}})
	appendSaleRecord(db, saleRecordEntry{SessionID: "s1", SeedHash: "seed1", ChunkIndex: 0, UnitPriceSatPer64K: 2, AmountSatoshi: 2, BuyerGatewayPeerID: "g1", ReleaseToken: "r"})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletSummary(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if int(body["flow_count"].(float64)) != 3 {
		t.Fatalf("flow_count mismatch: got=%v want=3", body["flow_count"])
	}
	if int(body["tx_event_count"].(float64)) != 2 {
		t.Fatalf("tx_event_count mismatch: got=%v want=2", body["tx_event_count"])
	}
	if int(body["sale_count"].(float64)) != 1 {
		t.Fatalf("sale_count mismatch: got=%v want=1", body["sale_count"])
	}
	if int(body["gateway_event_count"].(float64)) != 1 {
		t.Fatalf("gateway_event_count mismatch: got=%v want=1", body["gateway_event_count"])
	}
	if int64(body["total_in_satoshi"].(float64)) != 1000 {
		t.Fatalf("total_in_satoshi mismatch: got=%v want=1000", body["total_in_satoshi"])
	}
	if int64(body["total_out_satoshi"].(float64)) != 400 {
		t.Fatalf("total_out_satoshi mismatch: got=%v want=400", body["total_out_satoshi"])
	}
	if int64(body["total_used_satoshi"].(float64)) != 400 {
		t.Fatalf("total_used_satoshi mismatch: got=%v want=400", body["total_used_satoshi"])
	}
	if int64(body["total_returned_satoshi"].(float64)) != 600 {
		t.Fatalf("total_returned_satoshi mismatch: got=%v want=600", body["total_returned_satoshi"])
	}
	if int64(body["net_spent_satoshi"].(float64)) != -200 {
		t.Fatalf("net_spent_satoshi mismatch: got=%v want=-200", body["net_spent_satoshi"])
	}
	if int64(body["ledger_count"].(float64)) != 0 {
		t.Fatalf("ledger_count mismatch: got=%v want=0", body["ledger_count"])
	}
	if int64(body["ledger_total_in_satoshi"].(float64)) != 0 {
		t.Fatalf("ledger_total_in_satoshi mismatch: got=%v want=0", body["ledger_total_in_satoshi"])
	}
	if int64(body["ledger_total_out_satoshi"].(float64)) != 0 {
		t.Fatalf("ledger_total_out_satoshi mismatch: got=%v want=0", body["ledger_total_out_satoshi"])
	}
	if got, _ := body["balance_source"].(string); got != "onchain_realtime" {
		t.Fatalf("balance_source mismatch: got=%v want=onchain_realtime", body["balance_source"])
	}
}

func TestHandleWalletLedger_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletLedgerEntry(db, walletLedgerEntry{
		TxID:              "tx001",
		Direction:         "OUT",
		Category:          "FEE_POOL",
		AmountSatoshi:     120,
		CounterpartyLabel: "gateway:g1",
		Status:            "CONFIRMED",
		BlockHeight:       100,
		OccurredAtUnix:    1000,
		RawRefID:          "fee_pool:spend1",
		Note:              "open fee pool",
		Payload:           map[string]any{"a": 1},
	})
	appendWalletLedgerEntry(db, walletLedgerEntry{
		TxID:              "tx002",
		Direction:         "IN",
		Category:          "CHANGE",
		AmountSatoshi:     80,
		CounterpartyLabel: "self",
		Status:            "CONFIRMED",
		BlockHeight:       101,
		OccurredAtUnix:    1001,
		RawRefID:          "change:1",
		Note:              "change back",
		Payload:           map[string]any{"b": 2},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger?direction=OUT&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletLedger(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		Total int `json:"total"`
		Items []struct {
			ID        int64           `json:"id"`
			Direction string          `json:"direction"`
			Payload   json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if list.Total != 1 {
		t.Fatalf("list total mismatch: got=%d want=1", list.Total)
	}
	if len(list.Items) != 1 {
		t.Fatalf("list item count mismatch: got=%d want=1", len(list.Items))
	}
	if list.Items[0].Direction != "OUT" || len(list.Items[0].Payload) == 0 {
		t.Fatalf("unexpected list item: %+v", list.Items[0])
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger/detail?id=999999", nil)
	detailRec := httptest.NewRecorder()
	srv.handleWalletLedgerDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusNotFound {
		t.Fatalf("detail not found status mismatch: got=%d want=%d", detailRec.Code, http.StatusNotFound)
	}

	detailID := list.Items[0].ID
	detailReqOK := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger/detail?id="+itoa64(detailID), nil)
	detailRecOK := httptest.NewRecorder()
	srv.handleWalletLedgerDetail(detailRecOK, detailReqOK)
	if detailRecOK.Code != http.StatusOK {
		t.Fatalf("detail status mismatch: got=%d want=%d body=%s", detailRecOK.Code, http.StatusOK, detailRecOK.Body.String())
	}
}

type walletSyncMockChain struct {
	utxos   []woc.UTXO
	history []woc.AddressHistoryItem
	txs     map[string]woc.TxDetail
}

func (m *walletSyncMockChain) GetUTXOs(address string) ([]woc.UTXO, error) { return m.utxos, nil }
func (m *walletSyncMockChain) GetTipHeight() (uint32, error)               { return 100, nil }
func (m *walletSyncMockChain) Broadcast(txHex string) (string, error)      { return "mock-txid", nil }
func (m *walletSyncMockChain) GetAddressHistory(address string) ([]woc.AddressHistoryItem, error) {
	return m.history, nil
}
func (m *walletSyncMockChain) GetTxDetail(txid string) (woc.TxDetail, error) {
	return m.txs[txid], nil
}

func TestWalletLedger_QueryAfterChainSync(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	privHex := "1111111111111111111111111111111111111111111111111111111111111111"
	actor, err := dual2of2.BuildActor("client", privHex, false)
	if err != nil {
		t.Fatalf("build actor: %v", err)
	}
	addr, err := kmlibs.GetAddressFromPubKey(actor.PubKey, false)
	if err != nil {
		t.Fatalf("derive addr: %v", err)
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		t.Fatalf("build lock: %v", err)
	}
	lockHex := hex.EncodeToString(lock.Bytes())
	chain := &walletSyncMockChain{
		utxos: []woc.UTXO{{TxID: "tx-in-1", Vout: 0, Value: 1000}},
		history: []woc.AddressHistoryItem{
			{TxID: "tx-in-1", Height: 88},
		},
		txs: map[string]woc.TxDetail{
			"tx-in-1": {
				TxID: "tx-in-1",
				Vin:  []woc.TxInput{{TxID: "", Vout: 0}},
				Vout: []woc.TxOutput{
					{N: 0, Value: 0.00001, ScriptPubKey: woc.ScriptPubKey{Hex: lockHex}},
				},
			},
		},
	}
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = privHex
	srv := &httpAPIServer{
		db: db,
		rt: &Runtime{
			runIn: NewRunInputFromConfig(cfg, privHex),
			Chain: chain,
		},
	}
	if err := srv.syncWalletLedgerFromChain(t.Context()); err != nil {
		t.Fatalf("sync wallet ledger from chain: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ledger?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletLedger(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ledger status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		Total int `json:"total"`
		Items []struct {
			Direction     string `json:"direction"`
			Category      string `json:"category"`
			AmountSatoshi int64  `json:"amount_satoshi"`
			TxID          string `json:"txid"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode ledger list: %v", err)
	}
	if list.Total != 1 || len(list.Items) != 1 {
		t.Fatalf("ledger item mismatch: total=%d len=%d", list.Total, len(list.Items))
	}
	if list.Items[0].Direction != "IN" {
		t.Fatalf("direction mismatch: got=%s want=IN", list.Items[0].Direction)
	}
	if list.Items[0].Category != "REPAYMENT" {
		t.Fatalf("category mismatch: got=%s want=REPAYMENT", list.Items[0].Category)
	}
	if list.Items[0].AmountSatoshi != 1000 {
		t.Fatalf("amount mismatch: got=%d want=1000", list.Items[0].AmountSatoshi)
	}
	if list.Items[0].TxID != "tx-in-1" {
		t.Fatalf("txid mismatch: got=%s want=tx-in-1", list.Items[0].TxID)
	}
}
