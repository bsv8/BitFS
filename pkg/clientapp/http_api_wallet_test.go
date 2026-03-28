package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	bsvscript "github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
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

type walletAssetActionMockChain struct{}

func (walletAssetActionMockChain) GetUTXOs(string) ([]poolcore.UTXO, error) {
	return nil, errors.New("unexpected call")
}

func (walletAssetActionMockChain) GetTipHeight() (uint32, error) {
	return 100, nil
}

func (walletAssetActionMockChain) Broadcast(txHex string) (string, error) {
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(parsed.TxID().String())), nil
}

type walletBSV21ProviderStub struct {
	tokenDetails       walletBSV21TokenDetails
	validatedOutpoints []string
	submittedTokenIDs  []string
	submittedTxIDs     []string
	errDetails         error
	errValidate        error
	errSubmit          error
}

func (s *walletBSV21ProviderStub) GetTokenDetails(context.Context, string) (walletBSV21TokenDetails, error) {
	if s.errDetails != nil {
		return walletBSV21TokenDetails{}, s.errDetails
	}
	return s.tokenDetails, nil
}

func (s *walletBSV21ProviderStub) ValidateOutputs(_ context.Context, _ string, outpoints []string) ([]walletBSV21IndexedOutput, error) {
	if s.errValidate != nil {
		return nil, s.errValidate
	}
	items := s.validatedOutpoints
	if len(items) == 0 {
		items = outpoints
	}
	out := make([]walletBSV21IndexedOutput, 0, len(items))
	for _, item := range items {
		out = append(out, walletBSV21IndexedOutput{Outpoint: item})
	}
	return out, nil
}

func (s *walletBSV21ProviderStub) SubmitTx(_ context.Context, tokenID string, tx *txsdk.Transaction) error {
	if s.errSubmit != nil {
		return s.errSubmit
	}
	s.submittedTokenIDs = append(s.submittedTokenIDs, strings.TrimSpace(tokenID))
	if tx != nil {
		s.submittedTxIDs = append(s.submittedTxIDs, strings.ToLower(strings.TrimSpace(tx.TxID().String())))
	}
	return nil
}

func TestHandleWalletFundFlows_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	appendWalletFundFlow(db, walletFundFlowEntry{
		VisitID:         "visit-a",
		VisitLocator:    "node:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/movie",
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
		VisitID:         "visit-a",
		VisitLocator:    "node:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/movie",
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
		VisitID:         "visit-b",
		VisitLocator:    "bitfs:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?flow_type=direct_transfer_pool&visit_id=visit-a&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		Total int `json:"total"`
		Items []struct {
			ID           int64           `json:"id"`
			VisitID      string          `json:"visit_id"`
			VisitLocator string          `json:"visit_locator"`
			FlowType     string          `json:"flow_type"`
			Payload      json.RawMessage `json:"payload"`
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
	if list.Items[0].VisitID != "visit-a" {
		t.Fatalf("visit_id mismatch: got=%q want=%q", list.Items[0].VisitID, "visit-a")
	}
	if list.Items[0].VisitLocator == "" {
		t.Fatalf("visit_locator should not be empty")
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
	var detail struct {
		VisitID      string `json:"visit_id"`
		VisitLocator string `json:"visit_locator"`
	}
	if err := json.Unmarshal(detailRecOK.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.VisitID != "visit-a" {
		t.Fatalf("detail visit_id mismatch: got=%q want=%q", detail.VisitID, "visit-a")
	}
	if detail.VisitLocator == "" {
		t.Fatalf("detail visit_locator should not be empty")
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
	if got, _ := body["balance_source"].(string); got != "wallet_utxo_db" {
		t.Fatalf("balance_source mismatch: got=%v want=wallet_utxo_db", body["balance_source"])
	}
}

func TestHandleWalletSummary_IncludesSyncFailureAnchors(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db, runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 0, 0, now, `http 502: {"error":"http 404: Not Found"}`, "chain_utxo_worker", "periodic_tick", 0, "round-http-1", "wallet_chain_get_unconfirmed_history", walletChainUnconfirmedHistoryUpstreamPath(addr), 502,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state failed: %v", err)
	}

	srv := &httpAPIServer{db: db, rt: rt}
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
	if got, _ := body["wallet_utxo_sync_last_round_id"].(string); got != "round-http-1" {
		t.Fatalf("wallet_utxo_sync_last_round_id mismatch: got=%v want=round-http-1", body["wallet_utxo_sync_last_round_id"])
	}
	if got, _ := body["wallet_utxo_sync_last_failed_step"].(string); got != "wallet_chain_get_unconfirmed_history" {
		t.Fatalf("wallet_utxo_sync_last_failed_step mismatch: got=%v", body["wallet_utxo_sync_last_failed_step"])
	}
	if got, _ := body["wallet_utxo_sync_last_upstream_path"].(string); got != walletChainUnconfirmedHistoryUpstreamPath(addr) {
		t.Fatalf("wallet_utxo_sync_last_upstream_path mismatch: got=%v want=%v", body["wallet_utxo_sync_last_upstream_path"], walletChainUnconfirmedHistoryUpstreamPath(addr))
	}
	if got := int(body["wallet_utxo_sync_last_http_status"].(float64)); got != 502 {
		t.Fatalf("wallet_utxo_sync_last_http_status mismatch: got=%d want=502", got)
	}
}

func TestHandleWalletSummary_ReturnsDBBalanceWhenSyncStateHasError(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db, runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 1, 99904, now, "http 404: Not Found", "chain_utxo_worker", "periodic_tick", 0, "round-http-balance", "wallet_chain_get_confirmed_history_asc", walletChainConfirmedHistoryUpstreamPath(addr, whatsonchain.ConfirmedHistoryQuery{Order: "asc", Limit: 1000, Height: 1726038}), 404,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state failed: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"utxo-1", walletID, addr, "99cb8465b03fd016974a17b0fc05abf93718fdc02bda82d09f5ee4d5ac29156e", 0, 99904, "unspent", walletUTXOAllocationPlainBSV, "", "", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo failed: %v", err)
	}

	srv := &httpAPIServer{db: db, rt: rt}
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
	if got := uint64(body["onchain_balance_satoshi"].(float64)); got != 99904 {
		t.Fatalf("onchain_balance_satoshi mismatch: got=%d want=99904", got)
	}
	if got, ok := body["onchain_balance_error"].(string); ok && got != "" {
		t.Fatalf("onchain_balance_error mismatch: got=%q want empty", got)
	}
	if got, _ := body["wallet_utxo_sync_last_error"].(string); got != "http 404: Not Found" {
		t.Fatalf("wallet_utxo_sync_last_error mismatch: got=%v want=http 404: Not Found", body["wallet_utxo_sync_last_error"])
	}
}

func TestHandleWalletSummary_SeparatesPlainAndProtectedBalances(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db, runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 2, 107, now, "", "chain_utxo_worker", "periodic_tick", 0, "round-summary-assets", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state failed: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES
		 (?,?,?,?,?,?,?,?,?,?,?,?,?,?),
		 (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		walletID, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 100, "unspent", walletUTXOAllocationProtectedAsset, "indexed asset", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", now, now, 0,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1",
		walletID, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1, 7, "unspent", walletUTXOAllocationPlainBSV, "", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "", now, now, 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo failed: %v", err)
	}

	srv := &httpAPIServer{db: db, rt: rt}
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
	if got := uint64(body["onchain_balance_satoshi"].(float64)); got != 7 {
		t.Fatalf("onchain_balance_satoshi mismatch: got=%d want=7", got)
	}
	if got := int(body["wallet_total_unspent_utxo_count"].(float64)); got != 2 {
		t.Fatalf("wallet_total_unspent_utxo_count mismatch: got=%d want=2", got)
	}
	if got := uint64(body["wallet_total_unspent_satoshi"].(float64)); got != 107 {
		t.Fatalf("wallet_total_unspent_satoshi mismatch: got=%d want=107", got)
	}
	if got := uint64(body["wallet_plain_bsv_balance_satoshi"].(float64)); got != 7 {
		t.Fatalf("wallet_plain_bsv_balance_satoshi mismatch: got=%d want=7", got)
	}
	if got := uint64(body["wallet_protected_balance_satoshi"].(float64)); got != 100 {
		t.Fatalf("wallet_protected_balance_satoshi mismatch: got=%d want=100", got)
	}
}

func appendWalletUTXOForHTTPTest(t *testing.T, db *sql.DB, address string, utxoID string, txid string, vout uint32, value uint64, state string, allocationClass string, now int64) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo(utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID,
		walletIDByAddress(address),
		address,
		txid,
		vout,
		value,
		state,
		allocationClass,
		"",
		txid,
		"",
		now,
		now,
		0,
	); err != nil {
		t.Fatalf("seed wallet_utxo failed: %v", err)
	}
}

func appendWalletUTXOAssetForHTTPTest(t *testing.T, db *sql.DB, address string, utxoID string, assetGroup string, assetStandard string, assetKey string, assetSymbol string, quantityText string, sourceName string, payload map[string]any, updatedAtUnix int64) {
	t.Helper()
	payloadJSON := "{}"
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal wallet asset payload failed: %v", err)
		}
		payloadJSON = string(body)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_assets(utxo_id,wallet_id,address,asset_group,asset_standard,asset_key,asset_symbol,quantity_text,source_name,payload_json,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID,
		walletIDByAddress(address),
		address,
		assetGroup,
		assetStandard,
		assetKey,
		assetSymbol,
		quantityText,
		sourceName,
		payloadJSON,
		updatedAtUnix,
	); err != nil {
		t.Fatalf("seed wallet_utxo_assets failed: %v", err)
	}
}

func appendWalletUTXOEventForHTTPTest(t *testing.T, db *sql.DB, utxoID string, eventType string, refTxID string, refBusinessID string, note string, payload map[string]any, createdAtUnix int64) {
	t.Helper()
	payloadJSON := "{}"
	if payload != nil {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal wallet utxo event payload failed: %v", err)
		}
		payloadJSON = string(body)
	}
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_events(created_at_unix,utxo_id,event_type,ref_txid,ref_business_id,note,payload_json)
		 VALUES(?,?,?,?,?,?,?)`,
		createdAtUnix,
		strings.ToLower(strings.TrimSpace(utxoID)),
		strings.TrimSpace(eventType),
		strings.ToLower(strings.TrimSpace(refTxID)),
		strings.TrimSpace(refBusinessID),
		strings.TrimSpace(note),
		payloadJSON,
	); err != nil {
		t.Fatalf("seed wallet_utxo_events failed: %v", err)
	}
}

func appendWalletLocalBroadcastTxForHTTPTest(t *testing.T, db *sql.DB, address string, txHex string, updatedAtUnix int64) string {
	t.Helper()
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		t.Fatalf("parse local broadcast tx failed: %v", err)
	}
	txID := strings.ToLower(strings.TrimSpace(parsed.TxID().String()))
	if _, err := db.Exec(
		`INSERT INTO wallet_local_broadcast_txs(txid,wallet_id,address,tx_hex,created_at_unix,updated_at_unix,observed_at_unix)
		 VALUES(?,?,?,?,?,?,?)`,
		txID,
		walletIDByAddress(address),
		address,
		strings.ToLower(strings.TrimSpace(txHex)),
		updatedAtUnix,
		updatedAtUnix,
		0,
	); err != nil {
		t.Fatalf("seed wallet_local_broadcast_txs failed: %v", err)
	}
	return txID
}

func buildBSV20SourceTxHexForHTTPTest(t *testing.T, address string, tick string, amount string) string {
	t.Helper()
	addr, err := bsvscript.NewAddressFromString(strings.TrimSpace(address))
	if err != nil {
		t.Fatalf("parse wallet address failed: %v", err)
	}
	lockingScript, err := p2pkh.Lock(addr)
	if err != nil {
		t.Fatalf("build wallet lock script failed: %v", err)
	}
	payload, err := json.Marshal(map[string]string{
		"p":    "bsv-20",
		"op":   "transfer",
		"tick": strings.TrimSpace(tick),
		"amt":  strings.TrimSpace(amount),
	})
	if err != nil {
		t.Fatalf("marshal bsv20 payload failed: %v", err)
	}
	txBuilder := txsdk.NewTransaction()
	if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: lockingScript,
		ContentType:   walletTokenContentType,
		Data:          payload,
	}); err != nil {
		t.Fatalf("build bsv20 source tx failed: %v", err)
	}
	return strings.ToLower(strings.TrimSpace(hex.EncodeToString(txBuilder.Bytes())))
}

func buildBSV21SourceTxHexForHTTPTest(t *testing.T, address string, tokenID string, amount string) string {
	t.Helper()
	addr, err := bsvscript.NewAddressFromString(strings.TrimSpace(address))
	if err != nil {
		t.Fatalf("parse wallet address failed: %v", err)
	}
	lockingScript, err := p2pkh.Lock(addr)
	if err != nil {
		t.Fatalf("build wallet lock script failed: %v", err)
	}
	payload, err := json.Marshal(map[string]string{
		"p":   "bsv-20",
		"op":  "transfer",
		"id":  strings.TrimSpace(tokenID),
		"amt": strings.TrimSpace(amount),
	})
	if err != nil {
		t.Fatalf("marshal bsv21 payload failed: %v", err)
	}
	txBuilder := txsdk.NewTransaction()
	if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: lockingScript,
		ContentType:   walletTokenContentType,
		Data:          payload,
	}); err != nil {
		t.Fatalf("build bsv21 source tx failed: %v", err)
	}
	return strings.ToLower(strings.TrimSpace(hex.EncodeToString(txBuilder.Bytes())))
}

func TestHandleWalletTokenBalances(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)

	appendWalletUTXOForHTTPTest(t, db, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "20", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "20"}, now+1)

	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now+2)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", walletAssetGroupToken, "bsv21", "bsv21:token-1", "T21", "7", "onesat-stack", map[string]any{"id": "token-1", "amt": "7"}, now+2)

	appendWalletUTXOForHTTPTest(t, db, addr, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd:0", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", 0, 1, "spent", walletUTXOAllocationProtectedAsset, now+3)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "999", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "999"}, now+3)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/tokens/balances?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletTokenBalances(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("token balance status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body struct {
		WalletAddress string `json:"wallet_address"`
		Total         int    `json:"total"`
		Items         []struct {
			TokenStandard string `json:"token_standard"`
			AssetKey      string `json:"asset_key"`
			AssetSymbol   string `json:"asset_symbol"`
			QuantityText  string `json:"quantity_text"`
			OutputCount   int    `json:"output_count"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode token balances failed: %v", err)
	}
	if body.WalletAddress != addr {
		t.Fatalf("wallet_address mismatch: got=%q want=%q", body.WalletAddress, addr)
	}
	if body.Total != 2 {
		t.Fatalf("token balance total mismatch: got=%d want=2", body.Total)
	}
	if len(body.Items) != 2 {
		t.Fatalf("token balance item count mismatch: got=%d want=2", len(body.Items))
	}
	if body.Items[0].TokenStandard != "bsv20" || body.Items[0].AssetKey != "bsv20:demo" {
		t.Fatalf("unexpected first token item: %+v", body.Items[0])
	}
	if body.Items[0].QuantityText != "30" || body.Items[0].OutputCount != 2 {
		t.Fatalf("unexpected aggregated bsv20 item: %+v", body.Items[0])
	}
	if body.Items[1].TokenStandard != "bsv21" || body.Items[1].QuantityText != "7" {
		t.Fatalf("unexpected second token item: %+v", body.Items[1])
	}

	filterReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/tokens/balances?standard=bsv21&limit=10&offset=0", nil)
	filterRec := httptest.NewRecorder()
	srv.handleWalletTokenBalances(filterRec, filterReq)
	if filterRec.Code != http.StatusOK {
		t.Fatalf("token balance filtered status mismatch: got=%d want=%d body=%s", filterRec.Code, http.StatusOK, filterRec.Body.String())
	}
	var filtered struct {
		Total int `json:"total"`
		Items []struct {
			TokenStandard string `json:"token_standard"`
		} `json:"items"`
	}
	if err := json.Unmarshal(filterRec.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("decode filtered token balances failed: %v", err)
	}
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].TokenStandard != "bsv21" {
		t.Fatalf("unexpected filtered token balances: %+v", filtered)
	}
}

func TestHandleWalletTokenOutputs(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)

	appendWalletUTXOForHTTPTest(t, db, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1", walletAssetGroupToken, "bsv21", "bsv21:token-1", "T21", "7", "onesat-stack", map[string]any{"id": "token-1", "amt": "7"}, now+1)

	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1, "spent", walletUTXOAllocationProtectedAsset, now+2)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "999", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "999"}, now+2)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/tokens/outputs?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletTokenOutputs(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("token outputs status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body struct {
		WalletAddress string `json:"wallet_address"`
		Total         int    `json:"total"`
		Items         []struct {
			UTXOID        string `json:"utxo_id"`
			TokenStandard string `json:"token_standard"`
			AssetKey      string `json:"asset_key"`
			QuantityText  string `json:"quantity_text"`
			Payload       struct {
				Tick string `json:"tick"`
				ID   string `json:"id"`
			} `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode token outputs failed: %v", err)
	}
	if body.WalletAddress != addr {
		t.Fatalf("wallet_address mismatch: got=%q want=%q", body.WalletAddress, addr)
	}
	if body.Total != 2 || len(body.Items) != 2 {
		t.Fatalf("token outputs size mismatch: total=%d items=%d want=2", body.Total, len(body.Items))
	}
	if body.Items[0].UTXOID != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1" || body.Items[0].TokenStandard != "bsv21" {
		t.Fatalf("unexpected first token output item: %+v", body.Items[0])
	}
	if body.Items[0].Payload.ID != "token-1" {
		t.Fatalf("unexpected first token output payload: %+v", body.Items[0].Payload)
	}
	if body.Items[1].UTXOID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0" || body.Items[1].Payload.Tick != "DEMO" {
		t.Fatalf("unexpected second token output item: %+v", body.Items[1])
	}

	filterReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/tokens/outputs?standard=bsv20&asset_key=bsv20:demo&limit=10&offset=0", nil)
	filterRec := httptest.NewRecorder()
	srv.handleWalletTokenOutputs(filterRec, filterReq)
	if filterRec.Code != http.StatusOK {
		t.Fatalf("filtered token outputs status mismatch: got=%d want=%d body=%s", filterRec.Code, http.StatusOK, filterRec.Body.String())
	}
	var filtered struct {
		Total int `json:"total"`
		Items []struct {
			TokenStandard string `json:"token_standard"`
			AssetKey      string `json:"asset_key"`
		} `json:"items"`
	}
	if err := json.Unmarshal(filterRec.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("decode filtered token outputs failed: %v", err)
	}
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].TokenStandard != "bsv20" || filtered.Items[0].AssetKey != "bsv20:demo" {
		t.Fatalf("unexpected filtered token outputs: %+v", filtered)
	}
}

func TestHandleWalletTokenOutputDetailAndEvents(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)
	appendWalletUTXOEventForHTTPTest(t, db, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "detected", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", "detected by sync", map[string]any{"trigger": "test"}, now)
	appendWalletUTXOEventForHTTPTest(t, db, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "local_pending_spent", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "biz-1", "reserved by pending transfer", map[string]any{"txid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, now+1)

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/tokens/outputs/detail?standard=bsv20&utxo_id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", nil)
	detailRec := httptest.NewRecorder()
	srv.handleWalletTokenOutputDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("token output detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	var detail struct {
		UTXOID        string `json:"utxo_id"`
		TokenStandard string `json:"token_standard"`
		AssetKey      string `json:"asset_key"`
		QuantityText  string `json:"quantity_text"`
	}
	if err := json.Unmarshal(detailRec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode token output detail failed: %v", err)
	}
	if detail.UTXOID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0" || detail.TokenStandard != "bsv20" || detail.AssetKey != "bsv20:demo" || detail.QuantityText != "10" {
		t.Fatalf("unexpected token output detail: %+v", detail)
	}

	eventsReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/tokens/events?standard=bsv20&asset_key=bsv20:demo&limit=10&offset=0", nil)
	eventsRec := httptest.NewRecorder()
	srv.handleWalletTokenEvents(eventsRec, eventsReq)
	if eventsRec.Code != http.StatusOK {
		t.Fatalf("token events status mismatch: got=%d want=%d body=%s", eventsRec.Code, http.StatusOK, eventsRec.Body.String())
	}
	var events struct {
		Total int `json:"total"`
		Items []struct {
			UTXOID        string `json:"utxo_id"`
			AssetGroup    string `json:"asset_group"`
			AssetStandard string `json:"asset_standard"`
			AssetKey      string `json:"asset_key"`
			EventType     string `json:"event_type"`
			RefBusinessID string `json:"ref_business_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(eventsRec.Body.Bytes(), &events); err != nil {
		t.Fatalf("decode token events failed: %v", err)
	}
	if events.Total != 2 || len(events.Items) != 2 {
		t.Fatalf("token events size mismatch: total=%d items=%d want=2", events.Total, len(events.Items))
	}
	if events.Items[0].EventType != "local_pending_spent" || events.Items[0].RefBusinessID != "biz-1" {
		t.Fatalf("unexpected latest token event: %+v", events.Items[0])
	}
	if events.Items[0].UTXOID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0" || events.Items[0].AssetGroup != walletAssetGroupToken || events.Items[0].AssetStandard != "bsv20" || events.Items[0].AssetKey != "bsv20:demo" {
		t.Fatalf("unexpected token event asset binding: %+v", events.Items[0])
	}
}

func TestHandleWalletTokenSendPreview(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)
	appendWalletUTXOForHTTPTest(t, db, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "20", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "20"}, now+1)
	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1000, "unspent", walletUTXOAllocationPlainBSV, now+2)

	body := strings.NewReader(`{"token_standard":"bsv20","asset_key":"bsv20:demo","amount_text":"15","to_address":"` + toAddr + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/preview", body)
	rec := httptest.NewRecorder()
	srv.handleWalletTokenSendPreview(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("token send preview status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		Ok      bool   `json:"ok"`
		Code    string `json:"code"`
		Preview struct {
			Action               string   `json:"action"`
			Feasible             bool     `json:"feasible"`
			CanSign              bool     `json:"can_sign"`
			SelectedAssetUTXOIDs []string `json:"selected_asset_utxo_ids"`
			SelectedFeeUTXOIDs   []string `json:"selected_fee_utxo_ids"`
			FeeFundingTarget     uint64   `json:"fee_funding_target_bsv_sat"`
			Changes              []struct {
				AssetGroup   string `json:"asset_group"`
				AssetKey     string `json:"asset_key"`
				QuantityText string `json:"quantity_text"`
				Direction    string `json:"direction"`
			} `json:"changes"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode token send preview failed: %v", err)
	}
	if !resp.Ok || resp.Code != "OK" {
		t.Fatalf("token send preview response mismatch: %+v", resp)
	}
	if resp.Preview.Action != "tokens.send" || !resp.Preview.Feasible || resp.Preview.CanSign {
		t.Fatalf("unexpected token send preview state: %+v", resp.Preview)
	}
	if len(resp.Preview.SelectedAssetUTXOIDs) != 2 || len(resp.Preview.SelectedFeeUTXOIDs) != 1 {
		t.Fatalf("unexpected token send preview selections: %+v", resp.Preview)
	}
	if resp.Preview.FeeFundingTarget == 0 {
		t.Fatalf("expected fee funding target to be > 0")
	}
	if len(resp.Preview.Changes) < 3 {
		t.Fatalf("expected token send preview changes, got=%d", len(resp.Preview.Changes))
	}
	if resp.Preview.Changes[0].AssetGroup != walletAssetGroupToken || resp.Preview.Changes[0].AssetKey != "bsv20:demo" || resp.Preview.Changes[0].QuantityText != "15" {
		t.Fatalf("unexpected first token send preview change: %+v", resp.Preview.Changes[0])
	}
}

func TestHandleWalletTokenSendPreviewAndSign_BSV20(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain: walletAssetActionMockChain{},
	}
	rt.runIn.BSV.Network = "test"
	srv := &httpAPIServer{db: db, rt: rt}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	txHexA := buildBSV20SourceTxHexForHTTPTest(t, addr, "DEMO", "10")
	txidA := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexA, now)
	appendWalletUTXOForHTTPTest(t, db, addr, txidA+":0", txidA, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidA+":0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)

	txHexB := buildBSV20SourceTxHexForHTTPTest(t, addr, "DEMO", "20")
	txidB := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexB, now+1)
	appendWalletUTXOForHTTPTest(t, db, addr, txidB+":0", txidB, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidB+":0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "20", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "20"}, now+1)

	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1000, "unspent", walletUTXOAllocationPlainBSV, now+2)

	body := strings.NewReader(`{"token_standard":"bsv20","asset_key":"bsv20:demo","amount_text":"15","to_address":"` + toAddr + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/preview", body)
	rec := httptest.NewRecorder()
	srv.handleWalletTokenSendPreview(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("token send preview status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var previewResp struct {
		Ok      bool   `json:"ok"`
		Code    string `json:"code"`
		Preview struct {
			Feasible           bool     `json:"feasible"`
			CanSign            bool     `json:"can_sign"`
			SelectedFeeUTXOIDs []string `json:"selected_fee_utxo_ids"`
			TxID               string   `json:"txid"`
			PreviewHash        string   `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &previewResp); err != nil {
		t.Fatalf("decode token send preview failed: %v", err)
	}
	if !previewResp.Ok || previewResp.Code != "OK" {
		t.Fatalf("token send preview response mismatch: %+v", previewResp)
	}
	if !previewResp.Preview.Feasible || !previewResp.Preview.CanSign {
		t.Fatalf("expected signable token preview, got %+v", previewResp.Preview)
	}
	if len(previewResp.Preview.SelectedFeeUTXOIDs) != 1 || previewResp.Preview.TxID == "" || previewResp.Preview.PreviewHash == "" {
		t.Fatalf("unexpected signable token preview payload: %+v", previewResp.Preview)
	}

	signBody := strings.NewReader(`{"token_standard":"bsv20","asset_key":"bsv20:demo","amount_text":"15","to_address":"` + toAddr + `","expected_preview_hash":"` + previewResp.Preview.PreviewHash + `"}`)
	signReq := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/sign", signBody)
	signRec := httptest.NewRecorder()
	srv.handleWalletTokenSendSign(signRec, signReq)
	if signRec.Code != http.StatusOK {
		t.Fatalf("token send sign status mismatch: got=%d want=%d body=%s", signRec.Code, http.StatusOK, signRec.Body.String())
	}
	var signResp struct {
		Ok          bool   `json:"ok"`
		Code        string `json:"code"`
		SignedTxHex string `json:"signed_tx_hex"`
		TxID        string `json:"txid"`
		Preview     struct {
			PreviewHash string `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(signRec.Body.Bytes(), &signResp); err != nil {
		t.Fatalf("decode token send sign failed: %v", err)
	}
	if !signResp.Ok || signResp.Code != "OK" || signResp.SignedTxHex == "" || signResp.TxID == "" {
		t.Fatalf("token send sign response mismatch: %+v", signResp)
	}
	if signResp.Preview.PreviewHash != previewResp.Preview.PreviewHash {
		t.Fatalf("preview hash mismatch: got=%q want=%q", signResp.Preview.PreviewHash, previewResp.Preview.PreviewHash)
	}
}

func TestHandleWalletTokenSendSubmitProjectsTokenChange(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain: walletAssetActionMockChain{},
	}
	rt.runIn.BSV.Network = "test"
	srv := &httpAPIServer{db: db, rt: rt}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	txHexA := buildBSV20SourceTxHexForHTTPTest(t, addr, "DEMO", "10")
	txidA := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexA, now)
	appendWalletUTXOForHTTPTest(t, db, addr, txidA+":0", txidA, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidA+":0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "10", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "10"}, now)

	txHexB := buildBSV20SourceTxHexForHTTPTest(t, addr, "DEMO", "20")
	txidB := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexB, now+1)
	appendWalletUTXOForHTTPTest(t, db, addr, txidB+":0", txidB, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidB+":0", walletAssetGroupToken, "bsv20", "bsv20:demo", "DEMO", "20", "onesat-stack", map[string]any{"tick": "DEMO", "amt": "20"}, now+1)

	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1000, "unspent", walletUTXOAllocationPlainBSV, now+2)

	previewRec := httptest.NewRecorder()
	srv.handleWalletTokenSendPreview(previewRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/preview", strings.NewReader(`{"token_standard":"bsv20","asset_key":"bsv20:demo","amount_text":"15","to_address":"`+toAddr+`"}`)))
	var previewResp struct {
		Preview struct {
			PreviewHash string `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(previewRec.Body.Bytes(), &previewResp); err != nil {
		t.Fatalf("decode token send preview failed: %v", err)
	}

	signRec := httptest.NewRecorder()
	srv.handleWalletTokenSendSign(signRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/sign", strings.NewReader(`{"token_standard":"bsv20","asset_key":"bsv20:demo","amount_text":"15","to_address":"`+toAddr+`","expected_preview_hash":"`+previewResp.Preview.PreviewHash+`"}`)))
	var signResp struct {
		Ok          bool   `json:"ok"`
		SignedTxHex string `json:"signed_tx_hex"`
		TxID        string `json:"txid"`
	}
	if err := json.Unmarshal(signRec.Body.Bytes(), &signResp); err != nil {
		t.Fatalf("decode token send sign failed: %v", err)
	}
	if !signResp.Ok || signResp.SignedTxHex == "" || signResp.TxID == "" {
		t.Fatalf("token send sign failed: %+v", signResp)
	}
	rt.WalletAssetDetector = walletAssetDetectorStub{
		items: map[string]walletUTXOAssetClassification{
			signResp.TxID + ":1": {
				AllocationClass:  walletUTXOAllocationProtectedAsset,
				AllocationReason: "indexed bsv20 output",
				Assets: []walletUTXOAssetBinding{
					{
						UTXOID:        signResp.TxID + ":1",
						AssetGroup:    walletAssetGroupToken,
						AssetStandard: "bsv20",
						AssetKey:      "bsv20:demo",
						AssetSymbol:   "DEMO",
						QuantityText:  "15",
						SourceName:    "onesat-stack",
						Payload:       map[string]any{"tick": "DEMO", "amt": "15"},
					},
				},
			},
		},
	}

	submitRec := httptest.NewRecorder()
	srv.handleWalletTokenSendSubmit(submitRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/submit", strings.NewReader(`{"signed_tx_hex":"`+signResp.SignedTxHex+`"}`)))
	if submitRec.Code != http.StatusOK {
		t.Fatalf("token send submit status mismatch: got=%d want=%d body=%s", submitRec.Code, http.StatusOK, submitRec.Body.String())
	}
	var submitResp struct {
		Ok   bool   `json:"ok"`
		Code string `json:"code"`
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(submitRec.Body.Bytes(), &submitResp); err != nil {
		t.Fatalf("decode token send submit failed: %v", err)
	}
	if !submitResp.Ok || submitResp.Code != "OK" || submitResp.TxID == "" {
		t.Fatalf("token send submit response mismatch: %+v", submitResp)
	}
	var spentCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM wallet_utxo WHERE txid IN (?,?) AND state='spent'`, txidA, txidB).Scan(&spentCount); err != nil {
		t.Fatalf("query spent token inputs failed: %v", err)
	}
	if spentCount != 2 {
		t.Fatalf("expected both token inputs to be spent, got=%d", spentCount)
	}
	var changeQty string
	if err := db.QueryRow(`SELECT quantity_text FROM wallet_utxo_assets WHERE utxo_id=?`, signResp.TxID+":1").Scan(&changeQty); err != nil {
		t.Fatalf("query token change asset failed: %v", err)
	}
	if changeQty != "15" {
		t.Fatalf("token change quantity mismatch: got=%q want=%q", changeQty, "15")
	}
}

func TestHandleWalletTokenSendSign_BSV21(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	provider := &walletBSV21ProviderStub{
		tokenDetails: walletBSV21TokenDetails{
			Token: walletBSV21TokenData{
				Symbol: "T21",
			},
			Status: walletBSV21TokenStatus{
				IsActive:     true,
				FeePerOutput: 5,
				FeeAddress:   "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V",
			},
		},
	}
	rt := &Runtime{
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain:         walletAssetActionMockChain{},
		WalletBSV21Provider: provider,
	}
	rt.runIn.BSV.Network = "test"
	srv := &httpAPIServer{db: db, rt: rt}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	now := time.Now().Unix()
	txHexA := buildBSV21SourceTxHexForHTTPTest(t, addr, "token-1", "3")
	txidA := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexA, now)
	appendWalletUTXOForHTTPTest(t, db, addr, txidA+":0", txidA, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidA+":0", walletAssetGroupToken, "bsv21", "bsv21:token-1", "T21", "3", "onesat-stack", map[string]any{"id": "token-1", "amt": "3"}, now)

	txHexB := buildBSV21SourceTxHexForHTTPTest(t, addr, "token-1", "4")
	txidB := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexB, now+1)
	appendWalletUTXOForHTTPTest(t, db, addr, txidB+":0", txidB, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidB+":0", walletAssetGroupToken, "bsv21", "bsv21:token-1", "T21", "4", "onesat-stack", map[string]any{"id": "token-1", "amt": "4"}, now+1)

	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1000, "unspent", walletUTXOAllocationPlainBSV, now+2)

	previewRec := httptest.NewRecorder()
	srv.handleWalletTokenSendPreview(previewRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/preview", strings.NewReader(`{"token_standard":"bsv21","asset_key":"bsv21:token-1","amount_text":"5","to_address":"n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"}`)))
	if previewRec.Code != http.StatusOK {
		t.Fatalf("token send preview status mismatch: got=%d want=%d body=%s", previewRec.Code, http.StatusOK, previewRec.Body.String())
	}
	var previewResp struct {
		Ok      bool `json:"ok"`
		Preview struct {
			CanSign            bool     `json:"can_sign"`
			SelectedFeeUTXOIDs []string `json:"selected_fee_utxo_ids"`
			DetailLines        []string `json:"detail_lines"`
			TxID               string   `json:"txid"`
			PreviewHash        string   `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(previewRec.Body.Bytes(), &previewResp); err != nil {
		t.Fatalf("decode bsv21 token send preview failed: %v", err)
	}
	if !previewResp.Ok || !previewResp.Preview.CanSign || previewResp.Preview.TxID == "" || previewResp.Preview.PreviewHash == "" {
		t.Fatalf("unexpected bsv21 preview response: %+v body=%s", previewResp, previewRec.Body.String())
	}
	if len(previewResp.Preview.SelectedFeeUTXOIDs) != 1 {
		t.Fatalf("unexpected bsv21 fee selection: %+v", previewResp.Preview)
	}

	rec := httptest.NewRecorder()
	srv.handleWalletTokenSendSign(rec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/sign", strings.NewReader(`{"token_standard":"bsv21","asset_key":"bsv21:token-1","amount_text":"5","to_address":"n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V","expected_preview_hash":"`+previewResp.Preview.PreviewHash+`"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("token send sign status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		Ok          bool   `json:"ok"`
		Code        string `json:"code"`
		SignedTxHex string `json:"signed_tx_hex"`
		TxID        string `json:"txid"`
		Preview     struct {
			PreviewHash string `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode bsv21 token send sign failed: %v", err)
	}
	if !resp.Ok || resp.Code != "OK" || resp.SignedTxHex == "" || resp.TxID == "" {
		t.Fatalf("unexpected bsv21 sign response: %+v", resp)
	}
	if resp.Preview.PreviewHash != previewResp.Preview.PreviewHash {
		t.Fatalf("bsv21 preview hash mismatch: got=%q want=%q", resp.Preview.PreviewHash, previewResp.Preview.PreviewHash)
	}
}

func TestHandleWalletTokenSendSubmitProjectsBSV21Change(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	provider := &walletBSV21ProviderStub{
		tokenDetails: walletBSV21TokenDetails{
			Token: walletBSV21TokenData{
				Symbol: "T21",
			},
			Status: walletBSV21TokenStatus{
				IsActive:     true,
				FeePerOutput: 5,
				FeeAddress:   "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V",
			},
		},
	}
	rt := &Runtime{
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain:         walletAssetActionMockChain{},
		WalletBSV21Provider: provider,
	}
	rt.runIn.BSV.Network = "test"
	srv := &httpAPIServer{db: db, rt: rt}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	txHexA := buildBSV21SourceTxHexForHTTPTest(t, addr, "token-1", "7")
	txidA := appendWalletLocalBroadcastTxForHTTPTest(t, db, addr, txHexA, now)
	appendWalletUTXOForHTTPTest(t, db, addr, txidA+":0", txidA, 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, txidA+":0", walletAssetGroupToken, "bsv21", "bsv21:token-1", "T21", "7", "onesat-stack", map[string]any{"id": "token-1", "amt": "7"}, now)
	appendWalletUTXOForHTTPTest(t, db, addr, "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:0", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", 0, 1000, "unspent", walletUTXOAllocationPlainBSV, now+1)

	previewRec := httptest.NewRecorder()
	srv.handleWalletTokenSendPreview(previewRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/preview", strings.NewReader(`{"token_standard":"bsv21","asset_key":"bsv21:token-1","amount_text":"5","to_address":"`+toAddr+`"}`)))
	var previewResp struct {
		Preview struct {
			PreviewHash string `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(previewRec.Body.Bytes(), &previewResp); err != nil {
		t.Fatalf("decode bsv21 token send preview failed: %v", err)
	}

	signRec := httptest.NewRecorder()
	srv.handleWalletTokenSendSign(signRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/sign", strings.NewReader(`{"token_standard":"bsv21","asset_key":"bsv21:token-1","amount_text":"5","to_address":"`+toAddr+`","expected_preview_hash":"`+previewResp.Preview.PreviewHash+`"}`)))
	var signResp struct {
		Ok          bool   `json:"ok"`
		SignedTxHex string `json:"signed_tx_hex"`
		TxID        string `json:"txid"`
	}
	if err := json.Unmarshal(signRec.Body.Bytes(), &signResp); err != nil {
		t.Fatalf("decode bsv21 token send sign failed: %v", err)
	}
	if !signResp.Ok || signResp.SignedTxHex == "" || signResp.TxID == "" {
		t.Fatalf("bsv21 token send sign failed: %+v", signResp)
	}
	rt.WalletAssetDetector = walletAssetDetectorStub{
		items: map[string]walletUTXOAssetClassification{
			signResp.TxID + ":1": {
				AllocationClass:  walletUTXOAllocationProtectedAsset,
				AllocationReason: "indexed bsv21 output",
				Assets: []walletUTXOAssetBinding{
					{
						UTXOID:        signResp.TxID + ":1",
						AssetGroup:    walletAssetGroupToken,
						AssetStandard: "bsv21",
						AssetKey:      "bsv21:token-1",
						AssetSymbol:   "T21",
						QuantityText:  "2",
						SourceName:    "onesat-stack",
						Payload:       map[string]any{"id": "token-1", "amt": "2"},
					},
				},
			},
		},
	}

	submitRec := httptest.NewRecorder()
	srv.handleWalletTokenSendSubmit(submitRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/tokens/send/submit", strings.NewReader(`{"signed_tx_hex":"`+signResp.SignedTxHex+`"}`)))
	if submitRec.Code != http.StatusOK {
		t.Fatalf("bsv21 token send submit status mismatch: got=%d want=%d body=%s", submitRec.Code, http.StatusOK, submitRec.Body.String())
	}
	var submitResp struct {
		Ok   bool   `json:"ok"`
		Code string `json:"code"`
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(submitRec.Body.Bytes(), &submitResp); err != nil {
		t.Fatalf("decode bsv21 token send submit failed: %v", err)
	}
	if !submitResp.Ok || submitResp.Code != "OK" || submitResp.TxID == "" {
		t.Fatalf("bsv21 token send submit response mismatch: %+v", submitResp)
	}
	var spentCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM wallet_utxo WHERE txid=? AND state='spent'`, txidA).Scan(&spentCount); err != nil {
		t.Fatalf("query spent bsv21 input failed: %v", err)
	}
	if spentCount != 1 {
		t.Fatalf("expected bsv21 input to be spent, got=%d", spentCount)
	}
	var changeQty string
	if err := db.QueryRow(`SELECT quantity_text FROM wallet_utxo_assets WHERE utxo_id=?`, signResp.TxID+":1").Scan(&changeQty); err != nil {
		t.Fatalf("query bsv21 token change failed: %v", err)
	}
	if changeQty != "2" {
		t.Fatalf("bsv21 token change quantity mismatch: got=%q want=%q", changeQty, "2")
	}
	if len(provider.submittedTokenIDs) != 1 || provider.submittedTokenIDs[0] != "token-1" {
		t.Fatalf("unexpected bsv21 overlay submit tokens: %+v", provider.submittedTokenIDs)
	}
	if len(provider.submittedTxIDs) != 1 || provider.submittedTxIDs[0] != signResp.TxID {
		t.Fatalf("unexpected bsv21 overlay submit txids: %+v", provider.submittedTxIDs)
	}
}

func TestHandleWalletOrdinals_ListAndDetail(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", "1111111111111111111111111111111111111111111111111111111111111111", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-1", "image/png", "1", "onesat-stack", map[string]any{"origin": "origin-1", "contentType": "image/png"}, now)

	appendWalletUTXOForHTTPTest(t, db, addr, "2222222222222222222222222222222222222222222222222222222222222222:1", "2222222222222222222222222222222222222222222222222222222222222222", 1, 1, "unspent", walletUTXOAllocationProtectedAsset, now+1)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "2222222222222222222222222222222222222222222222222222222222222222:1", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-2", "text/plain", "1", "onesat-stack", map[string]any{"origin": "origin-2", "contentType": "text/plain"}, now+1)

	appendWalletUTXOForHTTPTest(t, db, addr, "3333333333333333333333333333333333333333333333333333333333333333:0", "3333333333333333333333333333333333333333333333333333333333333333", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now+2)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "3333333333333333333333333333333333333333333333333333333333333333:0", walletAssetGroupListing, "ordlock", "ordlock:3333333333333333333333333333333333333333333333333333333333333333:0", "", "1", "onesat-stack", map[string]any{"status": "listed"}, now+2)

	appendWalletUTXOForHTTPTest(t, db, addr, "4444444444444444444444444444444444444444444444444444444444444444:0", "4444444444444444444444444444444444444444444444444444444444444444", 0, 1, "spent", walletUTXOAllocationProtectedAsset, now+3)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "4444444444444444444444444444444444444444444444444444444444444444:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-spent", "image/jpeg", "1", "onesat-stack", map[string]any{"origin": "origin-spent"}, now+3)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ordinals?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletOrdinals(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ordinal list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var list struct {
		WalletAddress string `json:"wallet_address"`
		Total         int    `json:"total"`
		Items         []struct {
			UTXOID      string          `json:"utxo_id"`
			AssetKey    string          `json:"asset_key"`
			AssetSymbol string          `json:"asset_symbol"`
			Payload     json.RawMessage `json:"payload"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode ordinal list failed: %v", err)
	}
	if list.WalletAddress != addr {
		t.Fatalf("wallet_address mismatch: got=%q want=%q", list.WalletAddress, addr)
	}
	if list.Total != 2 || len(list.Items) != 2 {
		t.Fatalf("ordinal list size mismatch: total=%d items=%d want=2", list.Total, len(list.Items))
	}
	if list.Items[0].UTXOID != "2222222222222222222222222222222222222222222222222222222222222222:1" {
		t.Fatalf("unexpected first ordinal item: %+v", list.Items[0])
	}
	if len(list.Items[0].Payload) == 0 {
		t.Fatalf("ordinal payload should not be empty")
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ordinals/detail?utxo_id=1111111111111111111111111111111111111111111111111111111111111111:0", nil)
	detailRec := httptest.NewRecorder()
	srv.handleWalletOrdinalDetail(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("ordinal detail status mismatch: got=%d want=%d body=%s", detailRec.Code, http.StatusOK, detailRec.Body.String())
	}
	var detail struct {
		UTXOID   string `json:"utxo_id"`
		AssetKey string `json:"asset_key"`
		Payload  struct {
			Origin string `json:"origin"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(detailRec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode ordinal detail failed: %v", err)
	}
	if detail.UTXOID != "1111111111111111111111111111111111111111111111111111111111111111:0" {
		t.Fatalf("ordinal detail utxo mismatch: got=%q", detail.UTXOID)
	}
	if detail.AssetKey != "ordinal:origin-1" || detail.Payload.Origin != "origin-1" {
		t.Fatalf("unexpected ordinal detail: %+v", detail)
	}

	notFoundReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ordinals/detail?asset_key=ordinal:not-found", nil)
	notFoundRec := httptest.NewRecorder()
	srv.handleWalletOrdinalDetail(notFoundRec, notFoundReq)
	if notFoundRec.Code != http.StatusNotFound {
		t.Fatalf("ordinal detail not found status mismatch: got=%d want=%d", notFoundRec.Code, http.StatusNotFound)
	}
}

func TestHandleWalletOrdinalEvents(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", "1111111111111111111111111111111111111111111111111111111111111111", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-1", "image/png", "1", "onesat-stack", map[string]any{"origin": "origin-1"}, now)
	appendWalletUTXOEventForHTTPTest(t, db, "1111111111111111111111111111111111111111111111111111111111111111:0", "detected", "1111111111111111111111111111111111111111111111111111111111111111", "", "detected by sync", map[string]any{"trigger": "test"}, now)
	appendWalletUTXOEventForHTTPTest(t, db, "1111111111111111111111111111111111111111111111111111111111111111:0", "local_broadcast_spent", "2222222222222222222222222222222222222222222222222222222222222222", "", "spent by local ordinal transfer", map[string]any{"trigger": "wallet_ordinal_transfer_submit"}, now+1)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/ordinals/events?asset_key=ordinal:origin-1&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletOrdinalEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ordinal events status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body struct {
		Total int `json:"total"`
		Items []struct {
			UTXOID        string `json:"utxo_id"`
			AssetStandard string `json:"asset_standard"`
			AssetKey      string `json:"asset_key"`
			EventType     string `json:"event_type"`
			RefTxID       string `json:"ref_txid"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode ordinal events failed: %v", err)
	}
	if body.Total != 2 || len(body.Items) != 2 {
		t.Fatalf("ordinal events size mismatch: total=%d items=%d want=2", body.Total, len(body.Items))
	}
	if body.Items[0].EventType != "local_broadcast_spent" || body.Items[0].RefTxID != "2222222222222222222222222222222222222222222222222222222222222222" {
		t.Fatalf("unexpected latest ordinal event: %+v", body.Items[0])
	}
	if body.Items[0].UTXOID != "1111111111111111111111111111111111111111111111111111111111111111:0" || body.Items[0].AssetStandard != "ordinal" || body.Items[0].AssetKey != "ordinal:origin-1" {
		t.Fatalf("unexpected ordinal event asset binding: %+v", body.Items[0])
	}
}

func TestHandleWalletOrdinalTransferPreview_InsufficientFee(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	addr := "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb"
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", "1111111111111111111111111111111111111111111111111111111111111111", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-1", "image/png", "1", "onesat-stack", map[string]any{"origin": "origin-1"}, now)

	body := strings.NewReader(`{"utxo_id":"1111111111111111111111111111111111111111111111111111111111111111:0","to_address":"` + toAddr + `"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/ordinals/transfer/preview", body)
	rec := httptest.NewRecorder()
	srv.handleWalletOrdinalTransferPreview(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ordinal transfer preview status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		Ok      bool   `json:"ok"`
		Code    string `json:"code"`
		Preview struct {
			Action                 string   `json:"action"`
			Feasible               bool     `json:"feasible"`
			CanSign                bool     `json:"can_sign"`
			SelectedAssetUTXOIDs   []string `json:"selected_asset_utxo_ids"`
			SelectedFeeUTXOIDs     []string `json:"selected_fee_utxo_ids"`
			EstimatedNetworkFeeBSV uint64   `json:"estimated_network_fee_bsv_sat"`
			WarningLevel           string   `json:"warning_level"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode ordinal transfer preview failed: %v", err)
	}
	if !resp.Ok || resp.Code != "OK" {
		t.Fatalf("ordinal transfer preview response mismatch: %+v", resp)
	}
	if resp.Preview.Action != "ordinals.transfer" || resp.Preview.Feasible || resp.Preview.CanSign {
		t.Fatalf("unexpected ordinal transfer preview state: %+v", resp.Preview)
	}
	if len(resp.Preview.SelectedAssetUTXOIDs) != 1 || len(resp.Preview.SelectedFeeUTXOIDs) != 0 {
		t.Fatalf("unexpected ordinal transfer preview selections: %+v", resp.Preview)
	}
	if resp.Preview.EstimatedNetworkFeeBSV == 0 || resp.Preview.WarningLevel != "high" {
		t.Fatalf("unexpected ordinal transfer preview fee/warning: %+v", resp.Preview)
	}
}

func TestHandleWalletOrdinalTransferPreviewAndSign(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain: walletAssetActionMockChain{},
	}
	rt.runIn.BSV.Network = "test"
	srv := &httpAPIServer{db: db, rt: rt}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", "1111111111111111111111111111111111111111111111111111111111111111", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-1", "image/png", "1", "onesat-stack", map[string]any{"origin": "origin-1"}, now)
	appendWalletUTXOForHTTPTest(t, db, addr, "2222222222222222222222222222222222222222222222222222222222222222:1", "2222222222222222222222222222222222222222222222222222222222222222", 1, 1200, "unspent", walletUTXOAllocationPlainBSV, now+1)

	previewBody := strings.NewReader(`{"utxo_id":"1111111111111111111111111111111111111111111111111111111111111111:0","to_address":"` + toAddr + `"}`)
	previewReq := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/ordinals/transfer/preview", previewBody)
	previewRec := httptest.NewRecorder()
	srv.handleWalletOrdinalTransferPreview(previewRec, previewReq)
	if previewRec.Code != http.StatusOK {
		t.Fatalf("ordinal transfer preview status mismatch: got=%d want=%d body=%s", previewRec.Code, http.StatusOK, previewRec.Body.String())
	}
	var previewResp struct {
		Ok      bool   `json:"ok"`
		Code    string `json:"code"`
		Preview struct {
			Feasible           bool     `json:"feasible"`
			CanSign            bool     `json:"can_sign"`
			SelectedFeeUTXOIDs []string `json:"selected_fee_utxo_ids"`
			TxID               string   `json:"txid"`
			PreviewHash        string   `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(previewRec.Body.Bytes(), &previewResp); err != nil {
		t.Fatalf("decode ordinal transfer preview failed: %v", err)
	}
	if !previewResp.Ok || previewResp.Code != "OK" {
		t.Fatalf("ordinal transfer preview response mismatch: %+v", previewResp)
	}
	if !previewResp.Preview.Feasible || !previewResp.Preview.CanSign {
		t.Fatalf("expected signable ordinal preview, got %+v", previewResp.Preview)
	}
	if len(previewResp.Preview.SelectedFeeUTXOIDs) != 1 {
		t.Fatalf("expected one fee utxo, got %+v", previewResp.Preview.SelectedFeeUTXOIDs)
	}
	if previewResp.Preview.TxID == "" || previewResp.Preview.PreviewHash == "" {
		t.Fatalf("expected txid and preview_hash, got %+v", previewResp.Preview)
	}

	signBody := strings.NewReader(`{"utxo_id":"1111111111111111111111111111111111111111111111111111111111111111:0","to_address":"` + toAddr + `","expected_preview_hash":"` + previewResp.Preview.PreviewHash + `"}`)
	signReq := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/ordinals/transfer/sign", signBody)
	signRec := httptest.NewRecorder()
	srv.handleWalletOrdinalTransferSign(signRec, signReq)
	if signRec.Code != http.StatusOK {
		t.Fatalf("ordinal transfer sign status mismatch: got=%d want=%d body=%s", signRec.Code, http.StatusOK, signRec.Body.String())
	}
	var signResp struct {
		Ok          bool   `json:"ok"`
		Code        string `json:"code"`
		SignedTxHex string `json:"signed_tx_hex"`
		TxID        string `json:"txid"`
		Preview     struct {
			PreviewHash string `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(signRec.Body.Bytes(), &signResp); err != nil {
		t.Fatalf("decode ordinal transfer sign failed: %v", err)
	}
	if !signResp.Ok || signResp.Code != "OK" {
		t.Fatalf("ordinal transfer sign response mismatch: %+v", signResp)
	}
	if signResp.SignedTxHex == "" || signResp.TxID == "" {
		t.Fatalf("expected signed tx and txid, got %+v", signResp)
	}
	if signResp.Preview.PreviewHash != previewResp.Preview.PreviewHash {
		t.Fatalf("preview hash mismatch: got=%q want=%q", signResp.Preview.PreviewHash, previewResp.Preview.PreviewHash)
	}
}

func TestHandleWalletOrdinalTransferSubmitProjectsWallet(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain: walletAssetActionMockChain{},
	}
	rt.runIn.BSV.Network = "test"
	srv := &httpAPIServer{db: db, rt: rt}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	toAddr := "n3a8j8mQ93hZB6k7iLFKcdpShUMpKyFp8V"
	now := time.Now().Unix()

	appendWalletUTXOForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", "1111111111111111111111111111111111111111111111111111111111111111", 0, 1, "unspent", walletUTXOAllocationProtectedAsset, now)
	appendWalletUTXOAssetForHTTPTest(t, db, addr, "1111111111111111111111111111111111111111111111111111111111111111:0", walletAssetGroupOrdinal, "ordinal", "ordinal:origin-1", "image/png", "1", "onesat-stack", map[string]any{"origin": "origin-1"}, now)
	appendWalletUTXOForHTTPTest(t, db, addr, "2222222222222222222222222222222222222222222222222222222222222222:1", "2222222222222222222222222222222222222222222222222222222222222222", 1, 1200, "unspent", walletUTXOAllocationPlainBSV, now+1)

	previewRec := httptest.NewRecorder()
	srv.handleWalletOrdinalTransferPreview(previewRec, httptest.NewRequest(http.MethodPost, "/api/v1/wallet/ordinals/transfer/preview", strings.NewReader(`{"utxo_id":"1111111111111111111111111111111111111111111111111111111111111111:0","to_address":"`+toAddr+`"}`)))
	var previewResp struct {
		Preview struct {
			PreviewHash string `json:"preview_hash"`
		} `json:"preview"`
	}
	if err := json.Unmarshal(previewRec.Body.Bytes(), &previewResp); err != nil {
		t.Fatalf("decode ordinal transfer preview failed: %v", err)
	}
	signBody := strings.NewReader(`{"utxo_id":"1111111111111111111111111111111111111111111111111111111111111111:0","to_address":"` + toAddr + `","expected_preview_hash":"` + previewResp.Preview.PreviewHash + `"}`)
	signReq := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/ordinals/transfer/sign", signBody)
	signRec := httptest.NewRecorder()
	srv.handleWalletOrdinalTransferSign(signRec, signReq)
	if signRec.Code != http.StatusOK {
		t.Fatalf("ordinal transfer sign status mismatch: got=%d want=%d body=%s", signRec.Code, http.StatusOK, signRec.Body.String())
	}
	var signResp struct {
		Ok          bool   `json:"ok"`
		SignedTxHex string `json:"signed_tx_hex"`
		TxID        string `json:"txid"`
	}
	if err := json.Unmarshal(signRec.Body.Bytes(), &signResp); err != nil {
		t.Fatalf("decode ordinal transfer sign failed: %v", err)
	}
	if !signResp.Ok {
		t.Fatalf("ordinal transfer sign failed: %+v", signResp)
	}

	submitReq := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/ordinals/transfer/submit", strings.NewReader(`{"signed_tx_hex":"`+signResp.SignedTxHex+`"}`))
	submitRec := httptest.NewRecorder()
	srv.handleWalletOrdinalTransferSubmit(submitRec, submitReq)
	if submitRec.Code != http.StatusOK {
		t.Fatalf("ordinal transfer submit status mismatch: got=%d want=%d body=%s", submitRec.Code, http.StatusOK, submitRec.Body.String())
	}
	var submitResp struct {
		Ok   bool   `json:"ok"`
		Code string `json:"code"`
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(submitRec.Body.Bytes(), &submitResp); err != nil {
		t.Fatalf("decode ordinal transfer submit failed: %v", err)
	}
	if !submitResp.Ok || submitResp.Code != "OK" || submitResp.TxID == "" {
		t.Fatalf("ordinal transfer submit response mismatch: %+v", submitResp)
	}
	var ordinalState string
	if err := db.QueryRow(`SELECT state FROM wallet_utxo WHERE utxo_id=?`, "1111111111111111111111111111111111111111111111111111111111111111:0").Scan(&ordinalState); err != nil {
		t.Fatalf("query spent ordinal state failed: %v", err)
	}
	if ordinalState != "spent" {
		t.Fatalf("ordinal utxo should be spent, got=%q", ordinalState)
	}
	var remainingOrdinals int
	if err := db.QueryRow(`SELECT COUNT(*) FROM wallet_utxo_assets WHERE asset_group=? AND address=?`, walletAssetGroupOrdinal, addr).Scan(&remainingOrdinals); err != nil {
		t.Fatalf("query remaining ordinals failed: %v", err)
	}
	if remainingOrdinals != 0 {
		t.Fatalf("expected ordinal projection to be cleared, got=%d", remainingOrdinals)
	}
}

type walletChainHistory404Stub struct{}

func (walletChainHistory404Stub) BaseURL() string {
	return "http://127.0.0.1:19183/v1/bsv/test"
}

func (walletChainHistory404Stub) GetAddressConfirmedUnspent(context.Context, string) ([]whatsonchain.UTXO, error) {
	return nil, errors.New("unexpected call")
}

func (walletChainHistory404Stub) GetChainInfo(context.Context) (uint32, error) {
	return 0, errors.New("unexpected call")
}

func (walletChainHistory404Stub) GetAddressConfirmedHistory(context.Context, string) ([]whatsonchain.AddressHistoryItem, error) {
	return nil, errors.New("unexpected call")
}

func (walletChainHistory404Stub) GetAddressConfirmedHistoryPage(context.Context, string, whatsonchain.ConfirmedHistoryQuery) (whatsonchain.ConfirmedHistoryPage, error) {
	return whatsonchain.ConfirmedHistoryPage{}, &whatsonchain.HTTPError{StatusCode: http.StatusNotFound, Body: "Not Found"}
}

func (walletChainHistory404Stub) GetAddressUnconfirmedHistory(context.Context, string) ([]string, error) {
	return nil, errors.New("unexpected call")
}

func (walletChainHistory404Stub) GetTxHash(context.Context, string) (whatsonchain.TxDetail, error) {
	return whatsonchain.TxDetail{}, errors.New("unexpected call")
}

func TestCollectConfirmedHistoryRange_TreatsEmptyHTTP404AsDone(t *testing.T) {
	t.Parallel()

	meta := walletSyncRoundMeta{
		RoundID:        "round-empty-404",
		Address:        "mpH3Cxe4RZDWzwyZWxTqosoBrVKNgbJrjb",
		TriggerSource:  "periodic_tick",
		APIBaseURL:     "http://127.0.0.1:19183/v1/bsv/test",
		WalletChainTyp: "*whatsonchain.Client",
		StartedAtUnix:  time.Now().Unix(),
	}
	cursor := walletUTXOHistoryCursor{
		Address:             meta.Address,
		AnchorHeight:        1725947,
		NextConfirmedHeight: 1726038,
	}

	items, next, err := collectConfirmedHistoryRange(context.Background(), walletChainHistory404Stub{}, meta.Address, cursor, 1726051, meta)
	if err != nil {
		t.Fatalf("collectConfirmedHistoryRange failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("history item count mismatch: got=%d want=0", len(items))
	}
	if next.NextConfirmedHeight != 1726052 {
		t.Fatalf("next.NextConfirmedHeight mismatch: got=%d want=1726052", next.NextConfirmedHeight)
	}
	if next.NextPageToken != "" {
		t.Fatalf("next.NextPageToken mismatch: got=%q want empty", next.NextPageToken)
	}
	if next.LastError != "" {
		t.Fatalf("next.LastError mismatch: got=%q want empty", next.LastError)
	}
}

func TestHandleWalletSummary_IncludesStaleAndSchedulerDiagnostics(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db, runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	walletID := walletIDByAddress(addr)
	now := time.Now().Unix()
	runtimeStartedAt := now + 600
	if _, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		addr, walletID, 0, 0, now, `http 502: {"error":"context canceled"}`, "chain_utxo_worker", "periodic_tick", 0, "", "", "", 0,
	); err != nil {
		t.Fatalf("seed wallet_utxo_sync_state failed: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO scheduler_tasks(
			task_name,owner,mode,status,interval_seconds,created_at_unix,updated_at_unix,closed_at_unix,
			last_trigger,last_started_at_unix,last_ended_at_unix,last_duration_ms,last_error,in_flight,
			run_count,success_count,failure_count,last_summary_json,meta_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"chain_utxo_sync", "chain_maintenance", "static", "active", 10, now, now, 0,
		"periodic_tick", now+10, now+10, 0, "context canceled", 1,
		1, 0, 1, "{}", "{}",
	); err != nil {
		t.Fatalf("seed scheduler_tasks failed: %v", err)
	}

	rt.StartedAtUnix = runtimeStartedAt
	rt.chainMaint = &chainMaintainer{
		queue: make(chan chainTask, 2),
		status: chainSchedulerStatus{
			InFlight:          false,
			InFlightTaskType:  "",
			LastTaskStartedAt: now + 20,
			LastTaskEndedAt:   now + 21,
			LastError:         "",
		},
	}
	rt.chainMaint.queue <- chainTask{TaskType: chainTaskUTXO}
	srv := &httpAPIServer{db: db, rt: rt, startedAt: time.Unix(runtimeStartedAt+60, 0)}
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
	if got := int64(body["runtime_started_at_unix"].(float64)); got != runtimeStartedAt {
		t.Fatalf("runtime_started_at_unix mismatch: got=%d want=%d", got, runtimeStartedAt)
	}
	if stale, _ := body["wallet_utxo_sync_state_is_stale"].(bool); !stale {
		t.Fatalf("wallet_utxo_sync_state_is_stale mismatch: got=%v want=true", body["wallet_utxo_sync_state_is_stale"])
	}
	if got, _ := body["wallet_utxo_sync_state_stale_reason"].(string); got != "sync_state_older_than_runtime" {
		t.Fatalf("wallet_utxo_sync_state_stale_reason mismatch: got=%v want=sync_state_older_than_runtime", body["wallet_utxo_sync_state_stale_reason"])
	}
	if got, _ := body["wallet_utxo_sync_scheduler_status"].(string); got != "active" {
		t.Fatalf("wallet_utxo_sync_scheduler_status mismatch: got=%v want=active", body["wallet_utxo_sync_scheduler_status"])
	}
	if inFlight, _ := body["wallet_utxo_sync_scheduler_in_flight"].(bool); !inFlight {
		t.Fatalf("wallet_utxo_sync_scheduler_in_flight mismatch: got=%v want=true", body["wallet_utxo_sync_scheduler_in_flight"])
	}
	if got, _ := body["wallet_utxo_sync_scheduler_last_error"].(string); got != "context canceled" {
		t.Fatalf("wallet_utxo_sync_scheduler_last_error mismatch: got=%v want=context canceled", body["wallet_utxo_sync_scheduler_last_error"])
	}
	if got := int(body["chain_maintainer_queue_length"].(float64)); got != 1 {
		t.Fatalf("chain_maintainer_queue_length mismatch: got=%d want=1", got)
	}
	if inFlight, _ := body["chain_maintainer_in_flight"].(bool); inFlight {
		t.Fatalf("chain_maintainer_in_flight mismatch: got=%v want=false", body["chain_maintainer_in_flight"])
	}
	if got, _ := body["chain_maintainer_in_flight_task_type"].(string); got != "" {
		t.Fatalf("chain_maintainer_in_flight_task_type mismatch: got=%q want=\"\"", got)
	}
	if got := int64(body["chain_maintainer_last_task_started_at_unix"].(float64)); got != now+20 {
		t.Fatalf("chain_maintainer_last_task_started_at_unix mismatch: got=%d want=%d", got, now+20)
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

func TestInitIndexDB_MigrateLegacyWalletFundFlowsVisitColumns(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "legacy-client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE wallet_fund_flows(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at_unix INTEGER NOT NULL DEFAULT 0,
		flow_id TEXT NOT NULL DEFAULT '',
		flow_type TEXT NOT NULL DEFAULT '',
		ref_id TEXT NOT NULL DEFAULT '',
		stage TEXT NOT NULL DEFAULT '',
		direction TEXT NOT NULL DEFAULT '',
		purpose TEXT NOT NULL DEFAULT '',
		amount_satoshi INTEGER NOT NULL DEFAULT 0,
		used_satoshi INTEGER NOT NULL DEFAULT 0,
		returned_satoshi INTEGER NOT NULL DEFAULT 0,
		related_txid TEXT NOT NULL DEFAULT '',
		note TEXT NOT NULL DEFAULT '',
		payload_json TEXT NOT NULL DEFAULT '{}'
	)`); err != nil {
		t.Fatalf("create legacy wallet_fund_flows: %v", err)
	}

	if err := initIndexDB(db); err != nil {
		t.Fatalf("initIndexDB migrate legacy wallet_fund_flows: %v", err)
	}

	cols, err := tableColumns(db, "wallet_fund_flows")
	if err != nil {
		t.Fatalf("tableColumns wallet_fund_flows: %v", err)
	}
	if _, ok := cols["visit_id"]; !ok {
		t.Fatalf("visit_id column should be added")
	}
	if _, ok := cols["visit_locator"]; !ok {
		t.Fatalf("visit_locator column should be added")
	}
	if _, err := db.Exec(`INSERT INTO wallet_fund_flows(
		created_at_unix,visit_id,visit_locator,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		"visit-legacy",
		"node:legacy/index",
		"legacy-flow",
		"fee_pool",
		"legacy-ref",
		"legacy-stage",
		"out",
		broadcastmodule.QuoteServiceTypeNodeReachabilityQuery,
		-1,
		1,
		0,
		"legacy-tx",
		"legacy note",
		`{"ok":true}`,
	); err != nil {
		t.Fatalf("insert migrated wallet_fund_flows: %v", err)
	}
}
