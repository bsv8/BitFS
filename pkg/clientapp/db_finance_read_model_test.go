package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFinanceReadModel_ExposesNewFieldsAndCompatibility(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_finance_read_1",
		BaseTxID:          "base_tx_finance_read_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_finance_read_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_finance_read_1", baseTxHex, 700, 290, sellerPubHex)

	biz, err := dbGetFinanceBusiness(ctx, store, "biz_c2c_pay_"+sessionID+"_2")
	if err != nil {
		t.Fatalf("get finance business failed: %v", err)
	}
	wantAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	if biz.SourceType != "pool_allocation" || biz.SourceID != wantAllocationID {
		t.Fatalf("unexpected business source fields: %+v", biz)
	}
	if biz.AccountingScene != "c2c_transfer" || biz.AccountingSubtype != "chunk_pay" {
		t.Fatalf("unexpected business accounting fields: %+v", biz)
	}
	if biz.SceneType != "c2c_transfer" || biz.SceneSubType != "chunk_pay" || biz.RefID != sessionID {
		t.Fatalf("legacy business fields changed: %+v", biz)
	}

	bizPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          wantAllocationID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("list finance businesses by new filters failed: %v", err)
	}
	if bizPage.Total != 1 || len(bizPage.Items) != 1 {
		t.Fatalf("business page mismatch: total=%d items=%d", bizPage.Total, len(bizPage.Items))
	}
	if bizPage.Items[0].SourceType != "pool_allocation" || bizPage.Items[0].SourceID != wantAllocationID {
		t.Fatalf("business page source mismatch: %+v", bizPage.Items[0])
	}

	oldBizPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:        10,
		SceneType:    "c2c_transfer",
		SceneSubType: "chunk_pay",
		RefID:        sessionID,
	})
	if err != nil {
		t.Fatalf("list finance businesses by old filters failed: %v", err)
	}
	if oldBizPage.Total != 1 || len(oldBizPage.Items) != 1 {
		t.Fatalf("old business filter mismatch: total=%d items=%d", oldBizPage.Total, len(oldBizPage.Items))
	}

	var processID int64
	if err := db.QueryRow(
		`SELECT id FROM fin_process_events WHERE process_id=? AND scene_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&processID); err != nil {
		t.Fatalf("query process event id failed: %v", err)
	}
	proc, err := dbGetFinanceProcessEvent(ctx, store, processID)
	if err != nil {
		t.Fatalf("get finance process event failed: %v", err)
	}
	wantCloseAllocationID := directTransferPoolAllocationID(sessionID, "close", 3)
	if proc.SourceType != "pool_allocation" || proc.SourceID != wantCloseAllocationID {
		t.Fatalf("unexpected process source fields: %+v", proc)
	}
	if proc.AccountingScene != "c2c_transfer" || proc.AccountingSubtype != "close" {
		t.Fatalf("unexpected process accounting fields: %+v", proc)
	}
	if proc.SceneType != "c2c_transfer" || proc.SceneSubType != "close" || proc.RefID != sessionID {
		t.Fatalf("legacy process fields changed: %+v", proc)
	}

	procPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          wantCloseAllocationID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "close",
	})
	if err != nil {
		t.Fatalf("list finance process events by new filters failed: %v", err)
	}
	if procPage.Total != 1 || len(procPage.Items) != 1 {
		t.Fatalf("process page mismatch: total=%d items=%d", procPage.Total, len(procPage.Items))
	}
	if procPage.Items[0].SourceType != "pool_allocation" || procPage.Items[0].SourceID != wantCloseAllocationID {
		t.Fatalf("process page source mismatch: %+v", procPage.Items[0])
	}

	oldProcPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:        10,
		ProcessID:    "proc_c2c_transfer_" + sessionID,
		SceneType:    "c2c_transfer",
		SceneSubType: "close",
		RefID:        sessionID,
	})
	if err != nil {
		t.Fatalf("list finance process events by old filters failed: %v", err)
	}
	if oldProcPage.Total != 1 || len(oldProcPage.Items) != 1 {
		t.Fatalf("old process filter mismatch: total=%d items=%d", oldProcPage.Total, len(oldProcPage.Items))
	}
}

func TestFinanceReadModel_TracesByPoolAllocationID(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_finance_trace_1",
		BaseTxID:          "base_tx_finance_trace_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_finance_trace_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_finance_trace_1", baseTxHex, 700, 290, sellerPubHex)

	allocationID := directTransferPoolAllocationID(sessionID, "close", 3)
	bizPage, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, allocationID, 20, 0)
	if err != nil {
		t.Fatalf("trace businesses by pool_allocation_id failed: %v", err)
	}
	if bizPage.Total != 1 || len(bizPage.Items) != 1 {
		t.Fatalf("trace business page mismatch: total=%d items=%d", bizPage.Total, len(bizPage.Items))
	}
	if bizPage.Items[0].SourceType != "pool_allocation" || bizPage.Items[0].SourceID != allocationID {
		t.Fatalf("trace business source mismatch: %+v", bizPage.Items[0])
	}

	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, allocationID, 20, 0)
	if err != nil {
		t.Fatalf("trace process events by pool_allocation_id failed: %v", err)
	}
	if procPage.Total != 1 || len(procPage.Items) != 1 {
		t.Fatalf("trace process page mismatch: total=%d items=%d", procPage.Total, len(procPage.Items))
	}
	if procPage.Items[0].SourceType != "pool_allocation" || procPage.Items[0].SourceID != allocationID {
		t.Fatalf("trace process source mismatch: %+v", procPage.Items[0])
	}
}

func TestAdminFinanceHTTP_ReadsNewFieldsAndParams(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_finance_http_1",
		BaseTxID:          "base_tx_finance_http_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_finance_http_1")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_finance_http_1", baseTxHex, 700, 290, sellerPubHex)

	srv := &httpAPIServer{db: db, store: store}

	allocationID := directTransferPoolAllocationID(sessionID, "close", 3)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?pool_allocation_id="+allocationID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("business list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var businessResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &businessResp); err != nil {
		t.Fatalf("decode business list response failed: %v", err)
	}
	if len(businessResp.Items) != 1 {
		t.Fatalf("business list item count mismatch: %d", len(businessResp.Items))
	}
	if businessResp.Items[0].SourceType != "pool_allocation" || businessResp.Items[0].SourceID != allocationID {
		t.Fatalf("business list source mismatch: %+v", businessResp.Items[0])
	}
	if businessResp.Items[0].AccountingScene == "" || businessResp.Items[0].AccountingSubtype == "" {
		t.Fatalf("business list missing accounting fields: %+v", businessResp.Items[0])
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/detail?business_id=biz_c2c_close_"+sessionID, nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinessDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("business detail status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var businessDetail financeBusinessItem
	if err := json.Unmarshal(rec.Body.Bytes(), &businessDetail); err != nil {
		t.Fatalf("decode business detail failed: %v", err)
	}
	if businessDetail.SourceType != "pool_allocation" || businessDetail.SourceID != allocationID {
		t.Fatalf("business detail source mismatch: %+v", businessDetail)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?scene_type=c2c_transfer&scene_subtype=close&ref_id="+sessionID+"&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var processResp struct {
		Items []financeProcessEventItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &processResp); err != nil {
		t.Fatalf("decode process list response failed: %v", err)
	}
	if len(processResp.Items) != 1 {
		t.Fatalf("process list item count mismatch: %d", len(processResp.Items))
	}
	if processResp.Items[0].SceneType != "c2c_transfer" || processResp.Items[0].SceneSubType != "close" || processResp.Items[0].RefID != sessionID {
		t.Fatalf("process list legacy fields mismatch: %+v", processResp.Items[0])
	}

	var processID int64
	if err := db.QueryRow(
		`SELECT id FROM fin_process_events WHERE process_id=? AND scene_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&processID); err != nil {
		t.Fatalf("query process id failed: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events/detail?id="+itoa64(processID), nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEventDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process detail status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var processDetail financeProcessEventItem
	if err := json.Unmarshal(rec.Body.Bytes(), &processDetail); err != nil {
		t.Fatalf("decode process detail failed: %v", err)
	}
	if processDetail.SourceType != "pool_allocation" || processDetail.SourceID != allocationID {
		t.Fatalf("process detail source mismatch: %+v", processDetail)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?pool_allocation_id="+allocationID+"&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process list by pool_allocation_id status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &processResp); err != nil {
		t.Fatalf("decode process list by allocation failed: %v", err)
	}
	if len(processResp.Items) != 1 || processResp.Items[0].SourceID != allocationID {
		t.Fatalf("process list by allocation mismatch: %+v", processResp.Items)
	}
}
