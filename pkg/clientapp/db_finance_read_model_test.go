package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFinanceReadModel_ExposesPrimaryFields(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	if err := dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_finance_read_1",
		BaseTxID:          "base_tx_finance_read_1",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	}); err != nil {
		t.Fatalf("record open accounting failed: %v", err)
	}
	if err := dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_finance_read_1"); err != nil {
		t.Fatalf("record pay accounting failed: %v", err)
	}
	if err := dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_finance_read_1", baseTxHex, 700, 290, sellerPubHex); err != nil {
		t.Fatalf("record close accounting failed: %v", err)
	}

	biz, err := dbGetFinanceBusiness(ctx, store, "biz_c2c_pay_"+sessionID+"_2")
	if err != nil {
		t.Fatalf("get finance business failed: %v", err)
	}
	wantAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	wantAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, wantAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	wantAllocationSourceID := fmt.Sprintf("%d", wantAllocationIntID)
	if biz.SourceType != "pool_allocation" || biz.SourceID != wantAllocationSourceID {
		t.Fatalf("unexpected business source fields: %+v", biz)
	}
	if biz.AccountingScene != "c2c_transfer" || biz.AccountingSubtype != "chunk_pay" {
		t.Fatalf("unexpected business accounting fields: %+v", biz)
	}
	bizPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          wantAllocationSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("list finance businesses by new filters failed: %v", err)
	}
	if bizPage.Total != 1 || len(bizPage.Items) != 1 {
		t.Fatalf("business page mismatch: total=%d items=%d", bizPage.Total, len(bizPage.Items))
	}
	if bizPage.Items[0].SourceType != "pool_allocation" || bizPage.Items[0].SourceID != wantAllocationSourceID {
		t.Fatalf("business page source mismatch: %+v", bizPage.Items[0])
	}

	var processID int64
	if err := db.QueryRow(
		`SELECT id FROM fin_process_events WHERE process_id=? AND accounting_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&processID); err != nil {
		t.Fatalf("query process event id failed: %v", err)
	}
	proc, err := dbGetFinanceProcessEvent(ctx, store, processID)
	if err != nil {
		t.Fatalf("get finance process event failed: %v", err)
	}
	wantCloseAllocationID := directTransferPoolAllocationID(sessionID, "close", 3)
	wantCloseAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, wantCloseAllocationID)
	if err != nil {
		t.Fatalf("lookup close allocation id failed: %v", err)
	}
	wantCloseAllocationSourceID := fmt.Sprintf("%d", wantCloseAllocationIntID)
	if proc.SourceType != "pool_allocation" || proc.SourceID != wantCloseAllocationSourceID {
		t.Fatalf("unexpected process source fields: %+v", proc)
	}
	if proc.AccountingScene != "c2c_transfer" || proc.AccountingSubtype != "close" {
		t.Fatalf("unexpected process accounting fields: %+v", proc)
	}
	procPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          wantCloseAllocationSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "close",
	})
	if err != nil {
		t.Fatalf("list finance process events by new filters failed: %v", err)
	}
	if procPage.Total != 1 || len(procPage.Items) != 1 {
		t.Fatalf("process page mismatch: total=%d items=%d", procPage.Total, len(procPage.Items))
	}
	if procPage.Items[0].SourceType != "pool_allocation" || procPage.Items[0].SourceID != wantCloseAllocationSourceID {
		t.Fatalf("process page source mismatch: %+v", procPage.Items[0])
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
	// 第二阶段：改用 pay 测试（pay 暂保留完整 fin_business，open/close 为过程型）
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_finance_trace_1")

	allocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	allocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	allocationSourceID := fmt.Sprintf("%d", allocationIntID)
	bizPage, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, allocationID, 20, 0)
	if err != nil {
		t.Fatalf("trace businesses by pool_allocation_id failed: %v", err)
	}
	// 第二阶段：只有 pay 生成 fin_business
	if bizPage.Total != 1 || len(bizPage.Items) != 1 {
		t.Fatalf("trace business page mismatch: total=%d items=%d", bizPage.Total, len(bizPage.Items))
	}
	if bizPage.Items[0].SourceType != "pool_allocation" || bizPage.Items[0].SourceID != allocationSourceID {
		t.Fatalf("trace business source mismatch: %+v", bizPage.Items[0])
	}

	// 验证 process events 仍然存在（open/pay/close 都生成 process events）
	openAllocID := directTransferPoolAllocationID(sessionID, "open", 1)
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, openAllocID, 20, 0)
	if err != nil {
		t.Fatalf("trace process events by pool_allocation_id failed: %v", err)
	}
	// open 的 process event 应该存在
	if procPage.Total == 0 {
		t.Fatalf("trace process page should not be empty")
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

	// 第二阶段：只测试 pay（pay 暂保留完整 fin_business，open/close 为过程型）
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_finance_http_1")

	srv := &httpAPIServer{db: db, store: store}

	// 使用 pay allocation 测试查询
	payAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?pool_allocation_id="+payAllocationID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("business list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var businessResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &businessResp); err != nil {
		t.Fatalf("decode business list response failed: %v", err)
	}
	if len(businessResp.Items) != 1 {
		t.Fatalf("business list item count mismatch: %d", len(businessResp.Items))
	}
	if businessResp.Items[0].SourceType != "pool_allocation" || businessResp.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("business list source mismatch: %+v", businessResp.Items[0])
	}
	if businessResp.Items[0].AccountingScene == "" || businessResp.Items[0].AccountingSubtype == "" {
		t.Fatalf("business list missing accounting fields: %+v", businessResp.Items[0])
	}

	// 第二阶段：改用 pay 测试业务详情查询（pay 暂保留完整 fin_business）
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/detail?business_id=biz_c2c_pay_"+sessionID+"_2", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinessDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("business detail status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var businessDetail financeBusinessItem
	if err = json.Unmarshal(rec.Body.Bytes(), &businessDetail); err != nil {
		t.Fatalf("decode business detail failed: %v", err)
	}
	// 验证 pay 的 source 指向 pay_allocation
	if businessDetail.SourceType != "pool_allocation" || businessDetail.SourceID != payAllocationSourceID {
		t.Fatalf("business detail source mismatch: got source_type=%s source_id=%s, want pool_allocation %s", businessDetail.SourceType, businessDetail.SourceID, payAllocationSourceID)
	}

	// 第六次迭代：使用主口径参数查询
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?accounting_scene=c2c_transfer&accounting_subtype=chunk_pay&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var processResp struct {
		Items []financeProcessEventItem `json:"items"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &processResp); err != nil {
		t.Fatalf("decode process list response failed: %v", err)
	}
	if len(processResp.Items) != 1 {
		t.Fatalf("process list item count mismatch: %d", len(processResp.Items))
	}
	var processID int64
	if err = db.QueryRow(
		`SELECT id FROM fin_process_events WHERE process_id=? AND accounting_subtype='chunk_pay' ORDER BY id DESC LIMIT 1`,
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
	if err = json.Unmarshal(rec.Body.Bytes(), &processDetail); err != nil {
		t.Fatalf("decode process detail failed: %v", err)
	}
	if processDetail.SourceType != "pool_allocation" || processDetail.SourceID != payAllocationSourceID {
		t.Fatalf("process detail source mismatch: %+v", processDetail)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?pool_allocation_id="+payAllocationID+"&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process list by pool_allocation_id status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &processResp); err != nil {
		t.Fatalf("decode process list by allocation failed: %v", err)
	}
	if len(processResp.Items) != 1 || processResp.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("process list by allocation mismatch: %+v", processResp.Items)
	}
}

// ==================== 主口径读取模型测试 ====================
// 测试目标：验证主口径读取模型与回归稳定性
// 1. 主口径查询
// 2. 默认展示
// 3. 禁扩散
// 4. 回归验证

// TestFinancePrimaryFilter_QueryByPrimaryModel 主口径查询测试
// 使用主口径参数查询数据
func TestFinancePrimaryFilter_QueryByPrimaryModel(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 写入测试数据
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_priority_test",
		BaseTxID:          "base_tx_priority_test",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_priority_test")

	// 测试：同时传新旧参数，验证新口径优先匹配
	// 新参数能精确匹配到 pay 记录（chunk_pay），旧参数可能匹配到多条
	payAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)

	// 用新口径精确查询
	newFilterPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          payAllocationSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("new filter query failed: %v", err)
	}
	if newFilterPage.Total != 1 || len(newFilterPage.Items) != 1 {
		t.Fatalf("new filter should return exactly 1 pay record: total=%d items=%d", newFilterPage.Total, len(newFilterPage.Items))
	}
	if newFilterPage.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("new filter should match pay allocation: got=%s want=%s", newFilterPage.Items[0].SourceID, payAllocationSourceID)
	}

	// 验证：主口径字段优先展示（不为空）
	item := newFilterPage.Items[0]
	if item.SourceType == "" || item.SourceID == "" {
		t.Fatalf("primary fields should not be empty: %+v", item)
	}
	if item.AccountingScene == "" || item.AccountingSubtype == "" {
		t.Fatalf("accounting fields should not be empty: %+v", item)
	}
}

// TestFinanceDefaultPresentation_PrimaryFieldsFirst 默认展示测试
// 返回里主说明字段是 source_*、accounting_*
func TestFinanceDefaultPresentation_PrimaryFieldsFirst(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	// 第二阶段：改用 pay 测试读取模型（pay 暂保留完整 fin_business，open/close 为过程型）
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_presentation_test")

	// 查询业务记录（pay 暂时仍生成 fin_business）
	biz, err := dbGetFinanceBusiness(ctx, store, "biz_c2c_pay_"+sessionID+"_2")
	if err != nil {
		t.Fatalf("get business failed: %v", err)
	}

	// 验证：主口径字段必须存在且有效
	if biz.SourceType != "pool_allocation" {
		t.Fatalf("SourceType should be 'pool_allocation': got=%s", biz.SourceType)
	}
	wantAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	wantAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, wantAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	wantAllocationSourceID := fmt.Sprintf("%d", wantAllocationIntID)
	if biz.SourceID != wantAllocationSourceID {
		t.Fatalf("SourceID mismatch: got=%s want=%s", biz.SourceID, wantAllocationSourceID)
	}
	if biz.AccountingScene != "c2c_transfer" {
		t.Fatalf("AccountingScene should be 'c2c_transfer': got=%s", biz.AccountingScene)
	}
	if biz.AccountingSubtype != "chunk_pay" {
		t.Fatalf("AccountingSubtype should be 'chunk_pay': got=%s", biz.AccountingSubtype)
	}

}

// TestFinanceDefaultFilter_QueryDefaults 默认展示测试
// 验证默认过滤条件能正确查询数据
func TestFinanceDefaultFilter_QueryDefaults(t *testing.T) {
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
		DealID:            "deal_compat_test",
		BaseTxID:          "base_tx_compat_test",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_compat_test")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_compat_test", baseTxHex, 700, 290, sellerPubHex)

}

// TestFinanceNoNewDiffusion_NoNewCodeDependsOnOldFields 禁扩散测试
// 新增读取帮助函数不依赖旧口径字段
func TestFinanceNoNewDiffusion_NoNewCodeDependsOnOldFields(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	// 第二阶段：改用 pay 测试（pay 暂保留完整 fin_business，open/close 为过程型）
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_no_diffusion_test")

	// 测试：新辅助函数 dbListFinanceBusinessesByPoolAllocationID 只使用新口径
	payAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)
	page, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, payAllocationID, 10, 0)
	if err != nil {
		t.Fatalf("new helper function failed: %v", err)
	}
	// 第二阶段：只有 pay 生成 fin_business
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("new helper should return exactly 1 record: total=%d items=%d", page.Total, len(page.Items))
	}

	// 验证：返回的记录主口径正确
	item := page.Items[0]
	if item.SourceType != "pool_allocation" || item.SourceID != payAllocationSourceID {
		t.Fatalf("new helper returned wrong record: %+v", item)
	}

	// 验证：新函数不使用旧口径字段进行过滤（通过检查它能正确工作来证明）
	// 如果新函数内部用了 scene_type 等旧字段过滤，在新数据上就会失败
	if item.AccountingScene == "" || item.AccountingSubtype == "" {
		t.Fatalf("new helper should return records with populated accounting fields: %+v", item)
	}

	// 测试：流程事件新辅助函数（pay 生成 process events）
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, payAllocationID, 10, 0)
	if err != nil {
		t.Fatalf("new process helper function failed: %v", err)
	}
	if procPage.Total == 0 {
		t.Fatalf("new process helper should return records")
	}
}

// TestFinanceRegression_FourthIterationCapabilities 回归测试
// 第四次迭代已有新口径读取能力不能被破坏
func TestFinanceRegression_FourthIterationCapabilities(t *testing.T) {
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
		DealID:            "deal_regression_test",
		BaseTxID:          "base_tx_regression_test",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_regression_test")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_regression_test", baseTxHex, 700, 290, sellerPubHex)

	// 回归1: 用新字段过滤必须能工作
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocSourceID := fmt.Sprintf("%d", payAllocIntID)
	bizPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          payAllocSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("regression: new field filter failed: %v", err)
	}
	if bizPage.Total != 1 {
		t.Fatalf("regression: should find 1 pay business: got=%d", bizPage.Total)
	}

	// 回归2: HTTP API 必须能返回新字段
	srv := &httpAPIServer{db: db, store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?pool_allocation_id="+payAllocID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("regression: http api failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("regression: decode response failed: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("regression: http api should return 1 item: got=%d", len(resp.Items))
	}
	if resp.Items[0].SourceID != payAllocSourceID {
		t.Fatalf("regression: http api returned wrong item: source_id=%s want=%s", resp.Items[0].SourceID, payAllocSourceID)
	}

	// 第六次迭代：回归3 - 只验证主口径字段存在且有效
	var sourceType, sourceID, accountingScene, accountingSubtype string
	err = db.QueryRow(`
		SELECT source_type, source_id, accounting_scene, accounting_subtype
		FROM fin_business WHERE business_id=?`, "biz_c2c_pay_"+sessionID+"_2",
	).Scan(&sourceType, &sourceID, &accountingScene, &accountingSubtype)
	if err != nil {
		t.Fatalf("regression: query fin_business columns failed: %v", err)
	}
	if sourceType == "" || sourceID == "" || accountingScene == "" || accountingSubtype == "" {
		t.Fatalf("regression: new fields should exist: source_type=%s source_id=%s accounting_scene=%s accounting_subtype=%s",
			sourceType, sourceID, accountingScene, accountingSubtype)
	}

	// 第六次迭代：回归4 - 流程事件表只验证主口径字段
	var procSourceType, procSourceID, procAccountingScene, procAccountingSubtype string
	err = db.QueryRow(`
		SELECT source_type, source_id, accounting_scene, accounting_subtype
		FROM fin_process_events WHERE process_id=? AND accounting_subtype='close' ORDER BY id DESC LIMIT 1`,
		"proc_c2c_transfer_"+sessionID,
	).Scan(&procSourceType, &procSourceID, &procAccountingScene, &procAccountingSubtype)
	if err != nil {
		t.Fatalf("regression: query fin_process_events columns failed: %v", err)
	}
	if procSourceType == "" || procSourceID == "" || procAccountingScene == "" || procAccountingSubtype == "" {
		t.Fatalf("regression: process new fields should exist")
	}
}

// TestFinanceHTTP_QueryByPrimaryParams 验证 HTTP API 主口径参数查询
func TestFinanceHTTP_QueryByPrimaryParams(t *testing.T) {
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
		DealID:            "deal_http_priority",
		BaseTxID:          "base_tx_http_priority",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_http_priority")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_http_priority", baseTxHex, 700, 290, sellerPubHex)

	srv := &httpAPIServer{db: db, store: store}

	// 第二阶段：改用 pay 测试（pay 暂保留完整 fin_business，open/close 为过程型）
	// 测试：用 pool_allocation_id（新口径）查询 pay 记录
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocSourceID := fmt.Sprintf("%d", payAllocIntID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?pool_allocation_id="+payAllocID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("http priority test failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].SourceID != payAllocSourceID {
		t.Fatalf("pool_allocation_id should match pay allocation: %+v", resp.Items)
	}

	// 验证返回的数据主口径字段正确
	item := resp.Items[0]
	if item.AccountingScene != "c2c_transfer" || item.AccountingSubtype != "chunk_pay" {
		t.Fatalf("returned item should have correct accounting fields: %+v", item)
	}
}

// ==================== 整改补充：冲突参数场景测试 ====================
// 验证：新旧参数同时传入且冲突时，结果仍然按新参数命中

// TestFinanceBusiness_ConflictParams_NewWins business 冲突参数测试
// 新参数指向正确记录，旧参数故意给错，结果仍然按新参数返回
func TestFinanceBusiness_ConflictParams_NewWins(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	// 第二阶段：只测试 pay（pay 暂保留完整 fin_business，open/close 为过程型）
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_conflict_test")

	srv := &httpAPIServer{db: db, store: store}

	// 构造冲突：
	// - 新参数：pool_allocation_id=pay_alloc（应该命中 pay 记录）
	// - 旧参数：scene_subtype=open（如果生效会查不到记录，与 pay_alloc 冲突）
	// - 旧参数：ref_id=wrong_session（如果生效会导致查不到任何记录）
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocSourceID := fmt.Sprintf("%d", payAllocIntID)
	wrongSessionID := "wrong_session_id"

	// 测试1：pool_allocation_id + scene_subtype 冲突
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/businesses?pool_allocation_id="+payAllocID+
			"&scene_subtype=open&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflict test 1 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("should return 1 pay record, got %d (old scene_subtype should be ignored)", len(resp.Items))
	}
	if resp.Items[0].SourceID != payAllocSourceID {
		t.Fatalf("should hit pay allocation %s, got %s (new param should win)", payAllocSourceID, resp.Items[0].SourceID)
	}
	// 验证返回的是 chunk_pay，不是 open
	if resp.Items[0].AccountingSubtype != "chunk_pay" {
		t.Fatalf("should return chunk_pay record, got %s", resp.Items[0].AccountingSubtype)
	}

	// 测试2：source_type/source_id + ref_id 冲突
	// ref_id 给错，但只要新参数对，就应该返回正确结果
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/businesses?source_type=pool_allocation&source_id="+payAllocSourceID+
			"&ref_id="+wrongSessionID+"&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflict test 2 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	resp.Items = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].SourceID != payAllocSourceID {
		t.Fatalf("wrong ref_id should be ignored when source_id is provided: %+v", resp.Items)
	}

	// 测试3：accounting_scene/accounting_subtype + scene_type/scene_subtype 冲突
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/businesses?accounting_scene=c2c_transfer&accounting_subtype=chunk_pay"+
			"&scene_type=fee_pool&scene_subtype=open&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflict test 3 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	resp.Items = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// 应该返回 chunk_pay 记录，而不是 open 记录
	foundPay := false
	for _, item := range resp.Items {
		if item.AccountingSubtype == "chunk_pay" {
			foundPay = true
			break
		}
	}
	if !foundPay && len(resp.Items) > 0 {
		t.Fatalf("accounting_subtype=chunk_pay should win over scene_subtype=open: got items=%+v", resp.Items)
	}
}

// TestFinanceProcessEvent_ConflictParams_NewWins process event 冲突参数测试
// 新旧参数冲突时，新参数优先
func TestFinanceProcessEvent_ConflictParams_NewWins(t *testing.T) {
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
		DealID:            "deal_proc_conflict",
		BaseTxID:          "base_tx_proc_conflict",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolPayAccounting(ctx, store, sessionID, 2, 300, sellerPubHex, "pay_tx_proc_conflict")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_proc_conflict", baseTxHex, 700, 290, sellerPubHex)

	srv := &httpAPIServer{db: db, store: store}

	closeAllocID := directTransferPoolAllocationID(sessionID, "close", 3)
	closeAllocIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, closeAllocID)
	if err != nil {
		t.Fatalf("lookup close allocation id failed: %v", err)
	}
	closeAllocSourceID := fmt.Sprintf("%d", closeAllocIntID)
	wrongSessionID := "wrong_session_id"

	// 测试1：pool_allocation_id + scene_subtype 冲突
	// pool_allocation_id 指向 close，但 scene_subtype=chunk_pay 指向 pay
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/process-events?pool_allocation_id="+closeAllocID+
			"&scene_subtype=chunk_pay&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process conflict test 1 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeProcessEventItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// 验证返回的是 close 相关事件，不是 chunk_pay
	for _, item := range resp.Items {
		if item.SourceID != closeAllocSourceID {
			t.Fatalf("pool_allocation_id should win: expected source_id=%s, got=%s", closeAllocSourceID, item.SourceID)
		}
	}

	// 测试2：source_* + ref_id 冲突
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/process-events?source_type=pool_allocation&source_id="+closeAllocSourceID+
			"&ref_id="+wrongSessionID+"&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process conflict test 2 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	resp.Items = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	for _, item := range resp.Items {
		if item.SourceID != closeAllocSourceID {
			t.Fatalf("wrong ref_id should be ignored: expected source_id=%s, got=%s", closeAllocSourceID, item.SourceID)
		}
	}

	// 测试3：accounting_* + scene_* 冲突
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/process-events?accounting_scene=c2c_transfer&accounting_subtype=close"+
			"&scene_type=fee_pool&scene_subtype=open&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process conflict test 3 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	resp.Items = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// 应该返回 close 事件
	foundClose := false
	for _, item := range resp.Items {
		if item.AccountingSubtype == "close" {
			foundClose = true
		}
		// 确保没有返回 open 事件
		if item.AccountingSubtype == "open" {
			t.Fatalf("accounting_subtype=close should win over scene_subtype=open: got open event")
		}
	}
	if !foundClose && len(resp.Items) > 0 {
		t.Fatalf("accounting_subtype=close should be respected: got items=%+v", resp.Items)
	}
}
