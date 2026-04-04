package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
	if err := dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_finance_read_1"); err != nil {
		t.Fatalf("record pay accounting failed: %v", err)
	}
	if err := dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_finance_read_1", baseTxHex, 700, 290, sellerPubHex); err != nil {
		t.Fatalf("record close accounting failed: %v", err)
	}

	// 验证：pay 不再生成 biz_c2c_pay_* 正式收费对象
	var payBizCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE business_id=?`, "biz_c2c_pay_"+sessionID+"_2").Scan(&payBizCount); err != nil {
		t.Fatalf("query fin_business pay failed: %v", err)
	}
	if payBizCount != 0 {
		t.Fatalf("pay 不应生成 biz_c2c_pay_* 正式收费对象，got %d", payBizCount)
	}

	// 验证：pay 的 process event 存在且主口径字段正确
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)

	// 验证：pay 的 process event 存在且主口径字段正确
	payProcPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          payAllocationSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("list finance process events by new filters failed: %v", err)
	}
	if payProcPage.Total != 1 || len(payProcPage.Items) != 1 {
		t.Fatalf("process page mismatch: total=%d items=%d", payProcPage.Total, len(payProcPage.Items))
	}
	if payProcPage.Items[0].SourceType != "pool_allocation" || payProcPage.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("process page source mismatch: %+v", payProcPage.Items[0])
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
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_finance_trace_1")

	allocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	allocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	allocationSourceID := fmt.Sprintf("%d", allocationIntID)

	// 验证：pay 不再生成 fin_business
	bizPage, err := dbListFinanceBusinessesByPoolAllocationID(ctx, store, allocationID, 20, 0)
	if err != nil {
		t.Fatalf("trace businesses by pool_allocation_id failed: %v", err)
	}
	if bizPage.Total != 0 {
		t.Fatalf("pay 不应生成 fin_business，got %d", bizPage.Total)
	}

	// 验证 process events 存在（pay 生成 process events）
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, allocationID, 20, 0)
	if err != nil {
		t.Fatalf("trace process events by pool_allocation_id failed: %v", err)
	}
	if procPage.Total == 0 {
		t.Fatalf("trace process page should not be empty")
	}
	if procPage.Items[0].SourceID != allocationSourceID {
		t.Fatalf("trace process source mismatch: got=%s want=%s", procPage.Items[0].SourceID, allocationSourceID)
	}
}

func TestAdminFinanceHTTP_ReadsNewFieldsAndParams(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"

	// 第二阶段：只测试 pay（pay 不再生成 fin_business，只生成 process event + tx_breakdown）
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_finance_http_1")

	srv := &httpAPIServer{db: db, store: store}

	// 使用 pay allocation 测试查询
	payAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)
	// 使用 pay allocation 测试查询 - pay 不再生成 fin_business，所以返回空
	// 第九阶段整改：必须传 business_role 参数
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?business_role=formal&pool_allocation_id="+payAllocationID+"&limit=10", nil)
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
	if len(businessResp.Items) != 0 {
		t.Fatalf("pay 不生成 fin_business，应返回 0 条，got %d", len(businessResp.Items))
	}

	// 改用 process event 验证
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?pool_allocation_id="+payAllocationID+"&limit=10", nil)
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
	if processResp.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("process list source mismatch: %+v", processResp.Items[0])
	}
	if processResp.Items[0].AccountingScene == "" || processResp.Items[0].AccountingSubtype == "" {
		t.Fatalf("process list missing accounting fields: %+v", processResp.Items[0])
	}

	// 第二阶段：验证 pay 不生成 fin_business，改用 process event 测试
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?accounting_scene=c2c_transfer&accounting_subtype=chunk_pay&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var processResp2 struct {
		Items []financeProcessEventItem `json:"items"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &processResp2); err != nil {
		t.Fatalf("decode process list response failed: %v", err)
	}
	if len(processResp2.Items) != 1 {
		t.Fatalf("process list item count mismatch: %d", len(processResp2.Items))
	}
	if processResp2.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("process list source mismatch: %+v", processResp2.Items[0])
	}

	// 第六次迭代：使用主口径参数查询
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?accounting_scene=c2c_transfer&accounting_subtype=chunk_pay&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var processResp3 struct {
		Items []financeProcessEventItem `json:"items"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &processResp3); err != nil {
		t.Fatalf("decode process list response failed: %v", err)
	}
	if len(processResp3.Items) != 1 {
		t.Fatalf("process list item count mismatch: %d", len(processResp3.Items))
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
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_priority_test")

	// 测试：同时传新旧参数，验证新口径优先匹配
	// 新参数能精确匹配到 pay 记录（chunk_pay），旧参数可能匹配到多条
	payAllocationID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocationID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)

	// 用新口径精确查询（pay 不再生成 fin_business，改用 process events）
	newFilterPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
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
		t.Fatalf("new filter should return exactly 1 pay process record: total=%d items=%d", newFilterPage.Total, len(newFilterPage.Items))
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

	// 第二阶段：改用 pay 测试读取模型（pay 不再生成 fin_business，只生成 process event + tx_breakdown）
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_presentation_test")

	// 第二阶段：验证 pay 不生成 fin_business
	// 查询 process event 验证主口径字段
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocationIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocationSourceID := fmt.Sprintf("%d", payAllocationIntID)

	procPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          payAllocationSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("list process events failed: %v", err)
	}
	if procPage.Total != 1 || len(procPage.Items) != 1 {
		t.Fatalf("process page mismatch: total=%d items=%d", procPage.Total, len(procPage.Items))
	}

	// 验证：主口径字段必须存在且有效
	item := procPage.Items[0]
	if item.SourceType != "pool_allocation" {
		t.Fatalf("SourceType should be 'pool_allocation': got=%s", item.SourceType)
	}
	if item.SourceID != payAllocationSourceID {
		t.Fatalf("SourceID mismatch: got=%s want=%s", item.SourceID, payAllocationSourceID)
	}
	if item.AccountingScene != "c2c_transfer" {
		t.Fatalf("AccountingScene should be 'c2c_transfer': got=%s", item.AccountingScene)
	}
	if item.AccountingSubtype != "chunk_pay" {
		t.Fatalf("AccountingSubtype should be 'chunk_pay': got=%s", item.AccountingSubtype)
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
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_compat_test")
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

	// 第二阶段：改用 pay 测试（pay 不再生成 fin_business，只生成 process event + tx_breakdown）
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_no_diffusion_test")

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
	// 第二阶段：pay 不再生成 fin_business
	if page.Total != 0 {
		t.Fatalf("pay 不应生成 fin_business，got %d", page.Total)
	}

	// 测试：流程事件新辅助函数（pay 生成 process events）
	procPage, err := dbListFinanceProcessEventsByPoolAllocationID(ctx, store, payAllocationID, 10, 0)
	if err != nil {
		t.Fatalf("new process helper function failed: %v", err)
	}
	if procPage.Total == 0 {
		t.Fatalf("new process helper should return records")
	}
	if procPage.Items[0].SourceID != payAllocationSourceID {
		t.Fatalf("new process helper returned wrong source: got=%s want=%s", procPage.Items[0].SourceID, payAllocationSourceID)
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
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_regression_test")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_regression_test", baseTxHex, 700, 290, sellerPubHex)

	// 回归1: 用新字段过滤必须能工作（验证 pay 的 process event）
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocSourceID := fmt.Sprintf("%d", payAllocIntID)
	procPage, err := dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:             10,
		SourceType:        "pool_allocation",
		SourceID:          payAllocSourceID,
		AccountingScene:   "c2c_transfer",
		AccountingSubtype: "chunk_pay",
	})
	if err != nil {
		t.Fatalf("regression: process event query failed: %v", err)
	}
	if procPage.Total != 1 {
		t.Fatalf("regression: should find 1 pay process event: got=%d", procPage.Total)
	}

	// 回归2: HTTP API 必须能返回新字段（查 process events）
	srv := &httpAPIServer{db: db, store: store}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?pool_allocation_id="+payAllocID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("regression: http api failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeProcessEventItem `json:"items"`
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

	// 第六次迭代：回归3 - 验证 pay 不再生成 fin_business
	var payBusinessCount int
	err = db.QueryRow(`
		SELECT COUNT(1) FROM fin_business WHERE business_id=?`, "biz_c2c_pay_"+sessionID+"_2",
	).Scan(&payBusinessCount)
	if err != nil {
		t.Fatalf("regression: query fin_business count failed: %v", err)
	}
	if payBusinessCount != 0 {
		t.Fatalf("regression: pay 不应生成 fin_business，got %d", payBusinessCount)
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
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_http_priority")
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_http_priority", baseTxHex, 700, 290, sellerPubHex)

	srv := &httpAPIServer{db: db, store: store}

	// 第二阶段：改用 pay 测试（pay 不再生成 fin_business，只生成 process event + tx_breakdown）
	// 测试：用 pool_allocation_id（新口径）查询 pay 的 process event
	payAllocID := directTransferPoolAllocationID(sessionID, "pay", 2)
	payAllocIntID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, payAllocID)
	if err != nil {
		t.Fatalf("lookup pay allocation id failed: %v", err)
	}
	payAllocSourceID := fmt.Sprintf("%d", payAllocIntID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/process-events?pool_allocation_id="+payAllocID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("http priority test failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeProcessEventItem `json:"items"`
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

	// 第二阶段：只测试 pay（pay 不再生成 fin_business，只生成 process event + tx_breakdown）
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_conflict_test")

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

	// 测试1：pool_allocation_id 精确查询 pay 过程记录
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/process-events?pool_allocation_id="+payAllocID+"&limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflict test 1 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items []financeProcessEventItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("should return 1 pay process record, got %d", len(resp.Items))
	}
	if resp.Items[0].SourceID != payAllocSourceID {
		t.Fatalf("should hit pay allocation %s, got %s", payAllocSourceID, resp.Items[0].SourceID)
	}
	// 验证返回的是 chunk_pay
	if resp.Items[0].AccountingSubtype != "chunk_pay" {
		t.Fatalf("should return chunk_pay record, got %s", resp.Items[0].AccountingSubtype)
	}

	// 测试2：source_type/source_id 精确查询（改用 process events）
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/process-events?source_type=pool_allocation&source_id="+payAllocSourceID+"&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflict test 2 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	resp.Items = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].SourceID != payAllocSourceID {
		t.Fatalf("source_id query failed: %+v", resp.Items)
	}

	// 测试3：accounting_scene/accounting_subtype 精确查询（改用 process events）
	req = httptest.NewRequest(http.MethodGet,
		"/api/v1/admin/finance/process-events?accounting_scene=c2c_transfer&accounting_subtype=chunk_pay&limit=10", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceProcessEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conflict test 3 failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	resp.Items = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// 应该返回 chunk_pay 记录
	foundPay := false
	for _, item := range resp.Items {
		if item.AccountingSubtype == "chunk_pay" {
			foundPay = true
			break
		}
	}
	if !foundPay && len(resp.Items) > 0 {
		t.Fatalf("accounting_subtype=chunk_pay should match: got items=%+v", resp.Items)
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
	dbRecordDirectPoolPayAccounting(ctx, store, "biz_download_pool_test_"+sessionID, sessionID, 2, 300, "pay_tx_proc_conflict")
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

// ============================================================
// 第四阶段新增：Finance 三层分离测试
// ============================================================

// TestFinanceLayer_FormalBusinessOnly 正式 business 查询测试
// 验证：business_role=formal 只返回正式收费对象，不返回过程财务对象
func TestFinanceLayer_FormalBusinessOnly(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_layer_formal_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 写入正式 business（通过 CreateBusinessWithFrontTriggerAndPendingSettlement）
	businessID := "biz_download_pool_layer_formal_1"
	frontOrderID := "front_order_layer_formal_1"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:   frontOrderID,
		FrontType:      "download",
		FrontSubtype:   "direct_transfer",
		OwnerPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectID: "demand_layer_formal",
		Status:         "pending",
		Note:           "formal test",
		Payload:        map[string]any{"test": "formal"},
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:      frontOrderID,
		FrontType:         "download",
		FrontSubtype:      "direct_transfer",
		OwnerPubkeyHex:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectType:  "demand",
		TargetObjectID:    "demand_layer_formal",
		BusinessID:        businessID,
		BusinessRole:      "formal",
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:" + sellerPubHex,
		TriggerType:       "front_order",
		TriggerIDValue:    frontOrderID,
		TriggerRole:       "primary",
		SettlementID:      "set_download_pool_layer_formal_1",
		SettlementMethod:  SettlementMethodPool,
	}); err != nil {
		t.Fatalf("create business chain: %v", err)
	}

	// 写入过程 business
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_layer_formal",
		BaseTxID:          "base_tx_layer_formal",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_layer_formal", baseTxHex, 700, 290, sellerPubHex)

	// 正式查询：只返回正式收费对象
	formalPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:        50,
		BusinessRole: "formal",
	})
	if err != nil {
		t.Fatalf("formal business query failed: %v", err)
	}
	if formalPage.Total == 0 {
		t.Fatal("formal business query should return at least one record")
	}
	for _, item := range formalPage.Items {
		if item.BusinessRole != "formal" {
			t.Fatalf("formal query returned non-formal record: business_id=%s role=%s", item.BusinessID, item.BusinessRole)
		}
		if !strings.HasPrefix(item.BusinessID, "biz_download_pool_") {
			t.Fatalf("formal query returned non-download-pool record: %s", item.BusinessID)
		}
	}

	// 验证：business_role 字段已正确填充
	for _, item := range formalPage.Items {
		if item.BusinessRole == "" || item.BusinessRole == "unknown" {
			t.Fatalf("business_role should be populated: %+v", item)
		}
	}
}

// TestFinanceLayer_ProcessBusinessOnly 过程 business 查询测试
// 验证：business_role=process 只返回过程财务对象，不返回正式收费对象
func TestFinanceLayer_ProcessBusinessOnly(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 写入过程 business（seedDirectTransferPoolFacts 只写 pool session，不写 accounting）
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_layer_process",
		BaseTxID:          "base_tx_layer_process",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_layer_process", baseTxHex, 700, 290, sellerPubHex)

	// 过程查询：只返回过程财务对象
	processPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:        50,
		BusinessRole: "process",
	})
	if err != nil {
		t.Fatalf("process business query failed: %v", err)
	}
	if processPage.Total == 0 {
		t.Fatal("process business query should return at least one record")
	}
	for _, item := range processPage.Items {
		if item.BusinessRole != "process" {
			t.Fatalf("process query returned non-process record: business_id=%s role=%s", item.BusinessID, item.BusinessRole)
		}
		if !strings.HasPrefix(item.BusinessID, "biz_c2c_open_") && !strings.HasPrefix(item.BusinessID, "biz_c2c_close_") {
			t.Fatalf("process query returned non-c2c record: %s", item.BusinessID)
		}
	}
}

// TestFinanceLayer_NoMixing 正式/过程分离验证
// 验证：正式查询不会混入过程对象，过程查询不会混入正式对象
func TestFinanceLayer_NoMixing(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_layer_no_mix_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 同时写入正式和过程 business
	businessID := "biz_download_pool_layer_no_mix_1"
	frontOrderID := "front_order_layer_no_mix_1"
	if err := dbUpsertFrontOrder(ctx, store, frontOrderEntry{
		FrontOrderID:   frontOrderID,
		FrontType:      "download",
		FrontSubtype:   "direct_transfer",
		OwnerPubkeyHex: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectID: "demand_layer_no_mix",
		Status:         "pending",
		Note:           "no mix test",
		Payload:        map[string]any{"test": "no_mix"},
	}); err != nil {
		t.Fatalf("upsert front_order: %v", err)
	}
	if err := CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:      frontOrderID,
		FrontType:         "download",
		FrontSubtype:      "direct_transfer",
		OwnerPubkeyHex:    "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		TargetObjectType:  "demand",
		TargetObjectID:    "demand_layer_no_mix",
		BusinessID:        businessID,
		BusinessRole:      "formal",
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "direct_transfer",
		AccountingSubType: "download_pool",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:" + sellerPubHex,
		TriggerType:       "front_order",
		TriggerIDValue:    frontOrderID,
		TriggerRole:       "primary",
		SettlementID:      "set_download_pool_layer_no_mix_1",
		SettlementMethod:  SettlementMethodPool,
	}); err != nil {
		t.Fatalf("create business chain: %v", err)
	}

	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_layer_no_mix",
		BaseTxID:          "base_tx_layer_no_mix",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_layer_no_mix", baseTxHex, 700, 290, sellerPubHex)

	// 正式查询：不应包含过程对象
	formalPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:        50,
		BusinessRole: "formal",
	})
	if err != nil {
		t.Fatalf("formal query failed: %v", err)
	}
	for _, item := range formalPage.Items {
		if strings.HasPrefix(item.BusinessID, "biz_c2c_") {
			t.Fatalf("formal query should not contain c2c records: got %s", item.BusinessID)
		}
	}

	// 过程查询：不应包含正式对象
	processPage, err := dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:        50,
		BusinessRole: "process",
	})
	if err != nil {
		t.Fatalf("process query failed: %v", err)
	}
	for _, item := range processPage.Items {
		if strings.HasPrefix(item.BusinessID, "biz_download_pool_") {
			t.Fatalf("process query should not contain download_pool records: got %s", item.BusinessID)
		}
	}
}

// TestFinanceLayer_HTTPBusinessRoleParam HTTP API business_role 参数测试
// 验证：HTTP 接口正确支持 business_role 参数
func TestFinanceLayer_HTTPBusinessRoleParam(t *testing.T) {
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
		DealID:            "deal_layer_http",
		BaseTxID:          "base_tx_layer_http",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_layer_http", baseTxHex, 700, 290, sellerPubHex)

	srv := &httpAPIServer{db: db, store: store}

	// 测试：business_role=formal 应返回空（没有正式 business）
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?business_role=formal&limit=50", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("formal query failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var formalResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &formalResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	for _, item := range formalResp.Items {
		if item.BusinessRole != "formal" {
			t.Fatalf("formal query returned non-formal item: role=%s", item.BusinessRole)
		}
	}

	// 测试：business_role=process 应返回过程对象
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?business_role=process&limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("process query failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var processResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &processResp); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(processResp.Items) == 0 {
		t.Fatal("process query should return records")
	}
	for _, item := range processResp.Items {
		if item.BusinessRole != "process" {
			t.Fatalf("process query returned non-process item: role=%s", item.BusinessRole)
		}
	}

	// 测试：无效 business_role 应返回 400
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?business_role=invalid&limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid role should return 400: got %d", rec.Code)
	}

	// 第九阶段新增：不传 business_role 应返回 400
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing business_role should return 400: got %d", rec.Code)
	}
	var errResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("decode error response failed: %v", err)
	}
	if errMsg, ok := errResp["error"].(string); !ok || !strings.Contains(errMsg, "business_role is required") {
		t.Fatalf("expected business_role required error, got: %v", errResp)
	}
}

// TestFinanceFormalQuery_RejectsQ 第十一阶段：正式接口不支持 q 参数
// 验证：正式接口传 q 返回 400，q 只属于 compat/debug 入口
func TestFinanceFormalQuery_RejectsQ(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	store := newClientDB(db, nil)
	srv := &httpAPIServer{db: db, store: store}

	// 正式接口传 q 应该返回 400
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses?business_role=formal&q=test&limit=50", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinesses(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("formal query with q should return 400: got %d body=%s", rec.Code, rec.Body.String())
	}
	var errResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("decode error response failed: %v", err)
	}
	if errMsg, ok := errResp["error"].(string); !ok || !strings.Contains(errMsg, "q parameter is not supported") {
		t.Fatalf("expected q parameter error, got: %v", errResp)
	}
}

// TestFinanceCompat_AllBusinessesReturnsMixed 第十阶段：compat 入口可以查全量
// 验证：兼容/调试入口不强制 business_role，允许返回全量混合结果
func TestFinanceCompat_AllBusinessesReturnsMixed(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 写入过程 business
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_compat_test",
		BaseTxID:          "base_tx_compat_test",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_compat_test", baseTxHex, 700, 290, sellerPubHex)

	srv := &httpAPIServer{db: db, store: store}

	// 测试1：不传 business_role 应该成功返回全量
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/compat?limit=50", nil)
	rec := httptest.NewRecorder()
	srv.handleAdminFinanceBusinessesCompat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("compat all query failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var allResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &allResp); err != nil {
		t.Fatalf("decode compat response failed: %v", err)
	}
	// 验证返回中包含 formal 和 process 两种角色（seedDirectTransferPoolFacts 会写入过程对象）
	var formalCount, processCount int
	for _, item := range allResp.Items {
		switch item.BusinessRole {
		case "formal":
			formalCount++
		case "process":
			processCount++
		}
	}
	if formalCount == 0 && processCount == 0 {
		t.Fatal("compat query should return at least some records")
	}

	// 测试2：传 business_role=formal 应该只返回正式对象
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/compat?business_role=formal&limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinessesCompat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("compat formal query failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var formalResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &formalResp); err != nil {
		t.Fatalf("decode compat formal response failed: %v", err)
	}
	for _, item := range formalResp.Items {
		if item.BusinessRole != "formal" {
			t.Fatalf("compat formal query returned non-formal item: role=%s", item.BusinessRole)
		}
	}

	// 测试3：传 business_role=process 应该只返回过程对象
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/compat?business_role=process&limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinessesCompat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("compat process query failed: status=%d body=%s", rec.Code, rec.Body.String())
	}
	var processResp struct {
		Items []financeBusinessItem `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &processResp); err != nil {
		t.Fatalf("decode compat process response failed: %v", err)
	}
	for _, item := range processResp.Items {
		if item.BusinessRole != "process" {
			t.Fatalf("compat process query returned non-process item: role=%s", item.BusinessRole)
		}
	}

	// 测试4：非法 business_role 应该返回 400
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/compat?business_role=invalid&limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinessesCompat(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("compat invalid role should return 400: got %d", rec.Code)
	}

	// 测试5：compat 接口支持 q 参数（正式接口不支持）
	req = httptest.NewRequest(http.MethodGet, "/api/v1/admin/finance/businesses/compat?q=test&limit=50", nil)
	rec = httptest.NewRecorder()
	srv.handleAdminFinanceBusinessesCompat(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("compat query with q should return 200: got %d body=%s", rec.Code, rec.Body.String())
	}
}

// ============================================================
// 第七阶段新增：business_role 强约束测试
// ============================================================

// TestBusinessRole_StrongConstraint_RejectsEmpty 验证空角色被拒绝
func TestBusinessRole_StrongConstraint_RejectsEmpty(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)

	// 验证：写入空 business_role 应失败
	err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        "biz_test_empty_role",
		BusinessRole:      "", // 空值应被拒绝
		SourceType:        "test",
		SourceID:          "test_1",
		AccountingScene:   "test",
		AccountingSubType: "test",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:test",
		Status:            "pending",
		OccurredAtUnix:    time.Now().Unix(),
		IdempotencyKey:    "test_empty_role",
		Note:              "test empty role",
	})
	if err == nil {
		t.Fatal("expected error for empty business_role, got nil")
	}
	if !strings.Contains(err.Error(), "business_role is required") {
		t.Fatalf("expected business_role validation error, got: %v", err)
	}
}

// TestBusinessRole_StrongConstraint_RejectsInvalid 验证非法角色被拒绝
func TestBusinessRole_StrongConstraint_RejectsInvalid(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)

	// 验证：写入非法 business_role 应失败
	err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        "biz_test_invalid_role",
		BusinessRole:      "unknown", // 非法值应被拒绝
		SourceType:        "test",
		SourceID:          "test_1",
		AccountingScene:   "test",
		AccountingSubType: "test",
		FromPartyID:       "client:self",
		ToPartyID:         "seller:test",
		Status:            "pending",
		OccurredAtUnix:    time.Now().Unix(),
		IdempotencyKey:    "test_invalid_role",
		Note:              "test invalid role",
	})
	if err == nil {
		t.Fatal("expected error for invalid business_role, got nil")
	}
	if !strings.Contains(err.Error(), "must be 'formal' or 'process'") {
		t.Fatalf("expected business_role validation error, got: %v", err)
	}
}

// TestBusinessRole_Backfill_ReducesEmptyCount 验证回填减少空值数量
func TestBusinessRole_Backfill_ReducesEmptyCount(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	seedDirectTransferPoolFacts(t, db)

	ctx := context.Background()
	store := newClientDB(db, nil)
	sessionID := "sess_third_iter_1"
	sellerPubHex := "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	baseTxHex := "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000"

	// 写入过程 business（带正确 business_role）
	dbRecordDirectPoolOpenAccounting(ctx, store, directPoolOpenAccountingInput{
		SessionID:         sessionID,
		DealID:            "deal_backfill_test",
		BaseTxID:          "base_tx_backfill_test",
		BaseTxHex:         baseTxHex,
		ClientLockScript:  "",
		PoolAmountSatoshi: 990,
		SellerPubHex:      sellerPubHex,
	})
	dbRecordDirectPoolCloseAccounting(ctx, store, sessionID, 3, "close_tx_backfill_test", baseTxHex, 700, 290, sellerPubHex)

	// 验证：写入的记录都有 business_role
	var emptyRoleCount int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fin_business WHERE business_id LIKE 'biz_c2c_%' AND (business_role='' OR business_role IS NULL)`,
	).Scan(&emptyRoleCount); err != nil {
		t.Fatalf("query empty role count failed: %v", err)
	}
	if emptyRoleCount != 0 {
		t.Fatalf("new writes should have business_role set, got %d empty records", emptyRoleCount)
	}

	// 验证：回填后空值记录不增加
	emptyCountBefore, _ := CountEmptyBusinessRole(db)
	if err := backfillFinBusinessRole(db); err != nil {
		t.Fatalf("backfill failed: %v", err)
	}
	emptyCountAfter, _ := CountEmptyBusinessRole(db)
	if emptyCountAfter > emptyCountBefore {
		t.Fatalf("backfill should not increase empty role count: before=%d after=%d", emptyCountBefore, emptyCountAfter)
	}
}

// TestBusinessRole_BackfillRecordsHaveFormalRole 验证回填记录有正确角色
// 第七阶段整改：确保 biz_backfill_* 不会留下空 business_role
func TestBusinessRole_BackfillRecordsHaveFormalRole(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)

	// 模拟回填写入：直接插入带 business_role 的回填记录
	businessID := "biz_backfill_pool_test_1"
	if _, err := db.Exec(`
		INSERT INTO fin_business(business_id, business_role, source_type, source_id, accounting_scene, accounting_subtype,
			from_party_id, to_party_id, status, occurred_at_unix, idempotency_key, note, payload_json)
		VALUES(?, 'formal', 'front_order', 'fo_test', 'direct_transfer', 'pay', 'client:self', 'seller:test',
			'posted', 1700000000, 'backfill_test', 'test', '{}')`,
		businessID,
	); err != nil {
		t.Fatalf("insert backfill business failed: %v", err)
	}

	// 验证：回填记录有 formal 角色
	var role string
	if err := db.QueryRow(`SELECT business_role FROM fin_business WHERE business_id=?`, businessID).Scan(&role); err != nil {
		t.Fatalf("query business_role failed: %v", err)
	}
	if role != "formal" {
		t.Fatalf("backfill record should have formal role, got '%s'", role)
	}

	// 验证：business_role=formal 查询能查到回填记录
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE business_role='formal' AND business_id=?`, businessID).Scan(&count); err != nil {
		t.Fatalf("query formal business failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("formal query should find backfill record, got count=%d", count)
	}

	// 验证：business_role=process 查询不应查到回填记录
	if err := db.QueryRow(`SELECT COUNT(1) FROM fin_business WHERE business_role='process' AND business_id=?`, businessID).Scan(&count); err != nil {
		t.Fatalf("query process business failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("process query should not find backfill record, got count=%d", count)
	}
}
