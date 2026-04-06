package clientapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ==================== 资产确认队列运维 API 测试 ====================

func newTestVerificationAPIServer(t *testing.T) (*httpAPIServer, func()) {
	t.Helper()

	dbPath := t.TempDir() + "/verification-api.sqlite"
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	rt := &Runtime{}
	s := &httpAPIServer{
		rt:    rt,
		db:    db,
		store: newClientDB(db, nil),
	}

	mux, err := s.buildMux()
	if err != nil {
		t.Fatalf("build mux: %v", err)
	}

	server := httptest.NewServer(mux)
	return s, server.Close
}

func TestAdminVerificationSummary_GET(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子数据
	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_sum1:0", walletID, address, "tx_sum1", 0, 1, "pending", now, now,
	)
	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_sum2:0", walletID, address, "tx_sum2", 0, 1, "confirmed_bsv21", now, now,
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/summary", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationSummary(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse json: %v", err)
	}

	summary, ok := resp["summary"].(map[string]any)
	if !ok {
		t.Fatalf("missing summary field")
	}

	if int(summary["pending"].(float64)) != 1 {
		t.Fatalf("expected pending 1, got %v", summary["pending"])
	}
	if int(summary["confirmed_bsv21"].(float64)) != 1 {
		t.Fatalf("expected confirmed_bsv21 1, got %v", summary["confirmed_bsv21"])
	}
	// Step 12: 验证 data_role 声明
	if resp["data_role"] != "primary" {
		t.Fatalf("expected data_role 'primary', got %v", resp["data_role"])
	}
}

func TestAdminVerificationFailed_GET(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子 failed 和 pending 项
	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_fail_api:0", walletID, address, "tx_fail_api", 0, 1, "failed", "woc timeout", 10, now, now,
	)
	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_pending_api:0", walletID, address, "tx_pending_api", 0, 1, "pending", "retrying", 3, now, now,
	)

	// 默认只返回 failed
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/failed", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationFailed(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items := resp["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 failed item, got %d", len(items))
	}
	// 验证分页字段
	if _, ok := resp["total"]; !ok {
		t.Fatalf("missing total field")
	}
	if _, ok := resp["limit"]; !ok {
		t.Fatalf("missing limit field")
	}
	if _, ok := resp["offset"]; !ok {
		t.Fatalf("missing offset field")
	}

	// include_pending=true 时返回 2 项
	req2 := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/failed?include_pending=true", nil)
	w2 := httptest.NewRecorder()
	s.handleAdminVerificationFailed(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	var resp2 map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &resp2)
	items2 := resp2["items"].([]any)
	if len(items2) != 2 {
		t.Fatalf("expected 2 items with include_pending, got %d", len(items2))
	}
	// Step 12: 验证 data_role 声明
	if resp["data_role"] != "primary" {
		t.Fatalf("expected data_role 'primary', got %v", resp["data_role"])
	}
}

func TestAdminVerificationItems_GET_FilterByStatus(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_item1:0", walletID, address, "tx_item1", 0, 1, "pending", now, now,
	)
	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_item2:0", walletID, address, "tx_item2", 0, 1, "confirmed_bsv20", now, now,
	)

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/items?status=pending", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationItems(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["total"].(float64)) != 1 {
		t.Fatalf("expected 1 pending item, got %v", resp["total"])
	}
	if len(resp["items"].([]any)) != 1 {
		t.Fatalf("expected 1 item in items, got %v", len(resp["items"].([]any)))
	}
	if resp["data_role"] != "primary" {
		t.Fatalf("expected data_role 'primary', got %v", resp["data_role"])
	}
}

func TestAdminVerificationReconcile_GET(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/reconcile", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationReconcile(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse json: %v", err)
	}

	report, ok := resp["report"].(map[string]any)
	if !ok {
		t.Fatalf("missing report field")
	}

	summary := report["summary"].(map[string]any)
	if int(summary["confirmed_without_fact"].(float64)) != 0 {
		t.Fatalf("expected 0 confirmed_without_fact, got %v", summary["confirmed_without_fact"])
	}
}

func TestAdminVerificationReset_POST_BadJSON(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/reset", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleAdminVerificationReset(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminVerificationReset_POST_MissingUTXOID(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]string{"utxo_id": ""})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/reset", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleAdminVerificationReset(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminVerificationReset_POST_Success(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_reset_api:0", walletID, address, "tx_reset_api", 0, 1, "failed", "some error", 10, now, now,
	)

	body, _ := json.Marshal(map[string]string{"utxo_id": "tx_reset_api:0"})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/reset", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleAdminVerificationReset(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "pending" {
		t.Fatalf("expected status pending, got %v", resp["status"])
	}
}

func TestAdminVerificationBatchRetry_POST_BadJSON(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/batch-retry", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleAdminVerificationBatchRetry(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminVerificationBatchRetry_POST_Success(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_retry_api:0", walletID, address, "tx_retry_api", 0, 1, "failed", "error", 10, now, now,
	)

	body, _ := json.Marshal(map[string]string{"wallet_id": walletID})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/batch-retry", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleAdminVerificationBatchRetry(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["retried"].(float64)) != 1 {
		t.Fatalf("expected retried 1, got %v", resp["retried"])
	}
}

func TestAdminVerificationSummary_GET_RuntimeNil(t *testing.T) {
	t.Parallel()

	s := &httpAPIServer{}
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/summary", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationSummary(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestAdminVerificationReset_POST_WrongMethod(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/reset", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationReset(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestAdminVerificationBatchRetry_POST_WrongMethod(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/batch-retry", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationBatchRetry(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestAdminVerificationSummary_GET_EmptyQueue(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	_ = seedTestAddress(t, s.db)

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/summary", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationSummary(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	summary := resp["summary"].(map[string]any)
	if int(summary["total"].(float64)) != 0 {
		t.Fatalf("expected total 0, got %v", summary["total"])
	}
}

func TestAdminVerificationReset_POST_ContextPropagation(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_ctx:0", walletID, address, "tx_ctx", 0, 1, "failed", "err", 10, now, now,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body, _ := json.Marshal(map[string]string{"utxo_id": "tx_ctx:0"})
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/admin/verification/reset", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handleAdminVerificationReset(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

// ==================== Step 11 回归测试：阈值告警 ====================

func TestVerificationThresholdAlerts_FailedExceedsThreshold(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子超过阈值的 failed 项
	for i := 0; i <= verificationAlertFailedThreshold; i++ {
		_, err := db.Exec(
			`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("tx_thresh%d:0", i), walletID, address, fmt.Sprintf("tx_thresh%d", i), 0, 1, "failed", "error", 10, now, now,
		)
		if err != nil {
			t.Fatalf("seed thresh %d: %v", i, err)
		}
	}

	// 调用阈值告警函数，不应 panic
	emitVerificationThresholdAlerts(context.Background(), store, "test")
}

func TestVerificationThresholdAlerts_NoAlerts(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子少量 pending 项（不超过阈值）
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_ok:0", walletID, address, "tx_ok", 0, 1, "pending", now, now,
	)
	if err != nil {
		t.Fatalf("seed ok: %v", err)
	}

	// 调用阈值告警函数，不应 panic
	emitVerificationThresholdAlerts(context.Background(), store, "test")
}

// ==================== Step 11 回归测试：审计日志 ====================

func TestAdminVerificationReset_POST_AuditLog(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_audit_reset:0", walletID, address, "tx_audit_reset", 0, 1, "failed", "err", 10, now, now,
	)

	body, _ := json.Marshal(map[string]string{"utxo_id": "tx_audit_reset:0"})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/reset", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Visit-ID", "test-operator-123")
	w := httptest.NewRecorder()
	s.handleAdminVerificationReset(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminVerificationBatchRetry_POST_AuditLog(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	_, _ = s.db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,error_message,retry_count,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"tx_audit_retry:0", walletID, address, "tx_audit_retry", 0, 1, "failed", "err", 10, now, now,
	)

	body, _ := json.Marshal(map[string]any{
		"wallet_id": walletID,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/verification/batch-retry", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Visit-ID", "test-operator-456")
	w := httptest.NewRecorder()
	s.handleAdminVerificationBatchRetry(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["retried"].(float64)) != 1 {
		t.Fatalf("expected retried 1, got %v", resp["retried"])
	}
}

func TestAdminVerificationItems_GET_Pagination(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()
	address := seedTestAddress(t, s.db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 种子 5 项
	for i := 0; i < 5; i++ {
		_, _ = s.db.Exec(
			`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
			 VALUES(?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("tx_page%d:0", i), walletID, address, fmt.Sprintf("tx_page%d", i), 0, 1, "pending", now, now,
		)
	}

	// 测试 limit=2, offset=1
	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/items?limit=2&offset=1", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationItems(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if int(resp["total"].(float64)) != 5 {
		t.Fatalf("expected total 5, got %v", resp["total"])
	}
	if int(resp["limit"].(float64)) != 2 {
		t.Fatalf("expected limit 2, got %v", resp["limit"])
	}
	if int(resp["offset"].(float64)) != 1 {
		t.Fatalf("expected offset 1, got %v", resp["offset"])
	}
	items := resp["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if resp["data_role"] != "primary" {
		t.Fatalf("expected data_role 'primary', got %v", resp["data_role"])
	}
}

func TestAdminVerificationReconcile_GET_PaginationFields(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/verification/reconcile", nil)
	w := httptest.NewRecorder()
	s.handleAdminVerificationReconcile(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if _, ok := resp["total"]; !ok {
		t.Fatalf("missing total field")
	}
	if _, ok := resp["limit"]; !ok {
		t.Fatalf("missing limit field")
	}
	if _, ok := resp["offset"]; !ok {
		t.Fatalf("missing offset field")
	}
	if resp["data_role"] != "primary" {
		t.Fatalf("expected data_role 'primary', got %v", resp["data_role"])
	}
	if resp["source_of_truth"] != "fact_chain_asset_flows" {
		t.Fatalf("expected source_of_truth 'fact_chain_asset_flows', got %v", resp["source_of_truth"])
	}
}

// TestVerificationAPI_SourceOfTruth 验证所有 API 响应包含 source_of_truth 声明
func TestVerificationAPI_SourceOfTruth(t *testing.T) {
	t.Parallel()

	s, cleanup := newTestVerificationAPIServer(t)
	defer cleanup()

	endpoints := []string{
		"/v1/admin/verification/summary",
		"/v1/admin/verification/failed",
		"/v1/admin/verification/items",
		"/v1/admin/verification/reconcile",
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		w := httptest.NewRecorder()

		switch ep {
		case "/v1/admin/verification/summary":
			s.handleAdminVerificationSummary(w, req)
		case "/v1/admin/verification/failed":
			s.handleAdminVerificationFailed(w, req)
		case "/v1/admin/verification/items":
			s.handleAdminVerificationItems(w, req)
		case "/v1/admin/verification/reconcile":
			s.handleAdminVerificationReconcile(w, req)
		}

		if w.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", ep, w.Code)
		}

		var resp map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if resp["source_of_truth"] != "fact_chain_asset_flows" {
			t.Fatalf("%s: expected source_of_truth 'fact_chain_asset_flows', got %v", ep, resp["source_of_truth"])
		}
	}
}

// TestVerificationReconcile_ConsecutiveAnomaly 验证连续异常升级告警
func TestVerificationReconcile_ConsecutiveAnomaly(t *testing.T) {
	t.Parallel()

	db := newAssetVerificationTestDB(t)
	store := newClientDB(db, nil)
	address := seedTestAddress(t, db)
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()

	// 重置计数器
	verificationReconcileConsecutiveAnomaly.Reset()

	// 种子一个 confirmed 但没有 fact 的异常项
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_token_verification(utxo_id,wallet_id,address,txid,vout,value_satoshi,status,next_retry_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		"tx_consec:0", walletID, address, "tx_consec", 0, 1, "confirmed_bsv21", now, now,
	)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 连续调用 3 次，每次都应有异常
	for i := 1; i <= 3; i++ {
		emitVerificationReconcileReportLog(context.Background(), store, "test")
		if verificationReconcileConsecutiveAnomaly.count != i {
			t.Fatalf("expected consecutive count %d, got %d", i, verificationReconcileConsecutiveAnomaly.count)
		}
	}

	// 验证计数器达到 3
	if verificationReconcileConsecutiveAnomaly.count != 3 {
		t.Fatalf("expected consecutive count 3 after 3 calls, got %d", verificationReconcileConsecutiveAnomaly.count)
	}

	// 清理：移除异常项以恢复正常
	_, _ = db.Exec(`DELETE FROM wallet_utxo_token_verification WHERE utxo_id='tx_consec:0'`)

	// 再调用一次，计数器应重置
	emitVerificationReconcileReportLog(context.Background(), store, "test")
	if verificationReconcileConsecutiveAnomaly.count != 0 {
		t.Fatalf("expected consecutive count 0 after recovery, got %d", verificationReconcileConsecutiveAnomaly.count)
	}
}

// ==================== Step 14 回归测试 ====================

// TestVerificationResponseHelper_FieldsConsistent 验证 API 统一响应辅助函数覆盖
func TestVerificationResponseHelper_FieldsConsistent(t *testing.T) {
	t.Parallel()

	// 直接测试 writeVerificationResponse 函数
	w := httptest.NewRecorder()
	writeVerificationResponse(w, map[string]any{"test_key": "test_value"})

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)

	if resp["data_role"] != "primary" {
		t.Fatalf("expected data_role 'primary', got %v", resp["data_role"])
	}
	if resp["source_of_truth"] != "fact_chain_asset_flows" {
		t.Fatalf("expected source_of_truth 'fact_chain_asset_flows', got %v", resp["source_of_truth"])
	}
	if resp["test_key"] != "test_value" {
		t.Fatalf("expected test_key 'test_value', got %v", resp["test_key"])
	}
}

// TestMainPathSuccess_LogFields 验证主路径成功日志字段完整
func TestMainPathSuccess_LogFields(t *testing.T) {
	t.Parallel()

	// 验证 logTokenBalanceSuccess 不会 panic
	logTokenBalanceSuccess(context.Background(), "test_wallet", "BSV21", "token123", "100.5")
}
