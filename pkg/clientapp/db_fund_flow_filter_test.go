package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWalletFundFlows_MultiDirectionFilter 验证 direction 参数支持逗号分隔的多值 IN 筛选
func TestWalletFundFlows_MultiDirectionFilter(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	// 插入多条不同方向的 fund_flow 记录
	_, err := db.Exec(`INSERT INTO wallet_fund_flows(
		created_at_unix,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json
	) VALUES
		(1700000001, 'flow_1', 'payment', 'ref_1', 'stage_1', 'out', 'pay', -100, 100, 0, 'tx1', '', '{}'),
		(1700000002, 'flow_2', 'feepool', 'ref_2', 'stage_1', 'lock', 'lock', -200, 200, 0, 'tx2', '', '{}'),
		(1700000003, 'flow_3', 'feepool', 'ref_3', 'stage_2', 'settle', 'settle', 50, 0, 0, 'tx3', '', '{}'),
		(1700000004, 'flow_4', 'token', 'ref_4', 'stage_1', 'debit', 'token_op', -30, 30, 0, 'tx4', '', '{}'),
		(1700000005, 'flow_5', 'asset', 'ref_5', 'stage_1', 'in', 'receive', 500, 0, 0, 'tx5', '', '{}'),
		(1700000006, 'flow_6', 'token', 'ref_6', 'stage_1', 'credit', 'token_op', 100, 0, 0, 'tx6', '', '{}'),
		(1700000007, 'flow_7', 'adjust', 'ref_7', 'stage_1', 'internal', 'adjust', 0, 0, 0, 'tx7', '', '{}'),
		(1700000008, 'flow_8', 'info', 'ref_8', 'stage_1', 'info', 'info', 0, 0, 0, 'tx8', '', '{}')
	`)
	if err != nil {
		t.Fatalf("insert wallet_fund_flows: %v", err)
	}

	t.Run("single direction out returns only out records", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?direction=out", nil)
		rec := httptest.NewRecorder()
		srv.handleWalletFundFlows(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				Direction string `json:"direction"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if body.Total != 1 {
			t.Fatalf("total: got=%d want=1", body.Total)
		}
		if len(body.Items) != 1 || body.Items[0].Direction != "out" {
			t.Fatalf("items: got=%+v want=[out]", body.Items)
		}
	})

	t.Run("comma separated directions returns matching records", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?direction=out,lock,settle,debit", nil)
		rec := httptest.NewRecorder()
		srv.handleWalletFundFlows(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				Direction string `json:"direction"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		// out + lock + settle + debit = 4 条
		if body.Total != 4 {
			t.Fatalf("total: got=%d want=4", body.Total)
		}
		if len(body.Items) != 4 {
			t.Fatalf("items count: got=%d want=4", len(body.Items))
		}
		seen := make(map[string]int)
		for _, it := range body.Items {
			seen[it.Direction]++
		}
		if seen["out"] != 1 || seen["lock"] != 1 || seen["settle"] != 1 || seen["debit"] != 1 {
			t.Fatalf("direction distribution: got=%+v want out=1,lock=1,settle=1,debit=1", seen)
		}
	})

	t.Run("in and credit returns 2 records", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?direction=in,credit", nil)
		rec := httptest.NewRecorder()
		srv.handleWalletFundFlows(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if body.Total != 2 {
			t.Fatalf("total: got=%d want=2", body.Total)
		}
	})

	t.Run("no direction filter returns all records", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows", nil)
		rec := httptest.NewRecorder()
		srv.handleWalletFundFlows(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if body.Total != 8 {
			t.Fatalf("total: got=%d want=8", body.Total)
		}
	})
}
