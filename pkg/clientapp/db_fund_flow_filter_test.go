package clientapp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWalletFundFlows_MultiDirectionFilter 验证 direction 参数支持逗号分隔的多值 IN 筛选
// 设计说明：测试数据改用 fact_* 事实表，验证 API 组装查询正确性
func TestWalletFundFlows_MultiDirectionFilter(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	// 插入 fact_chain_asset_flows 测试数据（不同方向）
	_, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,amount_satoshi,quantity_text,occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
	) VALUES
		('flow_1', 'wallet1', 'addr1', 'OUT', 'BSV', '', 'utxo_1', 'tx1', 0, 100, '0', 1700000001, 1700000001, 'WOC', '', '{}'),
		('flow_2', 'wallet1', 'addr2', 'OUT', 'BSV', '', 'utxo_2', 'tx2', 0, 200, '0', 1700000002, 1700000002, 'WOC', '', '{}'),
		('flow_3', 'wallet1', 'addr3', 'IN', 'BSV', '', 'utxo_3', 'tx3', 0, 50, '0', 1700000003, 1700000003, 'WOC', '', '{}'),
		('flow_4', 'wallet1', 'addr4', 'OUT', 'BSV20', 'token1', 'utxo_4', 'tx4', 0, 30, '1000', 1700000004, 1700000004, 'WOC', '', '{}'),
		('flow_5', 'wallet1', 'addr5', 'IN', 'BSV', '', 'utxo_5', 'tx5', 0, 500, '0', 1700000005, 1700000005, 'WOC', '', '{}'),
		('flow_6', 'wallet1', 'addr6', 'IN', 'BSV21', 'token2', 'utxo_6', 'tx6', 0, 100, '500', 1700000006, 1700000006, 'WOC', '', '{}'),
		('flow_7', 'wallet1', 'addr7', 'OUT', 'BSV', '', 'utxo_7', 'tx7', 0, 0, '0', 1700000007, 1700000007, 'WOC', '', '{}'),
		('flow_8', 'wallet1', 'addr8', 'IN', 'BSV', '', 'utxo_8', 'tx8', 0, 0, '0', 1700000008, 1700000008, 'WOC', '', '{}')
	`)
	if err != nil {
		t.Fatalf("insert fact_chain_asset_flows: %v", err)
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
		// OUT 方向：flow_1, flow_2, flow_4, flow_7 = 4 条
		if body.Total != 4 {
			t.Fatalf("total: got=%d want=4", body.Total)
		}
		if len(body.Items) != 4 {
			t.Fatalf("items count: got=%d want=4", len(body.Items))
		}
		for _, it := range body.Items {
			if it.Direction != "OUT" {
				t.Fatalf("item direction: got=%s want=OUT", it.Direction)
			}
		}
	})

	t.Run("comma separated directions returns matching records", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?direction=OUT,IN", nil)
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
		// OUT + IN = 全部 8 条
		if body.Total != 8 {
			t.Fatalf("total: got=%d want=8", body.Total)
		}
		if len(body.Items) != 8 {
			t.Fatalf("items count: got=%d want=8", len(body.Items))
		}
		seen := make(map[string]int)
		for _, it := range body.Items {
			seen[it.Direction]++
		}
		if seen["OUT"] != 4 || seen["IN"] != 4 {
			t.Fatalf("direction distribution: got=%+v want OUT=4,IN=4", seen)
		}
	})

	t.Run("in returns 4 records", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?direction=in", nil)
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
		if body.Total != 4 {
			t.Fatalf("total: got=%d want=4", body.Total)
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

// TestWalletFundFlows_VisitIDDeprecated 验证 visit_id 参数已被废弃，返回 400
func TestWalletFundFlows_VisitIDDeprecated(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?visit_id=some-visit-id", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got=%d want=400 body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.Error != "visit_id filter is deprecated" {
		t.Fatalf("error: got=%q want=%q", body.Error, "visit_id filter is deprecated")
	}
}

// TestWalletFundFlows_DirectionCaseInsensitive 验证 direction 过滤大小写无关
func TestWalletFundFlows_DirectionCaseInsensitive(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	_, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,amount_satoshi,quantity_text,occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
	) VALUES
		('f1', 'w1', 'a1', 'OUT', 'BSV', '', 'u1', 't1', 0, 100, '0', 1700000001, 1700000001, 'WOC', '', '{}'),
		('f2', 'w1', 'a2', 'IN', 'BSV', '', 'u2', 't2', 0, 200, '0', 1700000002, 1700000002, 'WOC', '', '{}')
	`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	for _, dir := range []string{"out", "OUT", "Out"} {
		t.Run(dir, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?direction="+dir, nil)
			rec := httptest.NewRecorder()
			srv.handleWalletFundFlows(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status: got=%d want=200 body=%s", rec.Code, rec.Body.String())
			}
			var body struct {
				Total int `json:"total"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if body.Total != 1 {
				t.Fatalf("total: got=%d want=1 for direction=%s", body.Total, dir)
			}
		})
	}
}

// TestWalletFundFlows_PaginationStable 验证全局分页稳定：limit/offset 不重复不漏
func TestWalletFundFlows_PaginationStable(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	// 插入 10 条记录
	for i := 1; i <= 10; i++ {
		_, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
			flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,amount_satoshi,quantity_text,occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
		) VALUES(?, 'w1', ?, 'OUT', 'BSV', '', ?, 't?', 0, ?, '0', 1700000000+?, 1700000000+?, 'WOC', '', '{}')`,
			fmt.Sprintf("pf_%d", i), fmt.Sprintf("a%d", i), fmt.Sprintf("u%d", i), i*10, i, i)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// 第一页：limit=3, offset=0
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?limit=3&offset=0", nil)
	rec1 := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("page1 status: got=%d want=200", rec1.Code)
	}
	var body1 struct {
		Total int               `json:"total"`
		Items []walletFundFlowItem `json:"items"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &body1); err != nil {
		t.Fatalf("page1 unmarshal: %v", err)
	}
	if body1.Total != 10 {
		t.Fatalf("page1 total: got=%d want=10", body1.Total)
	}
	if len(body1.Items) != 3 {
		t.Fatalf("page1 items: got=%d want=3", len(body1.Items))
	}
	page1IDs := make(map[int64]bool)
	for _, it := range body1.Items {
		page1IDs[it.ID] = true
	}

	// 第二页：limit=3, offset=3
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?limit=3&offset=3", nil)
	rec2 := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("page2 status: got=%d want=200", rec2.Code)
	}
	var body2 struct {
		Total int               `json:"total"`
		Items []walletFundFlowItem `json:"items"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &body2); err != nil {
		t.Fatalf("page2 unmarshal: %v", err)
	}
	if len(body2.Items) != 3 {
		t.Fatalf("page2 items: got=%d want=3", len(body2.Items))
	}

	// 两页 ID 不重叠
	for _, it := range body2.Items {
		if page1IDs[it.ID] {
			t.Fatalf("page2 contains ID %d already in page1: pagination overlap", it.ID)
		}
	}
}

// TestWalletFundFlowDetail_RequiresFlowType 验证 detail 接口强制要求 flow_type
func TestWalletFundFlowDetail_RequiresFlowType(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	// 不传 flow_type → 400
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id=1", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got=%d want=400 body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.Error != "flow_type is required" {
		t.Fatalf("error: got=%q want=%q", body.Error, "flow_type is required")
	}
}

// TestWalletFundFlowDetail_HitsCorrectRecord 验证 id + flow_type 命中正确记录
func TestWalletFundFlowDetail_HitsCorrectRecord(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	_, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,amount_satoshi,quantity_text,occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
	) VALUES('detail_flow_1', 'w1', 'a1', 'OUT', 'BSV', '', 'u1', 'tx_detail1', 0, 999, '0', 1700000001, 1700000001, 'WOC', 'test note', '{}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id=1&flow_type=chain_asset", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got=%d want=200 body=%s", rec.Code, rec.Body.String())
	}
	var body walletFundFlowItem
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.FlowType != "chain_asset" {
		t.Fatalf("flow_type: got=%q want=chain_asset", body.FlowType)
	}
	if body.RelatedTxID != "tx_detail1" {
		t.Fatalf("related_txid: got=%q want=tx_detail1", body.RelatedTxID)
	}
	if body.AmountSatoshi != 999 {
		t.Fatalf("amount_satoshi: got=%d want=999", body.AmountSatoshi)
	}
}

// TestWalletFundFlowDetail_WrongFlowTypeReturns404 验证 flow_type 错误时返回 404
func TestWalletFundFlowDetail_WrongFlowTypeReturns404(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	_, err := db.Exec(`INSERT INTO fact_chain_asset_flows(
		flow_id,wallet_id,address,direction,asset_kind,token_id,utxo_id,txid,vout,amount_satoshi,quantity_text,occurred_at_unix,updated_at_unix,evidence_source,note,payload_json
	) VALUES('detail_flow_2', 'w1', 'a2', 'IN', 'BSV', '', 'u2', 'tx_detail2', 0, 100, '0', 1700000002, 1700000002, 'WOC', '', '{}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// id=1 存在于 chain_asset，但用 chain_payment 查询 → 404
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?id=1&flow_type=chain_payment", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got=%d want=404 body=%s", rec.Code, rec.Body.String())
	}
}
