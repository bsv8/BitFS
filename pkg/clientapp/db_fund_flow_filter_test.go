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
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 插入 fact_bsv_utxos 测试数据（不同方向）
	// OUT 方向：utxo_state='spent'
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json
	) VALUES
		('utxo_1', 'pubkey1', 'addr1', 'tx1', 0, 100, 'spent', 'plain_bsv', 'spend_tx1', 1700000001, 1700000001, 1700000001, '', '{}'),
		('utxo_2', 'pubkey1', 'addr2', 'tx2', 0, 200, 'spent', 'plain_bsv', 'spend_tx2', 1700000002, 1700000002, 1700000002, '', '{}'),
		('utxo_4', 'pubkey1', 'addr4', 'tx4', 0, 30, 'spent', 'token_carrier', 'spend_tx4', 1700000004, 1700000004, 1700000004, '', '{}'),
		('utxo_7', 'pubkey1', 'addr7', 'tx7', 0, 70, 'spent', 'plain_bsv', 'spend_tx7', 1700000007, 1700000007, 1700000007, '', '{}')
	`)
	if err != nil {
		t.Fatalf("insert fact_bsv_utxos spent: %v", err)
	}

	// IN 方向：utxo_state='unspent'
	_, err = db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,created_at_unix,updated_at_unix,note,payload_json
	) VALUES
		('utxo_3', 'pubkey1', 'addr3', 'tx3', 0, 50, 'unspent', 'plain_bsv', 1700000003, 1700000003, '', '{}'),
		('utxo_5', 'pubkey1', 'addr5', 'tx5', 0, 500, 'unspent', 'plain_bsv', 1700000005, 1700000005, '', '{}'),
		('utxo_8', 'pubkey1', 'addr8', 'tx8', 0, 80, 'unspent', 'plain_bsv', 1700000008, 1700000008, '', '{}')
	`)
	if err != nil {
		t.Fatalf("insert fact_bsv_utxos unspent: %v", err)
	}

	// Token IN 方向：lot_state='unspent' 或 'spent'
	_, err = db.Exec(`INSERT INTO fact_token_lots(
		lot_id,owner_pubkey_hex,token_id,token_standard,quantity_text,used_quantity_text,lot_state,mint_txid,created_at_unix,updated_at_unix,note,payload_json
	) VALUES
		('lot_6', 'pubkey1', 'token2', 'BSV21', '500', '0', 'unspent', 'tx6', 1700000006, 1700000006, '', '{}')
	`)
	if err != nil {
		t.Fatalf("insert fact_token_lots: %v", err)
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
		// OUT 方向：utxo_1, utxo_2, utxo_4, utxo_7 = 4 条 BSV 流出
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
		// OUT 4 条 + IN 4 条 = 8 条
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
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

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
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 插入 fact_bsv_utxos 测试数据
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json
	) VALUES
		('u1', 'pk1', 'a1', 't1', 0, 100, 'spent', 'plain_bsv', 'spend_tx', 1700000001, 1700000001, 1700000001, '', '{}'),
		('u2', 'pk1', 'a2', 't2', 0, 200, 'unspent', 'plain_bsv', '', 1700000002, 1700000002, 0, '', '{}')
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
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 插入 10 条 BSV UTXO 记录（5个unspent, 5个spent）
	for i := 1; i <= 5; i++ {
		_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
			utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,created_at_unix,updated_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("pf_in_%d", i), "pk1", fmt.Sprintf("a%d", i), fmt.Sprintf("t%d", i), 0, int64(i*10), "unspent", "plain_bsv", int64(1700000000+i), int64(1700000000+i))
		if err != nil {
			t.Fatalf("insert unspent %d: %v", i, err)
		}
	}
	for i := 6; i <= 10; i++ {
		_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
			utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("pf_out_%d", i), "pk1", fmt.Sprintf("a%d", i), fmt.Sprintf("t%d", i), 0, int64(i*10), "spent", "plain_bsv", fmt.Sprintf("spend_t%d", i), int64(1700000000+i), int64(1700000000+i), int64(1700000100+i))
		if err != nil {
			t.Fatalf("insert spent %d: %v", i, err)
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
		Total int                  `json:"total"`
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
		Total int                  `json:"total"`
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

// TestWalletFundFlows_ExposesAssetKind 验证流水返回里显式带出 asset_kind / token_id。
func TestWalletFundFlows_ExposesAssetKind(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 插入 BSV UTXO 记录
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,created_at_unix,updated_at_unix,note
	) VALUES
		('utxo_bsv_1', 'pk1', 'addr_kind', 'tx_bsv_1', 0, 1234, 'unspent', 'plain_bsv', 1700000101, 1700000101, 'bsv in')
	`)
	if err != nil {
		t.Fatalf("insert bsv utxo: %v", err)
	}

	// 插入 Token Lot 记录
	_, err = db.Exec(`INSERT INTO fact_token_lots(
		lot_id,owner_pubkey_hex,token_id,token_standard,quantity_text,used_quantity_text,lot_state,mint_txid,created_at_unix,updated_at_unix,note
	) VALUES
		('lot_token_1', 'pk1', 'token_kind_1', 'BSV21', '2500', '0', 'unspent', 'tx_token_1', 1700000102, 1700000102, 'token in')
	`)
	if err != nil {
		t.Fatalf("insert token lot: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows?limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlows(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body struct {
		Items []struct {
			FlowID        string `json:"flow_id"`
			AssetKind     string `json:"asset_kind"`
			TokenID       string `json:"token_id"`
			TokenStandard string `json:"token_standard"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body.Items) != 2 {
		t.Fatalf("item count mismatch: %+v", body.Items)
	}
	seen := map[string]struct {
		AssetKind     string
		TokenID       string
		TokenStandard string
	}{}
	for _, item := range body.Items {
		seen[item.FlowID] = struct {
			AssetKind     string
			TokenID       string
			TokenStandard string
		}{item.AssetKind, item.TokenID, item.TokenStandard}
	}
	if got := seen["utxo_bsv_1"]; got.AssetKind != "BSV" || got.TokenID != "" || got.TokenStandard != "" {
		t.Fatalf("bsv item mismatch: %+v", got)
	}
	if got := seen["lot_token_1"]; got.AssetKind != "BSV21" || got.TokenID != "token_kind_1" || got.TokenStandard != "BSV21" {
		t.Fatalf("token item mismatch: %+v", got)
	}
}

// TestWalletFundFlowDetail_RequiresFlowType 验证 detail 接口强制要求 flow_type
func TestWalletFundFlowDetail_RequiresFlowType(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 不传 flow_type → 400（先传 ref_id，再测 flow_type）
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?ref_id=1", nil)
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
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 插入 BSV UTXO spent 记录（chain_bsv_out）
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note
	) VALUES('detail_utxo_1', 'pk1', 'a1', 'tx_detail1', 0, 999, 'spent', 'plain_bsv', 'spend_tx_detail', 1700000001, 1700000001, 1700000001, 'test note')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?ref_id=detail_utxo_1&flow_type=chain_bsv_out", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got=%d want=200 body=%s", rec.Code, rec.Body.String())
	}
	var body walletFundFlowItem
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.FlowType != "chain_bsv_out" {
		t.Fatalf("flow_type: got=%q want=chain_bsv_out", body.FlowType)
	}
	if body.RelatedTxID != "spend_tx_detail" {
		t.Fatalf("related_txid: got=%q want=spend_tx_detail", body.RelatedTxID)
	}
	if body.AmountSatoshi != 999 {
		t.Fatalf("amount_satoshi: got=%d want=999", body.AmountSatoshi)
	}
}

// TestWalletFundFlowDetail_WrongFlowTypeReturns404 验证 flow_type 错误时返回 404
func TestWalletFundFlowDetail_WrongFlowTypeReturns404(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	// 插入 BSV UTXO unspent 记录
	_, err := db.Exec(`INSERT INTO fact_bsv_utxos(
		utxo_id,owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,spent_by_txid,created_at_unix,updated_at_unix,spent_at_unix,note,payload_json
	) VALUES('detail_utxo_2', 'pk1', 'a2', 'tx_detail2', 0, 100, 'unspent', 'plain_bsv', '', 1700000002, 1700000002, 0, '', '{}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// detail_utxo_2 是 unspent（chain_bsv_in），但用 chain_bsv_out 查询 → 404
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/fund-flows/detail?ref_id=detail_utxo_2&flow_type=chain_bsv_out", nil)
	rec := httptest.NewRecorder()
	srv.handleWalletFundFlowDetail(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got=%d want=404 body=%s", rec.Code, rec.Body.String())
	}
}
