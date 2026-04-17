package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleDirectAPIs_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db, store: newClientDB(db, nil)}

	_, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix)
		VALUES(?,?,?), (?,?,?)`,
		"dmd_1", "seed_1", 1700000000,
		"dmd_2", "seed_2", 1700000001,
	)
	if err != nil {
		t.Fatalf("insert biz_demands: %v", err)
	}
	_, err = db.Exec(`INSERT INTO biz_demand_quotes(
			demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?,?)`,
		"dmd_1", "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1000, 10, 3, 1234, "f1.bin", "application/javascript", "ff", 1893427200, 1700000001,
		"dmd_2", "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2000, 20, 4, 2234, "f2.bin", "text/css", "0f", 1893427200, 1700000002,
	)
	if err != nil {
		t.Fatalf("insert biz_demand_quotes: %v", err)
	}
	_, err = db.Exec(`INSERT INTO biz_demand_quote_arbiters(quote_id,arbiter_pub_hex)
		VALUES(?,?),(?,?),(?,?)`,
		1, "02cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		1, "03dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		2, "02eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	)
	if err != nil {
		t.Fatalf("insert biz_demand_quote_arbiters: %v", err)
	}
	_, err = db.Exec(`INSERT INTO proc_direct_transfer_pools(
		session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_1", "deal_1", "buyer_1", "seller_1", "arb_1", 1000, 2, 3, 30, 968, "ctx", "btx", "btxid_1", "active", 0.5, 6, 1700000004, 1700000005,
	)
	if err != nil {
		t.Fatalf("insert proc_direct_transfer_pools: %v", err)
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/direct/quotes?demand_id=dmd_1", nil)
		rec := httptest.NewRecorder()
		srv.handleDirectQuotes(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("quotes list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				ID           int64  `json:"id"`
				DemandID     string `json:"demand_id"`
				SellerPubHex string `json:"seller_pubkey_hex"`
				MimeType     string `json:"mime_hint"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode quotes list: %v", err)
		}
		if body.Total != 1 || len(body.Items) != 1 || body.Items[0].DemandID != "dmd_1" {
			t.Fatalf("unexpected quotes list body: %+v", body)
		}
		if body.Items[0].MimeType != "application/javascript" {
			t.Fatalf("quote list mime_hint mismatch: got=%q", body.Items[0].MimeType)
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/direct/quotes/detail?id=1", nil)
		recDetail := httptest.NewRecorder()
		srv.handleDirectQuoteDetail(recDetail, reqDetail)
		if recDetail.Code != http.StatusOK {
			t.Fatalf("quotes detail status mismatch: got=%d want=%d body=%s", recDetail.Code, http.StatusOK, recDetail.Body.String())
		}
		var detail struct {
			ID       int64  `json:"id"`
			MimeType string `json:"mime_hint"`
		}
		if err := json.Unmarshal(recDetail.Body.Bytes(), &detail); err != nil {
			t.Fatalf("decode quotes detail: %v", err)
		}
		if detail.ID != 1 || detail.MimeType != "application/javascript" {
			t.Fatalf("unexpected quotes detail body: %+v", detail)
		}
	}

	{
		mux, err := srv.buildMux()
		if err != nil {
			t.Fatalf("build mux: %v", err)
		}
		for _, path := range []string{
			"/api/v1/direct/deals",
			"/api/v1/direct/deals/detail",
			"/api/v1/direct/sessions",
			"/api/v1/direct/sessions/detail",
			"/api/v1/sales",
			"/api/v1/sales/detail",
		} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("deprecated route should be removed: path=%s code=%d body=%s", path, rec.Code, rec.Body.String())
			}
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/direct/transfer-pools?status=active", nil)
		rec := httptest.NewRecorder()
		srv.handleDirectTransferPoolsDebug(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("pools list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			DataRole   string `json:"data_role"`
			StatusNote string `json:"status_note"`
			Total      int    `json:"total"`
			Items      []struct {
				SessionID string `json:"session_id"`
				DealID    string `json:"deal_id"`
				Status    string `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode pools list: %v", err)
		}
		// 【第五步】验证运行时接口明确标注运行时语义
		if body.DataRole != "runtime_debug_only" {
			t.Fatalf("expected data_role=runtime_debug_only, got=%s", body.DataRole)
		}
		if body.StatusNote != "proc_direct_transfer_pools.status is protocol runtime status, not settlement status" {
			t.Fatalf("expected status_note to indicate runtime-only, got=%s", body.StatusNote)
		}
		if body.Total != 1 || len(body.Items) != 1 || body.Items[0].SessionID != "sess_1" {
			t.Fatalf("unexpected pools list body: %+v", body)
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/direct/transfer-pools/detail?session_id=sess_1", nil)
		recDetail := httptest.NewRecorder()
		srv.handleDirectTransferPoolDetailDebug(recDetail, reqDetail)
		if recDetail.Code != http.StatusOK {
			t.Fatalf("pools detail status mismatch: got=%d want=%d body=%s", recDetail.Code, http.StatusOK, recDetail.Body.String())
		}
		var detailBody struct {
			DataRole   string `json:"data_role"`
			StatusNote string `json:"status_note"`
			Item       struct {
				SessionID string `json:"session_id"`
				Status    string `json:"status"`
			} `json:"item"`
		}
		if err := json.Unmarshal(recDetail.Body.Bytes(), &detailBody); err != nil {
			t.Fatalf("decode pools detail: %v", err)
		}
		// 【第五步】验证运行时详情接口也明确标注运行时语义
		if detailBody.DataRole != "runtime_debug_only" {
			t.Fatalf("expected detail data_role=runtime_debug_only, got=%s", detailBody.DataRole)
		}
		if detailBody.StatusNote != "proc_direct_transfer_pools.status is protocol runtime status, not settlement status" {
			t.Fatalf("expected detail status_note to indicate runtime-only, got=%s", detailBody.StatusNote)
		}
		if detailBody.Item.SessionID != "sess_1" {
			t.Fatalf("unexpected detail item: %+v", detailBody.Item)
		}
	}
}

func TestHandlePurchaseAPIs_UsePurchasesAndDeprecateSales(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	srv := &httpAPIServer{db: db, store: store}

	if _, err := db.Exec(`INSERT INTO biz_demands(demand_id,seed_hash,created_at_unix) VALUES(?,?,?)`, "dmd_p1", "seed_p1", 1700000010); err != nil {
		t.Fatalf("insert demand: %v", err)
	}
	seedPurchase := purchaseDoneEntry{
		DemandID:      "dmd_p1",
		SellerPubHex:  "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArbiterPubHex: "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ChunkIndex:    0,
		ObjectHash:    "111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000",
		AmountSatoshi: 120,
	}
	chunkPurchase := purchaseDoneEntry{
		DemandID:      "dmd_p1",
		SellerPubHex:  seedPurchase.SellerPubHex,
		ArbiterPubHex: seedPurchase.ArbiterPubHex,
		ChunkIndex:    2,
		ObjectHash:    "0000ffffeeeeddddccccbbbbaaaa999988887777666655554444333322221111",
		AmountSatoshi: 33,
	}
	if err := dbAppendPurchaseDone(context.Background(), store, seedPurchase); err != nil {
		t.Fatalf("append seed purchase: %v", err)
	}
	if err := dbAppendPurchaseDone(context.Background(), store, chunkPurchase); err != nil {
		t.Fatalf("append chunk purchase: %v", err)
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/biz_purchases?demand_id=dmd_p1", nil)
		rec := httptest.NewRecorder()
		srv.handlePurchases(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("biz_purchases list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				ID            int64  `json:"id"`
				DemandID      string `json:"demand_id"`
				SellerPubHex  string `json:"seller_pubkey_hex"`
				ArbiterPubHex string `json:"arbiter_pubkey_hex"`
				ChunkIndex    uint32 `json:"chunk_index"`
				ObjectHash    string `json:"object_hash"`
				Amount        uint64 `json:"amount_satoshi"`
				Status        string `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode biz_purchases list: %v", err)
		}
		if body.Total != 2 || len(body.Items) != 2 {
			t.Fatalf("unexpected biz_purchases list: %+v", body)
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/biz_purchases/detail?id=1", nil)
		rec := httptest.NewRecorder()
		srv.handlePurchaseDetail(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("purchase detail status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var detail struct {
			DemandID   string `json:"demand_id"`
			ChunkIndex uint32 `json:"chunk_index"`
			Status     string `json:"status"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
			t.Fatalf("decode purchase detail: %v", err)
		}
		if detail.DemandID != "dmd_p1" || detail.ChunkIndex != 0 || detail.Status != "done" {
			t.Fatalf("unexpected purchase detail: %+v", detail)
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/biz_purchases/summary?demand_id=dmd_p1", nil)
		rec := httptest.NewRecorder()
		srv.handlePurchaseSummary(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("purchase summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var summary struct {
			SeedPurchaseCount  int64 `json:"seed_purchase_count"`
			ChunkPurchaseCount int64 `json:"chunk_purchase_count"`
			TotalPurchaseCount int64 `json:"total_purchase_count"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
			t.Fatalf("decode purchase summary: %v", err)
		}
		if summary.SeedPurchaseCount != 1 || summary.ChunkPurchaseCount != 1 || summary.TotalPurchaseCount != 2 {
			t.Fatalf("unexpected purchase summary: %+v", summary)
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
		rec := httptest.NewRecorder()
		srv.handleWalletSummary(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("wallet summary status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode wallet summary: %v", err)
		}
		if got, ok := body["purchase_count"].(float64); !ok || got != 2 {
			t.Fatalf("wallet summary purchase_count mismatch: %+v", body)
		}
		if _, ok := body["sale_count"]; ok {
			t.Fatalf("wallet summary should not expose sale_count anymore: %+v", body)
		}
	}

	{
	}
}
