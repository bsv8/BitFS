package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleDirectAPIs_ListAndDetail(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}

	_, err := db.Exec(`INSERT INTO direct_quotes(demand_id,seller_pubkey_hex,seed_price,chunk_price,expires_at_unix,recommended_file_name,mime_hint,available_chunk_bitmap_hex,seller_arbiter_pubkey_hexes_json,created_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?)`,
		"dmd_1", "seller_1", 1000, 10, 1700000100, "f1.bin", "application/javascript", "ff", `["arb_1","arb_2"]`, 1700000001,
		"dmd_2", "seller_2", 2000, 20, 1700000200, "f2.bin", "text/css", "0f", `["arb_3"]`, 1700000002,
	)
	if err != nil {
		t.Fatalf("insert direct_quotes: %v", err)
	}
	_, err = db.Exec(`INSERT INTO direct_deals(deal_id,demand_id,buyer_pubkey_hex,seller_pubkey_hex,seed_hash,seed_price,chunk_price,arbiter_pubkey_hex,status,created_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"deal_1", "dmd_1", "buyer_1", "seller_1", "seed_1", 1000, 10, "arb_1", "active", 1700000003,
	)
	if err != nil {
		t.Fatalf("insert direct_deals: %v", err)
	}
	_, err = db.Exec(`INSERT INTO direct_sessions(session_id,deal_id,chunk_price,paid_chunks,paid_amount,released_chunks,released_amount,status,created_at_unix,updated_at_unix)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		"sess_1", "deal_1", 10, 3, 30, 2, 20, "active", 1700000004, 1700000005,
	)
	if err != nil {
		t.Fatalf("insert direct_sessions: %v", err)
	}
	_, err = db.Exec(`INSERT INTO direct_transfer_pools(
		session_id,deal_id,buyer_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,pool_amount,spend_tx_fee,sequence_num,seller_amount,buyer_amount,current_tx_hex,base_tx_hex,base_txid,status,fee_rate_sat_byte,lock_blocks,created_at_unix,updated_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"sess_1", "deal_1", "buyer_1", "seller_1", "arb_1", 1000, 2, 3, 30, 968, "ctx", "btx", "btxid_1", "active", 0.5, 6, 1700000004, 1700000005,
	)
	if err != nil {
		t.Fatalf("insert direct_transfer_pools: %v", err)
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
				SellerPeerID string `json:"seller_pubkey_hex"`
				MIMEHint     string `json:"mime_hint"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode quotes list: %v", err)
		}
		if body.Total != 1 || len(body.Items) != 1 || body.Items[0].DemandID != "dmd_1" {
			t.Fatalf("unexpected quotes list body: %+v", body)
		}
		if body.Items[0].MIMEHint != "application/javascript" {
			t.Fatalf("quote list mime_hint mismatch: got=%q", body.Items[0].MIMEHint)
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/direct/quotes/detail?id=1", nil)
		recDetail := httptest.NewRecorder()
		srv.handleDirectQuoteDetail(recDetail, reqDetail)
		if recDetail.Code != http.StatusOK {
			t.Fatalf("quotes detail status mismatch: got=%d want=%d body=%s", recDetail.Code, http.StatusOK, recDetail.Body.String())
		}
		var detail struct {
			ID       int64  `json:"id"`
			MIMEHint string `json:"mime_hint"`
		}
		if err := json.Unmarshal(recDetail.Body.Bytes(), &detail); err != nil {
			t.Fatalf("decode quotes detail: %v", err)
		}
		if detail.ID != 1 || detail.MIMEHint != "application/javascript" {
			t.Fatalf("unexpected quotes detail body: %+v", detail)
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/direct/deals?status=active", nil)
		rec := httptest.NewRecorder()
		srv.handleDirectDeals(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("deals list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				DealID string `json:"deal_id"`
				Status string `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode deals list: %v", err)
		}
		if body.Total != 1 || len(body.Items) != 1 || body.Items[0].DealID != "deal_1" {
			t.Fatalf("unexpected deals list body: %+v", body)
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/direct/deals/detail?deal_id=deal_1", nil)
		recDetail := httptest.NewRecorder()
		srv.handleDirectDealDetail(recDetail, reqDetail)
		if recDetail.Code != http.StatusOK {
			t.Fatalf("deals detail status mismatch: got=%d want=%d body=%s", recDetail.Code, http.StatusOK, recDetail.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/direct/sessions?deal_id=deal_1", nil)
		rec := httptest.NewRecorder()
		srv.handleDirectSessions(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("sessions list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				SessionID string `json:"session_id"`
				DealID    string `json:"deal_id"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode sessions list: %v", err)
		}
		if body.Total != 1 || len(body.Items) != 1 || body.Items[0].SessionID != "sess_1" {
			t.Fatalf("unexpected sessions list body: %+v", body)
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/direct/sessions/detail?session_id=sess_1", nil)
		recDetail := httptest.NewRecorder()
		srv.handleDirectSessionDetail(recDetail, reqDetail)
		if recDetail.Code != http.StatusOK {
			t.Fatalf("sessions detail status mismatch: got=%d want=%d body=%s", recDetail.Code, http.StatusOK, recDetail.Body.String())
		}
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/api/v1/direct/transfer-pools?status=active", nil)
		rec := httptest.NewRecorder()
		srv.handleDirectTransferPools(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("pools list status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var body struct {
			Total int `json:"total"`
			Items []struct {
				SessionID string `json:"session_id"`
				DealID    string `json:"deal_id"`
				Status    string `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode pools list: %v", err)
		}
		if body.Total != 1 || len(body.Items) != 1 || body.Items[0].SessionID != "sess_1" {
			t.Fatalf("unexpected pools list body: %+v", body)
		}

		reqDetail := httptest.NewRequest(http.MethodGet, "/api/v1/direct/transfer-pools/detail?session_id=sess_1", nil)
		recDetail := httptest.NewRecorder()
		srv.handleDirectTransferPoolDetail(recDetail, reqDetail)
		if recDetail.Code != http.StatusOK {
			t.Fatalf("pools detail status mismatch: got=%d want=%d body=%s", recDetail.Code, http.StatusOK, recDetail.Body.String())
		}
	}
}
