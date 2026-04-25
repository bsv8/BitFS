package clientapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domainclient"
)

func TestNewRuntimeAPIHandler_DoesNotRequireRuntimeHTTPServer(t *testing.T) {
	t.Parallel()

	h, _ := newSecpHost(t)
	t.Cleanup(func() { _ = h.Close() })
	db := newWalletAPITestDB(t)

	cfg := Config{}
	cfg.Storage.DataDir = t.TempDir()
	rt := newRuntimeForTest(t, cfg, "", withRuntimeHost(h), withRuntimeFileStorage(newFileStorageRuntimeAdapter(newClientDB(db, nil), nil)))
	rt.HTTP = nil
	rt.modules = newModuleRegistry()
	if _, err := rt.modules.RegisterDomainResolveHook(domainbiz.ResolveProviderName, func(ctx context.Context, domain string) (string, error) {
		return "021111111111111111111111111111111111111111111111111111111111111111", nil
	}); err != nil {
		t.Fatalf("register domain resolve hook: %v", err)
	}

	handler, err := NewRuntimeAPIHandler(rt)
	if err != nil {
		t.Fatalf("new runtime api handler: %v", err)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings/user/schema", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("schema status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	badReq := httptest.NewRequest(http.MethodGet, "/api/v1/admin/config/schema", nil)
	badRec := httptest.NewRecorder()
	handler.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusNotFound {
		t.Fatalf("admin config route should be removed: got=%d want=%d body=%s", badRec.Code, http.StatusNotFound, badRec.Body.String())
	}

	resolveReq := httptest.NewRequest(http.MethodPost, "/api/v1/resolvers/resolve", strings.NewReader(`{"domain":"movie.david"}`))
	resolveRec := httptest.NewRecorder()
	handler.ServeHTTP(resolveRec, resolveReq)
	if resolveRec.Code != http.StatusOK {
		t.Fatalf("resolve status mismatch: got=%d body=%s", resolveRec.Code, resolveRec.Body.String())
	}
	if !strings.Contains(resolveRec.Body.String(), `"status":"ok"`) || !strings.Contains(resolveRec.Body.String(), `"pubkey_hex":"021111111111111111111111111111111111111111111111111111111111111111"`) {
		t.Fatalf("unexpected resolve body: %s", resolveRec.Body.String())
	}

	infoReq := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	infoRec := httptest.NewRecorder()
	handler.ServeHTTP(infoRec, infoReq)
	if infoRec.Code != http.StatusOK {
		t.Fatalf("info status mismatch: got=%d body=%s", infoRec.Code, infoRec.Body.String())
	}
	var infoBody struct {
		Ready   bool   `json:"ready"`
		Phase   string `json:"phase"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(infoRec.Body.Bytes(), &infoBody); err != nil {
		t.Fatalf("decode info body failed: %v", err)
	}
	if !infoBody.Ready || infoBody.Phase != "ready" {
		t.Fatalf("unexpected info state: %+v", infoBody)
	}

	legacyReq := httptest.NewRequest(http.MethodPost, "/api/v1/resolvers/resolve", strings.NewReader(`{"resolver_pubkey_hex":"03deadbeef","name":"movie.david"}`))
	legacyRec := httptest.NewRecorder()
	handler.ServeHTTP(legacyRec, legacyReq)
	if legacyRec.Code != http.StatusBadRequest {
		t.Fatalf("legacy status mismatch: got=%d body=%s", legacyRec.Code, legacyRec.Body.String())
	}
	if !strings.Contains(legacyRec.Body.String(), `"code":"BAD_REQUEST"`) {
		t.Fatalf("unexpected legacy body: %s", legacyRec.Body.String())
	}
}

func TestHandleWalletSummary_UsesStatusEnvelope(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	store := newClientDB(db, nil)
	rt := newRuntimeForTest(t, Config{}, strings.Repeat("11", 32), withRuntimeStore(store))
	srv := &httpAPIServer{
		ctx:   context.Background(),
		rt:    rt,
		db:    db,
		store: store,
	}

	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("client wallet address: %v", err)
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	if err := dbUpsertBSVUTXO(t.Context(), store, bsvUTXOEntry{
		UTXOID:         "wallet-summary-utxo:0",
		OwnerPubkeyHex: address,
		Address:        address,
		TxID:           "wallet-summary-utxo",
		Vout:           0,
		ValueSatoshi:   777,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now,
		UpdatedAtUnix:  now,
	}); err != nil {
		t.Fatalf("seed fact bsv utxo: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`, "wallet-summary-utxo:0", walletID, address, "wallet-summary-utxo", 0, 777, "unspent", "P2PKH", "", now, walletUTXOAllocationPlainBSV, "", "wallet-summary-utxo", "", now, now, 0); err != nil {
		t.Fatalf("seed wallet utxo: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/wallet/summary", nil)
	getRec := httptest.NewRecorder()
	srv.handleWalletSummary(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("summary status mismatch: got=%d want=%d body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var getBody map[string]json.RawMessage
	if err := json.Unmarshal(getRec.Body.Bytes(), &getBody); err != nil {
		t.Fatalf("decode summary body: %v", err)
	}
	if string(getBody["status"]) != `"ok"` {
		t.Fatalf("summary status envelope mismatch: %s", string(getRec.Body.Bytes()))
	}
	if _, ok := getBody["wallet_address"]; ok {
		t.Fatalf("wallet_address should stay inside data only: %s", string(getRec.Body.Bytes()))
	}
	var data map[string]any
	if err := json.Unmarshal(getBody["data"], &data); err != nil {
		t.Fatalf("decode summary data: %v", err)
	}
	if got, _ := data["wallet_address"].(string); strings.TrimSpace(got) != address {
		t.Fatalf("summary wallet address mismatch: got=%s want=%s", got, address)
	}
	balance, ok := data["wallet_plain_bsv_balance_satoshi"].(float64)
	if !ok {
		t.Fatalf("summary balance missing: %+v", data)
	}
	if got := int64(balance); got != 777 {
		t.Fatalf("summary balance mismatch: got=%d want=%d", got, 777)
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/wallet/summary", nil)
	postRec := httptest.NewRecorder()
	srv.handleWalletSummary(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("summary post status mismatch: got=%d want=%d body=%s", postRec.Code, http.StatusMethodNotAllowed, postRec.Body.String())
	}
	var postBody struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(postRec.Body.Bytes(), &postBody); err != nil {
		t.Fatalf("decode post error body: %v", err)
	}
	if postBody.Status != "error" || postBody.Error.Code != "METHOD_NOT_ALLOWED" {
		t.Fatalf("unexpected post envelope: %+v body=%s", postBody, postRec.Body.String())
	}
}
