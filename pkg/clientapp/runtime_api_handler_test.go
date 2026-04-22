package clientapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
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
