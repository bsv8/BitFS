//go:build with_indexresolve

package clientapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPAPISettingsIndexResolve(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	defer db.Close()

	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	closeModule, err := registerOptionalModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("register module failed: %v", err)
	}
	if closeModule != nil {
		t.Cleanup(closeModule)
	}
	srv := newHTTPAPIServer(rt, rt, db, store, nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}

	if _, err := db.Exec(
		`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("aa", 32),
		1,
		4096,
		"/tmp/movie.mp4",
		"movie.mp4",
		"video/mp4",
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	postReq := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"movie","seed_hash":"`+strings.Repeat("aa", 32)+`"}`))
	postRec := httptest.NewRecorder()
	handler.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("post status mismatch: got=%d body=%s", postRec.Code, postRec.Body.String())
	}
	var postBody struct {
		Status string `json:"status"`
		Data   struct {
			Route    string `json:"route"`
			SeedHash string `json:"seed_hash"`
		} `json:"data"`
	}
	if err := json.Unmarshal(postRec.Body.Bytes(), &postBody); err != nil {
		t.Fatalf("decode post body: %v", err)
	}
	if postBody.Status != "ok" {
		t.Fatalf("expected ok status: %s", postRec.Body.String())
	}
	if postBody.Data.Route != "/movie" || postBody.Data.SeedHash != strings.Repeat("aa", 32) {
		t.Fatalf("unexpected post data: %s", postRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/settings/index-resolve", nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status mismatch: got=%d body=%s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), `"status":"ok"`) || !strings.Contains(getRec.Body.String(), `"/movie"`) {
		t.Fatalf("unexpected get body: %s", getRec.Body.String())
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/v1/settings/index-resolve?route=movie", nil)
	delRec := httptest.NewRecorder()
	handler.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete status mismatch: got=%d body=%s", delRec.Code, delRec.Body.String())
	}
	if !strings.Contains(delRec.Body.String(), `"deleted":true`) {
		t.Fatalf("unexpected delete body: %s", delRec.Body.String())
	}

	badRouteReq := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"bad?route","seed_hash":"`+strings.Repeat("aa", 32)+`"}`))
	badRouteRec := httptest.NewRecorder()
	handler.ServeHTTP(badRouteRec, badRouteReq)
	if badRouteRec.Code != http.StatusBadRequest {
		t.Fatalf("bad route status mismatch: got=%d body=%s", badRouteRec.Code, badRouteRec.Body.String())
	}
	if !strings.Contains(badRouteRec.Body.String(), `"code":"ROUTE_INVALID"`) {
		t.Fatalf("unexpected bad route body: %s", badRouteRec.Body.String())
	}

	badSeedReq := httptest.NewRequest(http.MethodPost, "/api/v1/settings/index-resolve", strings.NewReader(`{"route":"movie","seed_hash":"123"}`))
	badSeedRec := httptest.NewRecorder()
	handler.ServeHTTP(badSeedRec, badSeedReq)
	if badSeedRec.Code != http.StatusBadRequest {
		t.Fatalf("bad seed status mismatch: got=%d body=%s", badSeedRec.Code, badSeedRec.Body.String())
	}
	if !strings.Contains(badSeedRec.Body.String(), `"code":"SEED_HASH_INVALID"`) {
		t.Fatalf("unexpected bad seed body: %s", badSeedRec.Body.String())
	}

	oldReq := httptest.NewRequest(http.MethodPost, "/api/v1/admin/routes/indexes", nil)
	oldRec := httptest.NewRecorder()
	handler.ServeHTTP(oldRec, oldReq)
	if oldRec.Code != http.StatusNotFound {
		t.Fatalf("old admin route should be 404, got=%d body=%s", oldRec.Code, oldRec.Body.String())
	}
}

func TestHTTPAPISettingsIndexResolveCapturedHandlerStillWorksAfterCleanup(t *testing.T) {
	t.Parallel()

	db := openResolveCallTestDB(t)
	defer db.Close()
	store := newClientDB(db, nil)
	rt := &Runtime{ctx: t.Context(), modules: newModuleRegistry()}
	closeModule, err := registerOptionalModules(t.Context(), rt, store)
	if err != nil {
		t.Fatalf("register module failed: %v", err)
	}
	srv := newHTTPAPIServer(rt, rt, db, store, nil, nil, nil, nil)
	handler, err := srv.Handler()
	if err != nil {
		t.Fatalf("build handler failed: %v", err)
	}
	if closeModule != nil {
		closeModule()
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings/index-resolve", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected handler to keep working, got=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected body after cleanup: %s", rec.Body.String())
	}
}
