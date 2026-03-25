package clientapp

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithAuth_PassThrough(t *testing.T) {
	t.Parallel()

	srv := &httpAPIServer{}
	called := false
	h := srv.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !called {
		t.Fatalf("next handler should be called")
	}
}

func TestWithAuth_InjectVisitMeta(t *testing.T) {
	t.Parallel()

	srv := &httpAPIServer{}
	h := srv.withAuth(func(w http.ResponseWriter, r *http.Request) {
		meta := requestVisitMetaFromContext(r.Context())
		if meta.VisitID != "visit-123" {
			t.Fatalf("visit id mismatch: got=%q want=%q", meta.VisitID, "visit-123")
		}
		if meta.VisitLocator != "node:abcdef/index" {
			t.Fatalf("visit locator mismatch: got=%q want=%q", meta.VisitLocator, "node:abcdef/index")
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/info", nil)
	req.Header.Set(headerVisitID, "visit-123")
	req.Header.Set(headerVisitLocator, "node:abcdef/index")
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
