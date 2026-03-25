package clientapp

import (
	"encoding/json"
	"net/http"
	"strings"
)

func (s *httpAPIServer) handleResolverResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req struct {
		ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
		Name              string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerResolverResolve(r.Context(), s.rt, TriggerResolverResolveParams{
		ResolverPubkeyHex: req.ResolverPubkeyHex,
		Name:              req.Name,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                resp.Ok,
		"code":              resp.Code,
		"message":           resp.Message,
		"name":              resp.Name,
		"target_pubkey_hex": resp.TargetPubkeyHex,
		"updated_at_unix":   resp.UpdatedAtUnix,
	})
}

func (s *httpAPIServer) handleAdminResolverRecords(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.db == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "db not initialized"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := listResolverRecords(s.db)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"total": len(items), "items": items})
	case http.MethodPost:
		var req struct {
			Name            string `json:"name"`
			TargetPubkeyHex string `json:"target_pubkey_hex"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		record, err := upsertResolverRecord(s.db, req.Name, req.TargetPubkeyHex)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"record": record,
		})
	case http.MethodDelete:
		name, err := deleteResolverRecord(s.db, strings.TrimSpace(r.URL.Query().Get("name")))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": true, "name": name})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
	}
}
