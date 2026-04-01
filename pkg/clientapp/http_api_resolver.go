package clientapp

import (
	"encoding/json"
	"net/http"
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
	resp, err := TriggerResolverResolve(r.Context(), s.store, s.rt, TriggerResolverResolveParams{
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
