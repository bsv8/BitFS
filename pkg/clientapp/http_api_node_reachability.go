package clientapp

import (
	"encoding/json"
	"net/http"
)

func (s *httpAPIServer) handleGatewayReachabilityAnnounce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req AnnounceNodeReachabilityParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerGatewayAnnounceNodeReachability(r.Context(), s.store, s.rt, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleGatewayReachabilityQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req QueryNodeReachabilityParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerGatewayQueryNodeReachability(r.Context(), s.store, s.rt, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
