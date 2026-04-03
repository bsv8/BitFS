package clientapp

import (
	"encoding/json"
	"net/http"
	"strings"
)

func (s *httpAPIServer) handleDomainRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req TriggerDomainRegisterNameParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerDomainRegisterName(r.Context(), s.store, s.rt, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleDomainSetTarget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s == nil || s.rt == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "runtime not initialized"})
		return
	}
	var req TriggerDomainSetTargetParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := TriggerDomainSetTarget(r.Context(), s.store, s.rt, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleDomainSettlementStatus 域名结算状态查询（第四步新主线）
// 设计：统一走 GetFrontOrderSettlementSummary 聚合读模型，不再直接查旧过程表
func (s *httpAPIServer) handleDomainSettlementStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	frontOrderID := strings.TrimSpace(r.URL.Query().Get("front_order_id"))
	if frontOrderID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "front_order_id is required"})
		return
	}

	summary, err := GetFrontOrderSettlementSummary(r.Context(), httpStore(s), frontOrderID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, summary)
}
