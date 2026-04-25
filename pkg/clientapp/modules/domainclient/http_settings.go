package domainclient

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/modulekit"
)

func (s *service) httpRoutes() []moduleapi.HTTPRoute {
	return []moduleapi.HTTPRoute{
		{Path: "/v1/domains/register", Handler: s.handleDomainRegister},
		{Path: "/v1/domains/set-target", Handler: s.handleDomainSetTarget},
		{Path: "/v1/domains/settlement-status", Handler: s.handleDomainSettlementStatus},
	}
}

func (s *service) settingsActions() []moduleapi.SettingsAction {
	return []moduleapi.SettingsAction{
		{
			Action: "settings.domainclient.register",
			Handler: func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
				if strings.TrimSpace(gotAction) != "settings.domainclient.register" {
					return nil, moduleapi.NewError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
				}
				var req TriggerDomainRegisterNameParams
				if err := decodeMapInto(payload, &req); err != nil {
					return nil, err
				}
				out, err := TriggerDomainRegisterName(ctx, s.backend, s.backend, req)
				if err != nil {
					return nil, toModuleAPIError(err)
				}
				return toMap(out), nil
			},
		},
		{
			Action: "settings.domainclient.set_target",
			Handler: func(ctx context.Context, gotAction string, payload map[string]any) (map[string]any, error) {
				if strings.TrimSpace(gotAction) != "settings.domainclient.set_target" {
					return nil, moduleapi.NewError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
				}
				var req TriggerDomainSetTargetParams
				if err := decodeMapInto(payload, &req); err != nil {
					return nil, err
				}
				out, err := TriggerDomainSetTarget(ctx, s.backend, s.backend, req)
				if err != nil {
					return nil, toModuleAPIError(err)
				}
				return toMap(out), nil
			},
		},
	}
}

func (s *service) handleDomainRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		modulekit.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	var req TriggerDomainRegisterNameParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}
	resp, err := TriggerDomainRegisterName(r.Context(), s.backend, s.backend, req)
	if err != nil {
		modulekit.WriteError(w, http.StatusBadRequest, httpErrorCode(err), MessageOf(err))
		return
	}
	modulekit.WriteOK(w, resp)
}

func (s *service) handleDomainSetTarget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		modulekit.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	var req TriggerDomainSetTargetParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "invalid json")
		return
	}
	resp, err := TriggerDomainSetTarget(r.Context(), s.backend, s.backend, req)
	if err != nil {
		modulekit.WriteError(w, http.StatusBadRequest, httpErrorCode(err), MessageOf(err))
		return
	}
	modulekit.WriteOK(w, resp)
}

func (s *service) handleDomainSettlementStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		modulekit.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	frontOrderID := strings.TrimSpace(r.URL.Query().Get("front_order_id"))
	if frontOrderID == "" {
		modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "front_order_id is required")
		return
	}
	summary, err := s.backend.GetFrontOrderSettlementSummary(r.Context(), frontOrderID)
	if err != nil {
		modulekit.WriteError(w, http.StatusInternalServerError, httpErrorCode(err), MessageOf(err))
		return
	}
	modulekit.WriteOK(w, summary)
}

func httpErrorCode(err error) string {
	code := CodeOf(err)
	if strings.TrimSpace(code) == "" {
		return "INTERNAL_ERROR"
	}
	return code
}
