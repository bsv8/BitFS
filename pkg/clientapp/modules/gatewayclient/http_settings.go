package gatewayclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/modulekit"
)

const (
	settingsActionGatewayAnnounce = "gatewayclient.reachability.announce"
	settingsActionGatewayQuery    = "gatewayclient.reachability.query"
)

type httpHandler struct {
	svc *service
}

func (s *service) httpRoutes() []moduleapi.HTTPRoute {
	h := &httpHandler{svc: s}
	return []moduleapi.HTTPRoute{
		{Path: "/v1/gatewayclient/reachability/announce", Handler: h.handleGatewayReachabilityAnnounce},
		{Path: "/v1/gatewayclient/reachability/query", Handler: h.handleGatewayReachabilityQuery},
		{Path: "/v1/gatewayclient/events", Handler: h.handleGatewayEventsHTTP},
		{Path: "/v1/gatewayclient/states", Handler: h.handleObservedGatewayStatesHTTP},
		{Path: "/v1/gatewayclient/node-reachability/cache", Handler: h.handleNodeReachabilityCacheHTTP},
		{Path: "/v1/gatewayclient/node-reachability/self", Handler: h.handleSelfNodeReachabilityHTTP},
	}
}

func (h *httpHandler) handleGatewayReachabilityAnnounce(w http.ResponseWriter, r *http.Request) {
	if h.svc == nil {
		modulekit.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "service not initialized")
		return
	}
	req, status, err := readHTTPMap(r)
	if err != nil {
		modulekit.WriteError(w, status, moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	ttlSeconds := uint32(readUint64(req, "ttl_seconds", 3600))
	resp, err := h.svc.AnnounceNodeReachability(r.Context(), ttlSeconds)
	if err != nil {
		modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	modulekit.WriteOK(w, map[string]any{
		"announce_result": resp,
	})
}

func (h *httpHandler) handleGatewayReachabilityQuery(w http.ResponseWriter, r *http.Request) {
	if h.svc == nil {
		modulekit.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "service not initialized")
		return
	}
	req, status, err := readHTTPMap(r)
	if err != nil {
		modulekit.WriteError(w, status, moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	target := strings.TrimSpace(readString(req, "target_node_pubkey_hex", ""))
	if target == "" {
		modulekit.WriteError(w, http.StatusBadRequest, "BAD_REQUEST", "target_node_pubkey_hex is required")
		return
	}
	resp, err := h.svc.QueryNodeReachability(r.Context(), target)
	if err != nil {
		modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	modulekit.WriteOK(w, map[string]any{
		"query_result": resp,
	})
}

func (h *httpHandler) handleGatewayEventsHTTP(w http.ResponseWriter, r *http.Request) {
	if h.svc == nil {
		modulekit.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "service not initialized")
		return
	}
	limit := readLimit(r, 100)
	out, err := h.svc.listGatewayEvents(r.Context(), limit)
	if err != nil {
		modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	modulekit.WriteOK(w, out)
}

func (h *httpHandler) handleObservedGatewayStatesHTTP(w http.ResponseWriter, r *http.Request) {
	if h.svc == nil {
		modulekit.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "service not initialized")
		return
	}
	limit := readLimit(r, 100)
	out, err := h.svc.listObservedGatewayStates(r.Context(), limit)
	if err != nil {
		modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	modulekit.WriteOK(w, out)
}

func (h *httpHandler) handleNodeReachabilityCacheHTTP(w http.ResponseWriter, r *http.Request) {
	if h.svc == nil {
		modulekit.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "service not initialized")
		return
	}
	req, status, err := readHTTPMap(r)
	if err != nil {
		modulekit.WriteError(w, status, moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	if target := strings.TrimSpace(readString(req, "target_node_pubkey_hex", "")); target != "" {
		out, err := h.svc.getNodeReachabilityCache(r.Context(), target)
		if err != nil {
			modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
			return
		}
		modulekit.WriteOK(w, out)
		return
	}
	limit := int(readUint64(req, "limit", 100))
	out, err := h.svc.listNodeReachabilityCaches(r.Context(), limit)
	if err != nil {
		modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	modulekit.WriteOK(w, out)
}

func (h *httpHandler) handleSelfNodeReachabilityHTTP(w http.ResponseWriter, r *http.Request) {
	if h.svc == nil {
		modulekit.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "service not initialized")
		return
	}
	req, status, err := readHTTPMap(r)
	if err != nil {
		modulekit.WriteError(w, status, moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	nodePubkeyHex := strings.TrimSpace(readString(req, "node_pubkey_hex", ""))
	out, err := h.svc.getSelfNodeReachabilityState(r.Context(), nodePubkeyHex)
	if err != nil {
		modulekit.WriteError(w, httpStatusFromModuleErr(err), moduleapi.CodeOf(err), moduleapi.MessageOf(err))
		return
	}
	modulekit.WriteOK(w, out)
}

func (s *service) SettingsHandler(ctx context.Context, action string, payload map[string]any) (map[string]any, error) {
	if err := s.requireHost(); err != nil {
		return nil, err
	}
	if err := s.requireStore(); err != nil {
		return nil, err
	}
	switch strings.TrimSpace(action) {
	case settingsActionGatewayAnnounce:
		ttlSeconds := uint32(readUint64(payload, "ttl_seconds", 3600))
		resp, err := s.AnnounceNodeReachability(ctx, ttlSeconds)
		if err != nil {
			return nil, err
		}
		return map[string]any{"announce_result": resp}, nil
	case settingsActionGatewayQuery:
		target := strings.TrimSpace(readString(payload, "target_node_pubkey_hex", ""))
		if target == "" {
			return nil, moduleapi.NewError("BAD_REQUEST", "target_node_pubkey_hex is required")
		}
		resp, err := s.QueryNodeReachability(ctx, target)
		if err != nil {
			return nil, err
		}
		return map[string]any{"query_result": resp}, nil
	case OBSActionGatewayEvents:
		return s.listGatewayEvents(ctx, int(readUint64(payload, "limit", 100)))
	case OBSActionObservedGatewayStates:
		return s.listObservedGatewayStates(ctx, int(readUint64(payload, "limit", 100)))
	case OBSActionNodeReachabilityCache:
		if target := strings.TrimSpace(readString(payload, "target_node_pubkey_hex", "")); target != "" {
			return s.getNodeReachabilityCache(ctx, target)
		}
		return s.listNodeReachabilityCaches(ctx, int(readUint64(payload, "limit", 100)))
	case OBSActionSelfNodeReachability:
		return s.getSelfNodeReachabilityState(ctx, strings.TrimSpace(readString(payload, "node_pubkey_hex", "")))
	default:
		return nil, moduleapi.NewError("UNSUPPORTED_SETTINGS_ACTION", "unsupported settings action")
	}
}

func (s *service) listGatewayEvents(ctx context.Context, limit int) (map[string]any, error) {
	events, err := s.store.ListGatewayEvents(ctx, limit)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"total":  len(events),
		"events": events,
	}, nil
}

func (s *service) listObservedGatewayStates(ctx context.Context, limit int) (map[string]any, error) {
	states, err := s.store.ListObservedGatewayStates(ctx, limit)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"total":  len(states),
		"states": states,
	}, nil
}

func (s *service) listNodeReachabilityCaches(ctx context.Context, limit int) (map[string]any, error) {
	caches, err := s.store.ListNodeReachabilityCaches(ctx, limit)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"total":  len(caches),
		"caches": caches,
	}, nil
}

func (s *service) getNodeReachabilityCache(ctx context.Context, nodePubkeyHex string) (map[string]any, error) {
	cache, found, err := s.store.GetNodeReachabilityCache(ctx, nodePubkeyHex)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, moduleapi.NewError("NOT_FOUND", "cache not found")
	}
	return map[string]any{"cache": cache}, nil
}

func (s *service) getSelfNodeReachabilityState(ctx context.Context, nodePubkeyHex string) (map[string]any, error) {
	if strings.TrimSpace(nodePubkeyHex) == "" {
		nodePubkeyHex = s.host.NodePubkeyHex()
	}
	state, found, err := s.store.GetSelfNodeReachabilityState(ctx, nodePubkeyHex)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, moduleapi.NewError("NOT_FOUND", "state not found")
	}
	return map[string]any{"state": state}, nil
}

func readHTTPMap(r *http.Request) (map[string]any, int, error) {
	if r == nil {
		return nil, http.StatusBadRequest, moduleapi.NewError("BAD_REQUEST", "request is required")
	}
	if r.Body == nil {
		return map[string]any{}, 0, nil
	}
	defer r.Body.Close()
	var req map[string]any
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, http.StatusBadRequest, moduleapi.NewError("BAD_REQUEST", "invalid json")
	}
	if req == nil {
		req = map[string]any{}
	}
	return req, 0, nil
}

func readString(payload map[string]any, key string, fallback string) string {
	if payload == nil {
		return fallback
	}
	v, ok := payload[key]
	if !ok {
		return fallback
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func readUint64(payload map[string]any, key string, fallback uint64) uint64 {
	if payload == nil {
		return fallback
	}
	v, ok := payload[key]
	if !ok {
		return fallback
	}
	switch typed := v.(type) {
	case float64:
		if typed < 0 {
			return fallback
		}
		return uint64(typed)
	case float32:
		if typed < 0 {
			return fallback
		}
		return uint64(typed)
	case int:
		if typed < 0 {
			return fallback
		}
		return uint64(typed)
	case int64:
		if typed < 0 {
			return fallback
		}
		return uint64(typed)
	case uint64:
		return typed
	case json.Number:
		n, err := typed.Int64()
		if err != nil || n < 0 {
			return fallback
		}
		return uint64(n)
	default:
		if parsed, err := strconv.ParseUint(strings.TrimSpace(fmt.Sprint(v)), 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func readLimit(r *http.Request, fallback int) int {
	if r == nil {
		return fallback
	}
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func httpStatusFromModuleErr(err error) int {
	switch strings.ToUpper(strings.TrimSpace(moduleapi.CodeOf(err))) {
	case "BAD_REQUEST":
		return http.StatusBadRequest
	case "NOT_FOUND":
		return http.StatusNotFound
	case "METHOD_NOT_ALLOWED":
		return http.StatusMethodNotAllowed
	case "UNSUPPORTED_SETTINGS_ACTION":
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
