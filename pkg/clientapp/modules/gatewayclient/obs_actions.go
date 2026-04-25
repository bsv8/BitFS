package gatewayclient

import (
	"context"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

const (
	OBSActionGatewayEvents         = "gateway_events"
	OBSActionGatewayEventDetail     = "gateway_event_detail"
	OBSActionObservedGatewayStates  = "observed_gateway_states"
	OBSActionNodeReachabilityCache  = "node_reachability_cache"
	OBSActionSelfNodeReachability   = "self_node_reachability"
)

// OBSActionHandler 处理模块内的观测动作。
func (s *service) OBSActionHandler(ctx context.Context, action string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
	if err := s.requireHost(); err != nil {
		return moduleapi.OBSActionResponse{}, err
	}
	if err := s.requireStore(); err != nil {
		return moduleapi.OBSActionResponse{}, err
	}

	switch strings.TrimSpace(action) {
	case OBSActionGatewayEvents:
		out, err := s.listGatewayEvents(ctx, int(readUint64(payload, "limit", 100)))
		if err != nil {
			return moduleapi.OBSActionResponse{}, err
		}
		return moduleapi.OBSActionResponse{OK: true, Result: "listed", Payload: out}, nil
	case OBSActionGatewayEventDetail:
		eventID := strings.TrimSpace(readString(payload, "event_id", ""))
		if eventID == "" {
			return moduleapi.OBSActionResponse{}, moduleapi.NewError("BAD_REQUEST", "event_id is required")
		}
		event, found, err := s.store.GetGatewayEvent(ctx, eventID)
		if err != nil {
			return moduleapi.OBSActionResponse{}, err
		}
		if !found {
			return moduleapi.OBSActionResponse{}, moduleapi.NewError("NOT_FOUND", "event not found")
		}
		return moduleapi.OBSActionResponse{OK: true, Result: "loaded", Payload: map[string]any{"event": event}}, nil
	case OBSActionObservedGatewayStates:
		out, err := s.listObservedGatewayStates(ctx, int(readUint64(payload, "limit", 100)))
		if err != nil {
			return moduleapi.OBSActionResponse{}, err
		}
		return moduleapi.OBSActionResponse{OK: true, Result: "listed", Payload: out}, nil
	case OBSActionNodeReachabilityCache:
		if target := strings.TrimSpace(readString(payload, "target_node_pubkey_hex", "")); target != "" {
			out, err := s.getNodeReachabilityCache(ctx, target)
			if err != nil {
				return moduleapi.OBSActionResponse{}, err
			}
			return moduleapi.OBSActionResponse{OK: true, Result: "loaded", Payload: out}, nil
		}
		out, err := s.listNodeReachabilityCaches(ctx, int(readUint64(payload, "limit", 100)))
		if err != nil {
			return moduleapi.OBSActionResponse{}, err
		}
		return moduleapi.OBSActionResponse{OK: true, Result: "listed", Payload: out}, nil
	case OBSActionSelfNodeReachability:
		out, err := s.getSelfNodeReachabilityState(ctx, strings.TrimSpace(readString(payload, "node_pubkey_hex", "")))
		if err != nil {
			return moduleapi.OBSActionResponse{}, err
		}
		return moduleapi.OBSActionResponse{OK: true, Result: "loaded", Payload: out}, nil
	default:
		return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
	}
}
