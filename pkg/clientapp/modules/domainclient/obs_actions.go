package domainclient

import (
	"context"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func (s *service) obsActions() []moduleapi.OBSAction {
	return []moduleapi.OBSAction{
		{
			Action: "obs.domainclient.register",
			Handler: func(ctx context.Context, gotAction string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
				if strings.TrimSpace(gotAction) != "obs.domainclient.register" {
					return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
				}
				var req TriggerDomainRegisterNameParams
				if err := decodeMapInto(payload, &req); err != nil {
					return moduleapi.OBSActionResponse{}, err
				}
				out, err := TriggerDomainRegisterName(ctx, s.backend, s.backend, req)
				if err != nil {
					return moduleapi.OBSActionResponse{}, toModuleAPIError(err)
				}
				return moduleapi.OBSActionResponse{OK: true, Result: "registered", Payload: toMap(out)}, nil
			},
		},
		{
			Action: "obs.domainclient.set_target",
			Handler: func(ctx context.Context, gotAction string, payload map[string]any) (moduleapi.OBSActionResponse, error) {
				if strings.TrimSpace(gotAction) != "obs.domainclient.set_target" {
					return moduleapi.OBSActionResponse{}, moduleapi.NewError("UNSUPPORTED_CONTROL_ACTION", "unsupported control action")
				}
				var req TriggerDomainSetTargetParams
				if err := decodeMapInto(payload, &req); err != nil {
					return moduleapi.OBSActionResponse{}, err
				}
				out, err := TriggerDomainSetTarget(ctx, s.backend, s.backend, req)
				if err != nil {
					return moduleapi.OBSActionResponse{}, toModuleAPIError(err)
				}
				return moduleapi.OBSActionResponse{OK: true, Result: "updated", Payload: toMap(out)}, nil
			},
		},
	}
}
