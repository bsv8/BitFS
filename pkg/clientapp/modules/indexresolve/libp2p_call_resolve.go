package indexresolve

import (
	"context"
	"encoding/json"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func handleIndexResolve(store ResolveStore) moduleapi.LibP2PHook {
	return func(ctx context.Context, ev moduleapi.LibP2PEvent) (moduleapi.LibP2PResult, error) {
		var body struct {
			Route string `json:"route"`
		}
		if err := json.Unmarshal(ev.Request.Body, &body); err != nil {
			return moduleapi.LibP2PResult{
				CallResp: moduleapi.CallResponse{
					Ok:          false,
					Code:        "BAD_REQUEST",
					Message:     "body must be valid JSON",
					ContentType: "application/json",
				},
			}, nil
		}
		if body.Route == "" {
			return moduleapi.LibP2PResult{
				CallResp: moduleapi.CallResponse{
					Ok:          false,
					Code:        "BAD_REQUEST",
					Message:     "route is required",
					ContentType: "application/json",
				},
			}, nil
		}
		manifest, err := BizResolve(ctx, store, body.Route)
		if err != nil {
			return moduleapi.LibP2PResult{
				CallResp: moduleapi.CallResponse{
					Ok:          false,
					Code:        moduleapi.CodeOf(err),
					Message:     moduleapi.MessageOf(err),
					ContentType: "application/json",
				},
			}, nil
		}
		respBody, _ := json.Marshal(manifest)
		return moduleapi.LibP2PResult{
			CallResp: moduleapi.CallResponse{
				Ok:          true,
				Code:        "OK",
				Message:     "",
				ContentType: "application/json",
				Body:        respBody,
			},
		}, nil
	}
}
