package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/domainsvc"
	"github.com/bsv8/BFTP/pkg/obs"
)

type TriggerResolverResolveParams struct {
	ResolverPubkeyHex string
	Name              string
}

type resolverResolveResp struct {
	Ok              bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok"`
	Code            string `protobuf:"bytes,2,opt,name=code,proto3" json:"code,omitempty"`
	Message         string `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	Name            string `protobuf:"bytes,4,opt,name=name,proto3" json:"name,omitempty"`
	TargetPubkeyHex string `protobuf:"bytes,5,opt,name=target_pubkey_hex,json=targetPubkeyHex,proto3" json:"target_pubkey_hex,omitempty"`
	UpdatedAtUnix   int64  `protobuf:"varint,6,opt,name=updated_at_unix,json=updatedAtUnix,proto3" json:"updated_at_unix,omitempty"`
}

// registerResolverHandlers 现在保留为空壳。
// 设计说明：
// - resolver 已经收敛成独立 domain 节点能力；
// - BitFS client 只保留“通过 peer.call 调 domain.v1.resolve”的调用入口；
// - 这样壳内只理解 name -> pubkey_hex，不再直接耦合 domain 的旧专用 paid proto。
func registerResolverHandlers(_ *Runtime) {}

func TriggerResolverResolve(ctx context.Context, rt *Runtime, p TriggerResolverResolveParams) (resolverResolveResp, error) {
	var out resolverResolveResp
	if rt == nil || rt.Host == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	resolverPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.ResolverPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("resolver_pubkey_hex invalid: %w", err)
	}
	name, err := normalizeResolverNameCanonical(p.Name)
	if err != nil {
		return out, err
	}
	payload, err := json.Marshal(map[string]any{"name": name})
	if err != nil {
		return out, err
	}
	resp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          resolverPubkeyHex,
		Route:       domainsvc.RouteDomainV1Resolve,
		ContentType: "application/json",
		Body:        payload,
	})
	if err != nil {
		return out, err
	}
	if !resp.Ok {
		out.Code = strings.ToUpper(strings.TrimSpace(resp.Code))
		out.Message = strings.TrimSpace(resp.Message)
		if out.Code == "" {
			out.Code = "ERROR"
		}
		return out, nil
	}
	var routeBody domainsvc.ResolveNamePaidResp
	if err := json.Unmarshal(resp.Body, &routeBody); err != nil {
		return out, fmt.Errorf("decode domain resolve body: %w", err)
	}
	out = resolverResolveResp{
		Ok:              routeBody.Success,
		Code:            strings.ToUpper(strings.TrimSpace(routeBody.Status)),
		Message:         strings.TrimSpace(routeBody.Error),
		Name:            strings.TrimSpace(routeBody.Name),
		TargetPubkeyHex: strings.TrimSpace(routeBody.TargetPubkeyHex),
		UpdatedAtUnix:   routeBody.ExpireAtUnix,
	}
	if out.Code == "" {
		if out.Ok {
			out.Code = "OK"
		} else {
			out.Code = "ERROR"
		}
	}
	if out.Ok {
		obs.Business("bitcast-client", "evt_trigger_domain_resolve_end", map[string]any{
			"resolver_pubkey_hex": resolverPubkeyHex,
			"name":                name,
			"target_pubkey_hex":   out.TargetPubkeyHex,
		})
	}
	return out, nil
}

func normalizeResolverNameCanonical(raw string) (string, error) {
	value, err := domainsvc.NormalizeName(raw)
	if err != nil {
		return "", err
	}
	if strings.Contains(value, "/") {
		return "", fmt.Errorf("resolver name must not contain /")
	}
	return value, nil
}
