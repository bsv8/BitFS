package domain

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
	domainmodule "github.com/bsv8/BFTP/pkg/modules/domain"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	oldproto "github.com/golang/protobuf/proto"
)

// Install 把 domain 解析 provider 接到主干能力上。
//
// 设计说明：
// - 这里只注册解析 provider 和能力声明；
// - 远端解析只依赖 host 的节点快照和 peer call 窄能力。
func Install(ctx context.Context, host moduleapi.Host) (func(), error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx is required")
	}
	if host == nil {
		return nil, fmt.Errorf("host is required")
	}

	return host.InstallModule(moduleapi.ModuleSpec{
		ID:      ModuleID,
		Version: CapabilityVersion,
		DomainResolvers: []moduleapi.DomainResolver{
			{
				Name: ResolveProviderName,
				Handler: func(ctx context.Context, rawDomain string) (string, error) {
					return resolveDomainRemote(ctx, host, rawDomain)
				},
			},
		},
	})
}

func resolveDomainRemote(ctx context.Context, host moduleapi.Host, rawDomain string) (string, error) {
	if ctx == nil {
		return "", NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return "", NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if host == nil {
		return "", NewError(CodeDomainResolverUnavailable, "host is required")
	}

	domainName, err := domainmodule.NormalizeName(rawDomain)
	if err != nil {
		return "", NewError(CodeBadRequest, err.Error())
	}
	gateways := normalizeDomainResolveGateways(host.GatewaySnapshot())
	if len(gateways) == 0 {
		return "", NewError(CodeDomainNotResolved, "domain not resolved")
	}
	for _, gateway := range gateways {
		if ctx.Err() != nil {
			return "", NewError(CodeRequestCanceled, ctx.Err().Error())
		}
		payload, err := oldproto.Marshal(&contractmessage.NameRouteReq{Name: domainName})
		if err != nil {
			return "", NewError(CodeBadRequest, err.Error())
		}
		resp, err := host.PeerCall(ctx, moduleapi.PeerCallRequest{
			To:          gateway.Pubkey,
			ProtocolID:  contractprotoid.ProtoDomainResolveNamePaid,
			ContentType: contractmessage.ContentTypeProto,
			Body:        payload,
		})
		if err != nil {
			if ctx.Err() != nil {
				return "", NewError(CodeRequestCanceled, ctx.Err().Error())
			}
			continue
		}
		if !resp.Ok {
			code := strings.ToUpper(strings.TrimSpace(resp.Code))
			switch code {
			case CodeBadRequest, CodeRequestCanceled, CodeDomainProviderSignatureInvalid:
				return "", NewError(code, strings.TrimSpace(resp.Message))
			default:
				continue
			}
		}
		var routeBody contractmessage.ResolveNamePaidResp
		if err := oldproto.Unmarshal(resp.Body, &routeBody); err != nil {
			continue
		}
		if !routeBody.Success {
			switch classifyDomainResolveStatus(routeBody.Status) {
			case CodeBadRequest, CodeDomainProviderSignatureInvalid:
				return "", NewError(classifyDomainResolveStatus(routeBody.Status), strings.TrimSpace(routeBody.Error))
			default:
				continue
			}
		}
		pubkeyHex, normalizeErr := normalizeCompressedPubKeyHex(routeBody.TargetPubkeyHex)
		if normalizeErr != nil {
			continue
		}
		return pubkeyHex, nil
	}
	return "", NewError(CodeDomainNotResolved, "domain not resolved")
}

func normalizeDomainResolveGateways(in []moduleapi.PeerNode) []moduleapi.PeerNode {
	out := make([]moduleapi.PeerNode, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		if !item.Enabled {
			continue
		}
		pubkeyHex, err := normalizeCompressedPubKeyHex(item.Pubkey)
		if err != nil {
			continue
		}
		if _, ok := seen[pubkeyHex]; ok {
			continue
		}
		seen[pubkeyHex] = struct{}{}
		copyItem := item
		copyItem.Pubkey = pubkeyHex
		out = append(out, copyItem)
	}
	return out
}

func classifyDomainResolveStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "BAD_REQUEST":
		return CodeBadRequest
	case "INVALID_SIGNATURE", "SIGNATURE_INVALID", "SIGNATURE_ERROR":
		return CodeDomainProviderSignatureInvalid
	default:
		return CodeDomainNotResolved
	}
}

func normalizeCompressedPubKeyHex(in string) (string, error) {
	pubHex := strings.ToLower(strings.TrimSpace(in))
	if pubHex == "" {
		return "", fmt.Errorf("pubkey hex required")
	}
	raw, err := hex.DecodeString(pubHex)
	if err != nil {
		return "", fmt.Errorf("invalid pubkey hex")
	}
	if len(raw) != 33 {
		return "", fmt.Errorf("invalid compressed pubkey length: got=%d want=33", len(raw))
	}
	if raw[0] != 0x02 && raw[0] != 0x03 {
		return "", fmt.Errorf("invalid compressed pubkey prefix")
	}
	return pubHex, nil
}
