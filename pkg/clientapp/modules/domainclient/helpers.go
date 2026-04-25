package domainclient

import (
	"context"
	"encoding/json"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
)

type service struct {
	backend Backend
}

func newService(backend Backend) *service {
	return &service{backend: backend}
}

func (s *service) resolveDomainRemote(ctx context.Context, rawDomain string) (string, error) {
	if ctx == nil {
		return "", NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return "", NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	domainName, err := domainwire.NormalizeName(rawDomain)
	if err != nil {
		return "", NewError(CodeBadRequest, err.Error())
	}
	// provider 只做直连解析，不再绕回 root 调度器。
	return s.backend.ResolveDomainToPubkeyDirect(ctx, domainName)
}

func decodeMapInto(in map[string]any, out any) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return err
	}
	return nil
}

func toMap(v any) map[string]any {
	raw, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	out := make(map[string]any)
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func toModuleAPIError(err error) error {
	if err == nil {
		return nil
	}
	if code := CodeOf(err); code != "" {
		return moduleapi.NewError(code, MessageOf(err))
	}
	return err
}
