package domainclient

import (
	"context"
	"encoding/json"
	"net/http"

	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
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
	// 这里先沿用 provider 注册协议，不把上层调度逻辑再拆成两层。
	return s.backend.ResolveDomainToPubkey(ctx, domainName)
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

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
