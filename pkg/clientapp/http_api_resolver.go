package clientapp

import (
	"encoding/json"
	"net/http"
	"strings"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
)

func (s *httpAPIServer) handleResolverResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeModuleResolveError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}
	if s == nil || s.rt == nil {
		writeModuleResolveError(w, http.StatusServiceUnavailable, domainbiz.CodeDomainResolverUnavailable, "runtime not initialized")
		return
	}
	var req struct {
		Domain            string `json:"domain"`
		ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
		Name              string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeModuleResolveError(w, http.StatusBadRequest, domainbiz.CodeBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.ResolverPubkeyHex) != "" || strings.TrimSpace(req.Name) != "" {
		writeModuleResolveError(w, http.StatusBadRequest, domainbiz.CodeBadRequest, "legacy resolver protocol is not supported")
		return
	}
	pubkeyHex, err := ResolveDomainToPubkey(r.Context(), s.rt, req.Domain)
	if err != nil {
		code := strings.TrimSpace(domainbiz.CodeOf(err))
		if code == "" {
			code = "INTERNAL_ERROR"
		}
		writeModuleResolveError(w, domainResolveHTTPStatusFromCode(code), code, domainbiz.MessageOf(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"data": map[string]any{
			"domain":     strings.TrimSpace(req.Domain),
			"pubkey_hex": pubkeyHex,
		},
	})
}

func writeModuleResolveError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"status": "error",
		"error": map[string]any{
			"code":    strings.TrimSpace(code),
			"message": strings.TrimSpace(message),
		},
	})
}

func domainResolveHTTPStatusFromCode(code string) int {
	switch strings.ToUpper(strings.TrimSpace(code)) {
	case domainbiz.CodeBadRequest:
		return http.StatusBadRequest
	case domainbiz.CodeRequestCanceled:
		return 499
	case domainbiz.CodeDomainResolverUnavailable:
		return http.StatusServiceUnavailable
	case domainbiz.CodeDomainNotResolved:
		return http.StatusNotFound
	case domainbiz.CodeDomainProviderSignatureInvalid:
		return http.StatusBadRequest
	default:
		return http.StatusBadRequest
	}
}
