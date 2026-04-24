package chainbridge

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	// DefaultProfile 避免把“空 profile”和“默认 profile”误当成两条不同 route。
	DefaultProfile = "default"

	WhatsOnChainProvider   = "whatsonchain"
	GorillaPoolARCProvider = "gorillapool_arc"
	TAALARCProvider        = "taal_arc"
	TAALLegacyProvider     = "taal_legacy"
)

type Route struct {
	Provider string `json:"provider"`
	Network  string `json:"network"`
	Profile  string `json:"profile,omitempty"`
}

func (r Route) Normalize() Route {
	out := Route{
		Provider: strings.ToLower(strings.TrimSpace(r.Provider)),
		Network:  strings.ToLower(strings.TrimSpace(r.Network)),
		Profile:  strings.ToLower(strings.TrimSpace(r.Profile)),
	}
	if out.Network == "mainnet" {
		out.Network = "main"
	}
	if out.Network == "testnet" {
		out.Network = "test"
	}
	if out.Profile == "" {
		out.Profile = DefaultProfile
	}
	return out
}

func (r Route) Key() string {
	n := r.Normalize()
	return n.Provider + "|" + n.Network + "|" + n.Profile
}

type AuthConfig struct {
	Mode  string `json:"mode,omitempty"`
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

func (c AuthConfig) Validate() error {
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	switch mode {
	case "", "none":
		return nil
	case "header", "query":
		if strings.TrimSpace(c.Name) == "" {
			return fmt.Errorf("auth name is required for mode %s", mode)
		}
		if strings.TrimSpace(c.Value) == "" {
			return fmt.Errorf("auth value is required for mode %s", mode)
		}
		return nil
	case "bearer":
		if strings.TrimSpace(c.Value) == "" {
			return fmt.Errorf("auth value is required for mode bearer")
		}
		return nil
	default:
		return fmt.Errorf("unsupported auth mode: %s", mode)
	}
}

func (c AuthConfig) Apply(req *http.Request) error {
	if req == nil {
		return fmt.Errorf("request is nil")
	}
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	switch mode {
	case "", "none":
		return nil
	case "header":
		req.Header.Set(strings.TrimSpace(c.Name), strings.TrimSpace(c.Value))
		return nil
	case "query":
		q := req.URL.Query()
		q.Set(strings.TrimSpace(c.Name), strings.TrimSpace(c.Value))
		req.URL.RawQuery = q.Encode()
		return nil
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.Value))
		return nil
	default:
		return fmt.Errorf("unsupported auth mode: %s", mode)
	}
}
