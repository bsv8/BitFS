package chainbridge

import (
	"testing"
)

func TestResolveAPIBaseURLFromEnv(t *testing.T) {
	t.Setenv(FeePoolChainBaseURLEnv, " http://127.0.0.1:18222/ ")
	if got, want := ResolveAPIBaseURLFromEnv("test"), "http://127.0.0.1:18222"; got != want {
		t.Fatalf("ResolveAPIBaseURLFromEnv()=%q, want %q", got, want)
	}
}

func TestNewDefaultFeePoolChainUsesEnvPort(t *testing.T) {
	t.Setenv(FeePoolChainBaseURLEnv, "http://127.0.0.1:18299/v1/bsv/test/")
	chain, err := NewDefaultFeePoolChain(RouteConfig{Provider: WhatsOnChainProvider, Network: "test"})
	if err != nil {
		t.Fatalf("NewDefaultFeePoolChain() error: %v", err)
	}
	if got, want := chain.BaseURL(), "http://127.0.0.1:18299/v1/bsv/test"; got != want {
		t.Fatalf("BaseURL()=%q, want %q", got, want)
	}
	if got, want := chain.ReadRoute(), (Route{Provider: WhatsOnChainProvider, Network: "test", Profile: "default"}); got != want {
		t.Fatalf("ReadRoute()=%+v, want %+v", got, want)
	}
	if got := chain.SubmitRoutes(); len(got) != 1 || got[0] != chain.ReadRoute() {
		t.Fatalf("unexpected SubmitRoutes(): %+v", got)
	}
}

func TestNewEmbeddedFeePoolChainRequiresExplicitProvider(t *testing.T) {
	if _, err := NewEmbeddedFeePoolChain(RouteConfig{Network: "test"}); err == nil {
		t.Fatalf("expected explicit provider error")
	}
}

func TestNewEmbeddedFeePoolChainRejectsBroadcastOnlyReadRoute(t *testing.T) {
	_, err := NewEmbeddedFeePoolChainFromPlan(FeePoolChainPlan{
		Routes: []RouteConfig{
			{Provider: GorillaPoolARCProvider, Network: "main"},
		},
		ReadRoute:    Route{Provider: GorillaPoolARCProvider, Network: "main"},
		SubmitRoutes: []Route{{Provider: GorillaPoolARCProvider, Network: "main"}},
	})
	if err == nil {
		t.Fatalf("expected read route capability error")
	}
}
