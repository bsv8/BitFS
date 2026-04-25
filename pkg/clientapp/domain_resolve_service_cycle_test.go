package clientapp

import (
	"context"
	"testing"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domainclient"
)

func TestResolveDomainToPubkeySkipsSelfProvider(t *testing.T) {
	t.Parallel()

	rt := newRuntimeForTest(t, Config{}, "")
	rt.modules = newModuleRegistry()
	mustUpdateRuntimeConfigMemoryOnly(t, rt, func(cfg *Config) {
		cfg.Domain.ResolveOrder = []string{domainbiz.ResolveProviderName}
	})

	backend := directResolverStub{pubkeyHex: "021111111111111111111111111111111111111111111111111111111111111111"}
	selfCalls := 0

	if _, err := rt.modules.RegisterDomainResolveHook(domainbiz.ResolveProviderName, func(ctx context.Context, domain string) (string, error) {
		selfCalls++
		return backend.ResolveDomainToPubkeyDirect(ctx, domain)
	}); err != nil {
		t.Fatalf("register self provider: %v", err)
	}

	pubkeyHex, err := ResolveDomainToPubkey(context.Background(), rt, " Movie.David ")
	if err != nil {
		t.Fatalf("resolve domain failed: %v", err)
	}
	if pubkeyHex != "021111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("unexpected pubkey: %s", pubkeyHex)
	}
	if selfCalls != 1 {
		t.Fatalf("unexpected self provider calls: %d", selfCalls)
	}
}

type directResolverStub struct {
	pubkeyHex string
	err       error
}

func (r directResolverStub) ResolveDomainToPubkeyDirect(context.Context, string) (string, error) {
	return r.pubkeyHex, r.err
}
