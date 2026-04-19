package clientapp

import (
	"context"
	"strings"
	"testing"

	domainbiz "github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
)

func TestResolveDomainToPubkeyHonorsProviderOrder(t *testing.T) {
	t.Parallel()

	rt := newRuntimeForTest(t, Config{}, "")
	rt.modules = newModuleRegistry()

	if _, err := rt.modules.RegisterDomainResolveHook("first", func(ctx context.Context, domain string) (string, error) {
		return "", domainbiz.NewError(domainbiz.CodeDomainNotResolved, "miss")
	}); err != nil {
		t.Fatalf("register first provider: %v", err)
	}
	if _, err := rt.modules.RegisterDomainResolveHook(domainbiz.ResolveProviderName, func(ctx context.Context, domain string) (string, error) {
		return "02" + strings.Repeat("11", 32), nil
	}); err != nil {
		t.Fatalf("register second provider: %v", err)
	}

	next := rt.RuntimeConfigService().Snapshot()
	next.Domain.ResolveOrder = []string{"first", domainbiz.ResolveProviderName}
	rt.config.cloneAndAssign(next)

	pubkeyHex, err := ResolveDomainToPubkey(context.Background(), rt, "movie.david")
	if err != nil {
		t.Fatalf("resolve domain: %v", err)
	}
	if pubkeyHex != "021111111111111111111111111111111111111111111111111111111111111111" {
		t.Fatalf("unexpected pubkey: %s", pubkeyHex)
	}
}

func TestResolveDomainToPubkeyUnavailableWhenNoProvider(t *testing.T) {
	t.Parallel()

	rt := newRuntimeForTest(t, Config{}, "")
	rt.modules = newModuleRegistry()
	next := rt.RuntimeConfigService().Snapshot()
	next.Domain.ResolveOrder = []string{domainbiz.ResolveProviderName}
	if err := rt.RuntimeConfigService().UpdateMemoryOnly(next); err != nil {
		t.Fatalf("update config: %v", err)
	}

	if _, err := ResolveDomainToPubkey(context.Background(), rt, "movie.david"); domainbiz.CodeOf(err) != domainbiz.CodeDomainResolverUnavailable {
		t.Fatalf("expected unavailable, got=%v code=%s", err, domainbiz.CodeOf(err))
	}
}
