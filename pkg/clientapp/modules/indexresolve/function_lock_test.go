//go:build !indexresolve_disabled

package indexresolve

import (
	"testing"
)

func TestFunctionLocksFreezeCriticalSignatures(t *testing.T) {
	t.Parallel()

	items := FunctionLocks()
	if len(items) == 0 {
		t.Fatal("function locks should not be empty")
	}
	signatures := map[string]string{}
	for _, item := range items {
		signatures[item.Symbol] = item.Signature
	}
	if got := signatures["NewService"]; got != "func NewService(store Store, runtime RuntimeReader) *Service" {
		t.Fatalf("unexpected NewService signature: %s", got)
	}
	if got := signatures["Service.Resolve"]; got != "func (s *Service) Resolve(ctx context.Context, rawRoute string) (Manifest, error)" {
		t.Fatalf("unexpected Service.Resolve signature: %s", got)
	}
}
