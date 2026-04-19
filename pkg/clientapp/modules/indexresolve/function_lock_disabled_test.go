//go:build indexresolve_disabled

package indexresolve

import "testing"

func TestFunctionLocksAreEmptyWhenModuleDisabled(t *testing.T) {
	t.Parallel()

	if items := FunctionLocks(); len(items) != 0 {
		t.Fatalf("function locks should be empty when module is disabled, got %d", len(items))
	}
}
