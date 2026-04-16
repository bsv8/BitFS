package clientapp

import (
	"path/filepath"
	"testing"
)

func TestResolveConfigRoot_UsesXDGConfigHomeWhenEmpty(t *testing.T) {
	xdgHome := filepath.Join(t.TempDir(), "xdg")
	t.Setenv("XDG_CONFIG_HOME", xdgHome)
	t.Setenv("HOME", filepath.Join(t.TempDir(), "home"))

	got := ResolveConfigRoot("")
	want := filepath.Clean(filepath.Join(xdgHome, "bitfs"))
	if got != want {
		t.Fatalf("resolve config root mismatch: got=%s want=%s", got, want)
	}
}

func TestResolveConfigPathAndKeyPath_FromConfigRoot(t *testing.T) {
	t.Parallel()

	root := filepath.Join("/tmp", "bitfs-root")
	if got, want := ResolveConfigPath(root), filepath.Clean(filepath.Join(root, "config.yaml")); got != want {
		t.Fatalf("resolve config path mismatch: got=%s want=%s", got, want)
	}
	if got, want := ResolveKeyFilePath(root), filepath.Clean(filepath.Join(root, "key.json")); got != want {
		t.Fatalf("resolve key path mismatch: got=%s want=%s", got, want)
	}
}
