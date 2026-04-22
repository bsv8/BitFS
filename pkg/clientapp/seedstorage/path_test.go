package seedstorage

import (
	"path/filepath"
	"testing"
)

func TestSeedDirPath(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join("/tmp", "vault", "config.yaml")
	got, err := SeedDirPath(configPath, "abcd1234")
	if err != nil {
		t.Fatalf("seed dir path failed: %v", err)
	}
	want := filepath.Join("/tmp", "vault", "abcd1234", "seeds")
	if got != want {
		t.Fatalf("seed dir path mismatch: got=%q want=%q", got, want)
	}
}
