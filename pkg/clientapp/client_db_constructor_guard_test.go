package clientapp

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNewClientDBOnlyAtRuntimeEntry(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("resolve caller path failed")
	}
	rootDir := filepath.Dir(thisFile)
	allowed := map[string]struct{}{
		"client_db.go": {},
		"run.go":       {},
	}
	var violations []string
	err := filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if !strings.Contains(string(body), "newClientDB(") {
			return nil
		}
		if _, ok := allowed[name]; ok {
			return nil
		}
		violations = append(violations, name)
		return nil
	})
	if err != nil {
		t.Fatalf("scan files failed: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("newClientDB is forbidden outside runtime entry: %s", strings.Join(violations, ", "))
	}
}
