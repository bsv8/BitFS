package clientapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEntBoundaryGuard(t *testing.T) {
	t.Helper()

	// 边界守卫必须扫整个 clientapp，不能靠人工维护文件清单。
	// 否则新增文件很容易重新把 ent.Tx / gen.Client 直通带回来。
	for _, rel := range listClientAppGoFiles(t) {
		path := filepath.Join(".", rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s failed: %v", rel, err)
		}
		content := string(data)
		for _, forbidden := range []string{
			"store.ent",
			"ent.Tx(",
			"func(*gen.Client)",
			"func(*gen.Tx)",
			"clientDBEntTx",
			"clientDBEntQuery",
			"dbInitEntry",
		} {
			if strings.Contains(content, forbidden) {
				t.Fatalf("%s contains forbidden marker %q", rel, forbidden)
			}
		}
	}

	for _, rel := range []string{"db_init_entry.go", "schema_init.go"} {
		if _, err := os.Stat(filepath.Join(".", rel)); err == nil {
			t.Fatalf("forbidden file still exists: %s", rel)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s failed: %v", rel, err)
		}
	}
}

func listClientAppGoFiles(t *testing.T) []string {
	t.Helper()

	var out []string
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "ent" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := strings.TrimPrefix(filepath.ToSlash(path), "./")
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walk clientapp go files failed: %v", err)
	}
	return out
}
