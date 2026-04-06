package clientapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestRuntimeFiles_DoNotReferenceLegacyAssetConsumption 保护运行时代码不再引用旧消费表语义。
// 设计说明：
// - 这里只检查运行时业务文件，不包含迁移文件和 schema 初始化文件
// - 用语法树和注释一起扫，避免旧函数名、旧表名、旧注释再混回主链路
func TestRuntimeFiles_DoNotReferenceLegacyAssetConsumption(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file failed")
	}
	pkgDir := filepath.Dir(currentFile)

	checkedFiles := []string{
		"db_asset_facts.go",
		"db_fund_flow.go",
		"db_finance.go",
		"db_pool_facts.go",
		"db_wallet.go",
		"http_api.go",
		"wallet_accounting.go",
		"wallet_token_facts_integration.go",
		"wallet_token_send.go",
	}

	const legacyTableName = "fact_asset_consumptions"
	const legacyFuncPrefix = "dbAppendAssetConsumption"

	for _, name := range checkedFiles {
		filePath := filepath.Join(pkgDir, name)
		fset := token.NewFileSet()
		fileNode, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s failed: %v", name, err)
		}

		for _, decl := range fileNode.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && strings.HasPrefix(fn.Name.Name, legacyFuncPrefix) {
				t.Fatalf("%s still declares legacy function %s", name, fn.Name.Name)
			}
		}

		ast.Inspect(fileNode, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				if n.Name == "assetConsumptionEntry" {
					t.Fatalf("%s still references legacy type %s", name, n.Name)
				}
				if strings.HasPrefix(n.Name, legacyFuncPrefix) {
					t.Fatalf("%s still references legacy function %s", name, n.Name)
				}
			case *ast.BasicLit:
				if n.Kind != token.STRING {
					return true
				}
				lit, err := strconv.Unquote(n.Value)
				if err != nil {
					t.Fatalf("unquote string literal in %s failed: %v", name, err)
				}
				if strings.Contains(lit, legacyTableName) || strings.Contains(lit, legacyFuncPrefix) {
					t.Fatalf("%s still contains legacy literal %q", name, lit)
				}
			}
			return true
		})

		for _, cg := range fileNode.Comments {
			for _, c := range cg.List {
				if strings.Contains(c.Text, legacyTableName) || strings.Contains(c.Text, legacyFuncPrefix) || strings.Contains(c.Text, "assetConsumptionEntry") {
					t.Fatalf("%s still contains legacy comment %q", name, c.Text)
				}
			}
		}
	}
}
