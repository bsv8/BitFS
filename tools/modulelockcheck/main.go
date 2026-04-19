package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
)

type moduleConfig struct {
	name     string
	dir      string
	provider func() []moduleapi.LockedFunction
}

var moduleConfigs map[string]moduleConfig

var readSignatureFn = readSignature

func main() {
	var (
		workspaceRootFlag string
		goBin             string
		modulesFlag       string
	)
	flag.StringVar(&workspaceRootFlag, "workspace-root", "", "workspace root path (contains go.work)")
	flag.StringVar(&goBin, "go-bin", "/home/david/.gvm/gos/go1.26.0/bin/go", "go binary path")
	flag.StringVar(&modulesFlag, "modules", "", "comma-separated modules to check")
	flag.Parse()

	workspaceRoot, err := resolveWorkspaceRoot(workspaceRootFlag)
	if err != nil {
		exitErr(err)
	}
	selectedModules, err := parseModules(modulesFlag)
	if err != nil {
		exitErr(err)
	}
	reg := modulelock.NewRegistry()
	if err := registerModuleProviders(reg, selectedModules); err != nil {
		exitErr(err)
	}
	items, missing := reg.Items(sortedModuleNames(selectedModules)...)
	if len(missing) > 0 {
		exitErr(fmt.Errorf("module not registered: %s", strings.Join(missing, ", ")))
	}
	if err := validateWhitelistShape(items); err != nil {
		exitErr(err)
	}
	if err := runChecks(workspaceRoot, goBin, selectedModules, items); err != nil {
		exitErr(err)
	}
	fmt.Println("[modulelock] ok")
}

func resolveWorkspaceRoot(input string) (string, error) {
	if v := strings.TrimSpace(input); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(filepath.Join(abs, "go.work")); err != nil {
			return "", fmt.Errorf("workspace root missing go.work: %s", abs)
		}
		return abs, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.work")); err == nil {
			return wd, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			break
		}
		wd = parent
	}
	return "", errors.New("cannot find workspace root with go.work")
}

func parseModules(raw string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	for _, item := range strings.Split(raw, ",") {
		name := strings.TrimSpace(strings.ToLower(item))
		if name == "" {
			continue
		}
		if _, ok := moduleConfigs[name]; !ok {
			return nil, fmt.Errorf("unsupported module: %s", item)
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return out, nil
	}
	return out, nil
}

func sortedModuleNames(selected map[string]struct{}) []string {
	out := make([]string, 0, len(selected))
	for name := range selected {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func registerModuleProviders(reg *modulelock.Registry, selected map[string]struct{}) error {
	for _, name := range sortedModuleNames(selected) {
		cfg := moduleConfigs[name]
		if _, err := reg.Register(cfg.name, func() []modulelock.LockedFunction {
			items := cfg.provider()
			out := make([]modulelock.LockedFunction, 0, len(items))
			for _, item := range items {
				out = append(out, modulelock.LockedFunction{
					ID:               item.ID,
					Module:           item.Module,
					Package:          item.Package,
					Symbol:           item.Symbol,
					Signature:        item.Signature,
					ObsControlAction: item.ObsControlAction,
					Note:             item.Note,
				})
			}
			return out
		}); err != nil {
			return err
		}
	}
	return nil
}

func validateWhitelistShape(items []modulelock.LockedFunction) error {
	seenID := map[string]struct{}{}
	seenSymbol := map[string]struct{}{}
	for i, item := range items {
		prefix := fmt.Sprintf("whitelist[%d]", i)
		if strings.TrimSpace(item.ID) == "" {
			return fmt.Errorf("%s id is required", prefix)
		}
		if _, ok := seenID[item.ID]; ok {
			return fmt.Errorf("%s duplicated id: %s", prefix, item.ID)
		}
		seenID[item.ID] = struct{}{}
		if strings.TrimSpace(item.Module) == "" {
			return fmt.Errorf("%s module is required", prefix)
		}
		if _, ok := moduleConfigs[strings.TrimSpace(item.Module)]; !ok {
			return fmt.Errorf("%s unsupported module: %s", prefix, item.Module)
		}
		if strings.TrimSpace(item.Package) == "" {
			return fmt.Errorf("%s package is required", prefix)
		}
		if strings.TrimSpace(item.Symbol) == "" {
			return fmt.Errorf("%s symbol is required", prefix)
		}
		if strings.TrimSpace(item.Signature) == "" {
			return fmt.Errorf("%s signature is required", prefix)
		}
		if !strings.HasPrefix(strings.TrimSpace(item.Signature), "func ") {
			return fmt.Errorf("%s signature must start with func", prefix)
		}
		if strings.TrimSpace(item.Note) == "" {
			return fmt.Errorf("%s note is required", prefix)
		}
		symbolKey := item.Module + "|" + item.Package + "|" + item.Symbol
		if _, ok := seenSymbol[symbolKey]; ok {
			return fmt.Errorf("%s duplicated module/package/symbol: %s", prefix, symbolKey)
		}
		seenSymbol[symbolKey] = struct{}{}
	}
	return nil
}

func runChecks(workspaceRoot string, goBin string, selected map[string]struct{}, items []modulelock.LockedFunction) error {
	goBin = strings.TrimSpace(goBin)
	if goBin == "" {
		return errors.New("go-bin is required")
	}
	goBinAbs, err := filepath.Abs(goBin)
	if err != nil {
		return err
	}
	if _, err := os.Stat(goBinAbs); err != nil {
		return fmt.Errorf("go binary not found: %s", goBinAbs)
	}
	goRoot := filepath.Dir(filepath.Dir(goBinAbs))
	pathPrefix := filepath.Dir(goBinAbs)

	var failed []string
	for _, item := range items {
		if _, ok := selected[strings.TrimSpace(item.Module)]; !ok {
			continue
		}
		cfg := moduleConfigs[strings.TrimSpace(item.Module)]
		moduleDir := filepath.Join(workspaceRoot, cfg.dir)
		got, err := readSignatureFn(goBinAbs, goRoot, pathPrefix, moduleDir, item.Package, item.Symbol)
		if err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", item.ID, err))
			continue
		}
		if strings.TrimSpace(got) != strings.TrimSpace(item.Signature) {
			failed = append(failed, fmt.Sprintf("%s: signature mismatch\n  want: %s\n  got:  %s", item.ID, item.Signature, got))
		}
	}
	if len(failed) == 0 {
		return nil
	}
	sort.Strings(failed)
	return fmt.Errorf("module function lock check failed:\n- %s", strings.Join(failed, "\n- "))
}

func readSignature(goBin string, goRoot string, goBinDir string, moduleDir string, pkg string, symbol string) (string, error) {
	cmd := exec.Command(goBin, "doc", "-u", strings.TrimSpace(pkg), strings.TrimSpace(symbol))
	cmd.Dir = moduleDir
	cmd.Env = enrichEnv(goRoot, goBinDir)
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err != nil {
		if sig, parseErr := readSignatureFromSource(moduleDir, pkg, symbol); parseErr == nil {
			return sig, nil
		}
		if text == "" {
			text = err.Error()
		}
		return "", fmt.Errorf("go doc failed: %s", text)
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "func ") {
			return line, nil
		}
	}
	if sig, parseErr := readSignatureFromSource(moduleDir, pkg, symbol); parseErr == nil {
		return sig, nil
	}
	return "", fmt.Errorf("cannot find func signature from go doc output: %s", text)
}

func enrichEnv(goRoot string, goBinDir string) []string {
	base := os.Environ()
	pathValue := os.Getenv("PATH")
	if strings.TrimSpace(pathValue) == "" {
		pathValue = goBinDir
	} else {
		pathValue = goBinDir + string(os.PathListSeparator) + pathValue
	}
	base = append(base, "PATH="+pathValue)
	if strings.TrimSpace(goRoot) != "" {
		base = append(base, "GOROOT="+goRoot)
	}
	if tags := normalizeGoTags(os.Getenv("BITFS_GO_TAGS")); tags != "" {
		goFlags := os.Getenv("GOFLAGS")
		if strings.TrimSpace(goFlags) != "" {
			goFlags = strings.TrimSpace(goFlags) + " "
		}
		goFlags += "-tags=" + tags
		base = append(base, "GOFLAGS="+goFlags)
	}
	return base
}

func normalizeGoTags(raw string) string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ",")
}

func readSignatureFromSource(moduleDir string, pkg string, symbol string) (string, error) {
	pkgDir := filepath.Join(moduleDir, filepath.FromSlash(strings.TrimPrefix(strings.TrimSpace(pkg), "./")))
	files, err := filepath.Glob(filepath.Join(pkgDir, "*.go"))
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no source files found in %s", pkgDir)
	}

	fset := token.NewFileSet()
	targetName, recvType := splitSymbol(strings.TrimSpace(symbol))
	for _, file := range files {
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			continue
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name == nil {
				continue
			}
			if !matchesDeclSymbol(fn, targetName, recvType) {
				continue
			}
			var buf bytes.Buffer
			if err := printer.Fprint(&buf, fset, fn); err != nil {
				return "", err
			}
			text := strings.TrimSpace(buf.String())
			if idx := strings.Index(text, "{"); idx >= 0 {
				text = strings.TrimSpace(text[:idx])
			}
			if text != "" {
				return text, nil
			}
		}
	}
	return "", fmt.Errorf("cannot find func signature from source: %s %s", pkg, symbol)
}

func splitSymbol(symbol string) (string, string) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return "", ""
	}
	if idx := strings.LastIndex(symbol, "."); idx >= 0 {
		return symbol[idx+1:], symbol[:idx]
	}
	return symbol, ""
}

func matchesDeclSymbol(fn *ast.FuncDecl, name string, recvType string) bool {
	if fn == nil || fn.Name == nil {
		return false
	}
	if fn.Name.Name != name {
		return false
	}
	if recvType == "" {
		return fn.Recv == nil
	}
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	got := receiverTypeName(fn.Recv.List[0].Type)
	return got == recvType
}

func receiverTypeName(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(v.X)
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	default:
		return ""
	}
}

func exitErr(err error) {
	fmt.Fprintf(os.Stderr, "[modulelock] %s\n", strings.TrimSpace(err.Error()))
	os.Exit(1)
}
