//go:build with_indexresolve

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp/modulelock"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/indexresolve"
)

func TestValidateWhitelistShapeRejectsDuplicateID(t *testing.T) {
	items := []modulelock.LockedFunction{
		{ID: "dup", Module: indexresolve.ModuleIdentity, Package: "./pkg/clientapp/modules/indexresolve", Symbol: "A", Signature: "func A()", Note: "n"},
		{ID: "dup", Module: indexresolve.ModuleIdentity, Package: "./pkg/clientapp/modules/indexresolve", Symbol: "B", Signature: "func B()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "duplicated id") {
		t.Fatalf("expected duplicate id error, got %v", err)
	}
}

func TestValidateWhitelistShapeRejectsBadSignature(t *testing.T) {
	items := []modulelock.LockedFunction{
		{ID: "one", Module: indexresolve.ModuleIdentity, Package: "./pkg/clientapp/modules/indexresolve", Symbol: "A", Signature: "not a func", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "signature must start with func") {
		t.Fatalf("expected signature error, got %v", err)
	}
}

func TestValidateWhitelistShapeRejectsEmptyField(t *testing.T) {
	items := []modulelock.LockedFunction{
		{ID: "one", Module: indexresolve.ModuleIdentity, Package: "", Symbol: "A", Signature: "func A()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "package is required") {
		t.Fatalf("expected empty field error, got %v", err)
	}
}

func TestIndexResolveWhitelistKeepsOnlyBizEntrypoints(t *testing.T) {
	items := indexresolve.FunctionLocks()
	if len(items) != 4 {
		t.Fatalf("expected 4 whitelist items, got %d", len(items))
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Symbol, "Biz") {
			t.Fatalf("unexpected lock symbol: %s", item.Symbol)
		}
	}
}

func TestRegistryItemsReportsMissingModule(t *testing.T) {
	reg := modulelock.NewRegistry()
	items, missing := reg.Items(indexresolve.ModuleIdentity)
	if len(items) != 0 {
		t.Fatalf("expected no items, got %d", len(items))
	}
	if len(missing) != 1 || missing[0] != indexresolve.ModuleIdentity {
		t.Fatalf("unexpected missing modules: %#v", missing)
	}
}

func TestRunChecksRejectsSignatureMismatch(t *testing.T) {
	tmp := t.TempDir()
	goBin := filepath.Join(tmp, "go")
	if err := os.WriteFile(goBin, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("setup fake go bin failed: %v", err)
	}
	originalReader := readSignatureFn
	t.Cleanup(func() { readSignatureFn = originalReader })
	readSignatureFn = func(goBin string, goRoot string, goBinDir string, moduleDir string, pkg string, symbol string) (string, error) {
		return "func wrong()", nil
	}
	selected := map[string]struct{}{indexresolve.ModuleIdentity: {}}
	items := []modulelock.LockedFunction{
		{ID: "bitfs.indexresolve.biz_resolve", Module: indexresolve.ModuleIdentity, Package: "./pkg/clientapp/modules/indexresolve", Symbol: "BizResolve", Signature: "func BizResolve(ctx context.Context, resolver ResolveReader, rawRoute string) (Manifest, error)", Note: "n"},
	}
	err := runChecks(tmp, goBin, selected, items)
	if err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("expected signature mismatch, got %v", err)
	}
}

func TestRegisterModuleProvidersRejectsDuplicate(t *testing.T) {
	reg := modulelock.NewRegistry()
	selected := map[string]struct{}{indexresolve.ModuleIdentity: {}}
	if err := registerModuleProviders(reg, selected); err != nil {
		t.Fatalf("register providers failed: %v", err)
	}
	if err := registerModuleProviders(reg, selected); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

func TestReadSignatureRejectsMissingGoDoc(t *testing.T) {
	_, err := readSignature("/bin/false", "", "", t.TempDir(), "./pkg/clientapp/modules/indexresolve", "Resolve")
	if err == nil {
		t.Fatal("expected go doc error")
	}
}

func TestRegistryItemsKeepModuleFiltered(t *testing.T) {
	reg := modulelock.NewRegistry()
	_, _ = reg.Register(indexresolve.ModuleIdentity, func() []modulelock.LockedFunction {
		return []modulelock.LockedFunction{{ID: "one", Module: indexresolve.ModuleIdentity, Package: "./pkg/clientapp/modules/indexresolve", Symbol: "A", Signature: "func A()", Note: "n"}}
	})
	_, _ = reg.Register("other", func() []modulelock.LockedFunction {
		return []modulelock.LockedFunction{{ID: "two", Module: "other", Package: "./other", Symbol: "B", Signature: "func B()", Note: "n"}}
	})
	items, missing := reg.Items(indexresolve.ModuleIdentity)
	if len(missing) != 0 {
		t.Fatalf("unexpected missing modules: %#v", missing)
	}
	if len(items) != 1 || items[0].ID != "one" {
		t.Fatalf("unexpected filtered items: %#v", items)
	}
}
