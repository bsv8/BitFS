package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func TestValidateWhitelistShapeRejectsDuplicateID(t *testing.T) {
	items := []moduleapi.LockedFunction{
		{ID: "dup", Module: "indexresolve", Package: "./pkg/clientapp/modules/indexresolve", Symbol: "A", Signature: "func A()", Note: "n"},
		{ID: "dup", Module: "indexresolve", Package: "./pkg/clientapp/modules/indexresolve", Symbol: "B", Signature: "func B()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "duplicated id") {
		t.Fatalf("expected duplicate id error, got %v", err)
	}
}

func TestValidateWhitelistShapeRejectsBadSignature(t *testing.T) {
	items := []moduleapi.LockedFunction{
		{ID: "one", Module: "indexresolve", Package: "./pkg/clientapp/modules/indexresolve", Symbol: "A", Signature: "not a func", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "signature must start with func") {
		t.Fatalf("expected signature error, got %v", err)
	}
}

func TestValidateWhitelistShapeRejectsEmptyField(t *testing.T) {
	items := []moduleapi.LockedFunction{
		{ID: "one", Module: "indexresolve", Package: "", Symbol: "A", Signature: "func A()", Note: "n"},
	}
	if err := validateWhitelistShape(items); err == nil || !strings.Contains(err.Error(), "package is required") {
		t.Fatalf("expected empty field error, got %v", err)
	}
}

func TestIndexResolveWhitelistKeepsOnlyBizEntrypoints(t *testing.T) {
	items, _ := clientapp.BuiltinModuleLockItems("indexresolve")
	if len(items) != 4 {
		t.Fatalf("expected 4 whitelist items, got %d", len(items))
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Symbol, "Biz") {
			t.Fatalf("unexpected lock symbol: %s", item.Symbol)
		}
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
	selected := map[string]struct{}{"indexresolve": {}}
	items := []moduleapi.LockedFunction{
		{ID: "bitfs.indexresolve.biz_resolve", Module: "indexresolve", Package: "./pkg/clientapp/modules/indexresolve", Symbol: "BizResolve", Signature: "func BizResolve(ctx context.Context, resolver ResolveReader, rawRoute string) (Manifest, error)", Note: "n"},
	}
	err := runChecks(tmp, goBin, selected, items)
	if err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("expected signature mismatch, got %v", err)
	}
}

func TestRunChecksRejectsSignatureMismatchWhenSelectedEmpty(t *testing.T) {
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
	selected := map[string]struct{}{}
	items := []moduleapi.LockedFunction{
		{ID: "bitfs.indexresolve.biz_resolve", Module: "indexresolve", Package: "./pkg/clientapp/modules/indexresolve", Symbol: "BizResolve", Signature: "func BizResolve(ctx context.Context, resolver ResolveReader, rawRoute string) (Manifest, error)", Note: "n"},
	}
	err := runChecks(tmp, goBin, selected, items)
	if err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("expected signature mismatch with empty selected, got %v", err)
	}
}

func TestReadSignatureRejectsMissingGoDoc(t *testing.T) {
	_, err := readSignature("/bin/false", "", "", t.TempDir(), "./pkg/clientapp/modules/indexresolve", "Resolve")
	if err == nil {
		t.Fatal("expected go doc error")
	}
}
